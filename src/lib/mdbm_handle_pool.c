/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <pthread.h>
#include <stdlib.h>
#include <libgen.h> /* for basename() */
#include <errno.h>
#include <string.h>
#include <sys/queue.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <mdbm_log.h>
#include <mdbm_handle_pool.h>
#include <mdbm_internal.h> /* for fast time functions */


#define LOG_LOCK_ACQUIRE_FAILURE(lock_name, purpose) \
  mdbm_logerror(LOG_ERR, 0, "Failed to acquire lock %s for purpose %s", \
      lock_name, purpose);

#define LOG_LOCK_RELEASE_FAILURE(lock_name, purpose) \
  mdbm_logerror(LOG_ERR, 0, "Failed to release lock %s for purpose %s", \
      lock_name, purpose);


/************************************************************
 * internals
 ***********************************************************/

struct mdbm_pool_entry {
  MDBM *mdbm_handle;
  LIST_ENTRY(mdbm_pool_entry) entries;
};

LIST_HEAD(mdbm_pool_list, mdbm_pool_entry);

typedef struct mdbm_pool_entry mdbm_pool_entry_t;
typedef struct mdbm_pool_list mdbm_pool_list_t;

struct mdbm_pool_locks_s {
  /* Duplication is treated as a special case of the many readers,
   * one writer problem. The need to grow the pool is treated as a
   * write use, holding a handle is treated as a read use.
   
   IMPORTANT: duplication_lock must always be acquired before transfer_handle_lock .   
  */
  
  pthread_rwlock_t duplication_lock;
  pthread_mutex_t transfer_handle_lock;
  pthread_cond_t handle_cond;
};
typedef struct mdbm_pool_locks_s mdbm_pool_locks_t;

/* Implementation wise, a struct like this is not strictly necessary;
 * it exists to make the code less difficult to understand. */

struct mdbm_pool_s {
  MDBM *original_handle;
  
  /* Each thread which fetches handles from this pool will get a
   * copy of one of these handles. */
  
  mdbm_pool_list_t dup_handle_stack;
  mdbm_pool_list_t reserve_handle_stack;
  
  mdbm_pool_locks_t *locks;
  
  /* This is the total number of handles. */
  int size;
};


/************************************************************
 * main section
 ***********************************************************/


static int init_locks(mdbm_pool_t *new_pool) {
  pthread_mutexattr_t attr;

  new_pool->locks = calloc(1, sizeof(mdbm_pool_locks_t));
  if (new_pool->locks == NULL) {
    return 0;
  }

  if (pthread_rwlock_init(&new_pool->locks->duplication_lock, NULL) != 0) {
    mdbm_logerror(LOG_ERR, 0, "Failed to initialize duplication_lock mutex");
    free(new_pool->locks);
    new_pool->locks = NULL;
    return 0;
  }

  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL);

  if (pthread_mutex_init(&new_pool->locks->transfer_handle_lock, &attr) != 0) {
    mdbm_logerror(LOG_ERR, 0, "Failed to initialize transfer_handle_lock mutex");
    pthread_mutexattr_destroy(&attr);
    pthread_rwlock_destroy(&new_pool->locks->duplication_lock);
    free(new_pool->locks);
    new_pool->locks = NULL;
    return 0;
  }

  pthread_mutexattr_destroy(&attr);

  if (pthread_cond_init (&new_pool->locks->handle_cond, NULL) != 0) {
    mdbm_logerror(LOG_ERR, 0, "Failed to initialize handle_cond condition variable");
    pthread_mutex_destroy(&new_pool->locks->transfer_handle_lock);
    pthread_rwlock_destroy(&new_pool->locks->duplication_lock);
    free(new_pool->locks);
    new_pool->locks = NULL;
    return 0;
  }

  return 1;
}

static void free_locks(mdbm_pool_t *pool) {
  pthread_rwlock_destroy(&pool->locks->duplication_lock);
  pthread_mutex_destroy(&pool->locks->transfer_handle_lock);
  pthread_cond_destroy(&pool->locks->handle_cond);

  free(pool->locks);
  pool->locks = NULL;
}

static void backoff_pool_size(mdbm_pool_t *pool, int backoff) {
  int i;
  mdbm_pool_entry_t *to_die;

  for (i = 0; pool->dup_handle_stack.lh_first && i < backoff; ++i) {
    to_die = pool->dup_handle_stack.lh_first;
    mdbm_close(to_die->mdbm_handle);
    LIST_REMOVE(to_die, entries);
    free(to_die);
  }
}

static int setup_pool(mdbm_pool_t *pool, int set_size) {
  int size;

  if (pthread_rwlock_wrlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("duplication_lock", "needed to increase pool size");
    return 0;
  }

  if (pthread_mutex_lock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("transfer_handle_lock", "needed to increase pool size");
    pthread_rwlock_unlock(&pool->locks->duplication_lock);
    return 0;
  }
  
  for (size = 0; size < set_size; ++size) {
    MDBM *dup_handle = mdbm_dup_handle(pool->original_handle, 0);
    if (!dup_handle) {
      mdbm_logerror(LOG_ERR, 0, "mdbm_dup_handle returned null."
        "%s stopped increasing pool size at %d when %d was requested.",
        __func__, size, set_size);
  
      /* we are going to back off half of the opened handles
       * to release resources */

      if (size > 1) {
        int backoff_size = size / 2;
        backoff_pool_size(pool, backoff_size);
        size -= backoff_size;
      }

      break;
    } else {
      mdbm_pool_entry_t* new_entry = calloc(1, sizeof(mdbm_pool_entry_t));
      if ( new_entry ) {
        new_entry->mdbm_handle = dup_handle;
        LIST_INSERT_HEAD(&pool->dup_handle_stack, new_entry, entries);
      } else {
        mdbm_logerror(LOG_ERR, 0, "Failed to calloc memory for new duplicated handle."
          " %s stopped increasing pool size at %d when %d was requested.",
          __func__, size, set_size);

        /* we are going to back off half of the opened handles
        * to release resources */

        mdbm_close(dup_handle);

        if (size > 1) {
          int backoff_size = size / 2;
          backoff_pool_size(pool, backoff_size);
          size -= backoff_size;
        }

        break;
      }
    }
  }
    
  pthread_mutex_unlock(&pool->locks->transfer_handle_lock);
  pthread_rwlock_unlock(&pool->locks->duplication_lock);

  /* make sure we have at least one entry in our pool */

  pool->size = size;
  if (pool->size == 0)
    return 0;
  else
    return 1;
}

static void free_handle_stack(mdbm_pool_t *dying) {
  mdbm_pool_entry_t *to_die;
  while ( dying->dup_handle_stack.lh_first ) {
    to_die = dying->dup_handle_stack.lh_first;
    mdbm_close(to_die->mdbm_handle);
    LIST_REMOVE( to_die, entries);
    free(to_die);
  }

  while ( dying->reserve_handle_stack.lh_first ) {
    to_die = dying->reserve_handle_stack.lh_first;
    LIST_REMOVE( to_die, entries);
    free(to_die);
  }
}

mdbm_pool_t *mdbm_pool_create_pool(MDBM *original_handle, int size) {
  mdbm_pool_t* new_pool = NULL;
  if (original_handle == NULL) {
    mdbm_logerror(LOG_ERR, 0, "Refusing to create pool for null handle.");
    return NULL;
  }

  if (size <= 0) {
    mdbm_logerror(LOG_ERR, 0, "Invalid pool size - must be positive integer.");
    return NULL;
  }

  new_pool = calloc(1, sizeof(mdbm_pool_t));
  if (new_pool == NULL) {
    mdbm_logerror(LOG_ERR, 0, "Failed to calloc new memory for mdbm handle pool.");
    return NULL;
  }

  if (!init_locks(new_pool)) {
    free(new_pool);
    return NULL;
  }

  new_pool->size = 0;
  new_pool->original_handle = original_handle;
  LIST_INIT(&new_pool->dup_handle_stack);
  LIST_INIT(&new_pool->reserve_handle_stack);

  if (!setup_pool(new_pool, size)) {
    free_locks(new_pool);
    free(new_pool);
    return NULL;
  }

  return new_pool;
}

int mdbm_pool_destroy_pool(mdbm_pool_t *pool) {
  if (pool == NULL) {
    return 0;
  }

  if (pthread_rwlock_wrlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("duplication_lock","destroying pool");
    return 0;
  }

  if (pthread_mutex_lock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("transfer_handle_lock", "destroying pool");
    pthread_rwlock_unlock(&pool->locks->duplication_lock);
    return 0;
  }

  free_handle_stack(pool);

  if (pthread_mutex_unlock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_RELEASE_FAILURE("transfer_handle_lock", "destroying pool");
  }

  if (pthread_rwlock_unlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_RELEASE_FAILURE("duplication_lock", "destroying pool");
  }

  free_locks(pool);
  free(pool);
  return 1;
}

static int wait_for_available_handle(mdbm_pool_t *pool) {
  int ret;
  uint64_t usec;
  struct timespec to;

  /* get the current time and add default 50 msec for timeout */

  usec = get_time_usec() + 50 * 1000;

  /* set timespec structure */

  to.tv_sec = usec / 1000000;
  to.tv_nsec = (usec % 1000000) * 1000;

  ret = pthread_cond_timedwait(&pool->locks->handle_cond, 
    &pool->locks->transfer_handle_lock, &to);

  return (ret == 0 || ret == ETIMEDOUT) ? 0 : -1;
}

MDBM *mdbm_pool_acquire_handle(mdbm_pool_t *pool) {
  mdbm_pool_entry_t *reserve_entry = NULL;
  MDBM *handle = NULL;
  if (pool == NULL) {
    return NULL;
  }

  if (pthread_rwlock_rdlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("duplication_lock", "access to handle");
    return NULL;
  }

  if (pthread_mutex_lock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("transfer_handle_lock",
      "access to handle");
    pthread_rwlock_unlock(&pool->locks->duplication_lock);
    return NULL;
  }

  while (! pool->dup_handle_stack.lh_first) {
    if (wait_for_available_handle(pool) != 0) {
      LOG_LOCK_RELEASE_FAILURE("handle_cond", "pthread_cond_timedwait returned failure");
      pthread_mutex_unlock(&pool->locks->transfer_handle_lock);
      pthread_rwlock_unlock(&pool->locks->duplication_lock);
      return NULL;
    }
  }

  reserve_entry = pool->dup_handle_stack.lh_first;
  LIST_REMOVE(reserve_entry, entries);

  handle = reserve_entry->mdbm_handle;
  reserve_entry->mdbm_handle = NULL;
  LIST_INSERT_HEAD(&pool->reserve_handle_stack, reserve_entry, entries);

  if (pthread_mutex_unlock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_RELEASE_FAILURE("transfer_handle_lock", "safe exit from acquire_handle.");
  }
  
  return handle;
}

int mdbm_pool_release_handle(mdbm_pool_t *pool, MDBM *db) {
  int ret = 1;
  mdbm_pool_entry_t *reserve_entry = NULL;
  if (pool == NULL || db == NULL) {
    return 0;
  }

  if (pthread_mutex_lock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("transfer_handle_lock",
      "completion of operations on handle.");
    pthread_rwlock_unlock(&pool->locks->duplication_lock);
    mdbm_close(db);
    return 0;
  }
  
  if (pool->reserve_handle_stack.lh_first) {
    reserve_entry = pool->reserve_handle_stack.lh_first;
    LIST_REMOVE(reserve_entry, entries);
  } else {
    if ((reserve_entry = calloc(1, sizeof(mdbm_pool_entry_t))) == NULL) {
      mdbm_logerror(LOG_ERR, 0, "Failed to allocate memory needed to return handle to pool.");
      pthread_mutex_unlock(&pool->locks->transfer_handle_lock);
      pthread_rwlock_unlock(&pool->locks->duplication_lock);
      mdbm_close(db);
      return 0;
    }
  }

  reserve_entry->mdbm_handle = db;
  LIST_INSERT_HEAD(&pool->dup_handle_stack, reserve_entry, entries);

  /* notify other waiting threads that we released a handle */

  if (pthread_cond_signal(&pool->locks->handle_cond)){
    ret = 0;
  }

  if (pthread_mutex_unlock(&pool->locks->transfer_handle_lock) != 0) {
    LOG_LOCK_RELEASE_FAILURE("transfer_handle_lock","safe exit from release_handle");
  }
  
  if (pthread_rwlock_unlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_RELEASE_FAILURE("duplication_lock", "safe exit from release_handle");
  }
    
  return ret;
}

MDBM *mdbm_pool_acquire_excl_handle(mdbm_pool_t *pool) {
  if (pool == NULL) {
    return NULL;
  }

  if (pthread_rwlock_wrlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_ACQUIRE_FAILURE("duplication_lock", "exclusive operations.");
    return NULL;
  }

  /* acquire_read_write_handle should never need to muck with the
   * handle stack, and pthread_rwlock_wrlock() will block until all 
   * the readers are done, so there's no need to acquire transfer_handle_lock.
   */

  return pool->original_handle;
}

int mdbm_pool_release_excl_handle(mdbm_pool_t *pool, MDBM *db) {
  if (pool == NULL || db == NULL) {
    return 0;
  }

  if (pthread_rwlock_unlock(&pool->locks->duplication_lock) != 0) {
    LOG_LOCK_RELEASE_FAILURE("duplication_lock", "completion of exclusive operations.");
    return 0;
  }

  return 1;
}

MDBM *mdbm_pool_acquire_ro_handle(mdbm_pool_t *pool) {
  return mdbm_pool_acquire_handle(pool);
}

int mdbm_pool_release_ro_handle(mdbm_pool_t *pool, MDBM *db) {
  return mdbm_pool_release_handle(pool, db);
}

MDBM *mdbm_pool_acquire_rw_handle(mdbm_pool_t *pool) {
  return mdbm_pool_acquire_excl_handle(pool);
}

int mdbm_pool_release_rw_handle(mdbm_pool_t *pool, MDBM *db) {
  return mdbm_pool_release_excl_handle(pool, db);
}


/************************************************************
 * config section
 ***********************************************************/

int mdbm_pool_parse_pool_size(const char *value) {
  char *p;
  char *data;
  char *token;
  char *token_next;
  int ret_value = -1;
  int def_value = 0;
  int test_value;
  char *self_name = NULL;
  char self_path[MAXPATHLEN] = {0,};

  if (readlink("/proc/self/exe", self_path, sizeof(self_path)) == -1) {
    return 0;
  }
  
  if ((self_name = basename(self_path)) == NULL) {
    return 0;
  }
  
  if ((data = strdup(value)) == NULL) {
    return 0;
  }
  
  token = strtok_r(data, ",", &token_next);
  while (token) {
    /* the format should be name=value. If it only contains
     * value then we'll use it as default for all apps 
     * if multiple name=value pairs specified, then first 
     * one wins, but if we have multiple values specified
     * then last one wins */

    if ((p = strchr(token, '=')) == NULL) {
      if ((test_value = atoi(token)) > 0) {
        def_value = test_value;
      }
    } else {
      *p++ = 0;
      if (strcmp(self_name, token) == 0) {
        ret_value = atoi(p);
        break;
      }
    }
      
    token = strtok_r(NULL, ",", &token_next);
  }

  free(data);
  if (ret_value < 0) {
    return def_value;
  } else {
    return ret_value;
  }
}

void mdbm_pool_verify_pool_size(int *vals, int count) {
  int i;
  int bCheck = 0;
  rlim_t current_size, proc_limit, file_limit;
  struct rlimit open_files_limit = {0,0};
  struct rlimit processes_or_threads_limit = {0,0};

  /* if we have negative values then reset them 
   * and keep track of if we need to process
   * anything at all (only for positive values) */

  for (i = 0; i < count; i++) {
    if (vals[i] > 0) {
      bCheck = 1;
    } else if (vals[i] < 0) {
      vals[i] = 0;
    }
  }
  
  if (bCheck == 0) {
    return;
  }

  /* only process our check if we have successful response 
   * from both of our limits */

  if (getrlimit(RLIMIT_NOFILE, &open_files_limit) != 0) {
    return;
  }

  if (getrlimit(RLIMIT_NPROC, &processes_or_threads_limit) != 0) {
    return;
  }

  /* Each handle uses 2 fd's: the DB and the lockfile. */
  file_limit = open_files_limit.rlim_cur / 2;
  /* Arbitrary overhead allowance of three-quarters. */
  proc_limit = processes_or_threads_limit.rlim_cur * 3 / 4;
      
  /* reset the max size to be 3/4 of the configured system max */
  
  for (i = 0; i < count; i++) {
    if (vals[i] <= 0) { continue; }
    current_size = vals[i];
    
    if (open_files_limit.rlim_cur != RLIM_INFINITY) {
      if (current_size > file_limit) {
        current_size = file_limit;
        mdbm_log(LOG_DEBUG, "resetting yahoo db pool handle size to %d, NOFILE limit %lu",
          vals[i], (unsigned long) open_files_limit.rlim_cur);
      }
    }

    if (processes_or_threads_limit.rlim_cur != RLIM_INFINITY) {
      if (proc_limit < current_size) {
        current_size = proc_limit;
        mdbm_log(LOG_DEBUG, "resetting yahoo db pool handle size to %d, NPROC limit %lu",
          vals[i], (long unsigned) processes_or_threads_limit.rlim_cur);
      }
    }

    /* might be unncessary checks but let's be absolutely
     * sure that we're decreasing the limit and not setting
     * to some invalid value */
    if (current_size > 0 && current_size < vals[i]) {
      vals[i] = current_size;
    }
  }
}

