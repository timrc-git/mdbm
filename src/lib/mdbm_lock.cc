/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include "mdbm_lock.hh"
#include "mdbm_internal.h"
#include "util.hh"

#include <dlfcn.h> // dynamic library/plugin loading

#include <string>
#include <map>

using namespace std;


// NOTE: this function breaks the nice encapsulation of MdbmLockBase
//   but we need a way for the tools to get the correct plugin and mode
//   for MDBM_ANY_LOCKS
int get_lock_flags(const char* dbname, int defaults) {
  char fn[MAXPATHLEN+1];
  struct stat st;
  int rc, flags = 0;
  int count = 0;

  bool has_mlock = false;

  // check for existence of (pthread multi-mutex):
  //   /tmp/.mlock-named/<dbname>._int_
  //
  snprintf(fn,sizeof(fn),"/tmp/.mlock-named/%s._int_", dbname);
  rc = stat(fn, &st);
  if (rc>=0) {
    //fprintf(stderr, "Found MLOCK; %s\n", fn);
    // pthreads, any
    // TODO open file and getMode() ? or just use ANY
    flags = MDBM_SINGLE_ARCH;// | MDBM_ANY_LOCKS;
    has_mlock = true;
    ++count;
  }

  if (!count) { 
    //fprintf(stderr, "Found  NO LOCKS; %s\n", dbname);
    return defaults; // FIXME is this right?
  }
  if (count > 1) {
    char buf[512];
    int off = 0;
    memset(buf, 0, 256);
    if (has_mlock) { off += sprintf(buf+off, "phreads-multi-lock "); }

    mdbm_logerror(LOG_ERR, 0, "%s: multiple different lockfiles exist! ( %s)\n", dbname, buf);
    return -1;
  }

  return flags;
}

static void delete_helper(const char* name, int &ret, int &er) {
  // TODO find out if the file is in use and don't delete
  // doesn't seem to be a good c/c++ way (so shell out to fuser/lsof?)
  int uret = unlink(name);
  if (uret && errno!=ENOENT) {
    ret = -1;
    er = errno;
  }
}


int do_delete_lockfiles(const char* dbname) {
    int ret = 0, errcode=0;
    char realname[MAXPATHLEN+1];
    char fn[MAXPATHLEN+1];

    if (dbname[0] == '/') {
        if (realpath(dbname, realname) == NULL) {
            mdbm_logerror(LOG_ERR, 0, "Unable to get realpath for %s", dbname);
            errno = ENOENT;
            return -1;
        }
        realname[MAXPATHLEN] = '\0';
    } else if (realpath(dbname, realname) == NULL) {
        int len;
        if (getcwd(realname, sizeof(realname)) == NULL) {
            mdbm_logerror(LOG_ERR, 0, "Unable to get current working directory");
            errno = ENOENT;
            return -1;
        }
        len = strlen(realname);
        if ((int)strlen(dbname) > (MAXPATHLEN-len)) {
            mdbm_logerror(LOG_ERR, 0, "Current working dir %s, when combined with file name %s, are too long",
                realname, dbname);
            errno = ENOENT;
            return -1;
        }
        realname[len] = '/';
        strncpy(realname+len+1, dbname, MAXPATHLEN-len);
        realname[MAXPATHLEN] = '\0';
    }

    snprintf(fn,sizeof(fn),"/tmp/.mlock-named/%s._int_", realname);
    delete_helper(fn, ret, errcode);

    errno=errcode;
    return ret;
}
//MdbmLockBase::MdbmLockBase() {
//}
//MdbmLockBase::~MdbmLockBase() {
//}


// NOTE: Function statics and file globals aren't threadsafe.
//   So, we have to initialize these in a library-constructor.

void __attribute__ ((constructor)) mdbm_lock_and_load(void);
void __attribute__ ((destructor)) mdbm_lock_and_unload(void);

static mdbm_lock_creator plugin_array[256];
static int plugin_count;

extern MdbmLockBase* pthread_lock_creator(void);

void mdbm_lock_and_load(void) {
    // Add initialization code…
    plugin_count = 0;
    for (int i=0; i<256; ++i) {
      plugin_array[i] = NULL;
    }

    // register PLock plugin explicitly, as it's not thread-safe to to rely on
    //   REGISTER_MDBM_LOCK_PLUGIN 
    mdbm_add_lock_plugin("plock", &pthread_lock_creator);
}

void mdbm_lock_and_unload(void) {
    // Add any library cleanup code…
}

//typedef map<string, mdbm_lock_creator> LockPluginMap;

// NOTE: Because of multi-threaded libraries that call mdbm_open() prior to
// main(), we have to be very careful about what structures we use, and
// how we initialize them. Currently this is done in the library constructor func:
//  mdbm_lock_and_load().
int mdbm_add_lock_plugin(const char* name, mdbm_lock_creator creator) {
  ++plugin_count;
  plugin_array[(uint8_t)name[0]]=creator;
  return plugin_count;
}

MdbmLockBase* mdbm_create_lock(const char* name) {
  if (!name) {
    return NULL;
  }
  mdbm_lock_creator creator = plugin_array[(uint8_t)name[0]];
  if (creator) {
    MdbmLockBase* locks = (creator)();
    if (!locks) {
      errno = ENOMEM;
    }
    return locks;
  }

  errno = ENODEV;
  return NULL;
}

#define LOCK_PRECOND(db)          \
    if (!db || !db->db_locks) {   \
      return ;                    \
    }                             \

#define LOCK_PRECOND_RET(db, failret) \
    if (!db || !db->db_locks) {   \
      return failret;             \
    }                             \

#define CAST_LOCKS(db) (db && db->db_locks) ? (MdbmLockBase*)db->db_locks : NULL

void print_lock_state_inner(struct mdbm_locks* locks) {
    if (locks) {
      ((MdbmLockBase*)locks)->printState();
    }
}

void print_lock_state(MDBM* db) {
    MdbmLockBase* locks = CAST_LOCKS(db);
    if (locks) {
      locks->printState();
    }
}

static void
lock_error(MDBM* db, const char* what, ...)
{
    MdbmLockBase* locks = CAST_LOCKS(db);
    int err = errno;
    const char* fname = db ? db->db_filename : NULL;
    int flen = fname ? strlen(fname) : 0;
    int len = strlen(what) + flen + 4;
    va_list args;
    char *fmtbuf = (char*)malloc(len);
    snprintf(fmtbuf,len,"%s: %s",fname,what);
    va_start(args,what);
    mdbm_log_vlogerror(LOG_ERR,0,fmtbuf,args);
    va_end(args);
    if (locks) {
      locks->printState();
    }
    free(fmtbuf);
    //print_trace();
    errno = err;
}


int db_is_locked(MDBM* db) {
  MdbmLockBase* locks = CAST_LOCKS(db);
  if (!locks) { // MDBM_OPEN_NOLOCK
    return 0;
  }
  return locks->getHeldCount(MLOCK_EXCLUSIVE, false) > 0; 
}

uint32_t db_get_lockmode(MDBM *db) {
  if (!db) {
    //fprintf(stderr, "lockmode NO LOCKS; NULL\n");
    return MDBM_OPEN_NOLOCK;
  }
  MdbmLockBase* locks = CAST_LOCKS(db);
  if (!locks) {
    //fprintf(stderr, "lockmode NO LOCKS; %s\n", db->db_filename);
    return MDBM_OPEN_NOLOCK;
  }
  int ret = locks->getMode();
  if (ret == MLOCK_SHARED) {
    //fprintf(stderr, "lockmode SHARED; %s\n", db->db_filename);
    return MDBM_RW_LOCKS;
  } else if (ret == MLOCK_INDEX) {
    //fprintf(stderr, "lockmode PART; %s\n", db->db_filename);
    return MDBM_PARTITIONED_LOCKS;
  } else if (ret == MLOCK_EXCLUSIVE) {
    //fprintf(stderr, "lockmode EXCL; %s\n", db->db_filename);
    return 0; // EXCLUSIVE locking
  }
  //fprintf(stderr, "lockmode DEFAULT NONE; %s\n", db->db_filename);
  return MDBM_OPEN_NOLOCK;
}

int db_get_part_count(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  MdbmLockBase* locks = CAST_LOCKS(db);
  uint32_t lockmode = db_get_lockmode(db);
  if (lockmode == MDBM_RW_LOCKS) {
    return locks->getCount(MLOCK_SHARED);
  } else if (lockmode == MDBM_PARTITIONED_LOCKS) {
    return locks->getCount(MLOCK_INDEX);
  }
  return 0;
}

int db_internal_is_owned(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  MdbmLockBase* locks = CAST_LOCKS(db);
  return locks->getHeldCount(MLOCK_INTERNAL, true) > 0;
}

/* NOTE: despite the documentation, this only checks ownership */
/* by the calling process, instead of ownership by *any* process. */
/* NOTE: this function indicates some form of exclusive ownership. */
int db_is_owned(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  MdbmLockBase* locks = CAST_LOCKS(db);
  return locks->getHeldCount(MLOCK_EXCLUSIVE, true) > 0;
}

int db_part_owned(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  uint32_t lockmode = db_get_lockmode(db);
  MdbmLockBase* locks = CAST_LOCKS(db);
  if (lockmode == MDBM_RW_LOCKS) {
    return locks->getHeldCount(MLOCK_SHARED, true);
  } else if (lockmode == MDBM_PARTITIONED_LOCKS) {
    return locks->getHeldCount(MLOCK_INDEX, true);
  }
  return 0;
}

int db_is_multi_lock(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  uint32_t lockmode = db_get_lockmode(db);
  if (lockmode == MDBM_RW_LOCKS) {
    return 1;
  } else if (lockmode == MDBM_PARTITIONED_LOCKS) {
    return 1;
  }
  return 0;
}

// NOTE: this actually returns >0 if the part/share
// lock count is >1
int db_multi_part_locked(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  MdbmLockBase* locks = CAST_LOCKS(db);
  uint32_t lockmode = db_get_lockmode(db);
  if (lockmode == MDBM_RW_LOCKS) {
    return locks->getHeldCount(MLOCK_SHARED, true) > 1;
  } else if (lockmode == MDBM_PARTITIONED_LOCKS) {
    return locks->getHeldCount(MLOCK_INDEX, true) > 1;
  }
  return 0;
}

//int get_lockfile_name(const char* dbname, char* lockname, int maxlen) {
//  if (!dbname || !dbname[0]) {
//    errno = EINVAL;
//    return -1;
//  }
//  MdbmLockBase* locks = CAST_LOCKS(db);
//  return locks->getFilename(dbname, lockname, maxlen);
//}


struct mdbm_locks* open_locks_inner(const char* dbname, int flags, int do_lock, int* need_check) {
  if (flags & MDBM_ANY_LOCKS) {
    int lock_flags = get_lock_flags(dbname, flags&MDBM_LOCK_MASK);
    if (lock_flags == -1) {
      // Note: this only happens if multiple different kinds of lockfiles already exist
      return NULL;
    }
    flags |= lock_flags;
  }

  MdbmLockBase* locks = NULL;
  //fprintf(stderr, "+++++++ CREATING PLOCK LOCKS +++++++\n");
  locks = mdbm_create_lock("plock");

  if (!locks) {
    int saveErr = errno;
    // error.. locks not created
    mdbm_logerror(LOG_ERR, 0, "%s: MDBM failed to create locks! \n", dbname);
    errno=saveErr;
    return NULL;
  }

  // Note: file mode (permissions) are expected to be handled by the lock plugins
  // (typically by stat()'ing the db file, and adjusting as necessary)
  MLockType type = MLOCK_EXCLUSIVE;
  int count = 1;
  if (flags & MDBM_RW_LOCKS) {
    type = MLOCK_SHARED;
    //count = NUM_CPU_CORES * 2;
    count = get_cpu_count() * 2;
  } else if (flags & MDBM_PARTITIONED_LOCKS) {
    type = MLOCK_INDEX;
#ifdef PARTITION_LOCK_COUNT
    count = PARTITION_LOCK_COUNT;
#elif defined(PARTITION_LOCK_CPU_MULTIPLIER)
    count = get_cpu_count() * PARTITION_LOCK_CPU_MULTIPLIER;
#else
    count = 128;
#endif // PARTITION_LOCK_COUNT
  }
  if (locks->open(dbname, flags, type, count, do_lock, need_check)) {
    delete locks;
    return NULL;
  }
  return (struct mdbm_locks*)locks;
}

int mdbm_internal_open_locks(MDBM* db, int flags, int do_lock, int* need_check) {
  if (!db) {
    return -1;
  }
  db->db_locks = open_locks_inner(db->db_filename, flags, do_lock, need_check);
  if (!db->db_locks) {
    // TODO FIXME error.. locks not created
    return -1;
  }
  int lockmode = db_get_lockmode(db);
  if (lockmode & MDBM_RW_LOCKS) {
    // FIXME horrible profusion of redundant flags. It needs to go away.
    db->db_flags |= MDBM_DBFLAG_RWLOCKS;
  }
  return 0;
}

void close_locks_inner(struct mdbm_locks* locks) {
  if (locks) {
    ((MdbmLockBase*)locks)->close();
    delete ((MdbmLockBase*)locks);
  }
}

void mdbm_internal_close_locks(MDBM* db) {
  LOCK_PRECOND(db);
  close_locks_inner(db->db_locks);
  db->db_locks = NULL;
}

int mdbm_internal_lock(MDBM* db) {
  int check = 0, upgrade = 0;
  LOCK_PRECOND_RET(db, -1);
  MdbmLockBase* locks = CAST_LOCKS(db);
  return locks->lock(MLOCK_INTERNAL, MLOCK_BLOCK, 0, check, upgrade);
}

int mdbm_internal_unlock(MDBM* db) {
  LOCK_PRECOND_RET(db, -1);
  MdbmLockBase* locks = CAST_LOCKS(db);
  return locks->unlock(MLOCK_INTERNAL);
}

#define TRY_OR_LOCK_TIMED(op, pg) \
    ret = locks->lock(op, MLOCK_ASYNC, pg, do_check, upgraded); /* try */ \
    /*fprintf(stderr, "Lock(ASYNC) EXCLUSIVE nonblock:%d ret:%d errno:%d (vs EWOULDBLOCK:%d)\n", nonblock, ret, errno, EWOULDBLOCK); */ \
    if (!nonblock && ret && errno==EWOULDBLOCK) { \
        uint64_t t0 = (db->db_rstats) ? get_time_usec() : 0; \
        /* NOTE("Lock(BLOCK) EXCLUSIVE"); */ \
        ret = locks->lock(op, MLOCK_BLOCK, pg, do_check, upgraded); /* block */ \
        if (db->db_rstats) { \
          db->db_lock_wait += get_time_usec() - t0; \
        } \
    } else { db->db_lock_wait = 0; }

#define TRY_OR_LOCK_TIMED_ERR(op, pg, msg)                     \
    TRY_OR_LOCK_TIMED(op, pg)                                  \
    if (ret) {                                                 \
        if (errno == EINPROGRESS) { do_check = 1; }            \
        else {                                                 \
            if (errno != EWOULDBLOCK) { lock_error(db, msg); } \
            return -1;                                         \
        }                                                      \
    }                                                          \


/**
 * \brief Locks a page this MDBM in the requested mode.
 * \param[in,out] db database handle
 * \param[in] pageno page number or locking mode (exlcusive or shared)
 * \param[in] nonblock boolean indicator whether it's a "nonblock" lock.
 *            If \a nonblock is nonzero, and taking out a lock would block,
 *            return -1 and errno = EWOULDBLOCK.
 * \param[in] check boolean indicator whether to do a integrity check.
 *            Integrity checks are done to test for lock upgrade success or
 *            process death of lock owner.
 * \return status code for success of lock
 * \retval -1 error
 * \retval  0 success
 * \retval  1 lock was upgraded
 */
int
do_lock_x(MDBM* db, int pageno, int nonblock, int check)
{
    int prot = 0;
    int lock_upgraded = 0;
    //struct mdbm_locks *db_locks = db->db_locks;

    if (MDBM_NOLOCK(db)) {
        return 1;
    }
    LOCK_PRECOND_RET(db, -1);
    MdbmLockBase* locks = CAST_LOCKS(db);

    for (;;) {
        int exclusive_nest = locks->getHeldCount(MLOCK_EXCLUSIVE, true);
        int part_nest = locks->getHeldCount(MLOCK_INDEX, true)
                      + locks->getHeldCount(MLOCK_SHARED, true);
        int part_count = locks->getCount(MLOCK_INDEX)
                       + locks->getCount(MLOCK_SHARED);
        int ret;
        int do_check = 0; /* If an integrity check needs to be done. */
        int upgraded = 0; /* If the lock was implicitly upgraded (prev EXCL owner died). */
        int part_num = -1;
        //fprintf(stderr, "@@@ do_lock_x pid:%d tid:%d, excl_nest:%d part_nest:%d part_count:%d pageno:%d\n", getpid(), gettid(), exclusive_nest, part_nest, part_count, pageno); 
        /*fprintf(stderr, "do_lock_x, part_count:%d\n", part_count); */
        if (part_count > 1) { /* shared/indexed locking (vs single-lock) */
            if (pageno == MDBM_LOCK_EXCLUSIVE || pageno == MDBM_LOCK_SHARED) {
                part_num = pageno;
            } else {
                /* TODO FIXME ??? part_num = MDBM_PAGENUM_TO_PARTITION(db, pageno); */
                part_num = pageno % part_count;
            }

            if (pageno == MDBM_LOCK_EXCLUSIVE || exclusive_nest > 0) {
                if (!exclusive_nest && part_nest>0) {
                    //fprintf(stderr, "@@@ do_lock_x-upgrade pid:%d tid:%d, \n", getpid(), gettid()); 
                    /*NOTE("Upgrade(TRY)"); */
                    TRY_OR_LOCK_TIMED_ERR(MLOCK_UPGRADE, part_num, "mlock_upgrade()");
                } else { /* exclusive lock already held, bump count */
                    //fprintf(stderr, "@@@ do_lock_x-exclusive pid:%d tid:%d\n",getpid(), gettid());
                    /*NOTE("Lock(ASYNC) EXCLUSIVE"); */
                    TRY_OR_LOCK_TIMED_ERR(MLOCK_EXCLUSIVE, part_num, "mlock_lock(exclusive)");
                    if (1 == locks->getHeldCount(MLOCK_EXCLUSIVE, true)) {
                      prot = 1;
                    }
                }
            } else if ((part_nest > 0) && (pageno != MDBM_LOCK_SHARED)) {
                if (!locks->getHeldCount(MLOCK_INDEX, true, part_num)) {
                    errno = EPERM;
                    lock_error(db,"another partition locking conflict (locked=? want=%d)"
                        " excl_nest=%d, part_nest=%d, part_count=%d   ",
                        part_num, 
                        exclusive_nest, part_nest, part_count);
                    return -1;
                } else {
                    /*fprintf(stderr, "@@@ do_lock_x-part pid:%d tid:%d, part_num:%d\n", getpid(), gettid(), part_num); */
                    /* don't worry about blocking... should already be held... */
                    locks->lock(MLOCK_INDEX, MLOCK_BLOCK, part_num, do_check, upgraded);
                }
            } else {
                if (pageno == MDBM_LOCK_SHARED) {
                    /*NOTE("Lock(ASYNC) ANY SHARED"); */
                    //fprintf(stderr, "@@@ do_lock_x-shared pid:%d tid:%d, \n", getpid(), gettid()); 
                    TRY_OR_LOCK_TIMED_ERR(MLOCK_SHARED, MLOCK_ANY, "mlock_lock(shared)");
                } else {
                    /*NOTE("Lock(ASYNC) PART"); */
                    /*lock_error(db,"Lock(ASYNC) Part (page:%d part:%d)",pageno,part_num); */
                    /*print_trace(); */
                    /*fprintf(stderr, "@@@ do_lock_x-index pid:%d tid:%d part_num:%d, \n", getpid(), gettid(), part_num); */
                    TRY_OR_LOCK_TIMED_ERR(MLOCK_INDEX, part_num, "mlock_lock(index)");
                }
                prot = 1;
            }
        } else { /* Single lock (not partitioned/shared) */
            /*NOTE("Lock(ASYNC) SINGLE"); */
            //fprintf(stderr, "@@@ do_lock_x-single pid:%d tid:%d, \n", getpid(), gettid()); 
            TRY_OR_LOCK_TIMED_ERR(MLOCK_EXCLUSIVE, part_num, "mlock_lock(single)");
            if (locks->getHeldCount(MLOCK_EXCLUSIVE, true) == 1) {
                prot = 1;
            }
        }
        /*////////////////////////////////////////////////////////*/
        if (prot && MDBM_USE_PROTECT(db)) {
            mdbm_protect(db,MDBM_PROT_ACCESS);
        }
        if (do_check && check && MDBM_IS_WINDOWED(db)) {
            mdbm_log(LOG_NOTICE,
                     "%s: previous lock owner died; use mdbm_check to test db integrity",
                     db->db_filename);
            check = 0;
        }
        if (do_check && check) {
            int nerr;
            exclusive_nest = locks->getHeldCount(MLOCK_EXCLUSIVE, true);
            part_nest = locks->getHeldCount(MLOCK_INDEX, true)
                      + locks->getHeldCount(MLOCK_SHARED, true);

            if (!exclusive_nest && !part_nest) {
                mdbm_log(LOG_NOTICE,"%s: attempting mdbm lock upgrade for db integrity check",
                         db->db_filename);
                if (do_lock_x(db,MDBM_LOCK_EXCLUSIVE,nonblock,MDBM_LOCK_NOCHECK) < 0) {
                    mdbm_logerror(LOG_NOTICE,0,
                                  "%s: mdbm lock upgrade failure",
                             db->db_filename);
                    do_unlock_x(db);
                    sleep(1);
                    continue;
                }
                lock_upgraded = 1;
            }
            mdbm_log(LOG_NOTICE,"%s: mdbm integrity check due to previous lock owner death",
                     db->db_filename);
            nerr = mdbm_check(db,3,1);
            if (lock_upgraded) {
                do_unlock_x(db);
            }
            if (nerr > 0) {
                mdbm_log(LOG_ERR,
                         "%s: do_lock db integrity check failure",
                         db->db_filename);
                do_unlock_x(db);
                errno = db->db_errno = EFAULT;
                return -1;
            } else {
                mdbm_log(LOG_NOTICE,"%s: mdbm integrity check passed",db->db_filename);
            }
        }
        return 1;
        /*////////////////////////////////////////////////////////*/

    }
    /* NOTREACHED */
}

int
mdbm_internal_do_lock(MDBM* db, int write, int nonblock, const datum* key,
        mdbm_hashval_t* hashval, mdbm_pagenum_t* pagenum)
{
    /*struct mdbm_locks *db_locks = db->db_locks; */

    if (db->db_errno) {
        errno = db->db_errno;
        return -1;
    }
    if (write == MDBM_LOCK_WRITE && MDBM_IS_RDONLY(db)) {
        errno = EPERM;
        return -1;
    }

    if (key && hashval) {
        *hashval = hash_value(db,key);
    }
    //fprintf(stderr, "do_lock pagenum:%p->%d multi:%d rw:%d \n", (void*)pagenum, pagenum?*pagenum:0, db_is_multi_lock(db), MDBM_RWLOCKS(db));
    if (!pagenum || !db_is_multi_lock(db) || MDBM_RWLOCKS(db)) {
        int locktype = ((write == MDBM_LOCK_WRITE
                        || write == MDBM_LOCK_EXCLUSIVE
                        || !MDBM_RWLOCKS(db))
                        ? MDBM_LOCK_EXCLUSIVE
                        : MDBM_LOCK_SHARED);
        /*if (MDBM_LOCK_SHARED==write) { fprintf(stderr, "do_lock pagenum:%p->%d multi:%d rw:%d locktype:%s\n", (void*)pagenum, pagenum?*pagenum:0, db_is_multi_lock(db), MDBM_RWLOCKS(db), locktype==MDBM_LOCK_EXCLUSIVE?"EXCL":"SHAR"); } */
        if (do_lock_x(db,locktype,nonblock,MDBM_LOCK_CHECK) < 0) {
            return -1;
        }

retry_replace:
        if (MDBM_REPLACED_OR_CHANGED(db) && locktype!=MDBM_LOCK_EXCLUSIVE) {
//fprintf(stderr, "***SHARED UPGRADE***\n");
            do_unlock_x(db); // release shared lock to upgrade
//fprintf(stderr, "***INTENTIONAL DELAY***\n");
            locktype = MDBM_LOCK_EXCLUSIVE;
            if (do_lock_x(db,locktype,nonblock,MDBM_LOCK_CHECK) < 0) {
                return -1;
            }
        } 

        if (db->db_dup_info) {
            if (db->db_dup_map_gen != db->db_dup_info->dup_map_gen) {
                sync_dup_map_gen(db);
            }
        }

        if (MDBM_REPLACED_OR_CHANGED(db)) {
          if (locktype!=MDBM_LOCK_EXCLUSIVE) {
//fprintf(stderr, "***SHARED RETRY***\n");
            goto retry_replace;
          } else {
//fprintf(stderr, "***SHARED REPLACED-OR_CHANGED***\n");
            if ((MDBM_IS_REPLACED(db) && mdbm_internal_replace(db) < 0)
                || (MDBM_SIZE_CHANGED(db) && mdbm_internal_remap(db,0,0) < 0))
            {
                int err = errno;
                do_unlock_x(db);
                errno = err;
                return -1;
            }
          }
        }

        if (db->db_hdr->h_dir_gen != db->db_dir_gen) {
            sync_dir(db,NULL);
        }
        if (pagenum && hashval) {
            *pagenum = hashval_to_pagenum(db,*hashval);
        }
#ifdef MDBM_BSOPS
        if (db->db_bsops) {
            if (db->db_bsops->bs_lock(db->db_bsops_data,key,write) < 0) {
                int err = errno;
                lock_error(db,"backing store part/rw lock failed; dumping cache lock state");
                do_unlock_x(db);
                errno = err;
                return -1;
            }
        }
#endif
        return 1;
    } else {
        /* partitioned lock */
        int prot = 0;
        if (MDBM_USE_PROTECT(db) && !db_part_owned(db)) {
            prot = 1;
        }
        for (;;) {
            int pageno;
            int lock_upgrade = 0;

            if (prot) {
                int protsz = (MDBM_DB_NUM_DIR_BYTES(db)+db->db_sys_pagesize-1)
                    & ~(db->db_sys_pagesize-1);
                if (mprotect(db->db_base,protsz,PROT_READ) < 0) {
                    mdbm_logerror(LOG_ERR,0,"%s: mprotect failure",db->db_filename);
                    return -1;
                }
                prot = 0;
            }
            pageno = hashval ? hashval_to_pagenum(db,*hashval) : *pagenum;
            if (do_lock_x(db,pageno,nonblock,MDBM_LOCK_CHECK) < 0) {
                return -1;
            }
            if (db->db_dup_info) {
                if (db->db_dup_map_gen != db->db_dup_info->dup_map_gen) {
                    sync_dup_map_gen(db);
                }
            }
            if (MDBM_REPLACED_OR_CHANGED(db)) {
                /* release partition lock for exclusive (to avoid deadlock) */
                do_unlock_x(db);
                lock_upgrade = 1;
                if (do_lock_x(db,MDBM_LOCK_EXCLUSIVE,nonblock,MDBM_LOCK_CHECK) < 0) {
                    if (errno != EDEADLK) {
                        return -1;
                    }
                    /* deadlock second chance? */
                    if (do_lock_x(db,MDBM_LOCK_EXCLUSIVE,nonblock,MDBM_LOCK_CHECK) < 0) {
                        return -1;
                    }
                }
                /* have to lock again, as lock_upgrade==1 implies double lock */
                if (do_lock_x(db,MDBM_LOCK_EXCLUSIVE,nonblock,MDBM_LOCK_CHECK) < 0) {
                    /* this shouldn't happen, as we have exclusive lock from above */
                    do_unlock_x(db); /* release exclusive */
                    return -1;
                }
                if (db->db_dup_info) {
                    if (db->db_dup_map_gen != db->db_dup_info->dup_map_gen) {
                        sync_dup_map_gen(db);
                    }
                }
            }

            if ((MDBM_IS_REPLACED(db) && mdbm_internal_replace(db) < 0)
                || (MDBM_SIZE_CHANGED(db) && mdbm_internal_remap(db,0,0) < 0))
            {
                int err = errno;
                if (lock_upgrade) {
                    do_unlock_x(db);
                }
                do_unlock_x(db);
                errno = err;
                return -1;
            }

            if (lock_upgrade) {
                do_unlock_x(db);
            }
            if (db->db_hdr->h_dir_gen != db->db_dir_gen) {
                sync_dir(db,NULL);
                /* page mapping may be wrong now; repeat locking loop */
                do_unlock_x(db);
                continue;
            }
            if (hashval) {
                *pagenum = pageno;
            }
#ifdef MDBM_BSOPS
            if (db->db_bsops) {
                if (db->db_bsops->bs_lock(db->db_bsops_data,key,write) < 0) {
                    int err = errno;
                    lock_error(db,"backing store part/rw lock failed; dumping cache lock state");
                    do_unlock_x(db);
                    errno = err;
                    return -1;
                }
            }
#endif
            return 1;
        }
    }
    /* NOTREACHED */
}

int
do_unlock_x(MDBM* db)
{
    LOCK_PRECOND_RET(db, -1);
    MdbmLockBase* locks = CAST_LOCKS(db);
    int prot = 0;
    //struct mdbm_locks *db_locks = db->db_locks;

    if (MDBM_NOLOCK(db)) {
        return 1;
    }

    {
      int exclusive_nest = locks->getHeldCount(MLOCK_EXCLUSIVE, true);
      int part_nest = locks->getHeldCount(MLOCK_INDEX, true);
      int share_nest = locks->getHeldCount(MLOCK_SHARED, true);
      //fprintf(stderr, "@@@ do_unlock_x pid:%d tid:%d, excl_nest:%d part_nest:%d share_nest:%d\n", getpid(), gettid(), exclusive_nest, part_nest, share_nest);
      if (exclusive_nest > 0) {
        /* exclusive lock is held... release it */
        if (locks->unlock(MLOCK_EXCLUSIVE) < 0) {
          lock_error(db, "mlock_unlock (exclusive) from mdbm_unlock");
          return -1;
        }
        if ((exclusive_nest == 1) && !part_nest && !share_nest) {
          prot = 1;
        }
      } else if (part_nest > 0 || share_nest > 0) {
        /* partition/shared lock is held... release it */
        if (locks->unlock(part_nest?MLOCK_INDEX:MLOCK_SHARED) < 0) {
          //int parts = locks->getCount(MLOCK_INDEX);
          //          + locks->getCount(MLOCK_SHARED);
          //fprintf(stderr, "ERROR in unlock! excl_nest:%d part_nest:%d/%d pid:%d\n", exclusive_nest, part_nest, parts, getpid());
          lock_error(db, "mlock_unlock (shared/part) from mdbm_unlock");
          return -1;
        }
        if ((part_nest == 1) || (share_nest == 0)) {
          prot = 1;
        }
      } else {
        //fprintf(stderr, "Unlock EPERM ERROR, lock ownership... excl:%d, part:%d\n", exclusive_nest, part_nest);
        errno = EPERM;
        lock_error(db,"lock not owned in mdbm_unlock");
        return -1;
      }
    }

    if (prot) {
        if (MDBM_IS_WINDOWED(db)) {
            release_window_pages(db);
        }
        if (MDBM_USE_PROTECT(db)) {
            mdbm_protect(db,MDBM_PROT_NOACCESS);
        }
    }
    return 1;
}

int
mdbm_internal_do_unlock(MDBM* db, const datum* key)
{
#ifdef MDBM_BSOPS
    if (db->db_bsops) {
        db->db_bsops->bs_unlock(db->db_bsops_data,key);
    }
#endif
    return do_unlock_x(db);
}

int do_lock_reset(const char* dbfilename, int flags) {
  //errno = ENOTSUP;
  //return -1;
  fprintf(stderr, "NOTE: Resetting locks for %s!\n", dbfilename);
  int need_check = 0;
  MdbmLockBase* locks = (MdbmLockBase*)open_locks_inner(dbfilename, flags|MDBM_ANY_LOCKS, 0, &need_check); 
  if (!locks) {
    return -1;
  }
  int ret = locks->reset(dbfilename, flags);

  delete locks;
  return ret;
}





