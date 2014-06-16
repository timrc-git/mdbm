/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

#include "mdbm_handle_pool.h"
/* #include "ccmp_internal.h" */

struct mdbm_pool_entry {
  MDBM *mdbm_handle;
  LIST_ENTRY(mdbm_pool_entry) entries;
};
typedef struct mdbm_pool_entry mdbm_pool_entry_t;

const char *mdbm_name= "/tmp/test_handle_pool.mdbm";

static MDBM *open_mdbm(char const* name) {
  MDBM *handle = mdbm_open(name, MDBM_O_FSYNC, O_RDONLY, 4 * 1024 , 0);
  return handle;
}

int create_and_test_size(MDBM *mdbm_handle, int pool_size) {
  mdbm_pool_t* pool = mdbm_pool_create_pool(mdbm_handle, pool_size);

  LIST_HEAD(test_mdbm_pool_handle_list, mdbm_pool_entry) acquired_handles;
  LIST_INIT(&acquired_handles);
  
  int ret = 0;
  int handles_extracted = 0;
  for (handles_extracted = 0; handles_extracted < pool_size; ++handles_extracted) {
      MDBM *new_handle = mdbm_pool_acquire_handle(pool);
      if (new_handle) {
	  mdbm_pool_entry_t* n1 = (mdbm_pool_entry_t*)calloc(1, sizeof(mdbm_pool_entry_t));
	  n1->mdbm_handle = new_handle;
	  LIST_INSERT_HEAD(&acquired_handles, n1, entries);
      } else {
	  fprintf(stderr, "unable to acquire mdbm handle count %d limit %d\n",
		  handles_extracted, pool_size);
	  
	  ret = 1;
	  break;
      }
    }
  
  while (acquired_handles.lh_first != NULL) {
      MDBM *release_this = acquired_handles.lh_first->mdbm_handle;
      mdbm_pool_entry_t *free_this = acquired_handles.lh_first;
      if (! mdbm_pool_release_handle(pool, release_this)) {
	  fprintf(stderr, "uanble to release mdbm handle\n");
	  ret = 1;
      }

      LIST_REMOVE(acquired_handles.lh_first, entries);
      free(free_this);
  }
  
  mdbm_pool_destroy_pool(pool);
  return ret;
}

int main(int argc, char const** argv) {
  int ret;

  if (argc != 2) {
      fprintf(stderr, "usage: create_destroy <pool_size>\n");
      exit(1);
    }

  MDBM *mdbm_handle = open_mdbm(mdbm_name);
  if (mdbm_handle == NULL) {
      fprintf(stderr, "mdbm_handle is null, aborting.!");
      exit(1);
  }
    
  ret = create_and_test_size(mdbm_handle, atoi(argv[1]));
  mdbm_close(mdbm_handle);
  
  return ret;
}
