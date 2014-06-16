/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/queue.h>

#include "mdbm_handle_pool.h"
/* #include "ccmp_internal.h" */

const char *mdbm_name= "/tmp/test_handle_pool.mdbm";

//Contains all a thread needs to know to get started.
typedef struct _reader_thread_input {
  int iterations;
  mdbm_pool_t* pool;
} reader_thread_input_t;

static MDBM *open_mdbm(char const* name) {
  MDBM *handle = mdbm_open(name, MDBM_O_FSYNC, O_RDONLY, 4 * 1024 , 0);
  return handle;
}

static void *acquire_wait_release(void* reader_thread_input_v) {
  int i;
  reader_thread_input_t* reader_thread_input= (reader_thread_input_t*)reader_thread_input_v;
  
  for (i = 0; i < reader_thread_input->iterations ; ++i) {
      MDBM *mdbm_handle = mdbm_pool_acquire_handle(reader_thread_input->pool);
      if (mdbm_handle == NULL) {
	  fprintf(stderr, "mdbm_handle is null, aborting!\n");
	  abort();
      }

      struct timespec centisecond;
      centisecond.tv_sec= 0; centisecond.tv_nsec= 10;
      nanosleep(&centisecond, NULL);
      
      if (!mdbm_pool_release_handle(reader_thread_input->pool, mdbm_handle)) {
	  fprintf(stderr, "Could not release mdbm_handle %p\n", (void*)mdbm_handle);
	  abort();
      }
  }
  
  return (void *) 0;
}

static void create_reader_threads(int threads, reader_thread_input_t* reader_thread_input,
				  pthread_t *thr_list) {
  int i;

  pthread_attr_t attributes;
  pthread_attr_init(&attributes);
  pthread_attr_setstacksize(&attributes, 64*1024);
  
  for (i = 0 ; i < threads ; ++i) {
      pthread_create(&thr_list[i], &attributes, &acquire_wait_release,
		     reader_thread_input);
  }
  
  pthread_attr_destroy(&attributes);
}

int main(int argc, char const** argv) {
  int i;

  if (argc != 3) {
      fprintf(stderr, "usage: create_destroy <pool_size> <num_threads>\n");
      exit(1);
  }

  MDBM *mdbm_handle = open_mdbm(mdbm_name);
  if (mdbm_handle == NULL) {
      fprintf(stderr, "mdbm_handle is null, aborting!");
      exit(1);
  }
    
  mdbm_pool_t *pool = mdbm_pool_create_pool(mdbm_handle, atoi(argv[1]));
  if (pool == NULL) {
      fprintf(stderr, "pool is null, aborting!");
      exit(1);
  }

  reader_thread_input_t* reader_thread_real= (reader_thread_input_t *) 
    calloc(1, sizeof(reader_thread_input_t));
  reader_thread_real->iterations = 1024;
  reader_thread_real->pool = pool;
  
  int num_threads = atoi(argv[2]);
  pthread_t *thr_list = (pthread_t *) malloc(num_threads * sizeof(pthread_t));
  if (thr_list == NULL) {
      fprintf(stderr, "Out of memory\n");
      exit(1);
  }
  
  create_reader_threads(num_threads, reader_thread_real, thr_list);
  
  for (i = 0; i < num_threads; i++) {
    pthread_join(thr_list[i], NULL);
  }
  
  mdbm_pool_destroy_pool(pool);
  mdbm_close(mdbm_handle);
  free(reader_thread_real);
  free(thr_list);

  return 0;
}
