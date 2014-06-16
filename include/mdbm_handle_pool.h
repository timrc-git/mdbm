/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM_HANDLE_POOL_H
#define MDBM_HANDLE_POOL_H

#ifdef	__cplusplus
extern "C" {
#endif

#include <mdbm.h>

typedef struct mdbm_pool_s mdbm_pool_t;

/**
 * Parse and return the configured pool size for the this process
 *
 * \param value configuration setting value. The format of the value
 *  is comma (,) separated list name=value pairs where
 *  name is the application basename (e.g. yapache, yjava_daemon) and the
 *  value is the number of handles in the pool.
 *
 * \return Number of configured number of mdbm handles for the pool
 */
int mdbm_pool_parse_pool_size(const char *value);

/**
 * Verifies and returns updated values for number of handles in the pool.
 * The maximum useful size of handles is affected by both the maximum allowed
 * number of open files (NOFILE) and the maximum number of processes (NPROC):

 * NOFILE limit: Each mdbm handle requires two open file entries (one for the
 *    db file and one for the lock file). Thus, if ynetdblib sees that the
 *    number of requested mdbm handles exceeds 3/4 of the maximum allowed
 *    number of open files ( determined by getrlimit(RLIMIT_NOFILE, ...),
 *    it will scale back accordingly.
 *
 * NPROC limit: Since each thread requires only one mdbm handle, and not all
 *    threads need to open ynetdblib mdbm files, there is no good
 *    reason to support having as many handles as the maximum number
 *    of processes. Thus, if ynetdblib sees that the number of
 *    requested mdbm handles exceeds 3/4 of the maximum allowed number
 *    of processes (determined by getrlimit(RLIMIT_NPROC, ...)
 *    it will scale back accordingly.
 * 
 * \param vals integer array that includes the values to be checked. If the
 *  value is larger than the system limit as described above, the array is
 *  updated to reflect the new value
 * \param count the number of integers in the vals array
 */
void mdbm_pool_verify_pool_size(int *vals, int count);

/** 
 * Create a pool of mdbm handles
 *
 * \param db MDBM handle that will be duplicated
 * \param size number of duplicated handles
 * 
 * \return pointer to newly created pool object. This object must
 *  destroyed using mdbm_pool_destroy_pool function.
 */
mdbm_pool_t *mdbm_pool_create_pool(MDBM *db, int size);

/**
 * Delete a mdbm handle pool
 *
 * \param pool pointer to a pool object that was returned from 
 *  calling mdbm_pool_create_pool function
 *
 * \return 1 if successful or 0 for failure.
 */ 
int mdbm_pool_destroy_pool(mdbm_pool_t *pool);

/** 
 * Acquire an MDBM handle for database operations. If there are no
 * available handles in the pool, the function will block until
 * there is one available.
 *
 * \param pool pointer to a pool object that was returned from 
 *  calling mdbm_pool_create_pool function
 *
 * \return pointer to MDBM handle or NULL for failure. The MDBM
 *  handle must be returned to the pool with the mdbm_pool_release_handle function
 */
MDBM *mdbm_pool_acquire_handle(mdbm_pool_t *pool);

/**
 * Release MDBM handle back to the pool
 *
 * \param pool pointer to a pool object that was returned from 
 *  calling mdbm_pool_create_pool function
 * \param db pointer to the MDBM handle to be released back to the pool.
 *  This pointer must have been acquired from the mdbm_pool_acquire_handle call.
 *
 * \return 1 if successful or 0 for failure.
 */
int mdbm_pool_release_handle(mdbm_pool_t *pool, MDBM *db);

/** 
 * Acquire an exclusive MDBM handle for database operations.
 * The original MDBM handle that was used for duplication is returned
 * to the caller. The function guarnatees that all of the duplicated
 * MDBM handles are in the pool and nobody is using them.
 *
 * \param pool pointer to a pool object that was returned from 
 *  calling mdbm_pool_create_pool function
 *
 * \return pointer to MDBM handle or NULL for failure. The MDBM
 *  handle must be returned to the pool with the mdbm_pool_release_handle function
 */
MDBM *mdbm_pool_acquire_excl_handle(mdbm_pool_t *pool);

/**
 * Release an exclusive MDBM handle
 *
 * \param pool pointer to a pool object that was returned from 
 *  calling mdbm_pool_create_pool function
 * \param db pointer to the exclusive MDBM handle to be released.
 *  This pointer must have been acquired from the mdbm_pool_acquire_excl_handle call.
 *
 * \return 1 if successful or 0 for failure.
 */
int mdbm_pool_release_excl_handle(mdbm_pool_t *pool, MDBM *db);

/**
 * \page example_usage "Using Core-Tech MDBM Handle Pool in your application"
 *
 * Properties wishing to use mdbm in multi-threaded applications must
 * make sure each thread uses a unique handle to carry out db operations.
 * There are two changes that are required to use this library:
 *
 * 1] Create an mdbm handle pool by duplicating your original mdbm handle
 *
 * \code
 *
 * // open mdbm file
 *
 * if ((db = mdbm_open(fn, flags, 0, 0, 0)) == NULL)
 *   return;
 *
 * // create the mdbm handle pool
 *
 * if ((pool = mdbm_pool_create_pool(db, pool_size, NULL)) == NULL) {
 *   mdbm_close(db);
 *   return;
 * }
 *
 * \endcode
 *
 * 2] Obtain a unique handle in your thread for your db operation
 *
 * \code
 *
 * // get mdbm handle from the pool
 * 
 * if ((dbh = mdbm_pool_acquire_handle(pool, NULL)) == NULL)
 *   return;
 * 
 * // lock the mdbm handle 
 * 
 * if (mdbm_lock(dbh) != 1) {
 *   mdbm_pool_release_handle(pool, dbh, NULL);
 *   return;
 * }
 * 
 * // carry out your mdbm operations ...
 * 
 * // unlock and return the handle to the pool
 *
 * mdbm_unlock(dbh);
 * mdbm_pool_release_handle(pool, dbh, NULL);
 *
 * \endcode
 *
 */

#ifdef	__cplusplus
}
#endif
#endif /* MDBM_HANDLE_POOL_H */
