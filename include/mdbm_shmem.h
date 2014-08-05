/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM_SHMEM_H_ONCE
#define MDBM_SHMEM_H_ONCE

#ifdef  __cplusplus
extern "C" {
#endif

#include <sys/types.h>

typedef struct mdbm_shmem_s {
    void* base;
    size_t size;
} mdbm_shmem_t;

#define MDBM_SHMEM_RDONLY       0x0000  /* open read-only */
#define MDBM_SHMEM_RDWR         0x0001  /* open read/write */
#define MDBM_SHMEM_CREATE       0x0002  /* create if doesn't exist */
#define MDBM_SHMEM_TRUNC        0x0004  /* trunc & init if does exist */

#define MDBM_SHMEM_PRIVATE      0x0100  /* private to this process */
#define MDBM_SHMEM_SYNC         0x0200  /* enable disk sync'ing */
#define MDBM_SHMEM_GUARD        0x0800  /* surround with guard pages */

#define MDBM_SHMEM_UNLINK       0x0010  /* delete mapped file during close */

/* mdbm_shmem_open
 *
 * If MDBM_SHMEM_CREATE or MDBM_SHMEM_TRUNC are specified, initial_size
 * must be > 0 and specifies the initial size of the mapped memory region.
 *
 * If MDBM_SHMEM_SYNC is set, the region will sync'ed to disk asychronously
 * by the syncer daemon.
 *
 * If pinit is not NULL, it points to a int which is set to 1 if
 * the caller should initialize the shmem region and then call
 * mdbm_shmem_init_complete().  It is set to 0 if other processes have
 * the region open or if the file existed and MDBM_SHMEM_TRUNC was not
 * specified.
 *
 */

mdbm_shmem_t* mdbm_shmem_open (const char* filename,
                               int flags, size_t initial_size, int* pinit);

/* mdbm_shmem_init_complete
 *
 * Should be called after completing initialization of the shmem region.
 * The region should be initialized by the calling application only if
 * mdbm_shmem_open returned with *pinit to 1.
 *
 */
int mdbm_shmem_init_complete (mdbm_shmem_t* shmem);

/* mdbm_shmem_close
 *
 * If MDBM_SHMEM_SYNC is set, the region will be explicitly sync'ed to
 * disk before closing.
 *
 */

int mdbm_shmem_close (mdbm_shmem_t* shmem, int flags);

int mdbm_shmem_fd (mdbm_shmem_t* shmem);

#ifdef  __cplusplus
}
#endif

#endif /* MDBM_SHMEM_H_ONCE */
