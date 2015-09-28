/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Modified by Yahoo! Inc.
 *
 * Modifications for:
 *      . Large object storage
 *      . Overflow pages 
 *      . Windowed mode access 
 *      . Hash-based intra-page scanning
 * By Rick Reed
 *
 * Conversion from proprietary to PThreads based locking by Tim Crowder.
 *
 * Modifications for:
 *      . Extensive documentation
 *      . Many added tools
 *      . Extensive unit/functional tests
 *      . Many bug fixes (valgrind clean!)
 *      . Performance enhancements
 * By
 *      Steve Carney
 *      Tim Crowder
 *      Max Kislik
 *      Mark Lakes
 *      Simon Baby
 *      Bhagyashri Mahule
 *      Lakshmanan Suryanarayanan
 *
 */

/*
 * Based on:
 * mdbm - ndbm work-alike hashed database library based on sdbm which is
 * based on Per-Aake Larson's Dynamic Hashing algorithms.
 * BIT 18 (1978).
 *
 * sdbm Copyright (c) 1991 by Ozan S. Yigit (oz@nexus.yorku.ca)
 *
 * Modifications that:
 *      . Allow 64 bit sized databases,
 *      . used mapped files & allow multi reader/writer access,
 *      . move directory into file, and
 *      . use network byte order for db data structures.
 *      . support fixed size db (with shake function support)
 *      . selectable hash functions
 *      . changed page layout to support data alignment
 *      . support btree pre-split and tree merge/compress
 *      . added a mdbm checker (cf fsck)
 *      . add a statistic/profiler function (for tuning)
 *      . support mdbm_firstkey(), mdbm_nextkey() call.
 * are:
 *      mdbm Copyright (c) 1995, 1996 by Larry McVoy, lm@sgi.com.
 *      mdbm Copyright (c) 1996 by John Schimmel, jes@sgi.com.
 *      mdbm Copyright (c) 1996 by Andrew Chang awc@sgi.com
 *
 * Modification that
 *      support NT/WIN98/WIN95 WIN32 environment
 *      support memory only (non-mmaped) database
 * are
 *      mdbm Copyright (c) 1998 by Andrew Chang awc@bitmover.com
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose and without fee is hereby granted,
 * provided that the above copyright notice appear in all copies and that
 * both that copyright notice and this permission notice appear in
 * supporting documentation.
 *
 * This file is provided AS IS with no warranties of any kind.  The author
 * shall have no liability with respect to the infringement of copyrights,
 * trade secrets or any patents by this file or any part thereof.  In no
 * event will the author be liable for any lost revenue or profits or
 * other special, indirect and consequential damages.
 */
#ifndef MDBM_H_ONCE
#define MDBM_H_ONCE
#ifdef  __cplusplus
extern "C" {
#endif

#define MDBM_API_VERSION 4

#include <fcntl.h>
#include <inttypes.h>
#include <sys/types.h>
#include <time.h>
#include <stdio.h>


/*
 * File format.
 *
 * The database is divided up into m_psize "pages".  The pages do not have
 * to be the same size as the system operating page size, but it is a good
 * idea to make one a multiple of the other.  The database won't create
 * pages smaller than 128 bytes.  Datums must fit on a single page so you
 * probably don't want to change this.  We default to 4K page size if no
 * blocksize is specified at create time.  The page size can't be bigger
 * than 64K because we use 16 bit offsets in the pages.
 *
 * Page types.  The first page is special.  It contains the database header,
 * as well as the first set of directory bits.  The directory bits occupy
 * 8 bytes.The point of putting the directory in
 * the first page was to provide for a very small memory footprint for
 * small databases; this allows this code to be used as a generic hashing
 * package.
 *
 * After the database reaches a size where the directory will no longer fit
 * in the 8 bytes of the first page, the data & directory are grown by
 * doubling its previous size.
 * The directory is copied to the end of data area.  The default page
 * get you to to a terabyte (2^40) database before copying
 * becomes a problem.
 *
 * Page format:
 *      +--------+-----+----+----------+
 *      |  key   | data     | key      |
 *      +--------+----------+----------+
 *      |  <---- - - - | data          |
 *      +--------+-----+----+----------+
 *      |        F R E E A R E A       |
 *      +--------------+---------------+
 * ino  | n | keyoff | datoff | keyoff |
 *      +------------+--------+--------+
 *
 * We hide our own data in a page by making keyoff == 0.  We do not allow
 * 0 length keys as a result.
 * Suppose we have a 1024 byte page, the header is 14 bytes
 * (i.e. sizeof(MDBM_hdr)),
 * the directory is 8 bytes. (i.e. total is 14 + 8 = 22 bytes )
 *
 * Page 0 looks like
 *      +--------+-----+----+----------+ first entry in page zero
 *      | 14 bytes header (never moves)| has a zero length key.
 *      |  and 8 bytes for directory   | data area has a mdbm header
 *      +--------+-----+----+----------+ and a directory bit array.
 *      |        F R E E A R E A       |
 *      +--------+-----+----+----------+
 * ino  | 2 | 0 | 22 |                 |
 *      +------------+--------+--------+
 *           ^^^
 *          this signifies a zero length key
 */


/*
 * If your enviromnet supports 64 bit int
 * define the following
 */
/* #define      SUPPORT_64BIT  */

#if 1
#define mdbm_big_t      int32_t
#define mdbm_ubig_t     uint32_t
#else
#ifdef  SUPPORT_64BIT
#define big int64_t
#define ubig uint64_t
#else
#define big int32_t
#define ubig uint32_t
#endif
#endif

/*#define mdbm_ubig_t     uint32_t     */
/*#define mdbm_ubig_t     unsigned int */

typedef struct {
    char *dptr;                 /**< Pointer to key or value data */
    int  dsize;                 /**< Number of bytes */
} datum;

typedef struct {
    datum key;                  /**< Key to access a database record */
    datum val;                  /**< Value associated with the above key */
} kvpair;

#define MDBM_KEYLEN_MAX         (1<<15)  /**< Maximum key size */
#define MDBM_VALLEN_MAX         (1<<24)  /**< Maximum key size */

struct mdbm_iter {
    mdbm_ubig_t m_pageno;       /**< Last page fetched */
    int         m_next;         /**< Index for getnext, or -1 */
};

typedef struct mdbm_iter MDBM_ITER;

/**
 * Initializes an MDBM iterator.
 * \param[in] i L-Value reference
 * Usage: MDBM_ITER_INIT (MDBM_ITER *i);
 */
#define MDBM_ITER_INIT(i)       { (i)->m_pageno = 0; (i)->m_next = -1; }
/** MDBM iterator static initialization */
#define MDBM_ITER_INITIALIZER   { 0, -1 }

#define MDBM_LOC_NORMAL 0
#define MDBM_LOC_ARENA  1


typedef struct mdbm MDBM;

/**
 * \defgroup FileManagementGroup File Management Group
 * The File Management Group contains the API the affects an MDBM at a file level.
 * \{
 */

/*
 * mdbm_open flags
 */

#define MDBM_O_RDONLY           0x00000000  /**< Read-only access */
#define MDBM_O_WRONLY           0x00000001  /**< Write-only access (deprecated in V3) */
#define MDBM_O_RDWR             0x00000002  /**< Read and write access */
#define MDBM_O_ACCMODE          (MDBM_O_RDONLY | MDBM_O_WRONLY | MDBM_O_RDWR)

#ifdef __linux__
#define MDBM_O_CREAT            0x00000040  /**< Create file if it does not exist */
#define MDBM_O_TRUNC            0x00000200  /**< Truncate file */
#define MDBM_O_FSYNC            0x00001000  /**< Sync file on close */
#define MDBM_O_ASYNC            0x00002000  /**< Perform asynchronous writes */
#else
#define MDBM_O_ASYNC            0x00000040  /**< Perform asynchronous writes */
#define MDBM_O_FSYNC            0x00000080  /**< Sync file on close */
#define MDBM_O_CREAT            0x00000200  /**< Create file if it does not exist */
#define MDBM_O_TRUNC            0x00000400  /**< Truncate file */
#endif
#define MDBM_O_DIRECT           0x00004000  /**< Perform direction I/O */

#define MDBM_NO_DIRTY           0x00010000  /**< Do not not track clean/dirty status */
#define MDBM_SINGLE_ARCH        0x00080000  /**< User *promises* not to mix 32/64-bit access */
#define MDBM_OPEN_WINDOWED      0x00100000  /**< Use windowing to access db */
#define MDBM_PROTECT            0x00200000  /**< Protect database except when locked */
#define MDBM_DBSIZE_MB          0x00400000  /**< Dbsize is specific in MB */
#define MDBM_STAT_OPERATIONS    0x00800000  /**< collect stats for fetch, store, delete */
#define MDBM_LARGE_OBJECTS      0x01000000  /**< Support large objects - obsolete */
#define MDBM_PARTITIONED_LOCKS  0x02000000  /**< Partitioned locks */
#define MDBM_RW_LOCKS           0x08000000  /**< Read-write locks */
#define MDBM_ANY_LOCKS          0x00020000  /**< Open, even if existing locks don't match flags */
/* #define MDBM_CREATE_V2          0x10000000  / * create a V2 db */
#define MDBM_CREATE_V3          0x20000000  /**< Create a V3 db */
/* Flag is reserved             0x40000000   */
#define MDBM_OPEN_NOLOCK        0x80000000  /**< Don't lock during open */

#define MDBM_DEMAND_PAGING      0x04000000  /**< (v2 only) */
#define MDBM_DBSIZE_MB_OLD      0x04000000  /**< (don't use -- conflicts with above) */


/* sanity checks */

#if MDBM_O_RDONLY != O_RDONLY
#error must have MDBM_O_RDONLY == O_RDONLY
#endif
#if MDBM_O_WRONLY != O_WRONLY
#error must have MDBM_O_WRONLY == O_WRONLY
#endif
#if MDBM_O_RDWR != O_RDWR
#error must have MDBM_O_RDWR == O_RDWR
#endif
#if MDBM_O_ACCMODE != O_ACCMODE
#error must have MDBM_O_ACCMODE == O_ACCMODE
#endif
#if MDBM_O_CREAT != O_CREAT
#error must have MDBM_O_CREAT == O_CREAT
#endif
#if MDBM_O_TRUNC != O_TRUNC
#error must have MDBM_O_TRUNC == O_TRUNC
#endif
#if MDBM_O_ASYNC != O_ASYNC
#error must have MDBM_O_ASYNC == O_ASYNC
#endif
#if MDBM_O_FSYNC != O_FSYNC
#if __GNUC__ < 4
/* NOTE: on RHEL6 these are not equal, this is a known problem. */
#error must have MDBM_O_FSYNC == O_FSYNC
#endif
#endif

/**
 * Creates and/or opens a database
 *
 * \param[in] file Name of the backing file for the database.
 * \param[in] flags Specifies the open-mode for the file, usually either
 *            (MDBM_O_RDWR|MDBM_O_CREAT) or (MDBM_O_RDONLY).  Flag
 *            MDBM_LARGE_OBJECTS may be used to enable large object support.
 *            Large object support can only be enabled when the database is first
 *            created.  Subsequent mdbm_open calls will ignore the flag.
 *            Flag MDBM_PARTITIONED_LOCKS may be used to enable
 *            partition locking a per mdbm_open basis.
 * \param[in] mode Used to set the file permissions if the file needs to
 *            be created.
 * \param[in] psize Specifies the page size for the database and is set
 *            when the database is created.
 *            The minimum page size is 128.
 *            In v2, the maximum is 64K.
 *            In v3, the maximum is 16M - 64.
 *            The default, if 0 is specified, is 4096.
 * \param[in] presize Specifies the initial size for the database.  The
 *            database will dynamically grow as records are added, but specifying
 *            an initial size may improve efficiency.  If this is not a multiple
 *            of \a psize, it will be increased to the next \a psize multiple.
 * \return MDBM handle or failure indicator
 * \retval Pointer to an MDBM handle, on success
 * \retval NULL on error, and errno is set
 *
 * Values for \a flags mask:
 *   - MDBM_O_RDONLY          - open for reading only
 *   - MDBM_O_RDWR            - open for reading and writing
 *   - MDBM_O_WRONLY          - same as RDWR (deprecated)
 *   - MDBM_O_CREAT           - create file if it does not exist (requires flag MDBM_O_RDWR)
 *   - MDBM_O_TRUNC           - truncate size to 0
 *   - MDBM_O_ASYNC           - enable asynchronous sync'ing by the kernel syncing process.
 *   - MDBM_O_FSYNC           - sync MDBM upon \ref mdbm_close
 *   - MDBM_O_DIRECT          - use O_DIRECT when accessing backing-store files
 *   - MDBM_NO_DIRTY          - do not not track clean/dirty status
 *   - MDBM_OPEN_WINDOWED     - use windowing to access db, only available with MDBM_O_RDWR
 *   - MDBM_PROTECT           - protect database except when locked (MDBM V3 only)
 *   - MDBM_DBSIZE_MB         - dbsize is specific in MB (MDBM V3 only)
 *   - MDBM_LARGE_OBJECTS     - support large objects
 *   - MDBM_PARTITIONED_LOCKS - partitioned locks
 *   - MDBM_RW_LOCKS          - read-write locks
 *   - MDBM_CREATE_V2         - create a V2 db (obsolete)
 *   - MDBM_CREATE_V3         - create a V3 db
 *   - MDBM_HEADER_ONLY       - map header only (internal use)
 *   - MDBM_OPEN_NOLOCK       - do not lock during open
 *   - MDBM_ANY_LOCKS         - (V4 only) treat the locking flags as only a suggestion
 *   - MDBM_SINGLE_ARCH       - (V4 only) user guarantees no mixed (32/64-bit) access
 *                                 in exchange for faster (pthreads) locking
 *
 * More information about the differences between V2 and V3 mdbm_open behavior:
 *   - In V2, when the \a psize (page size) parameter is invalid, the page size
 *     is silently converted to a valid value: if non-zero but less than 128, it
 *     becomes 128.  Page size is always rounded to the next power of 2.  If
 *     greater than 64K, it becomes 64K.
 *   - In V3, when the page size is invalid(less than 128, or greater than 16MB)
 *     mdbm_open detects an error and returns NULL.  Page size is always a
 *     multiple of 64 bytes.
 *
 */
extern MDBM* mdbm_open(const char *file, int flags, int mode, int psize, int presize);

/**
 * Closes the database.
 *
 * \param[in,out] db Database handle
 *
 * mdbm_close closes the database specified by the \a db argument.  The
 * in-memory pages are not flushed to disk by close.  They will be written to
 * disk over time as they are paged out, but an explicit \ref mdbm_sync call is
 * necessary before closing if on-disk consistency is required.
 */
extern  void    mdbm_close(MDBM *db);

/**
 * Closes an MDBM's underlying file descriptor.
 *
 * \param[in,out] db Database handle
  */
extern  void    mdbm_close_fd(MDBM *db);

/**
 * Duplicate an existing database handle.  The advantage of dup'ing a handle
 * over doing a separate \ref mdbm_open is that dup's handle share the same virtual
 * page mapping within the process space (saving memory).
 * Threaded applications should use pthread_mutex_lock and unlock around calls to mdbm_dup_handle.
 *
 * \param[in,out] db Database handle
 * \param[in]     flags Reserved for future use
 * \return Dup'd handle
 * \retval Pointer to an MDBM handle, on success
 * \retval NULL on error, and errno is set
 */
extern MDBM* mdbm_dup_handle(MDBM* db, int flags);

/**
 * msync's all pages to disk asynchronously.  The mdbm_sync call will return, and
 * mapped pages are scheduled to be flushed to disk.
 *
 * \param[in,out] db Database handle
 * \return Sync status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_sync(MDBM *db);

/**
 * fsync's an MDBM.  Syncs all pages to disk synchronously.  The mdbm_fsync call
 * will return after all pages have been flushed to disk.  The database is locked
 * while pages are flushed.
 *
 * \param[in,out] db Database handle
 * \return fsync status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_fsync(MDBM *db);

/**
 * Atomically replaces the database currently in oldfile \a db with the new
 * database in \a newfile.  The old database is locked while the new database
 * is renamed from newfile to oldfile and the old database is marked as having
 * been replaced.  This causes all processes that have the old database open
 * to reopen the new database on their next access.  Only database files of
 * the same version may be specified for oldfile and new file. For example,
 * mix and matching of v2 and v3 with oldfile and newfile is not allowed.
 *
 * This function will delete the old file; and rename the new file.
 *
 * \param[in,out] db Database handle
 * \param[in]     newfile Path of new file
 * \return Replace status
 * \retval -1 Error and errno is set.  The MDBM was not replaced.
 * \retval  0 Success and the MDBM was replaced
 */
extern int mdbm_replace_db(MDBM* db, const char* newfile);

/**
 * Atomically replaces an old database in \a oldfile with a new database in
 * \a newfile.  \a oldfile is deleted, and \a newfile is renamed to \a oldfile.
 *
 * The old database is locked (if the MDBM were opened with locking) while the
 * new database is renamed from \a newfile to \a oldfile, and the old database
 * is marked as having been replaced.  The marked old database causes all
 * processes that have the old database open to reopen using the new database on
 * their next access.
 *
 * Only database files of the same version may be specified for \a oldfile and
 * \a newfile. For example, mixing and matching of v2 and v3 for \a oldfile and
 * \a newfile is not allowed.
 *
 * mdbm_replace_file may be used if the MDBM is opened with locking or without
 * locking (using mdbm_open flag MDBM_OPEN_NOLOCK), and without per-access
 * locking, if all accesses are read (fetches) accesses across all programs that
 * open that MDBM.  If there are any write (store/delete) accesses, you must
 * open the MDBM with locking, and you must lock around all operations (fetch,
 * store, delete, iterate).
 *
 * \param[in] oldfile File to be replaced
 * \param[in] newfile Path of new file
 * \return Replace status
 * \retval -1 Error, and errno is set.  The MDBM was not replaced
 * \retval  0 Success, and the MDBM was replaced
 */
extern int mdbm_replace_file(const char* oldfile, const char* newfile);

/**
 * Atomically replaces the backing store database behind \a cache with the new
 * database in \a newfile.  Functions the same as mdbm_replace_db() for the
 * backing store, but in addition, it locks the cache for the entire operation,
 * and purges the cache of old entries if the replace succeeds.
 *
 * This function will delete the old file; and rename the new file.
 *
 * \param[in,out] db Database handle (cache with an MDBM backing-store)
 * \param[in]     newfile Path of new file
 * \return Replace status
 * \retval -1 Error and errno is set.  The MDBM was not replaced.
 * \retval  0 Success and the MDBM was replaced
 */
extern int mdbm_replace_backing_store(MDBM* cache, const char* newfile);

/**
 * Forces a db to split, creating N pages.  Must be called before any data is
 * inserted.  If N is not a multiple of 2, it will be rounded up.
 *
 * \param[in,out] db Database handle
 * \param[in]     N Target number of pages post split.
 *                If \a N is not larger than the initial size (ex., 0),
 *                a split will not be done and a success status is returned.
 * \return Split status
 * \retval -1 Error, and errno is set.  errno=EFBIG if MDBM not empty.
 * \retval  0 Success
 */
extern int mdbm_pre_split(MDBM *db, mdbm_ubig_t N);

#define MDBM_COPY_LOCK_ALL      0x01

/**
 * Copies the contents of a database to an open file handle.
 *
 * \param[in,out] db Database handle
 * \param[in]     fd Open file descriptor
 * \param[in]     flags
 * \return fcopy status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 *
 * Values for \a flags mask:
 *   - MDBM_COPY_LOCK_ALL - Whether lock for the duration of the copy.
 *     For a consistent snapshot of the entire database, this flag must be
 *     used.  Otherwise, consistency will only be on a per-page level.
 */
extern int mdbm_fcopy(MDBM* db, int fd, int flags);


#define MDBM_SAVE_COMPRESS_TREE 0x01000000      /* compress mdbm before saving */

/**
 * DEPRECATED!!  mdbm_save is only supported for V2 MDBMs.
 * Saves the current contents of the database in a transportable format (i.e.,
 * without all of the holes normally found in an MDBM database).  Include
 * MDBM_O_CREAT in flags to create a new save file, MDBM_O_TRUNC to truncate
 * an existing file.  Include both to either create a new file or truncate an
 * existing one, as required.
 *
 * \param[in,out] db Database handle
 * \param[in]     file File to save to
 * \param[in]     flags Mask indicating how file should be created Supported flags
 *                are: MDBM_O_CREAT, MDBM_O_TRUNC, MDBM_SAVE_COMPRESS_TREE(used to compress
 *                a database internally before saving to file)
 * \param[in]     mode Set as file mode permissions (ex: 0644); If 0 then ignored
 * \param[in]     compressionLevel If <= 0, then file is saved without compressing.
 *                If > zero, zlib compression at the specified level is applied
 * \return Save status
 * \retval  0 Success
 * \retval -1 Error, and errno is set
 */
extern int mdbm_save(MDBM *db, const char *file, int flags, int mode, int compressionLevel);

/**
 * DEPRECATED!!  mdbm_restore is only supported for V2 MDBMs.
 * Restores a database from the specified file (which was created using the
 * \ref mdbm_save function).  Any existing contents in the database are truncated
 * before restoring.
 *
 * \param[in,out] db Database handle
 * \param[in]     file File to restore from
 * \return Restore status
 * \retval  0 Success
 * \retval -1 Error, and errno is set
 */
extern int mdbm_restore(MDBM *db, const char *file);

/** \} FileManagementGroup */



/**
 * \defgroup ConfigurationGroup Configuration Group
 * The Configuration Group contains the API the gets/sets the underlying
 * configuration of an MDBM.  Most of the commands that set the configuration
 * must be done at MDBM-creation time.
 * \{
 */

/**
 * Prints to stdout various pieces of information, specifically: page size,
 * page count, hash function, running with or without locking
 *
 * NOTE: There is only a V2 implementation. V3 not currently supported.
 *
 * \param[in,out] db Database handle
 */
extern void mdbm_stat_header(MDBM *db);

/**
 * Gets the MDBM's hash function identifier.
 *
 * \param[in,out] db Database handle
 * \return Hash function identifier
 * \retval MDBM_HASH_CRC32   - Table based 32bit CRC
 * \retval MDBM_HASH_EJB     - From hsearch
 * \retval MDBM_HASH_PHONG   - Congruential hash
 * \retval MDBM_HASH_OZ      - From sdbm
 * \retval MDBM_HASH_TOREK   - From BerkeleyDB
 * \retval MDBM_HASH_FNV     - Fowler/Vo/Noll hash (DEFAULT)
 * \retval MDBM_HASH_STL     - STL string hash
 * \retval MDBM_HASH_MD5     - MD5
 * \retval MDBM_HASH_SHA_1   - SHA_1
 * \retval MDBM_HASH_JENKINS - Jenkins string
 * \retval MDBM_HASH_HSIEH   - Hsieh SuperFast
 */
extern int mdbm_get_hash(MDBM *db);

/**
 * Sets the hashing function for a given MDBM.  The hash function must be set
 * before storing anything to the db (this is not enforced, but entries stored
 * before the hash change will become inaccessible if the hash function is
 * changed).
 *
 * NOTE: setting the hash must be be done at creation time, or when there is
 * no data in an MDBM.  Changing the hash function when there is existing data
 * will result in not being able access that data in the future.
 *
 * \param[in,out] db Database handle
 * \param[in]     hashid Numeric identifier for new hash function.
 * \return Set hash status
 * \retval -1 Error, invalid \a hashid
 * \retval 0 Success
 *
 * Available Hash IDs are:
 *  - MDBM_HASH_CRC32   - Table based 32bit CRC
 *  - MDBM_HASH_EJB     - From hsearch
 *  - MDBM_HASH_PHONG   - Congruential hash
 *  - MDBM_HASH_OZ      - From sdbm
 *  - MDBM_HASH_TOREK   - From BerkeleyDB
 *  - MDBM_HASH_FNV     - Fowler/Vo/Noll hash (DEFAULT)
 *  - MDBM_HASH_STL     - STL string hash
 *  - MDBM_HASH_MD5     - MD5
 *  - MDBM_HASH_SHA_1   - SHA_1
 *  - MDBM_HASH_JENKINS - Jenkins string
 *  - MDBM_HASH_HSIEH   - Hsieh SuperFast
 */
extern int mdbm_set_hash(MDBM *db, int hashid);

/**
 * DEPRECATED.
 * Legacy version of mdbm_set_hash().
 * This function has inconsistent naming, and error return value.
 * It will be removed in a future version.
 *
 * \param[in,out] db Database handle
 * \param[in]     hashid Numeric identifier for new hash function.
 * \return Set hash status
 * \retval -1 Error, invalid \a hashid
 * \retval 1 Success
 */
extern int mdbm_sethash(MDBM *db, int hashid);


/**
 * Sets the size of item data value which will be put on the large-object heap
 * rather than inline.  The spill size can be changed at any point after the
 * db has been created.  However, it's a recommended practice to set the spill
 * size at creation time.
 *
 * NOTE: The database has to be opened with the MDBM_LARGE_OBJECTS flag for
 * spillsize to take effect.
 *
 * \param[in,out] db Database handle
 * \param[in]     size New large-object threshold size
 * \return Set spill size status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_setspillsize(MDBM* db, int size);


/*
 * mdbm_set_alignment flags
 */
#define MDBM_ALIGN_8_BITS       0x0 /**< 1-Byte data alignment */
#define MDBM_ALIGN_16_BITS      0x1 /**< 2-Byte data alignment */
#define MDBM_ALIGN_32_BITS      0x3 /**< 4-Byte data alignment */
#define MDBM_ALIGN_64_BITS      0x7 /**< 8-Byte data alignment */

/**
 * Gets the MDBM's record byte-alignment.
 *
 * \param[in,out] db Database handle
 * \return Alignment mask.
 * \retval 0 - 8-bit alignment
 * \retval 1 - 16-bit alignment
 * \retval 3 - 32-bit alignment
 * \retval 7 - 64-bit alignment
 */
extern int mdbm_get_alignment(MDBM *db);

/**
 * Sets a database's byte-size alignment for keys and values within a page.
 * This feature is useful for hardware/memory architectures that incur a
 * performance penalty for unaligned accesses.  Later (2006+) i386 and x86
 * architectures do not need special byte alignment, and should use the
 * default of 8-bit alignment.
 *
 * NOTE: setting the byte alignment must be be done at MDBM-creation time, or
 * when there is no data in an MDBM.  Changing the byte alignment when there
 * is existing data will result in in undefined behavior and probably a crash.
 *
 * \param[in,out] db Database handle
 * \param[in] align Alignment mask
 * \return Alignment status
 * \retval -1 Error, and errno is set
 * \retval 0 Success
 */
extern int mdbm_set_alignment(MDBM *db, int align);

/**
 * Gets the MDBM's size limit.  Returns the limit set for the size of the db
 * using the \ref mdbm_limit_size_v3 routine.
 *
 * \param[in,out] db Database handle
 * \return database size limit
 * \retval 0 No limit is set
 * \retval Total number of bytes for maximum database size, including header and directory
 */
extern uint64_t mdbm_get_limit_size(MDBM *db);

/*
 * Shake (page overflow) support
 */

/* NOT USED IN V4 */
/* struct mdbm_shake_data {        */
/*     uint32_t page_num;          */ /* index number of overflowing page */
/*     const char* page_begin;     */ /* beginning address of page */
/*     const char* page_end;       */ /* one byte past last byte of page */
/*     uint16_t page_free_space;   */ /* current free space on page */
/*     uint16_t space_needed;      */ /* space needed for current insert */
/*     uint16_t page_num_items;    */ /* number of items on page */
/*     uint16_t reserved;          */
/*     kvpair* page_items;         */ /* key/val pairs for all items on page */
/*     uint16_t* page_item_sizes;  */ /* total in-page size for each item on page */
/*     void* user_data;            */ /* user-data pointer passed to mdbm_limit_size_new */
/* };                              */
/*typedef int (*mdbm_shake_func)(MDBM *, const datum*, const datum*, struct mdbm_shake_data *);
  extern  int     mdbm_limit_size(MDBM *, mdbm_ubig_t,
                                  int (*func)(struct mdbm *, datum, datum, void *));
  extern  int     mdbm_limit_size_new(MDBM *, mdbm_ubig_t, mdbm_shake_func shake, void *);
*/

struct mdbm_shake_data_v3 {
    uint32_t    page_num;       /**< Index number of overflowing page */
    const char* page_begin;     /**< Beginning address of page */
    const char* page_end;       /**< One byte past last byte of page */
    uint32_t    page_free_space; /**< Current free space on page */
    uint32_t    space_needed;   /**< Space needed for current insert */
    uint32_t    page_num_items; /**< Number of items on page */
    uint32_t    reserved;       /**< Reserved */
    kvpair*     page_items;     /**< Key-Value pairs for all items on page */
    uint32_t*   page_item_sizes; /**< Total in-page size for each item on page */
    void* user_data;            /**< User-data pointer passed to \ref mdbm_limit_size_v3 */
};

typedef int (*mdbm_shake_func_v3)(MDBM *, const datum*, const datum*,
                                  struct mdbm_shake_data_v3 *);
/**
 * Limits the size of a V3 database to a maximum number of pages.  This
 * function causes the MDBM to shake whenever a full page would cause the db
 * to grow beyond the specified max number of pages.  The limit size for an
 * MDBM may be increased over time, but is may never be decreased.
 *
 * \param[in,out] db Database handle
 * \param[in]     max_page Maximum number of data pages
 * \param[in]     shake Shake function.
 * \param[in]     user Pointer to user data that will be passed to the \a shake function
 * \return Limit size status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_limit_size_v3(MDBM *db, mdbm_ubig_t max_page, mdbm_shake_func_v3 shake,void *user);

/**
 * Limits the internal page directory size to a number of \a pages.  The number
 * of pages is rounded up to a power of 2.
 *
 * \param[in,out] db Database handle
 * \param[in]     pages Number of data pages
 * \return Limit dir size status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_limit_dir_size(MDBM *db, int pages);

/**
 * Gets the on-disk format version number of an MDBM.
 *
 * \param[in,out] db Database handle
 * \return On-disk file format version number
 * \retval 2 - MDBM V2
 * \retval 3 - MDBM V3
 */
extern uint32_t mdbm_get_version (MDBM* db);

/**
 * Gets the current MDBM's size.
 *
 * \param[in,out] db Database handle
 * \return Size of database in bytes.
 */
extern uint64_t mdbm_get_size(MDBM *db);

/**
 * Get the MDBM's page size.
 *
 * \param[in,out] db Database handle
 * \return Number of bytes in a database page
 */
extern int mdbm_get_page_size(MDBM *db);

/*
 * Magic number which is also a version number.   If the database format
 * changes then we need another magic number.
 *
 * FIX: The magic number should be invariant across MDBM format versions
 * because the number in /usr/share/file/magic should identify a general class
 * of files.  The version number should immediately follow the magic number.
 * The magic number handling will have to change to reflect this fix.
 * --carney
 */
#define _MDBM_MAGIC             0x01023962 /**< V2 file identifier */
#define _MDBM_MAGIC_NEW         0x01023963 /**< V2 file identifier with large objects */
#define _MDBM_MAGIC_NEW2        0x01023964 /**< V3 file identifier */

#define MDBM_MAGIC              _MDBM_MAGIC_NEW2

/**
 * Gets the magic number from an MDBM.
 *
 * \param[in,out] db Database handle
 * \param[out]    magic MDBM magic number (internal version identifier)
 * \return Magic number status
 * \retval -3 Cannot read all of the magic number
 * \retval -2 File is truncated (empty)
 * \retval -1 Cannot read file
 * \retval  0 Magic number returned in \a magic
 */
extern int mdbm_get_magic_number(MDBM *db, uint32_t *magic);


/**
 * Sets the window size for an MDBM.  Windowing is typically used for a very
 * large data store where only part of it will be mapped to memory. In
 * windowing mode, pages are accessed through a "window" to the database.
 *
 * In order to use windowing mode, the page size must be a multiple of the
 * system page size (4K on most systems).  The system page size can be
 * retrieved by calling getpagesize or sysconf(_SC_PAGESIZE).
 *
 * NOTE: It is best if the minimum window size chosen is:
 * (4*max-large-object-size + n*8*pagesize)
 *
 * NOTE: API exists in MDBM V3 only, and using mdbm_set_window_size requires
 * opening MDBM with MDBM_O_RDWR.
 *
 * \param[in,out] db Database handle for windowed access, typically a backing store database
 * \param[in]     wsize Window size which must be at least 2X the page size
 *                used for the database.
 * \return Set window status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_set_window_size(MDBM* db, size_t wsize);

/** \} ConfigurationGroup */


/**
 * \defgroup RecordAccessGroup Record Access Group
 * The Record Access Group contains the API for fetching, storing, and
 * deleting records.
 * \{
 */

/**
 * Fetches the record specified by the \a key argument and returns a \a datum
 * that \b points to the value.  If such a record exists in the database, the
 * size and location (pointer) are stored in the returned \a datum.  If no
 * matching record exists, a null datum (dsize==0, dptr==NULL) is returned.
 *
 * The contents returned in \a datum has a <strong>pointer to a value</strong>
 * in mapped memory.  If there is any system-wide process that could modify your
 * MDBM, this value must be accessed in a locked context.
 *
 * For example, the following is correct code to copy a lookup value for subsequent return:
 * \code
 * char* getFoo(int maxLength)
 * {
 *   datum key = { "foo", 3 };
 *   datum value;
 *   char *buffer = (char*)malloc(maxLength+1);  // Skip return code check.
 *   mdbm_lock(db);   // Skip return code check.
 *   value = mdbm_fetch(db, key);
 *   if (value.dptr != NULL) {
 *     strncpy(buffer, value.dptr, maxLength);  // GOOD: copy-out done in locked context.
 *   } else {
 *     buffer[0] = '\0';
 *   };
 *   mdbm_unlock(db); // Skip return code check.
 *   return buffer;
 * }
 * \endcode
 *
 * \param[in,out] db Database handle
 * \param[in]     key Lookup key
 * \return datum is returned with found, or not found, information
 * \retval datum found, with size and pointer set, on success
 * \retval datum not found, with dsize==0, and dptr==NULL,
 *         and errno==ENOENT.  errno may have other values for failure (ex., EINVAL).
 */
extern datum mdbm_fetch(MDBM *db, datum key);

/**
 * Fetches the record specified by the \a key argument.  If such a record
 * exists in the database, the size and location (pointer) are stored in the datum
 * pointed to by \a val.  If no matching record exists, a null datum (dsize==0,
 * dptr==NULL) is returned.
 *
 * The contents returned in \a val is a <strong>pointer to a value</strong>
 * in mapped memory.  If there is any system-wide process that could modify your
 * MDBM, this value must be accessed in a locked context.
 *
 * For example, the following is correct code to copy a lookup value for subsequent return:
 * \code
 * char* getFoo(int maxLength)
 * {
 *   int rc;
 *   datum key = { "foo", 3 };
 *   datum value;
 *   char *buffer = (char*)malloc(maxLength+1);  // Skip return code check.
 *   mdbm_lock(db);    // Skip return code check.
 *   rc = mdbm_fetch_r(db, &key, &value, NULL);
 *   if (!rc) {
 *     strncpy(buffer, value.dptr, maxLength);  // GOOD: copy-out done in locked context.
 *   } else {
 *     buffer[0] = '\0';
 *   };
 *   mdbm_unlock(db);  // Skip return code check.
 *   return buffer;
 * }
 * \endcode
 *
 *
 * A record can be updated in-place by fetching the value datum, casting the
 * dptr as appropriate, and updating those contents.  However, the update must
 * not change the size of the record.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Pointer to \ref datum used as the index key
 * \param[out]    val Pointer to \ref datum returned
 * \param[in,out] iter MDBM iterator
 * \return Fetch status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_fetch_r(MDBM *db, datum *key, datum *val, MDBM_ITER *iter);

/**
 * Fetches and copies the record specified by the \a key argument.  If such a
 * record exists in the database, the size and location are stored in the
 * datum pointed to by val.  The same record is also copied into the \a buf
 * argument.  If no matching record exists, a null datum (dsize==0,
 * dptr==NULL) is returned.
 *
 * Note that a record can be updated in-place by fetching the value datum,
 * casting the dptr as appropriate, and updating the record.  However, the
 * update must not change the size of the record.
 *
 * buf.dptr must point to memory that has been previously malloc'd on the
 * heap.  buff.dptr will be realloc'd if is too small.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Lookup key
 * \param[out]    val Lookup value (pointer)
 * \param[in,out] buf Copy-out of lookup reference returned in \a val
 * \param[in]     flags Reserved for future use
 * \return Fetch status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_fetch_buf(MDBM *db, datum *key, datum *val, datum *buf, int flags);

/**
 * Fetches the next value for a key inserted via \ref mdbm_store_r with the
 * MDBM_INSERT_DUP flag set.  The order of values returned by iterating via
 * this function is not guaranteed to be the same order as the values were
 * inserted.
 *
 * As with any db iteration, record insertion and deletion during iteration
 * may cause the iteration to skip and/or repeat records.
 *
 * Calling this function with an iterator initialized via \ref MDBM_ITER_INIT
 * will cause this function to return the first value for the given key.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Pointer to \ref datum used as the index key
 * \param[out]    val Pointer to \ref datum returned
 * \param[in,out] iter MDBM iterator
 * \return Fetch duplicate status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_fetch_dup_r(MDBM *db, datum *key, datum *val, MDBM_ITER *iter);

/**
 * Fetches a string.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Stored key
 * \return Fetched string reference, or NULL
 * \retval Pointer to string, on success
 * \retval NULL if key does not exist
 */
extern char* mdbm_fetch_str(MDBM *db, const char *key);

 /*
 * MDBM fetch interface
 */
#define MDBM_FETCH_FLAG_DIRTY   0x01    /**< Cache entry is dirty */

struct mdbm_fetch_info {
    uint32_t    flags;                  /**< Entry flags */
    uint32_t    cache_num_accesses;     /**< Number of accesses to cache entry */
    uint32_t    cache_access_time;      /**< Last access time (LRU/LFU only) */
};

/**
 * Fetches and copies the record specified by the \a key argument.  If such a
 * record exists in the database, the size and location are stored in the
 * datum pointed to by val.  The same record is also copied into the \a buf
 * argument.  If no matching record exists, a null datum (dsize==0,
 * dptr==NULL) is returned.
 *
 * \ref mdbm_fetch_info is only supported by MDBM version 3 or higher.
 *
 * Note that a record can be updated in-place by fetching the value datum,
 * casting the dptr as appropriate, and updating the record.  However, the
 * update must not change the size of the record.
 *
 * Additional information is passed back in the \a info argument: entry flags,
 * the number of accesses to cache entry, and the last access time (LRU/LFU
 * only).
 *
 * buf.dptr cannot point to statically allocated memory.  It must point to
 * memory that has been previously malloc'd on the heap.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Pointer to \ref datum used as the index key
 * \param[out]    val Pointer to \ref datum returned
 * \param[out]    buf Copy found \ref datum into this parameter
 * \param[out]    info Pointer to additional information (struct mdbm_fetch_info)
 * \param[in,out] iter MDBM iterator
 * \return Fetch info status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_fetch_info(MDBM *db, datum *key, datum *val, datum *buf,
                           struct mdbm_fetch_info *info, MDBM_ITER *iter);

/**
 * Deletes a specific record.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Key to be removed
 * \return Delete status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_delete(MDBM *db, datum key);

/**
 * Deletes the record currently addressed by the \a iter argument.  After
 * deletion, the key and/or value returned by the iterating function is no
 * longer valid.  Calling \ref mdbm_next_r on the iterator will return the
 * key/value for the entry following the entry that was deleted.
 *
 * \param[in,out] db Database handle
 * \param[in,out] iter MDBM iterator pointing to item to be deleted
 * \return Delete status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_delete_r(MDBM *db, MDBM_ITER* iter);

/**
 * Deletes a string from the MDBM.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Stored key
 * \return Delete string status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_delete_str(MDBM *db, const char *key);


/*
 * mdbm_store flags
 */
#define MDBM_INSERT     0       /**< Insert if key does not exist; fail if exists */
#define MDBM_REPLACE    1       /**< Update if key exists; insert if does not exist */
#define MDBM_INSERT_DUP 2       /**< Insert new record (creates duplicate if key exists) */
#define MDBM_MODIFY     3       /**< Update if key exists; fail if does not exist */
#define MDBM_STORE_MASK 0x3     /**< Mask for all store options */

/** Extract the store mode from an options mask */
#define MDBM_STORE_MODE(f)      ((f) & MDBM_STORE_MASK)

#define MDBM_RESERVE    0x100   /**< Reserve space; Value not copied */
#define MDBM_CLEAN      0x200   /**< Mark entry as clean */
#define MDBM_CACHE_ONLY 0x400   /**< Do not operate on the backing store; use cache only */

#define MDBM_CACHE_REPLACE  0       /**< Update cache if key exists; insert if does not exist */
#define MDBM_CACHE_MODIFY   0x1000  /**< Update cache if key exists; do not insert if does not */
#define MDBM_CACHE_UPDATE_MODE(f)   ((f) & MDBM_CACHE_MODIFY)

#define MDBM_STORE_SUCCESS      0 /**< Returned if store succeeds */
#define MDBM_STORE_ENTRY_EXISTS 1 /**< Returned if MDBM_INSERT used and key exists */

/**
 * This is a wrapper around \ref mdbm_store_r, with a static iterator.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Stored key
 * \param[in]     val \a Key's value
 * \param[in]     flags Store flags
 * \return Store status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 * \retval  1 Flag MDBM_INSERT was specified, and the key already exists
 *
 * Values for \a flags mask:
 *   - MDBM_INSERT - Operation will fail if a record with the same key
 *     already exists in the database.
 *   - MDBM_REPLACE - A record with the same key will be replaced.
 *     If the key does not exist, the record will be created.
 *   - MDBM_INSERT_DUP - allows multiple records with the same key to be inserted.
 *     Fetching a record with the key will return only one of the duplicate
 *     records, and which record is returned is not defined.
 *   - MDBM_MODIFY - Store only if matching entry already exists.
 *   - MDBM_RESERVE - Reserve space; value not copied (\ref mdbm_store_r only)
 *   - MDBM_CACHE_ONLY  - Perform store only in the Cache, not in Backing Store.
 *   - MDBM_CACHE_MODIFY  - Update Cache only if key exists; update the Backing Store
 *
 * Insertion with flag \em MDBM_MODIFY set will fail if the key does not
 * already exist.
 *
 * Insertion with flag \em MDBM_RESERVE set will allocate space within the
 * MDBM for the object; but will not copy it.  The caller is responsible for
 * copying the value into the indicated position.
 *
 * Replacement with flag \em MDBM_CACHE_MODIFY, for a cache with a backing
 * store, has the effect of writing through the cache to the backing store
 * without updating the cache unless that cache entry already exists.
 *
 * NOTE: \em MDBM_CACHE_MODIFY has no effect if you set a cache-mode, but don't have
 * a backing store. In that case, use MDBM_MODIFY.
 *
 *
 * Quick option summary:
 *   - MDBM_INSERT  - Add if missing, fail if present
 *   - MDBM_REPLACE - Add if missing, replace if present
 *   - MDBM_MODIFY  - Fail if missing, replace if present
 *
 * NOTE: Ensure that the key and value datum pointers reference storage that
 * is external to the db and not the result of, for example, an
 * \ref mdbm_fetch, \ref mdbm_first, or \ref mdbm_next.  A store operation can
 * result in various db internal modifications that invalidate previously
 * returned key and value pointers.  Also be aware that mdbm_store_r
 * overwrites key.dptr and val.dptr, so to avoid memory leaks for the case
 * when key.dptr or val.dptr point to malloc-ed or new-ed memory, you should
 * keep a copy of above pointers and deallocate them when done.
 *
 * NOTE: If you do multiple stores to the same key using MDBM_REPLACE, the
 * corresponding value's location in memory can change after each store.  The
 * MDBM_REPLACE flag does not imply that the value will reuse the same location.
 * Even if you store a smaller value for an existing key, it probably won't
 * reuse the existing value's location.  In the current implementation, the only
 * time it tries to reuse a value's location is when replacing data with exactly
 * the same size.  This internal optimization is subject to change in any future
 * release.
 *
 * NOTE: It is possible, under some circumstances, with MDBM_REPLACE for the old
 * entry to be deleted, but insufficient space to be available for the new entry.
 * In this case, the errno will be EOVERFLOW.
 *
 * \todo doc, example MDBM_RESERVE
 */
extern int mdbm_store(MDBM *db, datum key, datum val, int flags);


/**
 * Stores the record specified by the \a key and \a val parameters.
 *
 * \param[in,out] db Database handle
 * \param[in]     key \ref datum to be used as the index key
 * \param[in]     val \ref datum containing the value to be stored
 * \param[in]     flags Type of store operation to be performed.
 *                See \ref mdbm_store for flag values.
 * \param[in,out] iter MDBM iterator
 * \return Store status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 * \retval  1 Flag MDBM_INSERT was specified, and the key already exists
 *
 * See \ref mdbm_store for additional usage information.
 */
extern int mdbm_store_r(MDBM *db, datum* key, datum* val, int flags, MDBM_ITER* iter);

/**
 * Stores a string into the MDBM.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Key for storage
 * \param[in]     val String to store
 * \param[in]     flags Type of store operation to be performed.
 * \return Store status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 * \retval  1 Flag MDBM_INSERT was specified, and the key already exists
 *
 * See \ref mdbm_store for additional usage information.
 */
extern int mdbm_store_str(MDBM *db, const char *key, const char *val, int flags);

/** \} RecordAccessGroup */

/**
 * \defgroup RecordIterationGroup Record Iteration Group
 * The Record Iteration Group contains the API for iterating over records in
 * an MDBM.
 * \{
 */

/**
 * Returns the first key/value pair from the database.  The order that records
 * are returned is not specified.
 *
 * \todo Is it safe to update/delete an entry returned via \ref mdbm_first?
 *
 * \param[in,out] db Database handle
 * \return kvpair.  If database is empty, return a null kvpair (key and value dsize==0, dptr==NULL).
 */
extern kvpair mdbm_first(MDBM *db);

/**
 * Fetches the first record in an MDBM.  Initializes the iterator, and returns
 * the first key/value pair from the db.  Subsequent calls to \ref mdbm_next_r or
 * \ref mdbm_nextkey_r with this iterator will loop through the entire db.
 *
 * \param[in,out] db Database handle
 * \param[in,out] iter MDBM iterator
 * \return kvpair.  If database is empty, return a null kvpair (key and value dsize==0, dptr==NULL).
 */
extern kvpair mdbm_first_r(MDBM *db, MDBM_ITER *iter);

/**
 * Returns the next key/value pair from the database.  The order that records
 * are returned is not specified.
 *
 * \todo Is it safe to update/delete an entry returned via mdbm_next?
 *
 * \param[in,out] db Database handle
 * \return kvpair.  If no more records exist,
 *         return a null kvpair (key and value dsize==0, dptr==NULL).
 */
extern kvpair mdbm_next(MDBM *db);

/**
 * Fetches the next record in an MDBM.  Returns the next key/value pair from
 * the db, based on the iterator.
 *
 * \param[in,out] db Database handle
 * \param[in,out] iter MDBM iterator
 * \return kvpair.  If no more records exist,
 *         return a null kvpair (key and value dsize==0, dptr==NULL).
 */
extern kvpair mdbm_next_r(MDBM *db, MDBM_ITER *iter);

/**
 * Returns the first key from the database.  The order that records are
 * returned is not specified.
 *
 * \param[in,out] db Database handle
 * \return datum.  If the database is empty, return a null datum (dsize==0, dptr==NULL).
 */
extern datum mdbm_firstkey(MDBM *db);

/**
 * Fetches the first key in an MDBM.  Initializes the iterator, and returns
 * the first key from the db.  Subsequent calls to \ref mdbm_next_r or
 * \ref mdbm_nextkey_r with this iterator will loop through the entire db.
 *
 * \param[in,out] db Database handle
 * \param[in,out] iter MDBM iterator
 * \return datum.  If the database is empty, return a null datum (dsize==0, dptr==NULL).
 */
extern datum mdbm_firstkey_r(MDBM *db, MDBM_ITER *iter);

/**
 * Returns the next key pair from the database.  The order that records are
 * returned is not specified.
 *
 * \param[in,out] db Database handle
 * \return datum.  If the database is empty, return a null datum (dsize==0, dptr==NULL).
 */
extern datum mdbm_nextkey(MDBM *db);

/**
 * Fetches the next key in an MDBM.  Returns the next key from the db.
 * Subsequent calls to \ref mdbm_next_r or mdbm_nextkey_r with this iterator
 * will loop through the entire db.
 *
 * \param[in,out] db Database handle
 * \param[in,out] iter MDBM iterator
 * \return datum.  If there are no more records, return a null datum (dsize==0, dptr==NULL).
 */
extern datum mdbm_nextkey_r(MDBM *db, MDBM_ITER *iter);

#define MDBM_ENTRY_DELETED      0x1
#define MDBM_ENTRY_LARGE_OBJECT 0x2

typedef struct mdbm_page_info {
    uint32_t page_num;
    uint32_t mapped_page_num;
    intptr_t page_addr;
    uint32_t page_size;
    uint32_t page_bottom_of_data;
    uint32_t page_free_space;
    uint32_t page_active_entries;
    uint32_t page_active_lob_entries;
    uint32_t page_active_space;
    uint32_t page_active_key_space;
    uint32_t page_active_val_space;
    uint32_t page_active_lob_space;
    uint32_t page_overhead_space;
    uint32_t page_deleted_entries;
    uint32_t page_deleted_space;
    uint32_t page_min_entry_size;
    uint32_t page_max_entry_size;
    uint32_t page_min_key_size;
    uint32_t page_max_key_size;
    uint32_t page_min_val_size;
    uint32_t page_max_val_size;
    uint32_t page_min_lob_size;
    uint32_t page_max_lob_size;
} mdbm_page_info_t;

typedef struct mdbm_entry_info {
    uint32_t entry_flags;
    uint32_t entry_index;
    uint32_t entry_offset;
    uint32_t key_offset;
    uint32_t val_offset;
    uint32_t lob_page_num;
    uint32_t lob_num_pages;
    intptr_t lob_page_addr;
    uint32_t cache_num_accesses;
    uint32_t cache_access_time;
    float    cache_priority;
} mdbm_entry_info_t;

typedef struct mdbm_iterate_info {
    mdbm_page_info_t  i_page;
    mdbm_entry_info_t i_entry;
} mdbm_iterate_info_t;

typedef int (*mdbm_iterate_func_t)(void* user, const mdbm_iterate_info_t* info, const kvpair* kv);

#define MDBM_ITERATE_ENTRIES    0x01 /**< Iterate over page entries */
#define MDBM_ITERATE_NOLOCK     0x80 /**< Iterate without locking */

/**
 * Iterates through all keys starting on page \a pagenum and continuing
 * through the rest of the database.  If \a flags contains
 * MDBM_ITERATE_ENTRIES, function \a func is invoked for each record.  If
 * \a flag does not have MDBM_ITERATE_ENTRIES set, then \a func is invoke once
 * per page with \a kv set to NULL.
 *
 * \param[in,out] db Database handle
 * \param[in]     pagenum Starting page number
 * \param[in]     func Function to invoke for each key
 * \param[in]     flags iteration control (below)
 * \param[in]     user User-supplied opaque pointer to pass to \a func
 * \return Iteration status
 * \retval -1 Error
 * \retval  0 Success
 *
 * Values for \a flags mask:
 *   - MDBM_ITERATE_NOLOCK  - Do not lock when iterating
 *   - MDBM_ITERATE_ENTRIES - Invoke \a func for each record
 */
extern int mdbm_iterate(MDBM* db, int pagenum, mdbm_iterate_func_t func, int flags, void* user);

/** \} RecordIterationGroup */


/**
 * \defgroup LockingGroup Locking Group
 * The Locking Group contain the API for using the various lock types:
 * exclusive, partition, and shared.
 * \{
 */

/**
 * Returns whether or not MDBM is locked by another process or thread.
 *
 * \param[in,out] db Database handle
 * \return Locked status
 * \retval 0 Database is not locked
 * \retval 1 Database is locked
 */
extern int mdbm_islocked(MDBM *db);

/**
 * Returns whether or not MDBM is currently locked (owned) by the calling process.
 * \em Owned MDBMs have multiple nested locks in place.
 *
 * \param[in,out] db Database handle
 * \return Owned status
 * \retval 0 Database is not owned
 * \retval 1 Database is owned
 */
extern int mdbm_isowned(MDBM *db);

#define MDBM_LOCKMODE_UNKNOWN   0xFFFFFFFF   /**< Returned by mdbm_get_lockmode for an unknown lock mode (MDBM V2). */

/**
 * Gets the MDBM's lock mode.
 *
 * \param[in,out] db Database handle
 * \return Lock mode
 * \retval 0                      - Exclusive locking
 * \retval MDBM_OPEN_NOLOCK       - No locking
 * \retval MDBM_PARTITIONED_LOCKS - Partitioned locking
 * \retval MDBM_RW_LOCKS          - Shared (read-write) locking
 */
extern uint32_t mdbm_get_lockmode(MDBM *db);

/**
 * Locks the database for exclusive access by the caller.  The lock is
 * nestable, so a caller already holding the lock may call mdbm_lock again as
 * long as an equal number of calls to \ref mdbm_unlock are made to release the
 * lock.
 *
 * \param[in,out] db Database handle
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been corrupted.
 * \retval  1 Success, exclusive lock was acquired
 */
extern int mdbm_lock(MDBM *db);

/**
 * Unlocks the database, releasing exclusive or shared access by the caller.  If
 * the caller has called \ref mdbm_lock or \ref mdbm_lock_shared multiple times
 * in a row, an equal number of unlock calls are required.  See \ref mdbm_lock
 * and \ref mdbm_lock_shared for usage.
 *
 * \param[in,out] db Database to be unlocked
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been
 *            corrupted, or the process/thread does not own the lock.
 * \retval  1 Success, exclusive lock was released
 */
extern int mdbm_unlock(MDBM *db);

/**
 * Attempts to exclusively lock an MDBM.
 *
 * \param[in,out] db Database handle
 * \return Lock status
 * \retval -1 Error, and errno is set
 * \retval  1 Success, exclusive lock was acquired
 */
extern int mdbm_trylock(MDBM *db);

/**
 * Locks a specific partition in the database for exclusive access by the
 * caller.  The lock is nestable, so a caller already holding the lock may
 * call mdbm_plock again as long as an equal number of calls to \ref mdbm_punlock
 * are made to release the lock.
 *
 * \param[in,out] db Database to be locked.
 * \param[in]     key Key to be hashed to determine page to lock
 * \param[in]     flags Ignored.
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been corrupted.
 * \retval  1 Success, partition lock was acquired
 */
extern int mdbm_plock(MDBM *db, const datum *key, int flags);

#define MDBM_LOCKMODE_UNKNOWN   0xFFFFFFFF   /**< Returned by mdbm_get_lockmode for an unknown lock mode (MDBM V2). */

/**
 * Unlocks a specific partition in the database, releasing exclusive access by
 * the caller.  If the caller has called \ref mdbm_plock multiple times in a row,
 * an equal number of unlock calls are required.  See \ref mdbm_plock for
 * usage.
 *
 * \param[in,out] db Database to be unlocked
 * \param[in]     key Ignored.
 * \param[in]     flags Ignored.
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been corrupted,
 *            or mdbm_plock is called without holding the lock
 * \retval  1 Success, partition lock was released
 */
extern int mdbm_punlock(MDBM *db, const datum *key, int flags);

/**
 * Tries to locks a specific partition in the database for exclusive access by
 * the caller.  The lock is nestable, so a caller already holding the lock may
 * call \ref mdbm_plock again as long as an equal number of calls to \ref mdbm_punlock
 * are made to release the lock.  See \ref mdbm_plock for usage.
 *
 * \param[in,out] db Database to be unlocked
 * \param[in]     key Key to be hashed to determine page to lock
 * \param[in]     flags Ignored.
 * \return Lock status
 * \retval -1 Error, and errno is set.  Partition is not locked.
 *            Typically, an error occurs if the database has been corrupted.
 * \retval  1 Success, partition lock was acquired
 */
extern int mdbm_tryplock(MDBM *db, const datum *key, int flags);

/**
 * Locks the database for shared access by readers, excluding access to
 * writers.  This is multiple-readers, one writer (MROW) locking. The database
 * must be opened with the MDBM_RW_LOCKS flag to enable shared locks.  Use
 * \ref mdbm_unlock to release a shared lock.
 *
 * Write access (ex. \ref mdbm_store and \ref mdbm_delete) must not be done in
 * the context of a shared lock. Write access must be done in the context of
 * an exclusive lock (\ref mdbm_lock and \ref mdbm_unlock).
 *
 * The lock is nestable, so a caller already holding the lock may call
 * mdbm_lock_shared again as long as an equal number of calls to \ref mdbm_unlock are
 * made to release the lock.
 *
 * \param[in,out] db Database handle
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been corrupted.
 * \retval  1 Success, shared lock was acquired
 */
extern int mdbm_lock_shared(MDBM *db);

/**
 * Locks the database for shared access by readers, excluding access to
 * writers.  This is the non-blocking version of \ref mdbm_lock_shared
 *
 * This is MROW locking. The database must be opened with the MDBM_RW_LOCKS
 * flag to enable shared locks.
 *
 * Write access (ex. \ref mdbm_store and \ref mdbm_delete) must not be done in the
 * context of a shared lock. Write access must be done in the context of an
 * exclusive lock (\ref mdbm_lock and \ref mdbm_unlock).
 *
 * \param[in,out] db Database handle
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been corrupted,
 *            or the database is already exclusively locked.
 * \retval  1 Success, shared lock was acquired
 */
extern int mdbm_trylock_shared(MDBM *db);

/**
 * Perform either partition, shared or exclusive locking based on the
 * locking-related flags supplied to \ref mdbm_open.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Key to be hashed to determine which page to lock (if needed)
 * \param[in]     flags MDBM_O_RDWR means lock to perform a write
 * \return Lock status
 * \retval -1 Error, and errno is set.
 *            Typically, an error occurs if the database has been corrupted.
 * \retval  1 Success, smart lock was acquired, or \a db was opened with MDBM_OPEN_NOLOCK.
 */
extern int mdbm_lock_smart(MDBM *db, const datum *key, int flags);

/**
 * Attempts to lock an MDBM based on the locking flags supplied to \ref mdbm_open.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Key to be hashed to determine page to lock (if needed)
 * \param[in]     flags MDBM_O_RDWR means lock to perform a write.
 * \return Lock status
 * \retval -1 Error, and errno is set
 * \retval  1 Success, smart lock was acquired, or \a db was opened with MDBM_OPEN_NOLOCK.
 */
extern int mdbm_trylock_smart(MDBM *db, const datum *key, int flags);

/** Unlock an MDBM based on the locking flags supplied to \ref mdbm_open.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Key to be hashed to determine page to lock (if needed)
 * \param[in]     flags Reserved for future use
 * \return Lock status
 * \retval -1 Error, and errno is set
 * \retval  1 Success, smart lock was released, or \a db was opened with MDBM_OPEN_NOLOCK.
 */
extern int mdbm_unlock_smart(MDBM *db, const datum *key, int flags);

/**
 * Resets the global lock ownership state of a database.
 * USE THIS FUNCTION WITH EXTREME CAUTION!
 * The global lock ownership state of an MDBM is visible to other
 * processes and is reset system-wide.  Resetting a lock state while other
 * threads/processes are accessing this database might cause those operations
 * to fail.
 *
 * \param[in] dbfilename Database file name
 * \param[in] flags Reserved for future use, and must be 0.
 * \return Lock reset status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_lock_reset(const char* dbfilename, int flags);

/**
 * Removes all lockfiles associated with an MDBM file.
 * USE THIS FUNCTION WITH EXTREME CAUTION!
 * NOTE: This is only intended to clean-up lockfiles when removing an MDBM file.
 * This function can only be used when all processes that access
 * the MDBM whose locks are being deleted are not running.
 * Calling it on an MDBM still in use will cause corruption and undefined behavior.
 * Deleting lockfiles resets lock ownership and locking mode (exclusive/partition/shared).
 *
 * \param[in] dbname Full path of MDBM file
 * \return Lockfile deletion status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_delete_lockfiles(const char* dbname);

/** \} LockingGroup */

/**
 * \defgroup DataManagementGroup Data Management Group
 * The Data Management Group contains the API managing data as a whole (not
 * record based), or on a large scale.
 * \{
 */

#define MDBM_SAVE_COMPRESS_TREE 0x01000000 /**< Compress MDBM before saving */

/**
 * Compresses the existing MDBM directory.  Attempts to rebalance the directory
 * and to compress the db to a smaller size.
 *
 * NOTE: This function does not work with Windowed-Mode. For that use case, you should
 * export your data and re-import it into a clean MDBM.
 *
 * NOTE: Support for this feature in V4 should be considered experimental. Please run
 * it on an offline DB, after making a backup of your uncompressed DB.
 *
 * \param[in,out] db Database handle
 */
extern void mdbm_compress_tree(MDBM *db);

/**
 * Truncates the MDBM to single empty page
 *
 * V3 WARNING: This loses the existing configuration information (ex. large object support
 * or a hash function that was set other than the default).
 *
 * \param[in,out] db Database handle
 */
extern void mdbm_truncate(MDBM *db);

/**
 * Prunes an MDBM.  Iterates through the database calling the supplied prune
 * function for each item in the database.  Prune function:
 *
 * \code
 *  int (*prune)(MDBM *, datum, datum, void *)
 * \endcode
 *
 * The user-supplied param pointer is passed as the 4th parameter to the prune
 * function.  If the prune function returns 1, the item is deleted.  If the
 * prune function returns 0, the item is retained.
 *
 * \param[in,out] db Database handle
 * \param[in]     prune Prune function
 * \param[in]     param User supplied param, passed to prune function.
 */
extern void mdbm_prune(MDBM* db, int (*prune)(MDBM *, datum, datum, void *), void *param);

/**
 * Purges (removes) all entries from an MDBM.  This does not change the MDBM's
 * configuration or general structure.
 *
 * \param[in,out] db Database handle
 */
extern void mdbm_purge(MDBM *db);

struct mdbm_clean_data {
    void* user_data;            /**< Opaque user data */
};

typedef int (*mdbm_clean_func)(MDBM *, const datum*, const datum*,
                               struct mdbm_clean_data *, int* quit);

/**
 * The specified cleaner function will be called by \ref mdbm_clean.  It can
 * be called by \ref mdbm_store should space be needed on a page.  \ref
 * mdbm_store will call it if there is no registered shake function (see \ref
 * mdbm_limit_size_v3) or the registered shake function does not clear enough
 * space for the store.  An entry marked clean means it may be re-used to
 * store new data.
 *
 * NOTE: V3 API
 *
 * The clean function is typedef'd like the following:
 *
 * \code
 * typedef int (*mdbm_clean_func)(MDBM *, const datum*, const datum*,
 *                               struct mdbm_clean_data *,
 *                               int* quit);
 * \endcode
 *
 * When the mdbm_clean_func is called to check an entry on a page, it will be
 * passed the data parameter from the call to mdbm_set_cleanfunc.
 *
 * The quit parameter is used by callers of the mdbm_clean_func to determine
 * whether other entries on the page should be cleaned. If the mdbm_clean_func
 * sets quit to true (non 0) then no other entries on the page will be checked
 * for cleaning. Otherwise \ref mdbm_clean or \ref mdbm_store will continue checking
 * entries on the page to determine if they can be re-used/cleaned.
 *
 * The mdbm_clean_func should return true (non 0) if the key/value may be
 * re-used, AKA clean.  IMPORTANT: To enable this feature cache mode must be
 * set on the database. Ex:
 * \code
 * mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);
 * mdbm_set_cleanfunc(dbh, mycleanerfunction, mycleanerdata);
 * mdbm_limit_size(dbh, limitNumPages, 0); // notice no shake function is specified
 * \endcode
 *
 * Both of the cache mode flags are required. The second
 * flag(MDBM_CACHEMODE_GDSF) used in the example can be replaced by
 * MDBM_CACHEMODE_LFU or MDBM_CACHEMODE_LRU.
 *
 * \param[in,out] db Database handle
 * \param[in]     func User-provided function to determine re-use of an entry
 * \param[in]     data Opaque data passed to the user-provided function \a func
 * \return Set clean function status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_set_cleanfunc(MDBM* db, mdbm_clean_func func, void* data);

/**
 * Mark entries clean/re-usable in the database for the specified page. If
 * pagenum is -1, then clean all pages.  It relies on the user provided
 * callback function set via \ref mdbm_set_cleanfunc to determine
 * re-usability/cleanliness of an entry. To be clean means an entry can be
 * re-used to store new data.
 *
 * NOTE: V3 API
 *
 * \param[in,out] db Database handle
 * \param[in]     pagenum Page number to start cleaning.
 *                If < 0, then clean all pages in the database.
 * \param[in]     flags Ignored
 * \return Clean status or count
 * \retval -1 Error
 * \retval Number of cleaned entries
 */
extern int mdbm_clean(MDBM* db, int pagenum, int flags);

/** \} DataManagementGroup */


/**
 * \defgroup DataIntegrityGroup Data Integrity Group
 * The Data Integrity Group contain the API to check the state of internal
 * structures, or to control general data access.
 * \{
 */

#define MDBM_CHECK_HEADER    0   /**< Check MDBM header for integrity */
#define MDBM_CHECK_CHUNKS    1   /**< Check MDBM header and chunks (page structure) */
#define MDBM_CHECK_DIRECTORY 2   /**< Check MDBM header, chunks, and directory */
#define MDBM_CHECK_ALL       3   /**< Check MDBM header, chunks, directory, and data  */

/**
 * Checks an MDBM's integrity, and displays information on standard output.
 *
 * \param[in,out] db Database handle
 * \param[in]     level Depth of checks
 * \param[in]     verbose Whether to display verbose information while checking
 * \return Check status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_check(MDBM* db, int level, int verbose);

/**
 * Checks integrity of an entry on a page.
 *
 * NOTE: This has not been implemented.
 *
 * \param[in,out] db Database handle
 * \param[in]     pagenum Page number
 * \param[in]     mapped_pagenum Mapped page number
 * \param[in]     index Entry number on a page
 */
extern void mdbm_chk_error(MDBM* db, int pagenum, int mapped_pagenum, int index);

/**
 * Checks the specified page for errors.  It will print errors found on the
 * page, including bad key size, bad val size, and bad offsets of various
 * fields.
 *
 * V2: Prints found errors to stdout. If no errors, then no printing
 * performed.  When it detects errors, it returns -1 and \ref mdbm_get_errno
 * returns EFAULT.
 *
 * Prints found errors via mdbm_log targeting LOG_CRITICAL.  If no
 * errors, then no logging performed.  When it detects errors, it returns -1
 * and errno is set to EFAULT.
 * Returns -1, and sets errno to EINVAL for invalid page numbers.
 *
 * \param[in,out] db Database handle
 * \param[in]     pagenum Page to check for errors
 * \return Check page status
 * \retval -1 Errors detected, and errno is set.
 *         This could be due to a locking error.
 *         For v2, the caller must call \ref mdbm_get_errno and check against EFAULT.
 *         For v3, the caller must check errno against EFAULT.
 * \retval 0 Success
 */
extern int mdbm_chk_page(MDBM* db, int pagenum);

/**
 * Checks the database for errors.  It will report same as mdbm_chk_page for
 * all pages in the database.  See v2 and v3 in \ref mdbm_chk_page to
 * determine if errors detected in the database.
 *
 * \param[in,out] db Database handle
 * \return Check all pages status
 * \retval -1 Errors detected, and errno is set.
 *         This could be due to a locking error.
 *         For v2, the caller must call \ref mdbm_get_errno and check against EFAULT.
 *         For v3, the caller must check errno against EFAULT.
 * \retval  0 success

 */
extern int mdbm_chk_all_page(MDBM* db);

#define MDBM_PROT_NONE          0 /**< Page no access */
#define MDBM_PROT_READ          1 /**< Page read access */
#define MDBM_PROT_WRITE         2 /**< Page write access */

#define MDBM_PROT_NOACCESS      MDBM_PROT_NONE /**< Page no access */
#define MDBM_PROT_ACCESS        4 /**< Page protection mask */

/**
 * Sets all database pages to \a protect permission.  This function is for
 * advanced users only.  Users that want to use the built-in \em protect
 * feature should specify MDBM_PROTECT in their \ref mdbm_open flags.
 *
 * \param[in,out] db Database handle
 * \param[in]     protect Permission mask
 * \return Protect status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 *
 * Values for \a protect mask:
 *   - MDBM_PROT_NONE     - no access
 *   - MDBM_PROT_READ     - read access
 *   - MDBM_PROT_WRITE    - write access
 *   - MDBM_PROT_ACCESS   - all access
 *   - MDBM_PROT_NOACCESS - no access (same as MDBM_PROT_NONE)
 *
 * NOTE: RHEL is unable to set MDBM_PROT_WRITE without MDBM_PROT_READ, so specifying
 * MDBM_PROT_WRITE does not protect against reads.
 */
extern int mdbm_protect(MDBM* db, int protect);

/** \} DataIntegrityGroup */


/**
 * \defgroup DataDisplayGroup Data Display Group
 * The Data Display Group contains the API to display page data.
 * These routines are intended for debugging purposes
 * \{
 */

/**
 * Dumps information for all pages, in version-specific format, to standard output.
 *
 * \param[in,out] db Database handle
 */
extern void mdbm_dump_all_page(MDBM *db);

/**
 * Dumps specified page's information, in version-specific format, to standard output.
 *
 * \param[in,out] db Database handle
 * \param[in]     pno Page number
 */
extern void mdbm_dump_page(MDBM *db, int pno);

/** \} DataDisplayGroup */

/**
 * \defgroup StatisticsGroup Statistics Group
 * The Statistics Group contains the API for capacity and performance indicators.
 * \{
 */


typedef struct mdbm_stats {
    mdbm_ubig_t s_size;
    mdbm_ubig_t s_page_size;
    mdbm_ubig_t s_page_count;
    mdbm_ubig_t s_pages_used;
    mdbm_ubig_t s_bytes_used;
    mdbm_ubig_t s_num_entries;
    mdbm_ubig_t s_min_level;
    mdbm_ubig_t s_max_level;
    mdbm_ubig_t s_large_page_size;
    mdbm_ubig_t s_large_page_count;
    mdbm_ubig_t s_large_threshold;
    mdbm_ubig_t s_large_pages_used;
    mdbm_ubig_t s_large_num_free_entries;
    mdbm_ubig_t s_large_max_free;
    mdbm_ubig_t s_large_num_entries;
    mdbm_ubig_t s_large_bytes_used;
    mdbm_ubig_t s_large_min_size;
    mdbm_ubig_t s_large_max_size;
    uint32_t    s_cache_mode;
} mdbm_stats_t;


typedef uint64_t mdbm_counter_t;

typedef enum {
    MDBM_STAT_TYPE_FETCH  = 0, /**< fetch* operations */
    MDBM_STAT_TYPE_STORE  = 1, /**< store* operations */
    MDBM_STAT_TYPE_DELETE = 2, /**< delete* operations */
    MDBM_STAT_TYPE_MAX    = MDBM_STAT_TYPE_DELETE
} mdbm_stat_type;

/**
 * Gets the number of operations performed for a stat \a type.
 *
 * \param[in,out] db Database handle
 * \param[in]     type Stat type to return
 * \param[out]    value Pointer to returned stat's value
 * \return Stat counter status
 * \retval -1 Error
 * \retval  0 Success
 *
 * Values for \a type:
 *   - MDBM_STAT_TYPE_FETCH  - For fetch* operations
 *   - MDBM_STAT_TYPE_STORE  - For store* operations
 *   - MDBM_STAT_TYPE_DELETE - For delete* operations
 *
 * Stat operations must be enabled for operations to be tracked.
 * Use \ref mdbm_enable_stat_operations to enable this feature.
 *
 * Once enabled, statistics are persisted in the MDBM
 * and are not reset on \ref mdbm_close.
 *
 * Use program `mdbm_stat -H' to display stat operation metrics stored in the header.
 */
extern int mdbm_get_stat_counter(MDBM *db, mdbm_stat_type type, mdbm_counter_t *value);

/**
 * Gets the last time when an \a type operation was performed.
 *
 * \param[in,out] db Database handle
 * \param[in]     type Stat type to return.
 * \param[out]    value Pointer to returned stat's value.
 * \return Stat time status
 * \retval -1 Error
 * \retval  0 Success
 *
 * Values for \a type:
 *   - MDBM_STAT_TYPE_FETCH  - For fetch* operations
 *   - MDBM_STAT_TYPE_STORE  - For store* operations
 *   - MDBM_STAT_TYPE_DELETE - For delete* operations
 *
 * Stat operations must be enabled for operations to be tracked.
 * Use \ref mdbm_enable_stat_operations to enable this feature.
 *
 * Once enabled, statistics are persisted in the MDBM
 * and are not reset on \ref mdbm_close.
 *
 * Use program `mdbm_stat -H' to display stat operation metrics stored in the header.
 */
extern int mdbm_get_stat_time(MDBM *db, mdbm_stat_type type, time_t *value);

/**
 * Resets the stat counter and last-time performed for fetch, store, and remove operations.
 *
 * \param[in,out] db Database handle
 *
 * Stat operations must be enabled for operations to be tracked.
 * Use \ref mdbm_enable_stat_operations to enable this feature.
 *  If stat operations are not enabled, using this function will merely reset and
 * already cleared storage.
 *
 * Use program `mdbm_stat -H' to display stat operation metrics stored in the header.
 */
extern void mdbm_reset_stat_operations(MDBM *db);

/**
 * Enables and disables gathering of stat counters and/or last-time performed
 * for fetch, store, and remove operations.
 *
 * \param[in,out] db Database handle
 * \param[in] flags
 *
 * Enables stat operations so that we can track one or both of:
 * 1. Operations counters fetch, store and remove.
 * 2. Last timestamp when a fetch, store or delete was performed.
 *
 * flags = MDBM_STATS_BASIC  enables gathering only the stats counters.
 * flags = MDBM_STATS_TIMED  enables gathering only the stats timestamps.
 * flags = (MDBM_STATS_BASIC | MDBM_STATS_TIMED)  enables both the stats counters and timestamps.
 * flags = 0  disables gathering of stats counters and timestamps.
 *
 * \retval -1 Error
 * \retval  0 Success

 * Use program `mdbm_stat -H' to display stat operation metrics stored in the header.
 */
extern int mdbm_enable_stat_operations(MDBM *db, int flags);


#define MDBM_CLOCK_STANDARD   0
#define MDBM_CLOCK_TSC        1

/**
 * Tells the MDBM library whether to use TSC (CPU TimeStamp Counters)
 * for timing the performance of fetch, store and delete operations.
 * The standard behavior of timed stat operations is to use clock_gettime(MONOTONIC)
 *
 * \param[in,out] db Database handle
 * \param[in] flags
 *
 * flags == MDBM_CLOCK_TSC       Enables use of TSC
 * flags == MDBM_CLOCK_STANDARD  Disables use of TSC
 *
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_set_stat_time_func(MDBM *db, int flags);


/* mdbm_set_stats_func flags */
#define MDBM_STATS_BASIC       0x1 /**< Basic stats only */
#define MDBM_STATS_TIMED       0x2 /**< SLOW! Stats that call get-time functions. */

/* Stat Callback Flags */
#define MDBM_STAT_CB_INC       0x0 /**< Basic stat. Value is a delta. */
#define MDBM_STAT_CB_SET       0x1 /**< Basic stat. Value is the current value. */
#define MDBM_STAT_CB_ELAPSED   0x2 /**< Value is a time delta. */
#define MDBM_STAT_CB_TIME      0x3 /**< Value is the stat time. */

/* Macro for creating a unique integer tag for all stats metrics */
#define COMBINE_STAT_TAG(tag, flag) (tag | (flag<<16))

/**
 * The various stats metrics (tags):
 * All time/latency related stats (defined below) require:
 * mdbm_set_stats_func(db, flags|MDBM_STAT_CB_TIME, ...)
 */

#define MDBM_STAT_TAG_FETCH             1   /* Successful fetch stats-callback counter */
#define MDBM_STAT_TAG_STORE             2   /* Successful store stats-callback counter */
#define MDBM_STAT_TAG_DELETE            3   /* Successful delete stats-callback counter */
#define MDBM_STAT_TAG_LOCK              4   /* lock stats-callback counter (not implemented) */
#define MDBM_STAT_TAG_FETCH_UNCACHED    5   /* Cache-miss with cache+backingstore */
#define MDBM_STAT_TAG_GETPAGE           6   /* Generic access counter in windowed mode */
#define MDBM_STAT_TAG_GETPAGE_UNCACHED  7   /* Windowed-mode "miss" (load new page into window) */
#define MDBM_STAT_TAG_CACHE_EVICT       8   /* Cache evict stats-callback counter */
#define MDBM_STAT_TAG_CACHE_STORE       9   /* Successful cache store counter (BS only) */
#define MDBM_STAT_TAG_PAGE_STORE        10  /* Successful page-level store indicator */
#define MDBM_STAT_TAG_PAGE_DELETE       11  /* Successful page-level delete indicator */
#define MDBM_STAT_TAG_SYNC              12  /* Counter of mdbm_syncs and fsyncs */
#define MDBM_STAT_TAG_FETCH_NOT_FOUND   13  /* Fetch cannot find a key in MDBM */
#define MDBM_STAT_TAG_FETCH_ERROR       14  /* Error occurred during fetch */
#define MDBM_STAT_TAG_STORE_ERROR       15  /* Error occurred during store (e.g. MODIFY failed) */
#define MDBM_STAT_TAG_DELETE_FAILED     16  /* Delete failed: cannot find a key in MDBM */

/** Fetch latency (expensive to collect)
 */
#define MDBM_STAT_TAG_FETCH_LATENCY  COMBINE_STAT_TAG(MDBM_STAT_TAG_FETCH, MDBM_STAT_CB_ELAPSED)
/** Store latency (expensive to collect)
 */
#define MDBM_STAT_TAG_STORE_LATENCY  COMBINE_STAT_TAG(MDBM_STAT_TAG_STORE, MDBM_STAT_CB_ELAPSED)
/** Delete latency (expensive to collect)
 */
#define MDBM_STAT_TAG_DELETE_LATENCY  COMBINE_STAT_TAG(MDBM_STAT_TAG_DELETE, MDBM_STAT_CB_ELAPSED)
/** timestamp of last fetch (not yet implemented)
 */
#define MDBM_STAT_TAG_FETCH_TIME  COMBINE_STAT_TAG(MDBM_STAT_TAG_FETCH, MDBM_STAT_CB_TIME)
/** timestamp of last store (not yet implemented)
 */
#define MDBM_STAT_TAG_STORE_TIME  COMBINE_STAT_TAG(MDBM_STAT_TAG_STORE, MDBM_STAT_CB_TIME)
/** timestamp of last delete (not yet implemented)
 */
#define MDBM_STAT_TAG_DELETE_TIME  COMBINE_STAT_TAG(MDBM_STAT_TAG_DELETE, MDBM_STAT_CB_TIME)
/** Cache miss latency for cache+Backingstore only (expensive to collect)
 */
#define MDBM_STAT_TAG_FETCH_UNCACHED_LATENCY \
     COMBINE_STAT_TAG(MDBM_STAT_TAG_FETCH_UNCACHED, MDBM_STAT_CB_ELAPSED)
/** access latency in windowed mode (expensive to collect)
 */
#define MDBM_STAT_TAG_GETPAGE_LATENCY  COMBINE_STAT_TAG(MDBM_STAT_TAG_GETPAGE, MDBM_STAT_CB_ELAPSED)
/** windowed-mode miss latency (expensive to collect)
 */
#define MDBM_STAT_TAG_GETPAGE_UNCACHED_LATENCY \
     COMBINE_STAT_TAG(MDBM_STAT_TAG_GETPAGE_UNCACHED, MDBM_STAT_CB_ELAPSED)
/** cache evict latency (expensive to collect)
 */
#define MDBM_STAT_TAG_CACHE_EVICT_LATENCY  \
     COMBINE_STAT_TAG(MDBM_STAT_TAG_CACHE_EVICT, MDBM_STAT_CB_ELAPSED)
/** Cache store latency in Cache+backingstore mode only (expensive to collect)
 */
#define MDBM_STAT_TAG_CACHE_STORE_LATENCY \
     COMBINE_STAT_TAG(MDBM_STAT_TAG_CACHE_STORE, MDBM_STAT_CB_ELAPSED)
/** Indicates a store occurred on a particular page.  Value returned by callback is the page
  * number.  It is up to the callback function to maintain a per-page count
  */
#define MDBM_STAT_TAG_PAGE_STORE_VALUE COMBINE_STAT_TAG(MDBM_STAT_TAG_PAGE_STORE, MDBM_STAT_CB_SET)
/** Indicates a delete occurred on a particular page.  Value returned by callback is the page
  * number.  It is up to the callback function to maintain a per-page count
  */
#define MDBM_STAT_TAG_PAGE_DELETE_VALUE \
     COMBINE_STAT_TAG(MDBM_STAT_TAG_PAGE_DELETE, MDBM_STAT_CB_SET)
/** mdbm_sync/fsync latency (expensive to collect)
 */
#define MDBM_STAT_TAG_SYNC_LATENCY COMBINE_STAT_TAG(MDBM_STAT_TAG_SYNC, MDBM_STAT_CB_ELAPSED)


/**
 * The general stats callback function definition.
 *
 * \param[in,out] db Database handle
 * \param[in]     tag Integer identifier of the stat.
 *                Use \ref mdbm_get_stat_name to get the string id.
 * \param[in]     flags A MDBM_STAT_CB_* value indicating the kind of value.
 * \param[in]     value Update to the indicated value
 * \param[in]     user User-defined callback for data passed into \ref mdbm_set_stats_func
 */
typedef void (*mdbm_stat_cb)(MDBM* db, int tag, int flags, uint64_t value, void* user);

/**
 * Gets the name of a stat.
 *
 * \param[in] tag Stat identifier
 * \return Name of stat identifier.  "unknown_tag" is return if \a tag is unknown.
 *
 */
extern const char* mdbm_get_stat_name(int tag);

/**
 * Sets the callback function for a stat.
 *
 * \param[in,out] db Database handle
 * \param[in]     flags Passed to callback function
 * \param[in]     cb Callback function
 * \param[in]     user Opaque user information
 * \return Set stats function status
 * \retval -1 Error, and errno is set
 * \retval  0 Success, and callback is set
 */
extern int mdbm_set_stats_func(MDBM* db, int flags, mdbm_stat_cb cb, void* user);

/**
 * Prints to stdout various pieces of information, specifically: Total mapped
 * db size, Total number of entries, ADDRESS SPACE page efficiency, ADDRESS
 * SPACE byte efficiency, PHYSICAL MEM/DISK SPACE efficiency, Average bytes
 * per record, Maximum B-tree level, Minimum B-tree level, Minimum free bytes
 * on page, Minimum free bytes on page post compress
 *
 * If there are large objects, it loops through and reports record entries per page.
 *
 * NOTE: There is only a V2 implementation. V3 not currently supported.
 *
 * \param[in,out] db Database handle
 */
extern void mdbm_stat_all_page(MDBM *db);

/**
 * Gets a a stats block with individual stat values.
 *
 * \param[in,out] db Database handle
 * \param[out]    s Stats block
 * \param[in]     stats_size Stats block \a s size.  Only as many stats will
 *                be returned as according to this size.  The stats block is
 *                filled from top-to-bottom.
 * \return Get stats status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_get_stats(MDBM* db, mdbm_stats_t* s, size_t stats_size);

typedef struct mdbm_db_info {
    uint32_t    db_page_size;
    uint32_t    db_num_pages;
    uint32_t    db_max_pages;
    uint32_t    db_num_dir_pages;
    uint32_t    db_dir_width;
    uint32_t    db_max_dir_shift;
    uint32_t    db_dir_min_level;
    uint32_t    db_dir_max_level;
    uint32_t    db_dir_num_nodes;
    uint32_t    db_hash_func;
    const char* db_hash_funcname;
    uint32_t    db_spill_size;
    uint32_t    db_cache_mode;
} mdbm_db_info_t;

/**
 * Gets configuration information about a database.
 *
 * \param[in,out] db Database handle
 * \param[out]    info Database information
 * \return Database info status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_get_db_info(MDBM* db, mdbm_db_info_t* info);

enum {
    MDBM_PTYPE_FREE = 0,        /**< Page type free */
    MDBM_PTYPE_DATA = 1,        /**< Page type data */
    MDBM_PTYPE_DIR = 2,         /**< Page type directory */
    MDBM_PTYPE_LOB = 3          /**< Page type large object */
};

typedef struct mdbm_chunk_info {
    uint32_t page_num;
    uint32_t page_type;
    uint32_t dir_page_num;
    uint32_t num_pages;
    uint32_t page_data;
    uint32_t page_size;
    uint32_t prev_num_pages;
    uint32_t num_entries;
    uint32_t used_space;
} mdbm_chunk_info_t;

typedef int (*mdbm_chunk_iterate_func_t)(void* user, const mdbm_chunk_info_t* info);

/**
 * Iterates over all pages (starting at page 0).  This function only does
 * per-page iteration, where mdbm_iterate is intended for per-record iteration
 * starting at a page number.
 *
 * \param[in,out] db Database handle
 * \param[in]     func Function to invoke for each chunk
 * \param[in]     flags Iterations flags (below)
 * \param[in]     user Opaque user pointer passed to \a func
 * \return Chunk iterate status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 *
 * Values for \a flags mask:
 *  - MDBM_ITERATE_NOLOCK - Do not lock during iteration
 */
extern int mdbm_chunk_iterate(MDBM* db, mdbm_chunk_iterate_func_t func, int flags, void* user);

#define MDBM_STAT_DELETED       0x1 /**< Deprecated */
#define MDBM_STAT_KEYS          0x2 /**< Deprecated */
#define MDBM_STAT_VALUES        0x4 /**< Deprecated */
#define MDBM_STAT_PAGES_ONLY    0x8 /**< Deprecated */
#define MDBM_STAT_NOLOCK        0x80 /**< Do not lock for stat operation */

#define MDBM_STAT_BUCKETS 20    /**< Number of buckets for stat histograms */

typedef struct mdbm_bucket_stat {
    uint32_t num_pages;
    uint32_t min_bytes;
    uint32_t max_bytes;
    uint32_t min_free_bytes;
    uint32_t max_free_bytes;
    uint64_t sum_entries;
    uint64_t sum_bytes;
    uint64_t sum_free_bytes;
} mdbm_bucket_stat_t;

typedef struct mdbm_stat_info {
    int flags;

    uint64_t num_active_entries;
    uint64_t num_active_lob_entries;
    uint64_t sum_key_bytes;
    uint64_t sum_lob_val_bytes;
    uint64_t sum_normal_val_bytes;
    uint64_t sum_overhead_bytes;

    uint32_t min_entry_bytes;
    uint32_t max_entry_bytes;
    uint32_t min_key_bytes;
    uint32_t max_key_bytes;
    uint32_t min_val_bytes;
    uint32_t max_val_bytes;
    uint32_t min_lob_bytes;
    uint32_t max_lob_bytes;
    uint32_t max_page_used_space;

    uint32_t max_data_pages;
    uint32_t num_free_pages;
    uint32_t num_active_pages;
    uint32_t num_normal_pages;
    uint32_t num_oversized_pages;
    uint32_t num_lob_pages;

    mdbm_bucket_stat_t buckets[MDBM_STAT_BUCKETS+1];

    uint32_t max_page_entries;
    uint32_t min_page_entries;
} mdbm_stat_info_t;


/**
 * Gets overall database stats.
 *
 * \param[in,out] db Database handle
 * \param[out]    info Database page-based stats
 * \param[out]    stats Database record-based stats
 * \param[in]     flags Iteration control (below)
 * \return Database stats status
 * \retval -1 Error, errno is set
 * \retval  0 Success
 *
 * Values for \a flags mask:
 *   - MDBM_STAT_NOLOCK    - Do not lock for overall operation
 *   - MDBM_ITERATE_NOLOCK - Do no lock for page-based iteration
 */
extern int mdbm_get_db_stats(MDBM* db, mdbm_db_info_t* info, mdbm_stat_info_t* stats, int flags);

typedef struct mdbm_window_stats {
    uint64_t w_num_reused;
    uint64_t w_num_remapped;
    uint32_t w_window_size;
    uint32_t w_max_window_used;
} mdbm_window_stats_t;

/**
 * Used to retrieve statistics about windowing usage on the associated database.
 *
 * The V3 statistics structure is as follows. It may be extended in later versions, in which
 * case the s_size parameter should be set to the sizeof the struct used for that version.
 * \code
 * typedef struct mdbm_window_stats {
 *     uint64_t w_num_reused;
 *     uint64_t w_num_remapped;
 *     uint32_t w_window_size;
 *     uint32_t w_max_window_used;
 * } mdbm_window_stats_t;
 * \endcode
 *
 * NOTE: V3 API
 *
 * \param[in,out] db Database handle
 * \param[out]    stats Pointer to struct that will receive the statistics
 * \param[in]     s_size Size of \a stats.
 *                Should be sizeof(mdbm_window_stats_t) for V3.
 * \return Get window stats status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_get_window_stats(MDBM* db, mdbm_window_stats_t* stats, size_t s_size);

/**
 * Counts the number of records in an MDBM.
 *
 * \param[in] db Database handle.
 *  \return Number of records stored in an MDBM (use instead of count_all_page).
 */
extern uint64_t mdbm_count_records(MDBM *db);

/**
 * Counts the number of pages used by an MDBM.
 *
 * \param[in] db Database handle.
 *  \return Number of pages: Count of all directory+data+LargeObject pages used by an MDBM
 */
extern mdbm_ubig_t mdbm_count_pages(MDBM *db);


/** \} StatisticsGroup */

/**
 * \defgroup CacheAndBackingStoreGroup Cache and Backing Store Group
 * The Cache and Backing Store Group contain the API for configuring caches
 * and backing stores.
 * \{
 */

#define MDBM_CACHEMODE_NONE     0 /**< No caching behavior */
#define MDBM_CACHEMODE_LFU      1 /**< Entry with smallest number of accesses is evicted */
#define MDBM_CACHEMODE_LRU      2 /**< Entry with oldest access time is evicted */
#define MDBM_CACHEMODE_GDSF     3 /**< Greedy dual-size frequency (size and frequency) eviction */
#define MDBM_CACHEMODE_MAX      3 /**< Maximum cache mode value */

#define MDBM_CACHEMODE_EVICT_CLEAN_FIRST  0x10 /**< add to cachemode to evict clean items 1st */
/** Extracts cache mode */
#define MDBM_CACHEMODE(m)       ((m)&3)
/** Defines a mask for the cache mode, including control (eviction) bit. */
#define MDBM_CACHEMODE_BITS     (3 | MDBM_CACHEMODE_EVICT_CLEAN_FIRST)

/**
 * Manage the database as a cache. mdbm_set_cachemode must be called before
 * data is inserted.  Tracking metadata is stored with each entry which allows
 * MDBM to do cache eviction via LRU, LFU, and GDSF
 * (greedy-dual-size-frequency). MDBM also supports clean/dirty tracking and
 * the application can supply a callback (see \ref mdbm_set_backingstore)
 * which is called by MDBM when a dirty entry is about to be evicted allowing
 * the application to sync the entry to a backing store or perform some other
 * type of "clean" operation.
 *
 * NOTE: V3 API
 *
 * \param[in,out] db Database handle
 * \param[in]     cachemode caching mode value (below)
 * \return Set cache mode status
 * \retval -1 Error, with errno set.
 *         - ENOSYS: wrong MDBM version
 *         - EINVAL: bad \a cachemode value, or empty MDBM
 *         - other: for locking errors
 * \retval  0 Success
 *
 * Values for \a cachemode:
 *   - MDBM_CACHEMODE_NONE - no cache mode
 *   - MDBM_CACHEMODE_LFU  - least frequently used
 *   - MDBM_CACHEMODE_LRU  - least recently used
 *   - MDBM_CACHEMODE_GDSF - greedy dual-size frequency
 */
extern int mdbm_set_cachemode(MDBM* db, int cachemode);

/**
 * Returns the current cache style of the database. See the cachemode parameter in
 * \ref mdbm_set_cachemode for the valid values.
 *
 * NOTE: V3 API
 *
 * \param[in,out] db Database handle
 * \return Current cache mode
 * \retval -1 error, with errno set.
 *         - ENOSYS: wrong MDBM version
 * \retval MDBM_CACHEMODE_NONE
 * \retval MDBM_CACHEMODE_LFU
 * \retval MDBM_CACHEMODE_LRU
 * \retval MDBM_CACHEMODE_GDSF
 */
extern int mdbm_get_cachemode(MDBM* db);

/**
 * Returns the cache mode as a string. See \ref mdbm_set_cachemode
 *
 * NOTE: V3 API
 *
 * \param[in,out] cachemode Cache mode type (below)
 * \return Printable string representing a valid cache mode, otherwise the string "unknown"
 *
 * Values for \a cachemode:
 *   - MDBM_CACHEMODE_NONE - no cache mode
 *   - MDBM_CACHEMODE_LFU  - least frequently used
 *   - MDBM_CACHEMODE_LRU  - least recently used
 *   - MDBM_CACHEMODE_GDSF - greedy dual-size frequency
 */
extern const char* mdbm_get_cachemode_name(int cachemode);

typedef struct mdbm_bsops {
    void* (*bs_init)(MDBM* db, const char* filename, void* opt, int flags);
    int   (*bs_term)(void* data, int flags);
    int   (*bs_lock)(void* data, const datum* key, int flags);
    int   (*bs_unlock)(void* data, const datum* key);
    int   (*bs_fetch)(void* data, const datum* key, datum* val, datum* buf, int flags);
    int   (*bs_store)(void* data, const datum* key, const datum* val, int flags);
    int   (*bs_delete)(void* data, const datum* key, int flags);
    void* (*bs_dup)(MDBM* db, MDBM* newdb, void* data);
} mdbm_bsops_t;

#define MDBM_BSOPS_FILE ((const mdbm_bsops_t*)-1) /**< Backing store File identifier */
#define MDBM_BSOPS_MDBM ((const mdbm_bsops_t*)-2) /**< Backing store MDBM identifier */

/**
 * The backing-store support controls the in-memory cache as a read-through,
 * write-through cache.  The backing-store interface uses a plugin model where
 * each plugin supplies lock, unlock, fetch, store, and delete functions. Two
 * predefined plugins are available: file-based(MDBM_BSOPS_FILE) and
 * MDBM-based (MDBM_BSOPS_MDBM).  Typically, a window will be set to access the
 * backing store via \ref mdbm_set_window_size since the backing store may be
 * very large. Please refer to \ref mdbm_set_window_size for window size
 * restrictions.
 *
 * NOTE: The file-based backing store (MDBM_BSOPS_FILE) is really just for 
 * demonstration purposes, and should not be used.
 *
 * An \ref mdbm_store on \a cachedb must be used to update data in the backing
 * store.  An in-place update (fetching the data address using \ref mdbm_fetch and
 * directly modifying the data within the mapped db) will not get written
 * through to the backing store.
 *
 * Once backing store is set on the opened cache, it cannot be removed from
 * the open cache.
 *
 * If you use MDBM_BSOPS_MDBM to set an MDBM backing store, the \a cachedb
 * will \e own the MDBM handle of the backing store, and \a cachedb will close
 * it when the cache MDBM handle is closed.  You should not make any MDBM
 * calls directly using the backing store MDBM handle after passing it to
 * mdbm_set_backingstore.
 *
 * NOTE: V3 API
 *
 * \param[in,out] cachedb Database handle (for the cache database)
 * \param[in]     bsops Can be MDBM_BSOPS_FILE, MDBM_BSOPS_MDBM, or user-supplied functions
 * \param[in]     opt Depends on the bsops parameter. (ex., for MDBM_BSOPS_FILE
 *                \a opt should be a file path, or for MDBM_BSOPS_MDBM \a opt should be an
 *                MDBM database handle (opened with MDBM_OPEN_WINDOWED flag))
 * \param[in]     flags Depends on the \a bsops.
 *                Specify 0 if \a bsops is MDBM_BSOPS_MDBM.
 *                Specify O_DIRECT to prevent paging out important data
 *                when the bsops is MDBM_BSOPS_FILE.
 * \return Set backingstore status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_set_backingstore(MDBM* cachedb, const mdbm_bsops_t* bsops, void* opt, int flags);

/** \} CacheAndBackingStoreGroup */


/**
 * \defgroup ImportExportGroup Import and Export Group
 * The Import and Export Group contains the APIs for reading and writing records to a file in
 * cdb_dump or db_dump format.
 *
 * Utility functions for converting from MDBM files into a textual representation.
 * These functions generate data in one of 2 formats:
 *   - The "printable" BerkeleyDB db_{dump,load} format
 *   - The cdb_dump format, which is a little more compact because it uses binary
 *      representation instead of escaped hex sequences: http://cr.yp.to/cdb/cdbmake.html
 * \{
 */

/**
 * Export API: Use cdb_dump format to write a key/value pair to file opened
 * with fopen.
 *
 * \param[in]     kv Key+Value pair
 * \param[in,out] fp FILE pointer (return value of fopen)
 * \return Dump kvpair to file status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_cdbdump_to_file(kvpair kv, FILE *fp);

/**
 * Export API: Finalize and close a file written in cdb_dump format and opened
 * with fopen.  Adds newline character as the file "trailer" and closes the
 * file.
 *
 * \param[in,out] fp FILE pointer (return value of fopen)
 * \return Dump trailer to file status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_cdbdump_trailer_and_close(FILE *fp);

/**
 * Export API: Use cdb_dump format to write a key/value pair to a malloc'd buffer
 *
 * \param[in]     kv Key+Value pair
 * \param[in,out] datasize Size of cdb_dump data to be added added to \a bufptr
 * \param[in,out] bufptr  malloc'd buffer.  bufptr may be realloc'd if \a
 *                datasize is greater than \a bufsize
 * \param[in]     bufsize Size of malloc'd buffer
 * \param[in]     buf_offset Offset in \a bufptr to start writing data
 * \return Add record status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_cdbdump_add_record(kvpair kv, uint32_t *datasize, char **bufptr,
                                   uint32_t *bufsize, uint32_t buf_offset);


/**
 * Export API: Use db_dump format to write a key/value pair to a file opened with fopen.
 *
 * \param[in]     kv Key+Value pair
 * \param[in,out] fp FILE pointer (return value of fopen)
 * \return Dump to file status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_dbdump_to_file(kvpair kv, FILE *fp);

/**
 * Export API: Finalize and close a file written in db_dump format and opened
 * with fopen.  No trailer written: just closes the file (using fclose).
 *
 * \param[in,out] fp FILE pointer (return value of fopen)
 * \return Dump trailer status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int mdbm_dbdump_trailer_and_close(FILE *fp);

/**
 * Export API: Use db_dump format to write a key/value pair to a malloc'd buffer.
 * mdbm_dbdump_add_record may realloc \a bufptr.
 *
 * \param[in]     kv Key+Value pair
 * \param[in,out] datasize Size of db_dump data to be added added to \a bufptr
 * \param[in,out] bufptr  malloc'd buffer.  bufptr may be realloc'd if \a
 *                datasize is greater than \a bufsize
 * \param[in]     bufsize Size of malloc'd buffer
 * \param[in]     buf_offset Offset in \a bufptr to start writing data
 * \return Dump add record status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 */
extern int
mdbm_dbdump_add_record(kvpair kv, uint32_t *datasize,
                       char **bufptr, uint32_t *bufsize, uint32_t buf_offset);

/**
 * Export API: Write the DBdump header to FILE
 *
 * \param[in,out]  db     handle of MDBM whose data is being exported
 * \param[in]    fp       file handle/pointer to read from
 */
extern int mdbm_dbdump_export_header(MDBM *db, FILE *fp);

/**
 * Import API: Read the DBdump header from FILE
 * Function will set, if available at the beginning of the file pointed to by "fp",
 * the following parameter values:
 *
 * \param[in]    fp       file handle/pointer to read from
 * \param[out]   pgsize   page-size read from "fp"
 * \param[out]   pgcount  page-count read from "fp"
 * \param[out]   large    large-object-support
 * \param[out]   lineno   The line number following the header (for tracking progress)
 */
extern int
mdbm_dbdump_import_header(FILE *fp, int *pgsize, int *pgcount, int *large, uint32_t *lineno);

/**
 * Import API: Read data from FILE into MDBM, using DBdump format.
 *   MDBM locking and unlocking is done one record at a time based on locking set up when
 *   the MDBM was opened.
 *
 * \param[in,out]  db          handle of MDBM into which data is being imported
 * \param[in]      fp          file handle/pointer to read from
 * \param[in]      input_file  name of the file pointed to by "fp".
 * \param[in]      store_flag  MDBM_INSERT | MDBM_REPLACE | MDBM_INSERT_DUP | MDBM_MODIFY
 * \param[in,out]  lineno      current line number, keep track of the current line number
 *                             to generate meaningful error messages.  Precondition: set to 1
 *                             or pass line number previously set by mdbm_dbdump_import_header.
 *                             Postcondition: lineno will hold the last line number read
 *                             successfully.
 */
extern int
mdbm_dbdump_import(MDBM *db, FILE *fp, const char *input_file, int store_flag, uint32_t *lineno);

/**
 * Import API: Read data from FILE into MDBM, using Cdb format.
 *   MDBM locking and unlocking is done one record at a time based on locking set up when
 *   the MDBM was opened.
 *
 * \param[in,out]  db          handle of MDBM into which data is being imported
 * \param[in]      fp          file handle/pointer to read from
 * \param[in]      input_file  name of the file pointed to by "fp".
 * \param[in]      store_flag  MDBM_INSERT | MDBM_REPLACE | MDBM_INSERT_DUP | MDBM_MODIFY
 */
extern int mdbm_cdbdump_import(MDBM *db, FILE *fp, const char *input_file, int store_flag);

/** \} ImportExportGroup */

/**
 * \defgroup MiscellaneousGroup Miscellaneous Group
 * The Miscellaneous Group contains the API for routines that don't clearly
 * fit into any of the other module API groups.
 * \{
 */

/**
 * Returns the value of internally saved errno.  Contains the value of errno
 * that is set during some lock failures.  Under other circumstances,
 * mdbm_get_errno will not return the actual value of the errno variable.
 *
 * \param[in,out] db Database handle
 * \return Saved errno value, or zero if OK
 */
extern int mdbm_get_errno(MDBM *db);

/**
 * Given a hash function code, get the hash value for the given key.
 * See \ref mdbm_sethash for the list of valid hash function codes.
 *
 * \param[in]  key Key
 * \param[in]  hashFunctionCode for a valid hash function (below)
 * \param[out] hashValue is calculated according to the \a hashFunctionCode
 * \return Get Hash status
 * \retval -1 Error, and errno is set
 * \retval  0 Success
 *
 * Values for \a hashFunctionCode:
 *   - MDBM_HASH_CRC32   - Table based 32bit CRC
 *   - MDBM_HASH_EJB     - From hsearch
 *   - MDBM_HASH_PHONG   - Congruential hash
 *   - MDBM_HASH_OZ      - From sdbm
 *   - MDBM_HASH_TOREK   - From BerkeleyDB
 *   - MDBM_HASH_FNV     - Fowler/Vo/Noll hash (DEFAULT)
 *   - MDBM_HASH_STL     - STL string hash
 *   - MDBM_HASH_MD5     - MD5
 *   - MDBM_HASH_SHA_1   - SHA_1
 *   - MDBM_HASH_JENKINS - Jenkins string
 *   - MDBM_HASH_HSIEH   - Hsieh SuperFast
 */
extern int mdbm_get_hash_value(datum key, int hashFunctionCode, uint32_t *hashValue);

/**
 * Gets the MDBM page number for a given key.
 * The key does not actually have to exist.
 *
 * \param[in,out] db Database handle
 * \param[in]     key Lookup key
 * \return Page number or error indicator
 * \retval -1 Error, and errno is set
 * \retval Page number where the parameter key would be stored.
 */
extern mdbm_ubig_t mdbm_get_page(MDBM *db, const datum *key);

/**
 * Preload mdbm: Read every 4k bytes to force all pages into memory
 *
 * \param[in,out] db Database handle
 * \return preload status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_preload(MDBM* db);


/**
 * Check mdbm page residency: count the number of DB pages mapped into memory.
 * The counts are in units of the system-page-size (typically 4k)
 *
 * \param[in,out] db Database handle
 * \param[out] pgs_in count of memory resident pages 
 * \param[out] pgs_out count of swapped out pages 
 * \return check status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_check_residency(MDBM* db, mdbm_ubig_t *pgs_in, mdbm_ubig_t *pgs_out);

/**
 * Make a file sparse. Read every blockssize bytes and for all-zero blocks punch a hole in the file.
 * This can make a file with lots of zero-bytes use less disk space.
 * NOTE: For MDBM files that may be modified, the DB should be opened, and exclusive-locked
 * for the duration of the sparsify operation.
 * NOTE: This function is linux-only.
 *
 * \param[in] filename Name of the file to make sparse.
 * \param[in] blocksize Minimum size to consider for hole-punching, <=0 to use the system block-size
 * \return sparsify status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_sparsify_file(const char* filename, int blocksize);

/**
 * mdbm_lock_pages: Locks MDBM data pages into memory.
 *
 * When running MDBM as root, mdbm_lock_pages will expand the amount of RAM that can be locked to
 * infinity using setrlimit(RLIMIT_MEMLOCK).  When not running as root, mdbm_lock_pages will expand
 * the amount of RAM that can be locked up to the maximum allowed (retrieved using getrlimit(MEMLOCK),
 * and normally a very small amount), and if the MDBM is larger than the amount of RAM that can be
 * locked, a warning will be logged but mdbm_lock_pages will return 0 for success.
 *
 * \param[in,out] db Database handle
 * \return lock pages status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_lock_pages(MDBM* db);

/**
 * mdbm_unlock_pages: Releases MDBM data pages from always staying in memory
 *
 * \param[in,out] db Database handle
 * \return unlock pages status
 * \retval -1 Error
 * \retval  0 Success
 */
extern int mdbm_unlock_pages(MDBM* db);

/** \} MiscellaneousGroup */



/*
 * TODO
 * TODO Merge remnants are here. Verify where they should go.
 * TODO
 */



#define MDBM_MINPAGE    128      /**< Size should be >= header, but this cuts off header stats, which is ok */
#define MDBM_PAGE_ALIGN 64

/* Maximum page size is 16MB-64bytes, because any page size above that would be rounded to 16MB,
 * which does not fit in the 24 bits allocated for the in-page offset to an object */
#define MDBM_MAXPAGE    ((16*1024*1024) - MDBM_PAGE_ALIGN)

#define MDBM_PAGESIZ    4096    /**< A good default. Size should be >= header  */
#define MDBM_MIN_PSHIFT 7       /**< this must match MDBM_MINPAGE above */
#define MDBM_MAX_SHIFT  ((sizeof( void * )*8)-1) /**< Base # shifts on ptr size */

extern  void    mdbm_lock_dump(MDBM* db);


int mdbm_get_db_stats(MDBM* db, mdbm_db_info_t* dbinfo, mdbm_stat_info_t* dbstats, int flags);


/* If we're using OpenSSL, map the FreeBSD symbol names (used
* within mdbm) to match the OpenSSL symbol names. */
#ifdef USE_OPENSSL
# define MD5Init   MD5_Init
# define MD5Final  MD5_Final
# define MD5Update MD5_Update
#endif /* USE_OPENSSL */


typedef mdbm_ubig_t ((* mdbm_hash_t)(const uint8_t *buf, int len));
extern mdbm_hash_t mdbm_hash_funcs[];

/*
 * Hash functions
 */
#define MDBM_HASH_CRC32         0       /**< table based 32bit crc */
#define MDBM_HASH_EJB           1       /**< from hsearch */
#define MDBM_HASH_PHONG         2       /**< congruential hash */
#define MDBM_HASH_OZ            3       /**< from sdbm */
#define MDBM_HASH_TOREK         4       /**< from Berkeley db */
#define MDBM_HASH_FNV           5       /**< Fowler/Vo/Noll hash */
#define MDBM_HASH_STL           6       /**< STL string hash */
#define MDBM_HASH_MD5           7       /**< MD5 */
#define MDBM_HASH_SHA_1         8       /**< SHA_1 */
#define MDBM_HASH_JENKINS       9       /**< JENKINS */
#define MDBM_HASH_HSIEH         10      /**< HSIEH SuperFastHash */
#define MDBM_MAX_HASH           10      /* bump up if adding more */

/** Define the hash function to use on a newly created file */
#ifndef MDBM_DEFAULT_HASH
#  define MDBM_DEFAULT_HASH     5       /* MDBM_HASH_FNV is best */
#endif

#define MDBM_CONFIG_DEFAULT_HASH        MDBM_HASH_FNV


#define MDBM_BAD_HASH_NO(n)     ((n) < 0 || (n) > MDBM_MAX_HASH)

typedef uint32_t mdbm_hashval_t;
/*typedef mdbm_hashval_t (*mdbm_hash_t)(const unsigned char *buf, int len); */
/*extern mdbm_hash_t mdbm_hash_funcs[]; */
/*#define MDBM_HASH_MD5           7     */ /* MD5 */
/*#define MDBM_HASH_SHA_1         8     */ /* SHA_1 */
/*#define MDBM_MAX_HASH           10    */
#define MDBM_HASH_MAX           MDBM_MAX_HASH

/* Which hash function to use on a newly created file */
/*#define MDBM_DEFAULT_HASH       MDBM_CONFIG_DEFAULT_HASH */

/* count_all_page() commented out: use mdbm_count_records() instead */
/* extern  mdbm_ubig_t     count_all_page(MDBM *db); */

/* optional - may be user supplied.  */
extern  mdbm_ubig_t  mdbm_hash(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash0(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash1(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash2(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash3(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash4(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash5(unsigned char *, int);
extern  mdbm_ubig_t  mdbm_hash6(unsigned char *, int);


#ifdef  __cplusplus
}
#endif

#endif  /* MDBM_H_ONCE */

