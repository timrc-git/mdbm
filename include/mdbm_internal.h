/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM3_INTERNAL_H_ONCE
#define MDBM3_INTERNAL_H_ONCE

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <syslog.h>

#ifdef __linux__
#include <sys/vfs.h>
#endif

#include "mdbm_shmem.h"
#include "mdbm_stats.h"

#ifdef __MACH__
/* TODO use fcntl() and F_NOCACHE instead of O_DIRECT */
#  define O_DIRECT 0
#  define O_NOATIME 0
#  define mincore_vec_type char*
#else
#define mincore_vec_type unsigned char*
#endif


/*

MDBM structure:

The MDBM file is broken up into "chunks", which are 1 or more contiguous physical pages
(in units of the db-page-size).

Each chunk starts with a mdbm_page_t structure (including the initial one).
Each chunk contains a size (p_num_pages), and an offset to the previous chunk 
(p_prev_num_pages). Together, these form a doubly linked list.

The p_type value near the start of each chunk identifies the type, 
MDBM_PTYPE_ xxx, where xxx is DIR, DATA, LOB, or FREE.

The first chunk (type DIR) will contain the file header, directory bit vector, and 
page-table.
Successive chunks include normal pages, oversized pages, large-object pages,
and free-pages.

Free-pages (type FREE) contain a "next" pointer (p_next_free), and the first page is 
referenced by h_first_free, forming a singly-linked-list. Newly freed pages should 
be inserted into the free list sorted by address, and adjacent free pages should be 
coalesced into a single chunk.

The directory is a linear array of bits, used to convert hash values of keys
into "logical pages". Logical pages determine an index into the page table,
which is a linear array of integers that point to actual physical pages in the db.
This allows physical pages to be changed (moved/expanded/defragged/etc) very quickly.

Data pages (type DATA) contain a series of entry meta-data structures (mdbm_entry_t) 
at the beginning (lowest VMA) of the page, and the actual key and value data grows 
down from the end (highest VMA) of the page(s). Oversize data pages may span multiple 
contiguous physical pages.
The entry structure contains the length and part of the hash for the key to enable
fast scanning of a page. Deleted entries are signaled by a key length of 0.
(This allows fast deletion, at the cost of possible defragmentation later.)

Large-object (type LOB) pages contain db entries that would overwhelm a single 
data-page. Because we return found entries to users as a pointer,length pair, we 
can't fragment these entries, so they are stored externally to the data page to 
which they logically belong. The l_pagenum entry points back to the logical page 
the large-object belongs to.

NOTE: some DBs may contain an extra, unmerged free-page at the end of the DB, which
is not listed in the free-list. The existence of h_num_pages (which gives the last
physical page in the DB) and h_last_chunk (which gives the last used physical chunk
in the db) creates some ambiguity, and the various check_xxx() functions stop at 
h_last_chunk. It is currently unclear if this ambiguity is by design, or accidental.


*/

#ifdef  __cplusplus
extern "C" {
#endif

/* FIXME hacky, but it seems to work on linux... (NOTE: sizeof(int) doesn't) */
#define IS_32BIT() (sizeof(long)==(32/8))

#define MDBM_HEADER_ONLY        0x40000000      /* map header only (internal use) */
#define MDBM_CREATE_V2          0x10000000  /* create a V2 db */

#define MDBM_LOCK_MODE_MASK \
  (MDBM_PARTITIONED_LOCKS | MDBM_RW_LOCKS | MDBM_OPEN_NOLOCK)

#define MDBM_LOCK_MASK \
  (MDBM_SINGLE_ARCH | MDBM_ANY_LOCKS | MDBM_LOCK_MODE_MASK)

#define MDBM_STAT_TAG_FETCHES           1
#define MDBM_STAT_TAG_STORES            2
#define MDBM_STAT_TAG_DELETES           3
#define MDBM_STAT_TAG_LOCK              4
#define MDBM_STAT_TAG_FETCHES_UNCACHED  5
#define MDBM_STAT_TAG_GETPAGE           6
#define MDBM_STAT_TAG_GETPAGE_UNCACHED  7
#define MDBM_STAT_TAG_CACHE_EVICT       8
#define MDBM_STAT_TAG_CACHE_STORE       9
#define MDBM_STAT_TAG_PAGE_STORE        10
#define MDBM_STAT_TAG_PAGE_DELETE       11
#define MDBM_STAT_TAG_FLUSH             12
 

/**
 * * Define/Reserve a 20 entry mdbm_counter_t storage structure.
 */
typedef struct mdbm_hdr_stats {
  /* NOTE: ONLY USE EXPLICITLY SIZED TYPES FOR ON-DISK STRUCTURES */
    mdbm_counter_t      s_fetches;
    uint64_t            s_last_fetch;
    mdbm_counter_t      s_stores;
    uint64_t            s_last_store;
    mdbm_counter_t      s_deletes;
    uint64_t            s_last_delete;
#if 0
    mdbm_counter_t      s_locks;
    mdbm_counter_t      s_locks_waited;
    mdbm_counter_t      s_locks_wait_time;
    mdbm_counter_t      s_pages_gc;
#else
    mdbm_counter_t      s_reserved[14];
#endif
} mdbm_hdr_stats_t;

typedef struct mdbm_hdr {
  /* NOTE: ONLY USE EXPLICITLY SIZED TYPES FOR ON-DISK STRUCTURES */
    uint32_t            h_magic;         /* magic number, determines filetype */
    uint16_t            h_dbflags;       /* options: large-objects, etc. */
    uint8_t             h_cache_mode;    /* cache type */
    uint8_t             h_pad1;          /* unused padding */
    uint8_t             h_dir_shift;     /* log2(logical_pages) */
    uint8_t             h_hash_func;     /* hash function used, determines key->page mapping */
    uint8_t             h_max_dir_shift; /* user-set maximum number of log2(logical_pages) */
    uint32_t            h_dir_gen;       /* "generation" (as in age) change counter */
    uint32_t            h_pagesize;      /* size of a single (non-oversize) db-page */
    uint32_t            h_num_pages;     /* current number of pages */
    uint32_t            h_max_pages;     /* maximum pages we're allowed to grow to */
    uint32_t            h_spill_size;    /* threshold for deciding an object is "large" */
    uint32_t            h_last_chunk;    /* last group of pages in the DB */
    uint32_t            h_first_free;    /* First free page number */
    uint32_t            h_pad4[8];       /* unused padding (future expansion) */
    mdbm_hdr_stats_t    h_stats;         /* store/fetch/delete statistics */
} mdbm_hdr_t;
#define MDBM_HDR_T_SIZE sizeof(mdbm_hdr_t)

/* Break the compile if someone accidentally changes the header field size */
#define CCASSERT(predicate, name) \
  extern char CONSTRAINT_VIOLATED__##name[2*((predicate)!=0)-1];
CCASSERT(sizeof(mdbm_hdr_t)==232, MDBM3_HEADER_SIZE_CHANGE__THIS_BREAKS_FILE_COMPATIBILITY)

#define MDBM_HFLAG_ALIGN_2      0x0001  /* MDBM_ALIGN_16_BITS */
#define MDBM_HFLAG_ALIGN_4      0x0003  /* MDBM_ALIGN_32_BITS */
#define MDBM_HFLAG_ALIGN_8      0x0007  /* MDBM_ALIGN_64_BITS */
#define MDBM_ALIGN_MASK         0x0007

#define MDBM_HFLAG_PERFECT      0x0008
#define MDBM_HFLAG_REPLACED     0x0010
#define MDBM_HFLAG_LARGEOBJ     0x0020

#define MDBM_DIRSHIFT_MAX       24
#define MDBM_NUMPAGES_MAX       (1<<MDBM_DIRSHIFT_MAX)

#define MDBM_PAGENUM_TO_PARTITION(db, pageno) (pageno % db_get_part_count(db))

extern int mdbm_get_partition_number(const MDBM* db, datum key);

mdbm_hdr_t* mdbm_get_header (MDBM* db);
extern mdbm_hdr_stats_t* mdbm_hdr_stats(MDBM* db);
uint32_t mdbm_get_version (MDBM* db);

extern int remap_is_limited(uint32_t sys_pagesize);

extern const char* MDBM_HASH_FUNCNAMES[];

extern const char* mdbm_get_filename(MDBM* db);

extern int mdbm_get_fd(MDBM *db);

extern int mdbm_init_rstats(MDBM* db, int flags);

#define MDBM_SANITY_CHECKS
#define MDBM_BSOPS

extern int mdbm_sanity_check;

#define CHECK_DB_PARTIAL(db,l)  \
    if (mdbm_sanity_check && (check_db(db,mdbm_sanity_check,l,1) > 0)) { \
      mdbm_log(LOG_ERR, "fail CHECK_DB_PARTIAL, aborting...\n"); \
      abort(); \
    } 

#define CHECK_DB(db)    CHECK_DB_PARTIAL(db,10)

#ifdef MDBM_SANITY_CHECKS
#define CHECK_LOCK_DB_ISOWNED   \
  if (!lock_db_isowned(db)) { \
    mdbm_log(LOG_ERR, "fail CHECK_LOCK_DB_ISOWNED, aborting...\n"); \
    abort(); \
  }
#else
#define CHECK_LOCK_DB_ISOWNED
#endif

/* Be aware, since these data structures are written to disk and the ordering of how
 * bit fields are packed within a 32 bit field is implementation dependent, future
 * versions of the gcc compiler may cause MDBM ABI breakage, i.e. in the future
 * mdbm_page.p_flags and mdbm_page.p_type could be placed ahead of mdbm_page.p_num.
 */

typedef uint32_t mdbm_pagenum_t;
typedef uint8_t mdbm_dir_t;

typedef struct mdbm_page {
    union {
        uint32_t p_data;         /* MDBM_PTYPE_DIR,  _MDBM_MAGIC_NEW2 for page 0, otherwise 0 */
        uint32_t p_num_entries;  /* MDBM_PTYPE_DATA, entry count */
        uint32_t p_next_free;    /* MDBM_PTYPE_FREE, next free page index */
        uint32_t p_vallen;       /* MDBM_PTYPE_LOB,  large object value size */
    } p;
    unsigned int p_num:24;       /* page number that this chunk belongs to */
    unsigned int p_type:4;       /* labels as data/dir/lob/free/etc */
    unsigned int p_flags:4;      /* uniformly 0, why is this here? */
    unsigned int p_num_pages:24; /* chunk size in db-pages */
    unsigned int p_r0:8;         /* uniformly 0, padding */
    unsigned int p_prev_num_pages:24; /* size of previous chunk, for backwards chaining chunks */
    unsigned int p_r1:8;         /* uniformly 0, padding */
} mdbm_page_t;

#define MDBM_PAGE_T_SIZE        16

typedef struct mdbm_ptentry {
    unsigned int pt_pagenum:24;  /* index for the page data, or 0 */
    unsigned int pt_r0:8;        /* padding */
} mdbm_ptentry_t;
#define MDBM_PTENTRY_T_SIZE     4


typedef struct mdbm_entry {
    union {
        struct {
            unsigned int len:16;   /* key length */
            unsigned int hash:16;  /* cached hash value (hi 16 of 32-bits) for fast compares */
        } key;
        uint32_t match;            /* len and hash bundled for fast compare */
    } e_key;
    unsigned int e_offset:24;      /* allows for byte-alignment */
    unsigned int e_flags:8;        /* LOB and caching flags (dirty, sync-error, etc.) */
} mdbm_entry_t;
#define MDBM_ENTRY_T_SIZE       8

#define MDBM_EFLAG_PAD_MASK     0x07
#define MDBM_EFLAG_LARGEOBJ     0x08
#define MDBM_EFLAG_DIRTY        0x10
#define MDBM_EFLAG_SYNC_ERROR   0x20

#define MDBM_TOP_OF_PAGE_MARKER 0xffff0000

typedef struct mdbm_entry_lob {
    unsigned int l_pagenum:24;    /* Data page this LOB is logically on. */
    unsigned int l_flags:8;       /* LOB flags (uniformly 0 for now). */
    uint32_t l_vallen;            /* Size of the LOB */
} mdbm_entry_lob_t;
#define MDBM_ENTRY_LOB_T_SIZE   8

typedef struct mdbm_entry_data {
    mdbm_page_t* e_page;
    mdbm_entry_t* e_entry;
    int e_index;
} mdbm_entry_data_t;

typedef struct mdbm_cache_entry { /* cache data for eviction decisions */
    uint32_t    c_num_accesses;   /* access count for most-heavily-used */
    union {
        uint32_t        access_time; /* last access time for least-recently-used */
        float           priority;    /* arbitrary priority for retention */
    } c_dat;
} mdbm_cache_entry_t;
#define MDBM_CACHE_ENTRY_T_SIZE 8

typedef struct mdbm_wpage {  /* window page entry */
    TAILQ_ENTRY(mdbm_wpage)     hash_link;  /* link to next entry */
    int                         pagenum;    /* index of the page */
    uint32_t                    num_pages;  /* count of pages (db sized?) */
    uint32_t                    mapped_off; /* offset from (file-start, bytes mod syspage size) */
    uint32_t                    mapped_len; /* bytes mapped in this chunk (mod syspage size) */
} mdbm_wpage_t;

typedef TAILQ_HEAD(mdbm_wpage_head_s, mdbm_wpage) mdbm_wpage_head_t;

typedef struct mdbm_window_data {
    char*                       base;           /* start of the memory map */
    size_t                      base_len;       /* length of the memory map  */
    int                         num_pages;      /* number of pages in the window */
    int                         first_free;     /* index (units of db pagesize) */
    int                         max_first_free; /* largest free chunk? (db pagesize) */
    mdbm_wpage_t*               wpages;         /* array of entries (allocated as 1 chunk) */

    mdbm_wpage_head_t           *buckets;       /* TAILQ-style start of window-pages list */
    uint32_t                    num_buckets;

    uint64_t                    num_reused;     /* stats for re-use (already mapped in window) */
    uint64_t                    num_remapped;   /* stats for re-map (not currently in window) */
} mdbm_window_data_t;


/* This structure is used for communicating mapping changes
 * between threads sharing a dup(licate) MDBM handle.
 * Given that, perhaps the fields should be volatile/sig_atomic_t/etc
 * especially dup_map_gen and dup_map_gen_marker.
 */
typedef struct mdbm_dup_info {
    uint64_t    dup_map_gen; /* count of the last started update */
    uint64_t    nrefs;       /* number of handles using the map */

    int         dup_fd;             /* shared file descriptor (vs per-handle) */
    char*       dup_base;           /* shared mmap start */
    size_t      dup_base_len;       /* shared mmap len */
    /* char*       dup_page_map_base; */
    /* int         dup_page_map_size; */

    uint64_t    dup_map_gen_marker; /* count of the last completed update */
} mdbm_dup_info_t;

typedef uint64_t (*mdbm_time_func_t)();

#define MDBM_DBVER_3    (-3)
#define MDBM_RSTATS_VERSION_0   (MDBM_RSTATS_VERSION-1)
#define MDBM_RSTATS_VERSION_1   (MDBM_RSTATS_VERSION)

struct mdbm_locks;


struct mdbm {
    uint32_t            guard_padding_1;  /* Guard padding against handle corruption */
    intptr_t            db_ver_flag;  /* db version (h_magic) */
    int                 db_flags;     /* flags: permissions, large-object, etc */
    int                 db_cache_mode;/* cache type */ 
    struct mdbm_locks*  db_locks;     /* locking structure */
    int                 db_fd;        /* file descriptor for the mmap */
    mdbm_hdr_t*         db_hdr;       /* (mdbm_hdr_t*)(db_base + MDBM_PAGE_T_SIZE); */
    char*               db_base;      /* start of mmap  */
    size_t              db_base_len;  /* length of mmap */
    int                 db_pagesize;  /* size of a single (non-oversize) page */
    unsigned            db_num_pages; /* number of physical pages in the db (h_num_pages) */
    int                 db_align_mask;/* alignment requirement for keys and values */
    mdbm_dir_t*         db_dir;       /* (mdbm_dir_t*)malloc(MDBM_DIR_SIZE(hdr->h_dir_shift)); */
    int                 db_dir_flags; /* directory flags: perfect hash, etc */
    uint32_t            db_dir_gen;   /* "generation" (as in age) change counter */
    int                 db_dir_shift; /* log2(logical_pages) */
    int                 db_max_dir_shift; /* user-set maximum number of log2(logical_pages) */
    int                 db_max_dirbit;/* it's actually the number of logical pages */
    int                 db_errno;     /* saved errno for some operations */
    mdbm_ptentry_t*     db_ptable;    /* MDBM_PTABLE_PTR(MDBM_DIR_PTR(db),db->db_dir_shift); */
    uint32_t            guard_padding_2;  /* Guard padding against handle corruption */
    mdbm_hash_t         db_hashfunc;  /* hash function used, determines key->page mapping */
    MDBM_ITER           db_iter;      /* iterator for internal use */
    mdbm_shake_func_v3  db_shake_func;/* user function to make room on a page */
    void*               db_shake_data;/* opaque user data for shake function */
    int                 db_spillsize; /* threshold for deciding an object is "large" */
    char                db_filename[MAXPATHLEN+1];
    mdbm_clean_func     db_clean_func; /* user provided clean function */
    void*               db_clean_data; /* user provided clean data */
#ifdef MDBM_BSOPS
    const mdbm_bsops_t* db_bsops;     /* backing store (user) function structure */
    void*               db_bsops_data;/* opaque backing store data */
#endif
    mdbm_rstats_t*      db_rstats;    /* realtime statistics structure (shared memory) */
    struct mdbm_rstats_mem* db_rstats_mem;
    uint32_t            guard_padding_3;  /* Guard padding against handle corruption */
    mdbm_window_data_t  db_window;    /* "window" data for partially mmap-ing the db */
    uint64_t            db_lock_wait; /* locking latency time */
    uint32_t            db_sys_pagesize; /* system (OS) page size */
    mdbm_dup_info_t*    db_dup_info;    /* info for shared mmaps (dup'ed handle) */  
    uint64_t            db_dup_map_gen; /* last update from dup_info  */
    void*               m_stat_cb_user; /* opaque user stats callback data */
    int                 m_stat_cb_flags;/* user stats callback options */
    mdbm_stat_cb        m_stat_cb;      /* user stats callback function */
    int             db_stats_flags;
    mdbm_time_func_t    db_get_usec;
    uint32_t            guard_padding_4;  /* Guard padding against handle corruption */
};


#define MDBM_DBFLAG_RDONLY      0x01
#define MDBM_DBFLAG_ASYNC       0x02
#define MDBM_DBFLAG_FSYNC       0x04
#define MDBM_DBFLAG_RWLOCKS     0x08

#define MDBM_DBFLAG_SHAKE_MASK  0x30
#define MDBM_DBFLAG_SHAKE_V1    0x00
#define MDBM_DBFLAG_SHAKE_V2    0x10
#define MDBM_DBFLAG_SHAKE_V3    0x20

#define MDBM_DBFLAG_HDRONLY     0x100
#define MDBM_DBFLAG_PROTECT     0x200
#define MDBM_DBFLAG_WINDOWED    0x400
#define MDBM_DBFLAG_NOLOCK      0x800

#define MDBM_DBFLAG_RSTATS_THIST 0x1000
#define MDBM_DBFLAG_HUGEPAGES   0x2000
#define MDBM_DBFLAG_NO_DIRTY    0x4000

/* #define MDBM_DBFLAG_STAT_OPERATIONS 0x00010000 */
/* #define MDBM_DBFLAG_MEMONLYCACHE    0x00020000 // V3 version collides with MDBM_ANY_LOCKS */
#define MDBM_DBFLAG_MEMONLYCACHE    0x00008000
#define MDBM_DBFLAG_LOCK_PAGES      0x00040000

#ifdef __linux__
#define MDBM_HUGETLBFS_MAGIC    0x958458f6
#endif

static inline int MDBM_DBFLAG_SHAKE_VER(const MDBM* db)
{
    return db->db_flags & MDBM_DBFLAG_SHAKE_MASK;
}

static inline int
MDBM_DB_CACHEMODE(const MDBM* db)
{
    return MDBM_CACHEMODE(db->db_cache_mode);
}

#define MDBM_LOCK_EXCLUSIVE     (-1)
#define MDBM_LOCK_SHARED        (-2)

#define MDBM_LOCK_WAIT          0
#define MDBM_LOCK_NOWAIT        1

#define MDBM_LOCK_READ          0
#define MDBM_LOCK_WRITE         1

#define MDBM_LOCK_NOCHECK       0
#define MDBM_LOCK_CHECK         1

#define MDBM_NUM_PLOCKS         128

/* 
 * The functions below, called mdbm_internal_* are MDBM-internal and should not be used
 * by external users.  Their names were changed due to the introduction of non-statically
 * scoped functions due to the addition of libmdbmcommon, which caused namespace collisions
 * with other software that links with the MDBM libraries.
 */
extern void mdbm_internal_close_locks(MDBM* db);
extern int mdbm_internal_open_locks(MDBM* db, int flags, int do_lock, int* need_check);
extern int mdbm_internal_lock(MDBM* db);
extern int mdbm_internal_unlock(MDBM* db);
extern int mdbm_internal_do_lock(MDBM* db, int write, int nonblock, const datum* key,
                                 mdbm_hashval_t* hashval, mdbm_pagenum_t* pagenum);
extern int mdbm_internal_do_unlock(MDBM* db, const datum* key);
extern int mdbm_internal_fcopy(MDBM* db, int fd, int flags, int tries);

extern int do_lock_x(MDBM* db, int pageno, int nonblock, int check);
extern int do_unlock_x(MDBM* db);
extern uint32_t db_get_lockmode(MDBM *db);
extern int db_get_part_count(MDBM* db);
extern int lock_db_isowned(MDBM* db);
extern int db_is_locked(MDBM* db);
extern int db_is_owned(MDBM* db);
extern int db_is_multi_lock(MDBM* db);
extern int db_multi_part_locked(MDBM* db);
extern int db_internal_is_owned(MDBM* db);
extern int do_lock_reset(const char* dbfilename, int flags);




static inline int
do_read_lock(MDBM* db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_READ,MDBM_LOCK_WAIT,NULL,NULL,NULL);
}

static inline int
do_write_lock(MDBM* db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_WRITE,MDBM_LOCK_WAIT,NULL,NULL,NULL);
}

static inline int
lock_db(MDBM* db)
{
    return do_write_lock(db);
}

/* Get exclusive lock (prevent modification), without write perm check. */
static inline int
lock_db_ex(MDBM* db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_EXCLUSIVE,MDBM_LOCK_WAIT,NULL,NULL,NULL);
}


static inline void
unlock_db(MDBM* db)
{
    (void)mdbm_internal_do_unlock(db,NULL);
}

static inline int
trylock_db(MDBM* db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_WRITE,MDBM_LOCK_NOWAIT,NULL,NULL,NULL);
}

static inline int
MDBM_USE_PROTECT(const MDBM* db)
{
    return db->db_flags & MDBM_DBFLAG_PROTECT;
}

static inline int
MDBM_NOLOCK(const MDBM* db)
{
    return db->db_flags & MDBM_DBFLAG_NOLOCK;
}

static inline int
MDBM_RWLOCKS(const MDBM* db)
{
    return db->db_flags & MDBM_DBFLAG_RWLOCKS;
}


static inline int
lock_internal_isowned(MDBM* db)
{
    return MDBM_NOLOCK(db) || db_internal_is_owned(db);
}

#define CHECK_LOCK_INTERNAL_ISOWNED \
  if (!lock_internal_isowned(db) && !mdbm_isowned(db)) { \
    mdbm_log(LOG_ERR, "fail CHECK_LOCK_INTERNAL_ISOWNED, aborting...\n"); \
    abort(); \
  }



static inline void
MDBM_INIT_TOP_ENTRY(mdbm_entry_t* ep, int offset)
{
    ep->e_offset = offset;
    ep->e_flags = 0;
    ep->e_key.match = MDBM_TOP_OF_PAGE_MARKER;
}

static inline mdbm_entry_t*
MDBM_ENTRY(const mdbm_page_t* page, int index)
{
    return (mdbm_entry_t*)((char*)page + MDBM_PAGE_T_SIZE) + index;
}

static inline int
MDBM_ENTRY_INDEX(const mdbm_page_t* page, const mdbm_entry_t* ep)
{
    return ep - MDBM_ENTRY(page,0);
}

static inline int
MDBM_ENTRY_LARGEOBJ(const mdbm_entry_t* ep)
{
    return (ep->e_flags & MDBM_EFLAG_LARGEOBJ) != 0;
}

static inline int
MDBM_ENTRY_DIRTY(const mdbm_entry_t* ep)
{
    return (ep->e_flags & MDBM_EFLAG_DIRTY) != 0;
}

static inline int
MDBM_KEY_OFFSET(const mdbm_entry_t* ep)
{
    return ep->e_offset;
}

static inline char*
MDBM_KEY_PTR(const mdbm_entry_data_t* e)
{
    return (char*)e->e_page + MDBM_KEY_OFFSET(e->e_entry);
}

static inline char*
MDBM_KEY_PTR1 (const mdbm_page_t* p, const mdbm_entry_t* ep)
{
    return (char*)p + MDBM_KEY_OFFSET(ep);
}

static inline int
MDBM_KEY_LEN(const mdbm_entry_data_t* e)
{
    return e->e_entry->e_key.key.len;
}

static inline int
MDBM_KEY_LEN1 (const mdbm_entry_t* ep)
{
    return ep->e_key.key.len;
}

static inline int
MDBM_ALIGN_LEN(const MDBM* db, int len)
{
    return (len & db->db_align_mask) ? (len + db->db_align_mask) & ~db->db_align_mask : len;
}

static inline int
MDBM_ALIGN_PAD_BYTES(const MDBM* db, int len)
{
    return (db->db_align_mask+1 - (len & db->db_align_mask)) & db->db_align_mask;
}

static inline int
MDBM_PAD_BYTES(const mdbm_entry_t* ep)
{
    return ep->e_flags & MDBM_EFLAG_PAD_MASK;
}

static inline void
MDBM_SET_PAD_BYTES(mdbm_entry_t* ep, int bytes)
{
    ep->e_flags = (ep->e_flags & ~MDBM_EFLAG_PAD_MASK) | bytes;
}

static inline int
MDBM_VAL_LEN(const MDBM* db, const mdbm_entry_data_t* e)
{
    return e->e_entry[0].e_offset - e->e_entry[1].e_offset
        - MDBM_ALIGN_LEN(db,e->e_entry[1].e_key.key.len) - MDBM_PAD_BYTES(e->e_entry);
}

static inline int
MDBM_VAL_LEN1 (const MDBM* db, const mdbm_entry_t* ep)
{
    return ep[0].e_offset - ep[1].e_offset
        - MDBM_ALIGN_LEN(db,ep[1].e_key.key.len) - MDBM_PAD_BYTES(ep);
}

static inline int
MDBM_VAL_OFFSET(const MDBM* db, const mdbm_entry_t* ep)
{
    return ep[1].e_offset + MDBM_ALIGN_LEN(db,ep[1].e_key.key.len);
}

static inline char*
MDBM_VAL_PTR(const MDBM* db, const mdbm_entry_data_t* e)
{
    return (char*)e->e_page + MDBM_VAL_OFFSET(db,e->e_entry);
}

static inline char*
MDBM_VAL_PTR1 (const MDBM* db, const mdbm_page_t* p, const mdbm_entry_t* ep)
{
    return (char*)p + MDBM_VAL_OFFSET(db,ep);
}

static inline char*
MDBM_VAL_PTR2 (const mdbm_page_t* page, int offset, int vlen)
{
    return (char*)page + offset - vlen;
}

static inline int
MDBM_ENTRY_KVSIZE(const MDBM* db, const mdbm_entry_t* ep)
{
    return ep[0].e_offset - ep[1].e_offset - MDBM_ALIGN_LEN(db,ep[1].e_key.key.len)
        + MDBM_ALIGN_LEN(db,ep[0].e_key.key.len);
}

static inline int
MDBM_ENTRY_SIZE(const MDBM* db, const mdbm_entry_t* ep)
{
    return MDBM_ENTRY_KVSIZE(db,ep) + MDBM_ENTRY_T_SIZE;
}

static inline int
MDBM_ENTRY_OFFSET(const MDBM* db, const mdbm_entry_t* ep)
{
    return MDBM_VAL_OFFSET(db,ep);
}

static inline int
MDBM_ENTRY_LEN(const MDBM* db, const mdbm_entry_t* ep)
{
    return MDBM_KEY_OFFSET(ep) + ep->e_key.key.len - MDBM_VAL_OFFSET(db,ep);
}

static inline int
MDBM_LOB_ENABLED (const MDBM* db)
{
  return db->db_hdr->h_dbflags & MDBM_HFLAG_LARGEOBJ;
}

static inline mdbm_entry_lob_t*
MDBM_LOB_PTR1 (const MDBM* db, const mdbm_page_t* p, const mdbm_entry_t* ep)
{
    char* v = MDBM_VAL_PTR1(db,p,ep);
    if (MDBM_DB_CACHEMODE(db)) {
        v += MDBM_CACHE_ENTRY_T_SIZE;
    }
    return (mdbm_entry_lob_t*)v;
}

static inline char*
MDBM_LOB_VAL_PTR(const mdbm_page_t* p)
{
    return (char*)p + MDBM_PAGE_T_SIZE;
}

static inline int
MDBM_LOB_VAL_LEN(const mdbm_page_t* p)
{
    return p->p.p_vallen;
}

static inline int
MDBM_DATA_PAGE_BOTTOM_OF_DATA(const mdbm_page_t* p)
{
    return MDBM_ENTRY(p,p->p.p_num_entries)->e_offset;
}

static inline char*
MDBM_DATA_PAGE_BASE(const mdbm_page_t* p)
{
    return (char*)p + MDBM_PAGE_T_SIZE;
}

static inline int
MDBM_PAGE_FREE_BYTES(const mdbm_page_t* p)
{
    return MDBM_DATA_PAGE_BOTTOM_OF_DATA(p)
        - MDBM_PAGE_T_SIZE - (p->p.p_num_entries+1)*MDBM_ENTRY_T_SIZE;
}

static inline mdbm_hashval_t
MDBM_HASH_VALUE(const MDBM* db, const void* d, int dsize)
{
    return db->db_hashfunc((unsigned char*)d,dsize);
}

static inline mdbm_hashval_t
hash_value(MDBM* db, const datum* key)
{
    return MDBM_HASH_VALUE(db,key->dptr,key->dsize);
}

static inline int
MDBM_NUM_PAGES_ROUNDED(int pagesize, size_t bytes)
{
    return (bytes + pagesize - 1) / pagesize;
    /*int ret = (bytes + pagesize - 1) / pagesize; */
    /*assert(ret*pagesize >= (int)bytes); */
    /*return ret; */
}

static inline int
MDBM_DB_NUM_PAGES_ROUNDED(const MDBM* db, size_t bytes)
{
    return MDBM_NUM_PAGES_ROUNDED(db->db_pagesize,bytes);
}

static inline mdbm_dir_t*
MDBM_DIR_PTR(MDBM* db)
{
    return (mdbm_dir_t*)(db->db_base + MDBM_PAGE_T_SIZE + MDBM_HDR_T_SIZE);
}

static inline int
MDBM_DIR_BIT(const MDBM* db, int bit)
{
    /* db_dir is allocated on the heap and copied-on-modify from the db */
    return (db->db_dir[(bit) >> 3] & (1 << ((bit) & 0x7)));
}

static inline void
MDBM_SET_DIR_BIT(MDBM* db, int bit)
{
    mdbm_dir_t* d = MDBM_DIR_PTR(db) + (bit >> 3);
    *d = (char)(*d | (1 << ((bit) & 0x7)));
}

static inline void
MDBM_CLEAR_DIR_BIT(MDBM* db, int bit)
{
    mdbm_dir_t* d = MDBM_DIR_PTR(db) + (bit >> 3);
    *d = (char)(*d & ~(1 << ((bit) & 0x7)));
}



/* gen stands for generation (not generate) */
/* these track/update changes to the db for shared mappings */
extern void update_dup_map_gen(MDBM* db);
extern void sync_dup_map_gen(MDBM* db);

extern mdbm_pagenum_t MDBM_HASH_MASKS[];

static inline mdbm_pagenum_t
MDBM_HASH_MASK(int level)
{
    return MDBM_HASH_MASKS[level];
}

static inline int MDBM_DIR_WIDTH(int dir_shift) { return 1<<dir_shift; }
static inline int MDBM_DIR_SIZE(int dir_shift) { return dir_shift<3 ? 1 : 1<<(dir_shift-3); }

static inline mdbm_ptentry_t*
MDBM_PTABLE_PTR(mdbm_dir_t* dir, int dir_shift)
{
    return (mdbm_ptentry_t*)(dir + MDBM_DIR_SIZE(dir_shift));
}

static inline int
MDBM_PTABLE_SIZE(int dir_shift)
{
    return MDBM_DIR_WIDTH(dir_shift)*MDBM_PTENTRY_T_SIZE;
}

static inline int
MDBM_NUM_DIR_BYTES(int dir_shift)
{
    return MDBM_PAGE_T_SIZE
        + MDBM_HDR_T_SIZE
        + MDBM_DIR_SIZE(dir_shift)
        + MDBM_PTABLE_SIZE(dir_shift);
}

static inline int
MDBM_DB_NUM_DIR_BYTES(MDBM* db)
{
    return MDBM_NUM_DIR_BYTES(db->db_dir_shift);
}

static inline int
MDBM_NUM_DIR_PAGES(int pagesize, int dir_shift)
{
    return MDBM_NUM_PAGES_ROUNDED(pagesize,MDBM_NUM_DIR_BYTES(dir_shift));
}

static inline int
MDBM_DB_NUM_DIR_PAGES(MDBM* db)
{
    return MDBM_NUM_DIR_PAGES(db->db_pagesize,db->db_dir_shift);
}

static inline int
MDBM_IS_WINDOWED(const MDBM* db)
{
    return db->db_flags & MDBM_DBFLAG_WINDOWED;
}

static inline int
MDBM_GET_STAT_OPERATIONS_OPS(const MDBM* db)
{
    return db->db_stats_flags & MDBM_STATS_BASIC;
}

static inline int
MDBM_GET_STAT_OPERATIONS_TIMES(const MDBM* db)
{
    return db->db_stats_flags & MDBM_STATS_TIMED;
}

extern mdbm_page_t* get_window_page(MDBM* db,
                                    mdbm_page_t* page,
                                    int pagenum, int npages,
                                    uint32_t off, uint32_t len);

void release_window_pages(MDBM* db);
void release_window_page(MDBM* db, void* p);

static inline unsigned int 
MDBM_GET_PAGE_INDEX(MDBM* db, int pagenum) 
{
    return db->db_ptable[pagenum].pt_pagenum;
}

static inline void
MDBM_SET_PAGE_INDEX(MDBM* db, int pagenum, int index) 
{
    db->db_ptable[pagenum].pt_pagenum = index;
}


static inline mdbm_page_t*
MDBM_PAGE_PTR(MDBM* db, int pagenum)
{
    if (!MDBM_IS_WINDOWED(db)) {
      return  ((mdbm_page_t*)(db->db_base + (size_t)pagenum*db->db_pagesize));
    } else {
      /* check for windowed mode and ensure the page is currently mapped */
      /* it was relying on remap() mapping in the start of every page in sys_page */
      /* sized chunks, which means we actually need pages*4k+fudge window size */
      /* but release_window_page(s)() doesn't restore this 4k_page_start mapping */
      return get_window_page(db, NULL, pagenum, 1, 0, 0);
    }
}


static inline size_t
MDBM_DB_SIZE(const MDBM* db)
{
    return (size_t)db->db_num_pages * db->db_pagesize;
}

static inline size_t
MDBM_DB_MAP_SIZE(const MDBM* db)
{
    return ((db->db_flags & MDBM_DBFLAG_HDRONLY)
            ? MDBM_HDR_T_SIZE
            : ((size_t)db->db_num_pages * db->db_pagesize));
}

static inline size_t
MDBM_HDR_DB_SIZE(const mdbm_hdr_t* hdr)
{
    return (size_t)hdr->h_num_pages * hdr->h_pagesize;
}

static inline uint64_t
MDBM_HDR_DB_SIZE_64 (const mdbm_hdr_t* hdr)
{
    return (uint64_t)hdr->h_num_pages * hdr->h_pagesize;
}

static inline int
MDBM_DB_DIR_WIDTH(const MDBM* db)
{
    return MDBM_DIR_WIDTH(db->db_dir_shift);
}

static inline int
MDBM_DB_DIR_SIZE(const MDBM* db)
{
    return MDBM_DIR_SIZE(db->db_dir_shift);
}

static inline int
MDBM_DB_PTABLE_SIZE(const MDBM* db)
{
    return MDBM_PTABLE_SIZE(db->db_dir_shift);
}

static inline int
MDBM_IS_RDONLY(const MDBM* db)
{
    return db->db_flags & MDBM_DBFLAG_RDONLY;
}

static inline int
MDBM_IS_REPLACED(const MDBM* db)
{
    /* Not safe, if the db was already remap'd by another thread. */
    return db->db_hdr->h_dbflags & MDBM_HFLAG_REPLACED;
}

static inline int
MDBM_DUP_IS_REPLACED(const MDBM* db)
{
    /* check if the db was remap'd by another thread
     * if so, we can't touch pages or header fields safely */
    return db->db_dup_info &&
      (db->db_dup_map_gen != db->db_dup_info->dup_map_gen);
}

static inline int
MDBM_SIZE_CHANGED(const MDBM* db)
{
    return (db->db_hdr->h_num_pages != db->db_num_pages
            || db->db_hdr->h_dir_shift != db->db_dir_shift);
}

static inline int
MDBM_REPLACED_OR_CHANGED(const MDBM* db)
{
    /* DUP check MUST be first to avoid invalid access to the old map. */
    return MDBM_DUP_IS_REPLACED(db)
      || MDBM_IS_REPLACED(db) || MDBM_SIZE_CHANGED(db);
}

static inline mdbm_page_t*
MDBM_CHUNK_PTR(MDBM* db, int pagenum, int npages)
{
    mdbm_page_t* page;

    if (MDBM_IS_WINDOWED(db)) {
        page = get_window_page(db,NULL, pagenum, npages ? npages : 1,0,0);
        if (!npages && page->p_num_pages > 1) {
            page = get_window_page(db,page,0,0,0,0);
        }
    } else {
        page = MDBM_PAGE_PTR(db,pagenum);
    }
    return page;
}

static inline mdbm_page_t*
MDBM_PAGE_TO_CHUNK_PTR(MDBM* db, mdbm_page_t* page)
{
    if (MDBM_IS_WINDOWED(db)) {
        page = get_window_page(db,page,0,0,0,0);
    }
    return page;
}

static inline mdbm_page_t*
MDBM_MAPPED_PAGE_PTR(MDBM* db, int pagenum)
{
    return MDBM_CHUNK_PTR(db,pagenum,0);
}


extern void
MDBM_ENTRY_LOB_ALLOC_LEN1 (MDBM* db, const mdbm_page_t* p, const mdbm_entry_t* ep,
                           uint32_t* vallen, uint32_t* lob_alloclen);

static inline void
MDBM_ENTRY_LOB_ALLOC_LEN(MDBM* db, const mdbm_entry_data_t* e,
                         uint32_t* vallen, uint32_t* lob_alloclen)
{
    MDBM_ENTRY_LOB_ALLOC_LEN1(db,e->e_page,e->e_entry,vallen,lob_alloclen);
}

/*#  define get_cpu_count()  sysconf(_SC_NPROCESSORS_CONF) */
/*#  define get_time_usec() fast_time_usec() */

/* log levels from syslog.h */
/*#define LOG_EMERG       0      */ /* system is unusable */
/*#define LOG_ALERT       1      */ /* action must be taken immediately */
/*#define LOG_CRIT        2      */ /* critical conditions */
/*#define LOG_ERR         3      */ /* error conditions */
/*#define LOG_WARNING     4      */ /* warning conditions */
/*#define LOG_NOTICE      5      */ /* normal but significant condition */
/*#define LOG_INFO        6      */ /* informational */
/*#define LOG_DEBUG       7      */ /* debug-level messages */

#include "mdbm_log.h"
/*#include "../lib/shmem.h" */

/*#include "stall_signals.h" */
extern void hold_signals();
extern void resume_signals();
#  define MDBM_SIG_DEFER        hold_signals()
#  define MDBM_SIG_ACCEPT       resume_signals()

#ifdef __MACH__
static inline uint64_t fast_time_usec(void) {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return tp.tv_sec*1000000 + tp.tv_usec;
}

static inline time_t get_time_sec() {
  return time(NULL);
}
#else /* linux */
static inline uint64_t fast_time_usec(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((uint64_t)ts.tv_sec)*1000000 +ts.tv_nsec/1000;
}

static inline time_t get_time_sec() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec;
}
#endif

#define get_time_usec() fast_time_usec()

#define get_cpu_count()  sysconf(_SC_NPROCESSORS_CONF)

#define MDBM_INC_STAT_64(var)   __sync_fetch_and_add(var, 1);
#define MDBM_ADD_STAT_64(var,a) __sync_fetch_and_add(var, a);


static inline void
MDBM_INC_STAT_COUNTER(mdbm_rstats_val_t* v)
{
    MDBM_INC_STAT_64(&v->num);
}

static inline int
MDBM_DO_STATS(const MDBM* db)
{
  if (db->m_stat_cb || db->db_rstats) {
    return 1;
  }
  return 0;
}

static inline int
MDBM_DO_STAT_TIME(const MDBM* db)
{
  if ((db->m_stat_cb_flags & MDBM_STATS_TIMED) || db->db_rstats) {
    return 1;
  }
  return 0;
}

static inline int
MDBM_DO_STAT_PAGE(const MDBM* db)
{
  if (db->m_stat_cb_flags && db->m_stat_cb) {
    return 1;
  }
  return 0;
}

static inline void
MDBM_PAGE_STAT(const MDBM* db, uint64_t page, int tag)
{
    if (db->m_stat_cb) {
      (*db->m_stat_cb)((MDBM*)db, COMBINE_STAT_TAG(tag, MDBM_STAT_CB_SET),
                       MDBM_STAT_CB_SET, page, db->m_stat_cb_user);
    }
}

static inline void
MDBM_INC_STAT_ERROR_COUNTER(MDBM* db, mdbm_rstats_val_t* v, int tag)
{
    if (db->db_rstats) {
      MDBM_INC_STAT_64(&v->num_error);
    }
    if (db->m_stat_cb) {
      (*db->m_stat_cb)(db, COMBINE_STAT_TAG(tag, MDBM_STAT_CB_INC),
                       MDBM_STAT_CB_INC, 1, db->m_stat_cb_user);
    }
}
  
static inline void
MDBM_ADD_STAT_ERROR_COUNTER(MDBM* db, mdbm_rstats_val_t* v, uint64_t add, int tag)
{
    if (add) {
      if (v) {
        MDBM_ADD_STAT_64(&v->num_error,add);
      }
      if (db->m_stat_cb) {
        (*db->m_stat_cb)(db, COMBINE_STAT_TAG(tag, MDBM_STAT_CB_INC),
                         MDBM_STAT_CB_INC, add, db->m_stat_cb_user);
      }
    }
}
  

extern void
MDBM_ADD_STAT_THIST(mdbm_rstats_val_t* v, uint64_t t);

static inline void
MDBM_ADD_STAT_TIME(const MDBM* db, mdbm_rstats_val_t* v, uint64_t t)
{
    MDBM_ADD_STAT_64(&v->sum_usec,t);
    if (db->db_flags & MDBM_DBFLAG_RSTATS_THIST) {
        MDBM_ADD_STAT_THIST(v,t);
    }
}


static inline void
MDBM_ADD_STAT(const MDBM* db, mdbm_rstats_val_t* v, uint64_t t_op, int tag)
{
    if (db->db_rstats) {
      MDBM_INC_STAT_COUNTER(v);
      MDBM_ADD_STAT_TIME(db,v,t_op);
    }
    if (db->m_stat_cb) {
      (*db->m_stat_cb)((MDBM*)db, COMBINE_STAT_TAG(tag, MDBM_STAT_CB_INC),
                       MDBM_STAT_CB_INC, 1, db->m_stat_cb_user);
      if (db->m_stat_cb_flags & MDBM_STATS_TIMED) {
        (*db->m_stat_cb)((MDBM*)db, COMBINE_STAT_TAG(tag, MDBM_STAT_CB_ELAPSED),
                         MDBM_STAT_CB_ELAPSED, t_op, db->m_stat_cb_user);
      }
    }
}

static inline void
MDBM_ADD_STAT_WAIT(const MDBM* db, mdbm_rstats_val_t* v, uint64_t t_op, int tag)
{
    MDBM_ADD_STAT(db,v,t_op, tag);
    if (db->db_lock_wait) {
        if (v) {
            MDBM_INC_STAT_64(&v->num_lock_wait);
            MDBM_ADD_STAT_64(&v->sum_lock_wait_usec,db->db_lock_wait);
        }
    }
}



extern int 
mdbm_internal_replace(MDBM* db);

extern int
mdbm_internal_remap(MDBM *db, size_t dbsize, int flags);

extern void
sync_dir(MDBM* db, mdbm_hdr_t* hdr);

extern mdbm_pagenum_t
hashval_to_pagenum(const MDBM *db, mdbm_hashval_t hashval);

extern void print_page_table(MDBM *db);

extern void mdbm_internal_print_free_list(MDBM *db);

extern void dump_chunk_headers(MDBM* db);
extern void dump_mdbm_header(MDBM* db);

extern int fcopy_header(MDBM* db, int fd, int flags);
extern int fcopy_body(MDBM* db, int fd, int flags);

extern char mdbm_internal_hex_to_byte(int c1, int c2);

#define ERROR() fprintf(stderr, "ERROR (%d %s) in %s() %s:%d\n", errno, strerror(errno), __func__, __FILE__, __LINE__);
#define NOTE(desc) fprintf(stderr, "NOTICE %s in %s() %s:%d\n", desc, __func__, __FILE__, __LINE__);


#ifdef  __cplusplus
}
#endif


#endif  /* MDBM3_INTERNAL_H_ONCE */
