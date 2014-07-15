/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifdef __linux__
/* _GNU_SOURCE is needed for O_NOATIME, etc. */
#define _GNU_SOURCE
#include <sys/vfs.h>
#endif

#include <assert.h>
#include <ctype.h>
#include <dlfcn.h>
#include <errno.h>
#include <sys/types.h>
#include <regex.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/queue.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <netinet/in.h>
#include <syscall.h>
#include <signal.h>
#include <sys/resource.h> /* used for rlimit */
#include <execinfo.h>
#include <sys/time.h>
#include <inttypes.h>

/* FreeBSD4 doesn't have stdint.h, but FreeBSD6 masks it. */
#ifdef FREEBSD
#ifndef UINT16_MAX
#define UINT16_MAX 0xffff
#endif
#endif

#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_stats.h"
#include "atomic.h"

#define MDBM_SANITY_CHECKS
#define MDBM_BSOPS

int mdbm_sanity_check;

/*
//////////////////////////////////////////////////////////////////////
// Legacy limit-size functions, just for temporary link-time 
// compatibility. These will simply error out if invoked.
//////////////////////////////////////////////////////////////////////
*/
/*
struct mdbm;
int mdbm_limit_size(MDBM *db, mdbm_ubig_t a, int (*func)(struct mdbm *, datum k, datum v, void *x)) {
  mdbm_log(LOG_ERR, "ERROR: mdbm_limit_size() is no longer supported.");
  abort();
}

struct mdbm_shake_data;
typedef int (*mdbm_shake_func)(MDBM *, const datum*, const datum*, struct mdbm_shake_data *);
int mdbm_limit_size_new (MDBM *db, mdbm_ubig_t a, mdbm_shake_func shake, void *x) {
  mdbm_log(LOG_ERR, "ERROR: mdbm_limit_size_new() is no longer supported.");
  abort();
}
*/


/* 
// NOTE: RTLD_LOCAL used by perl and java horks function visibility to plugins.
// So, explicitly load a sub-library with RTLD_GLOBAL in a library-constructor.
*/
void __attribute__ ((constructor)) mdbm_load_common_globals(void);
void __attribute__ ((destructor)) mdbm_unload_common_globals(void);


/* static void* common_handle; */
/* #define LIBDIR "/usr/local/lib" */
void mdbm_load_common_globals(void) {
  /*
    int dlflags = RTLD_NOW|RTLD_GLOBAL;
    common_handle = NULL;
    // try to load sub-library 
    // try default path resolution first (allow local test version to override install path)
    common_handle = dlopen("libmdbmcommon.so", dlflags);

    if (!common_handle) {
      if (IS_32BIT()) {
        common_handle = dlopen(LIBDIR"/libmdbmcommon.so", dlflags);
      } else {
        common_handle = dlopen(LIBDIR"64/libmdbmcommon.so", dlflags);
      }
    }
    if (!common_handle) {
      mdbm_log(LOG_ERR, "MDBM error failed to load libmdbmcommon library! \n");
    }
  */
}
void mdbm_unload_common_globals(void) {
  /*
    // Add any library cleanup code...
    if (common_handle) {
      dlclose(common_handle);
    }
  */
}


/* assumes unit is a power-of-two */
#define ASSERT_MULTIPLE(val, unit)               \
  if (val & (unit-1)) {                          \
    fprintf(stderr, "ERROR: not a multiple... "  \
           "line %d, "#val" %d vs "#unit" %d\n", \
            __LINE__, val, unit);                \
    abort();                                     \
  }

/* this pins the pages in memory */
static int pin_pages(MDBM *db);

static int mdbm_set_window_size_internal(MDBM* db, size_t wsize);

extern int do_delete_lockfiles(const char* dbname);

inline uint64_t
get_gtod_usec()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
}

volatile static uint64_t last_usec;  /* Last usec time value from gettimeofday */
volatile static uint64_t last_tsc;   /* TSC when when gettimeofday was called */
volatile static uint64_t hi_tsc;     /* Keeps track of high water mark TSC value */
volatile static uint64_t next_gtod_tsc;   /* timestamp, in units of TSC, when to re-read gtod */
volatile static uint64_t tsc_per_usec;    /* TSC clock cycles per microsecond */

/* Intel (and later model AMD) Fetch Time-StampCounter
 * WANRING:  This value may be affected by speedstep and may vary randomly across cores. */
__inline__ uint64_t rdtsc(void)
{
    uint32_t lo, hi;
    /* We cannot use "=A", since this would use %rax on x86_64 and
     * return only the lower 32bits of the TSC */
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (((uint64_t)hi) << 32) | lo;
}

uint64_t
tsc_get_usec(void)
{
    uint64_t cur_tsc = rdtsc();
    uint64_t cur_usec;
    uint64_t new_tsc_per_usec;

    fprintf(stderr, "@@@ tsc_get_usec() tsc/usec:%"PRIu64" cur_tsc:%"PRIu64" last:%"PRIu64" last_usec:%"PRIu64" @@@\n", 
            tsc_per_usec, cur_tsc, last_tsc, last_usec);
    if (tsc_per_usec && (cur_tsc < next_gtod_tsc)) {
        /* do not re-read from gtod on high-water-mark failures, for performance reasons: */
        if (cur_tsc >= hi_tsc) {    /* if core switching occurs, just use high-water-mark */
            hi_tsc = cur_tsc;
        }

        return ((hi_tsc - last_tsc) / tsc_per_usec) + last_usec;
    }

    /* more than 2 seconds passed or first call: reload values from gettimeofday */
    cur_usec = get_gtod_usec();
    cur_tsc = rdtsc();   /* after gtod, because gettimeofday is slow */

    /* Check for errors: do not allow division by zero or negative times */
    if ((cur_usec <= last_usec) || (cur_tsc <= last_tsc)) {
        last_tsc = hi_tsc = cur_tsc;
        last_usec = cur_usec;
        next_gtod_tsc = 0;
        return last_usec;  /* Trust only gettimeofday */
    }

    /* Use new gettimeofday to update tsc_per_usec */
    new_tsc_per_usec = (cur_tsc - last_tsc) / (cur_usec - last_usec);
    if (tsc_per_usec) {
        tsc_per_usec = (new_tsc_per_usec + tsc_per_usec) >> 1;   /* average them out */
    } else {
        tsc_per_usec = new_tsc_per_usec;
    }

    last_tsc = hi_tsc = cur_tsc;

    next_gtod_tsc = cur_tsc + (tsc_per_usec * 2000000);  /* next time to reload from gtod */
    return (last_usec = cur_usec);
}


static int
inc_dup_ref(MDBM* db)
{
    uint64_t v, vnew;
    do {
        v = db->db_dup_info->nrefs;
        vnew = v+1;
    } while (!atomic_cmp_and_set_64_bool(&db->db_dup_info->nrefs,v,vnew));
    return vnew;
}

static int
dec_dup_ref(MDBM* db)
{
    uint64_t v, vnew;
    do {
        v = db->db_dup_info->nrefs;
        vnew = v-1;
    } while (!atomic_cmp_and_set_64_bool(&db->db_dup_info->nrefs,v,vnew));
    return vnew;
}

static uint64_t
dup_map_gen_start(MDBM* db)
{
    uint64_t v = ++db->db_dup_info->dup_map_gen;
    return v;
}

static void
dup_map_gen_end(MDBM* db, uint64_t v)
{
    atomic_read_barrier();
    db->db_dup_info->dup_map_gen_marker = v;
}

#define MDBM_HASH_MASK_COMPUTE(l)       ((1<<(l))-1)

mdbm_pagenum_t MDBM_HASH_MASKS[] = {
    MDBM_HASH_MASK_COMPUTE(0),
    MDBM_HASH_MASK_COMPUTE(1),
    MDBM_HASH_MASK_COMPUTE(2),
    MDBM_HASH_MASK_COMPUTE(3),
    MDBM_HASH_MASK_COMPUTE(4),
    MDBM_HASH_MASK_COMPUTE(5),
    MDBM_HASH_MASK_COMPUTE(6),
    MDBM_HASH_MASK_COMPUTE(7),
    MDBM_HASH_MASK_COMPUTE(8),
    MDBM_HASH_MASK_COMPUTE(9),
    MDBM_HASH_MASK_COMPUTE(10),
    MDBM_HASH_MASK_COMPUTE(11),
    MDBM_HASH_MASK_COMPUTE(12),
    MDBM_HASH_MASK_COMPUTE(13),
    MDBM_HASH_MASK_COMPUTE(14),
    MDBM_HASH_MASK_COMPUTE(15),
    MDBM_HASH_MASK_COMPUTE(16),
    MDBM_HASH_MASK_COMPUTE(17),
    MDBM_HASH_MASK_COMPUTE(18),
    MDBM_HASH_MASK_COMPUTE(19),
    MDBM_HASH_MASK_COMPUTE(20),
    MDBM_HASH_MASK_COMPUTE(21),
    MDBM_HASH_MASK_COMPUTE(22),
    MDBM_HASH_MASK_COMPUTE(23),
    MDBM_HASH_MASK_COMPUTE(24),
    MDBM_HASH_MASK_COMPUTE(25),
    MDBM_HASH_MASK_COMPUTE(26),
    MDBM_HASH_MASK_COMPUTE(27),
    MDBM_HASH_MASK_COMPUTE(28),
    MDBM_HASH_MASK_COMPUTE(29),
    MDBM_HASH_MASK_COMPUTE(30),
    ~0
};

void
MDBM_ENTRY_LOB_ALLOC_LEN1 (MDBM* db, const mdbm_page_t* p, const mdbm_entry_t* ep,
                           uint32_t* vallen, uint32_t* lob_alloclen)
{
    char* v = MDBM_VAL_PTR1(db,p,ep);
    mdbm_entry_lob_t* lp;
    uint32_t lob_vallen;

    *vallen = MDBM_VAL_LEN1(db,ep);
    if (MDBM_DB_CACHEMODE(db)) {
        *vallen -= MDBM_CACHE_ENTRY_T_SIZE;
        v += MDBM_CACHE_ENTRY_T_SIZE;
    }
    lp = (mdbm_entry_lob_t*)v;
    if (*vallen >= MDBM_ENTRY_LOB_T_SIZE) {
        lob_vallen = lp->l_vallen;
    } else {
        mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
        lob_vallen = MDBM_LOB_VAL_LEN(lob);
    }
    *lob_alloclen = MDBM_DB_NUM_PAGES_ROUNDED(db,lob_vallen + MDBM_PAGE_T_SIZE)*db->db_pagesize;
}


/* Macro for expanding the integer tags into a convenient string */

#define EXPAND_STAT_CASES(tag, label) \
  case COMBINE_STAT_TAG(tag, MDBM_STAT_CB_INC)  :    return label;           \
  case COMBINE_STAT_TAG(tag, MDBM_STAT_CB_SET) :     return label "Val";     \
  case COMBINE_STAT_TAG(tag, MDBM_STAT_CB_ELAPSED) : return label "Latency"; \
  case COMBINE_STAT_TAG(tag, MDBM_STAT_CB_TIME) :    return label "Time";

const char* mdbm_get_stat_name(int tag)
{
  switch (tag) {
    EXPAND_STAT_CASES(MDBM_STAT_TAG_FETCH,             "Fetch")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_STORE,             "Store")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_DELETE,            "Delete")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_LOCK,              "Lock")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_FETCH_UNCACHED,    "FetchUncached")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_GETPAGE,           "GetPage")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_GETPAGE_UNCACHED,  "GetPageUncached")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_CACHE_EVICT,       "CacheEvict")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_CACHE_STORE,       "CacheStore")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_PAGE_STORE,        "PageStore")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_PAGE_DELETE,       "PageDelete")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_SYNC,              "MdbmSync")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_FETCH_NOT_FOUND,   "FetchNotFound")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_FETCH_ERROR,       "FetchError")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_STORE_ERROR,       "StoreError")
    EXPAND_STAT_CASES(MDBM_STAT_TAG_DELETE_FAILED,     "DeleteFailed")
  }
  return "unknown_tag";
}

int
mdbm_set_stats_func(MDBM* db, int flags, mdbm_stat_cb cb, void* user)
{
  if (db->db_ver_flag != MDBM_DBVER_3) {
    mdbm_logerror(LOG_ERR,0,"%s: cannot set stats callback for V2 mdbm.",db->db_filename);
    return -1;
  }
  db->m_stat_cb_flags = flags;
  db->m_stat_cb = cb;
  db->m_stat_cb_user = user;
  return 0;
}


inline mdbm_hdr_stats_t*
mdbm_hdr_stats(MDBM* db)
{
    return &db->db_hdr->h_stats;
}

void
MDBM_ADD_STAT_THIST(mdbm_rstats_val_t* v, uint64_t t)
{
    int i;
    if (t <= 100) {
        i = t/10;
    } else if (t <= 1000) {
        i = 1*9+t/100;
    } else if (t <= 10000) {
        i = 2*9+t/1000;
    } else if (t <= 100000) {
        i = 3*9+t/10000;
    } else if (t <= 1000000) {
        i = 4*9+t/100000;
    } else if (t <= 10000000) {
        i = 5*9+t/1000000;
    } else {
        i = 6*9+1;
    }
    MDBM_INC_STAT_64(v->thist+i);
}


/**
 * Allow to separately enable timed and non-timed statistics tracking because timed
 * operations are expensive.
 */
int
mdbm_enable_stat_operations(MDBM *db, int flags)
{
    if ((db == NULL) || (flags > (MDBM_STATS_BASIC | MDBM_STATS_TIMED))) {
        mdbm_log(LOG_ERR, "%s: STAT_OPERATIONS invalid arguments", db->db_filename);
        return -1;
    }
    if (db->db_ver_flag != MDBM_DBVER_3) {
        mdbm_log(LOG_ERR, "%s: STAT_OPERATIONS not allowed on MDBM V2", db->db_filename);
        return -1;
    }
    /* Must have write access: keeping stats on fetches still writes to MDBM */
    if (db->db_flags & MDBM_DBFLAG_RDONLY) {
        mdbm_log(LOG_ERR,
            "%s: STAT_OPERATIONS (even of fetches only) require Read/Write access",
            db->db_filename);
        return -1;
    }

    db->db_stats_flags = flags;
    return 0;
}

/**
 * Set or reset stats measurement time function inside the MDBM handle.
 */
int
mdbm_set_stat_time_func(MDBM *db, int flags)
{
    db->db_get_usec = (mdbm_time_func_t)fast_time_usec;
#ifndef DISABLE_TSC
    if (flags == MDBM_CLOCK_TSC) {
        db->db_get_usec = &tsc_get_usec;
    }
#endif
    return 0;
}

#define MDBM_GUARD_PADDING  (~MDBM_MAGIC)

/**
 * Check if the padding, and therefore the handle, became corrupt.
 * Return 0 if OK, -1 if not
 */
static inline int 
check_guard_padding(MDBM *db, int printMsg)
{
    if ((db->guard_padding_1 != MDBM_GUARD_PADDING) ||
        (db->guard_padding_2 != MDBM_GUARD_PADDING) ||
        (db->guard_padding_3 != MDBM_GUARD_PADDING) ||
        (db->guard_padding_4 != MDBM_GUARD_PADDING)) {
        if (printMsg) {
            char printbuf[40];
            int left = sizeof(printbuf)-1;

            printbuf[0] = '\0';
            if (db->guard_padding_1 != MDBM_GUARD_PADDING) {
                strncat(printbuf, "1 ", left); left -=2;
            }
            if (db->guard_padding_2 != MDBM_GUARD_PADDING) {
                strncat(printbuf, "2 ", left); left -=2;
            }
            if (db->guard_padding_3 != MDBM_GUARD_PADDING) {
                strncat(printbuf, "3 ", left); left -=2;
            }
            if (db->guard_padding_4 != MDBM_GUARD_PADDING) {
                strncat(printbuf, "4 ", left); left -=2;
            }
            mdbm_logerror(LOG_ERR, 0, 
                "%s: MDBM handle is damaged, user should check for array overrun errors, etc."
                "\nbad-guards=%s\n", db->db_filename, printbuf);
        }
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

mdbm_pagenum_t
hashval_to_pagenum(const MDBM *db, mdbm_hashval_t hashval)
{
#if 1
    int hashbit;

    if (db->db_dir_flags & MDBM_HFLAG_PERFECT) {
        hashbit = db->db_dir_shift;
    } else {
        mdbm_hashval_t hv = hashval;
        int dirbit = 0;

        hashbit = 0;
        while (dirbit < db->db_max_dirbit && MDBM_DIR_BIT(db,dirbit)) {
            dirbit = (dirbit << 1) + (hv & 1) + 1;
            hashbit++;
            hv >>= 1;
        }
    }
    return MDBM_HASH_MASK(hashbit) & hashval;
#else
    /* FIXME Why is this here? */
    int dirbit = 0;
    int hashbit = 0;
    int hashbit_shift = 1;

    if (db->db_max_dirbit > 0) {
        while (dirbit < db->db_max_dirbit && MDBM_DIR_BIT(db,dirbit)) {
            dirbit = (dirbit << 1) + ((hashval & hashbit_shift) ? 2 : 1);
            hashbit++;
            hashbit_shift <<= 1;
        }
    }
    return (hashval & (hashbit_shift - 1));
#endif
}

static void
alloc_free_chunk(MDBM* db, int npages, int n, int prev)
{
    mdbm_page_t* page;

    assert(n <= db->db_hdr->h_last_chunk);
    if (prev < 0) {
        int p;
        prev = 0;
        for (p = db->db_hdr->h_first_free; p && p != n;) {
            mdbm_page_t* page = MDBM_PAGE_PTR(db,p);
            prev = p;
            p = page->p.p_next_free;
        }
    }

    page = MDBM_PAGE_PTR(db,n);
    assert(page->p_type == MDBM_PTYPE_FREE);
    if (page->p_num_pages > npages) {
        int n1 = n + npages;
        int n1pages = page->p_num_pages - npages;
        mdbm_page_t* next_page = MDBM_PAGE_PTR(db,n1);

        next_page->p_type = MDBM_PTYPE_FREE;
        next_page->p_num_pages = n1pages;
        next_page->p_prev_num_pages = npages;
        next_page->p.p_next_free = page->p.p_next_free;
        page->p.p_next_free = n1;
        if (n == db->db_hdr->h_last_chunk) {
            db->db_hdr->h_last_chunk = n+npages;
        } else {
            next_page = MDBM_PAGE_PTR(db,n1 + n1pages);
            next_page->p_prev_num_pages = n1pages;
        }
    }
    if (prev) {
        mdbm_page_t* pprev = MDBM_PAGE_PTR(db,prev);
        pprev->p.p_next_free = page->p.p_next_free;
    } else {
        assert(!page->p.p_next_free
               || MDBM_PAGE_PTR(db,page->p.p_next_free)->p_type == MDBM_PTYPE_FREE);
        db->db_hdr->h_first_free = page->p.p_next_free;
    }
}

/**
 * \brief Checks the integrity of the database header.
 * \param[in,out] mdbm handle
 * \param[in] h header
 * \param[in] whether to display information messages
 * \return number of check failures
 */
static int
check_db_header(MDBM* db, mdbm_hdr_t* h, int verbose)
{
    int nerr = 0;

    if (h->h_magic != _MDBM_MAGIC_NEW2) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_magic (0x%x) invalid",
                     db->db_filename,h->h_magic);
        }
        nerr++;
    }
    if (h->h_dbflags
        & ~(MDBM_ALIGN_MASK|MDBM_HFLAG_PERFECT|MDBM_HFLAG_REPLACED|MDBM_HFLAG_LARGEOBJ))
    {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_dbflags (0x%x) invalid",
                     db->db_filename,h->h_dbflags);
        }
        nerr++;
    }
    if ((h->h_cache_mode & ~MDBM_CACHEMODE_BITS)
        || (MDBM_CACHEMODE(h->h_cache_mode) > MDBM_CACHEMODE_MAX))
    {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_cachemode (0x%x) invalid",
                     db->db_filename,h->h_cache_mode);
        }
        nerr++;
    }
    if (h->h_dir_shift > MDBM_DIRSHIFT_MAX) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_dir_shift (%u) invalid",
                     db->db_filename,h->h_dir_shift);
        }
        nerr++;
    }
    if (h->h_hash_func > MDBM_HASH_MAX) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_hash_func (%u) invalid",
                     db->db_filename,h->h_hash_func);
        }
        nerr++;
    }
    if (h->h_max_dir_shift > MDBM_DIRSHIFT_MAX) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_max_dir_shift (%u) invalid",
                     db->db_filename,h->h_max_dir_shift);
        }
        nerr++;
    }
    if (h->h_max_dir_shift && h->h_dir_shift > h->h_max_dir_shift) {
        if (verbose) {
            mdbm_log(LOG_WARNING,
                     "%s: h_dir_shift (%u) > h_max_dir_shift (%u)",
                     db->db_filename,h->h_dir_shift,h->h_max_dir_shift);
        }
    }
    if (h->h_pagesize < MDBM_MINPAGE || h->h_pagesize > MDBM_MAXPAGE
        || (h->h_pagesize & (MDBM_PAGE_ALIGN-1)))
    {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_pagesize (%u) invalid",
                     db->db_filename,h->h_pagesize);
        }
        nerr++;
    }
    if (h->h_num_pages > MDBM_NUMPAGES_MAX) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_num_pages (%u) invalid",
                     db->db_filename,h->h_num_pages);
        }
        nerr++;
    }
    if (h->h_max_pages > MDBM_NUMPAGES_MAX) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_max_pages (%u) invalid",
                     db->db_filename,h->h_max_pages);
        }
        nerr++;
    }
    if (h->h_max_pages && h->h_num_pages > h->h_max_pages) {
        if (verbose) {
            mdbm_log(LOG_WARNING,
                     "%s: h_num_pages (%u) > h_max_pages (%u)",
                     db->db_filename,h->h_num_pages,h->h_max_pages);
        }
    }
    if (h->h_spill_size > h->h_pagesize) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_spill_size (%u) > h_pagesize (%u)",
                     db->db_filename,h->h_spill_size,h->h_pagesize);
        }
        nerr++;
    }
    if (h->h_last_chunk >= h->h_num_pages) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_last_chunk (%u) >= h_num_pages (%u)",
                     db->db_filename,h->h_last_chunk,h->h_num_pages);
        }
        nerr++;
    }
    if (h->h_first_free >= h->h_num_pages) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_first_free (%u) >= h_num_pages (%u)",
                     db->db_filename,h->h_first_free,h->h_num_pages);
        }
        nerr++;
    }
    if (h->h_first_free > 0 && h->h_first_free == h->h_last_chunk) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: h_first_free (%u) == h_last_chunk (%u)",
                     db->db_filename,h->h_first_free,h->h_last_chunk);
        }
        nerr++;
    }
    if (verbose > 1) {
        mdbm_log(LOG_INFO,
                 "%s: header checked (%d error(s))",db->db_filename,nerr);
    }
    return nerr;
}

/**
 * \brief Checks the integrity of the free-list.
 * \param[in,out] db mdbm handle
 * \param[in] verbose whether to display information messages
 * \return number of check failures
 */
static int
check_db_chunks(MDBM* db, int verbose)
{
    int pg;
    int cur_free;
    int nerr = 0;
    mdbm_hdr_t* h = db->db_hdr;
    int nchunks = 0;

    pg = 0;
    cur_free = h->h_first_free;
    while (pg < h->h_num_pages) {
        mdbm_page_t* page = MDBM_PAGE_PTR(db,pg);
        nchunks++;
        if (page->p_type == MDBM_PTYPE_FREE) {
            if (cur_free < 0) {
                if (verbose) {
                    mdbm_log(LOG_NOTICE,
                             "%s: resuming free-list checking with chunk %u",
                             db->db_filename,pg);
                }
                cur_free = pg;
            }
            if (cur_free > 0 && pg == cur_free) {
                cur_free = page->p.p_next_free;
            } else {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: chunk %u is free but not on free-list (next=%u)",
                             db->db_filename,pg,cur_free);
                }
                nerr++;
            }
        } else if (cur_free > 0 && pg == cur_free) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %u on free-list but not marked free (type=%u)",
                         db->db_filename,pg,page->p_type);
            }
            nerr++;
            if (verbose) {
                mdbm_log(LOG_NOTICE,
                         "%s: free-list checking suspended",
                         db->db_filename);
            }
            cur_free = -1;
        }
        if (cur_free > 0 && pg+page->p_num_pages > cur_free) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %u (npages=%u) overlaps next free-list chunk (%u)",
                         db->db_filename,pg,page->p_num_pages,cur_free);
            }
            nerr++;
            if (verbose) {
                mdbm_log(LOG_NOTICE,
                         "%s: free-list checking suspended",
                         db->db_filename);
            }
            cur_free = -1;
        }
        if (pg == h->h_last_chunk) {
            if (cur_free > 0) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: free-list (next=%u) runs past last chunk (%u)",
                             db->db_filename,cur_free,pg);
                }
                nerr++;
            }
            break;
        }
        if (MDBM_PAGE_PTR(db,pg+page->p_num_pages)->p_prev_num_pages != page->p_num_pages) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %d size (npages=%u) != chunk %d prev size (npages=%u)",
                         db->db_filename,pg,page->p_num_pages,pg+page->p_num_pages,
                         MDBM_PAGE_PTR(db,pg+page->p_num_pages)->p_prev_num_pages);
            }
            nerr++;
        }
        if (pg+page->p_num_pages > h->h_num_pages) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %d (npages=%u) overlaps end of db (npages=%u)",
                         db->db_filename,pg,page->p_num_pages,h->h_num_pages);
            }
            nerr++;
        }
        if (page->p_num_pages < 1) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %d invalid size (npages=%u); chunk scan aborted",
                         db->db_filename,pg,page->p_num_pages);
            }
            nerr++;
            break;
        }
        pg += page->p_num_pages;
    }
    if (verbose > 1) {
        mdbm_log(LOG_INFO,
                 "%s: %d chunks checked (%d error(s))",db->db_filename,nchunks,nerr);
    }
    return nerr;
}

static int
check_db_dir_entry(MDBM* db, int i, int verbose)
{
    mdbm_hdr_t* h = db->db_hdr;
    int p;
    int nerr = 0;

    if ((p = MDBM_GET_PAGE_INDEX(db,i)) != 0) {
        if (p > h->h_last_chunk) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: directory page %u maps to chunk %u past last chunk (%u)",
                         db->db_filename,i,p,h->h_last_chunk);
            }
            nerr++;
        } else {
            mdbm_page_t* page = MDBM_PAGE_PTR(db,p);
            if (page->p_type != MDBM_PTYPE_DATA) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: directory page %u maps to non-data chunk %u"
                             " (type=%u npages=%u)",
                             db->db_filename,i,p,page->p_type,page->p_num_pages);
                }
                nerr++;
            } else if (page->p_num != i) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: directory page %u != chunk %u directory page (%u)",
                             db->db_filename,i,p,page->p_num);
                }
                nerr++;
            }
        }
    }
    return nerr;
}

#define MDBM_PAGE_NOALLOC       0
#define MDBM_PAGE_ALLOC         1
#define MDBM_PAGE_EXISTS        2

#define MDBM_PAGE_NOMAP         0
#define MDBM_PAGE_MAP           1

#define MDBM_ALLOC_NO_UNLOCK    0
#define MDBM_ALLOC_CAN_UNLOCK   1

static void* alloc_chunk(MDBM* db, int type, int npages, int n0, int n1, int map, int lock);
static int free_chunk(MDBM* db, int pagenum, int* prevp);

static mdbm_page_t* pagenum_to_page(MDBM* db, int pagenum, int alloc, int map);


/**
 * \brief Checks the integrity of a data page.
 * - Once a specific check fails, no further checks are done.
 * - Check number of page entries in within bounds.
 * - Check number of free bytes is non-negative.
 * - Check top-of-page-marker is correct.
 * - Check each entry (key) offset is within page.
 * - Check each key length is within page.
 * - Check each entry match field is within page.
 * - Check each value length is within page.
 * - If non-windowed: check valid hash field.
 * - If non-windowed, LOB: LOB chunk is within bounds (header last_chunk).
 * - If non-windowed, LOB: LOB chunk has corresponding LOB page-type.
 * - If non-windowed, LOB: LOB internal page number matches \a pnum.
 * \param[in,out] db handle
 * \param[in] pnum page number
 * \param[in] verbose whether to display information messages
 * \param[in] level integrity check level
 * \return number of check failures
 */
static int
check_db_page(MDBM *db, int pnum, int verbose, int level)
{
    mdbm_page_t* page;
    mdbm_entry_t* ep;
    int index = -1;
    int max_page_entries;
    int mapped_pnum;
    int nerr = 0;

    if ((nerr = check_db_dir_entry(db,pnum,0)) > 0) {
        return nerr;
    }

    if ((page = pagenum_to_page(db,pnum,MDBM_PAGE_NOALLOC,MDBM_PAGE_MAP)) == NULL) {
        return 0;
    }
    mapped_pnum = MDBM_GET_PAGE_INDEX(db,pnum);

    max_page_entries = ((page->p_num_pages*db->db_pagesize - MDBM_PAGE_T_SIZE)
                        / MDBM_ENTRY_T_SIZE) - 1;
    if (page->p.p_num_entries > max_page_entries) {
        if (verbose) {
            mdbm_log(LOG_CRIT,"%s (page %d/%d): invalid num entries: %u",
                     db->db_filename,pnum,mapped_pnum,page->p.p_num_entries);
        }
        nerr++;
    } else if (MDBM_PAGE_FREE_BYTES(page) < 0) {
        if (verbose) {
            mdbm_log(LOG_CRIT,"%s (page %d/%d):"
                     " invalid page free bytes: %d (bottom-of-data=%d numentries=%d)",
                     db->db_filename,pnum,mapped_pnum,
                     MDBM_PAGE_FREE_BYTES(page),
                     MDBM_DATA_PAGE_BOTTOM_OF_DATA(page),
                     page->p.p_num_entries);
        }
        nerr++;
    } else if (MDBM_ENTRY(page,page->p.p_num_entries)->e_key.match != MDBM_TOP_OF_PAGE_MARKER) {
        if (verbose) {
            mdbm_log(LOG_CRIT,"%s (page %d/%d):"
                     " bad top-of-page marker: %08x (numentries=%d)",
                     db->db_filename,pnum,mapped_pnum,
                     MDBM_ENTRY(page,page->p.p_num_entries)->e_key.match,
                     page->p.p_num_entries);
        }
        nerr++;
    } else {
        int offset_limit;

        offset_limit = page->p_num_pages*db->db_pagesize;
        for (index = 0; index < page->p.p_num_entries; index++) {
            ep = MDBM_ENTRY(page,index);
            if (ep->e_offset > offset_limit) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s (page %d/%d): invalid entry offset (index %d): %u",
                             db->db_filename,pnum,mapped_pnum,index,ep->e_offset);
                }
                nerr++;
            } else if (index > 0 && ep->e_key.key.len > ep[-1].e_offset - ep[0].e_offset) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s (page %d/%d): invalid keylen or offset problem (index %d):"
                             " keylen=%u offsets=%d-%d=%d",
                             db->db_filename,pnum,mapped_pnum,index,ep->e_key.key.len,
                             ep[-1].e_offset,ep[0].e_offset,
                             ep[-1].e_offset - ep[0].e_offset);
                }
                nerr++;
            } else if (ep->e_key.match == MDBM_TOP_OF_PAGE_MARKER) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s (page %d/%d): premature top-of-page marker (index %d)",
                             db->db_filename,pnum,mapped_pnum,index);
                    }
                    nerr++;
            } else if (ep->e_key.match) {
                if (MDBM_VAL_LEN1(db,ep) < 0) {
                    if (verbose) {
                        mdbm_log(LOG_CRIT,
                                 "%s (page %d/%d): invalid vallen (index %d): vallen=%d",
                                 db->db_filename,pnum,mapped_pnum,index,
                                 MDBM_VAL_LEN1(db,ep));
                    }
                    nerr++;
                } else if (!MDBM_IS_WINDOWED(db)) {
                    if (level > 3) {
                        mdbm_hashval_t h = MDBM_HASH_VALUE(db,MDBM_KEY_PTR1(page,ep),
                                                           MDBM_KEY_LEN1(ep));
                        if (ep->e_key.key.hash != (h>>16)) {
                            if (verbose) {
                                mdbm_log(LOG_CRIT,
                                         "%s (page %d/%d): invalid key hash (index %d)",
                                         db->db_filename,pnum,mapped_pnum,index);
                            }
                            nerr++;
                        }
                    }
                    if (MDBM_ENTRY_LARGEOBJ(ep)) {
                        mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
                        if (lp->l_pagenum > db->db_hdr->h_last_chunk) {
                            if (verbose) {
                                mdbm_log(LOG_CRIT,
                                         "%s (page %d/%d):"
                                         " invalid lob chunk (index %d): l_pagenum=%u",
                                         db->db_filename,pnum,mapped_pnum,index,
                                         lp->l_pagenum);
                            }
                            nerr++;
                        } else {
                            mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
                            if (lob->p_type != MDBM_PTYPE_LOB) {
                                if (verbose) {
                                    mdbm_log(LOG_CRIT,
                                             "%s (page %d/%d): chunk is not lob (index %d):"
                                             " l_pagenum=%u type=%u",
                                             db->db_filename,pnum,mapped_pnum,index,
                                             lp->l_pagenum,lob->p_type);
                                }
                                nerr++;
                            } else if (lob->p_num != pnum) {
                                if (verbose) {
                                    mdbm_log(LOG_CRIT,
                                             "%s (page %d/%d): lob page ref mismatch (index %d):"
                                             " l_pagenum=%u lob.p_num=%u pnum=%u",
                                             db->db_filename,pnum,mapped_pnum,index,
                                             lp->l_pagenum,lob->p_num,pnum);
                                }
                                nerr++;
                            }
                        }
                    }
                }
                offset_limit = ep->e_offset;
            }
        }
    }

    if (MDBM_IS_WINDOWED(db)) {
        release_window_page(db,page);
    }

    return nerr;
}

/**
 * \brief Checks the integrity of the page directory.
 * \param[in,out] db handle
 * \param[in] verbose whether to display information messages
 * \return number of check failures
 */
static int
check_db_dir(MDBM* db, int verbose)
{
    int nerr = 0;
    int i;
    int p;
    mdbm_page_t* page;
    mdbm_hdr_t* h = db->db_hdr;
    int ndir = 0;

    page = MDBM_PAGE_PTR(db,0);
    if (page->p_type != MDBM_PTYPE_DIR) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: chunk 0 is not directory (type=%u, npages=%u)",
                     db->db_filename,page->p_type,page->p_num_pages);
        }
        nerr++;
    }
    if (db->db_dir_gen == db->db_hdr->h_dir_gen
        && memcmp(db->db_dir,MDBM_DIR_PTR(db),MDBM_DB_DIR_SIZE(db)))
    {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: db_dir (0x%p) out-of-sync with directory on page 0",
                     db->db_filename,db->db_dir);
        }
        nerr++;
    }
    if (MDBM_DB_NUM_DIR_PAGES(db) > page->p_num_pages) {
        if (verbose) {
            mdbm_log(LOG_CRIT,
                     "%s: directory chunk too small (npages=%u) for directory size (npages=%u)",
                     db->db_filename,page->p_num_pages,MDBM_DB_NUM_DIR_PAGES(db));
        }
        nerr++;
    }

    for (i = 0; i <= db->db_max_dirbit; i++) {
        nerr += check_db_dir_entry(db,i,verbose);
        ndir++;
    }

    p = 0;
    while (p <= h->h_last_chunk) {
        page = MDBM_PAGE_PTR(db,p);
        if (page->p_type == MDBM_PTYPE_DATA) {
            if (page->p_num > db->db_max_dirbit) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: invalid directory page (%u) referenced by chunk %u",
                             db->db_filename,page->p_num,p);
                }
                nerr++;
            } else if (MDBM_GET_PAGE_INDEX(db,page->p_num) != p) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: chunk/directory page mismatch (chunk=%u dir[%u]=%u)",
                             db->db_filename,p,page->p_num,MDBM_GET_PAGE_INDEX(db,page->p_num));
                }
                nerr++;
            }
        }
        if (page->p_num_pages < 1) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %d invalid size (npages=%u); dir/page check aborted",
                         db->db_filename,p,page->p_num_pages);
            }
            nerr++;
            break;
        }
        p += page->p_num_pages;
    }
    if (verbose > 1) {
        mdbm_log(LOG_INFO,
                 "%s: %d directory entries checked (%d error(s))",db->db_filename,ndir,nerr);
    }
    return nerr;
}

/**
 * \brief Checks the integrity of all data pages.
 * \param[in,out] db handle
 * \param[in] verbose whether to display information messages
 * \param[in] level integrity check level
 * \return number of check failures
 */
static int
check_db_pages(MDBM* db, int verbose, int level)
{
    int nerr = 0;
    int i;
    int npages = 0;

    for (i = 0; i <= db->db_max_dirbit; i++) {
        if (MDBM_GET_PAGE_INDEX(db,i)) {
            nerr += check_db_page(db,i,verbose,level);
            npages++;
        }
    }
    if (verbose > 1) {
        mdbm_log(LOG_INFO,
                 "%s: %d db pages checked (%d error(s))",db->db_filename,npages,nerr);
    }
    return nerr;
}

/**
 * \brief Checks the integrity of all large objects.
 * \param[in,out] db handle
 * \param[in] verbose whether to display information messages
 * \return number of check failures
 */
static int
check_db_lobs(MDBM* db, int verbose)
{
    int nerr = 0;
    mdbm_hdr_t* h = db->db_hdr;
    int p;
    int nlobs = 0;

    p = 0;
    while (p <= h->h_last_chunk) {
        mdbm_page_t* lob = MDBM_PAGE_PTR(db,p);
        if (lob->p_type == MDBM_PTYPE_LOB) {
            if (lob->p_num > db->db_max_dirbit) {
                if (verbose) {
                    mdbm_log(LOG_CRIT,
                             "%s: invalid lob page ref: chunk=%u p_num=%u",
                             db->db_filename,p,lob->p_num);
                    nerr++;
                }
            } else {
                mdbm_page_t* page;
                if ((page = pagenum_to_page(db,lob->p_num,MDBM_PAGE_NOALLOC,MDBM_PAGE_NOMAP))) {
                    mdbm_entry_t* ep;

                    for (ep = MDBM_ENTRY(page,0); ep->e_key.match != MDBM_TOP_OF_PAGE_MARKER; ep++)
                    {
                        if (ep->e_key.match && MDBM_ENTRY_LARGEOBJ(ep)) {
                            mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
                            if (lp->l_pagenum == p) {
                                break;
                            }
                        }
                    }
                } else {
                    if (verbose) {
                        mdbm_log(LOG_CRIT,
                                 "%s: lob page ref not allocated: chunk=%u p_num=%u",
                                 db->db_filename,p,lob->p_num);
                        nerr++;
                    }
                }
            }
            nlobs++;
        }
        if (lob->p_num_pages < 1) {
            if (verbose) {
                mdbm_log(LOG_CRIT,
                         "%s: chunk %d invalid size (npages=%u); lob check aborted",
                         db->db_filename,p,lob->p_num_pages);
            }
            nerr++;
            break;
        }
        p += lob->p_num_pages;
    }
    if (verbose > 1) {
        mdbm_log(LOG_INFO,
                 "%s: %d large objects checked (%d error(s))",db->db_filename,nlobs,nerr);
    }
    return nerr;
}

/**
 * \brief Check integrity of file header, free-list, data pages, and large objects.
 * \param[in,out] db mdbm handle
 * \param[in] level minimum level of checking
 * \param[in] maxlevel maximum level of checking
 * \param[in] verbose whether to display check information
 */
static int
check_db(MDBM* db, int level, int maxlevel, int verbose)
{
    int nerr = 0;

    if (check_guard_padding(db, 0) != 0) {
        ++nerr;
    }

    nerr += check_db_header(db,db->db_hdr,verbose);
    if (level <= maxlevel && level > 0) {
        nerr += check_db_chunks(db,verbose);
        if (level <= maxlevel && level > 1) {
            nerr += check_db_dir(db,verbose);
            if (level <= maxlevel && level > 2) {
                nerr += check_db_pages(db,verbose,level);
                nerr += check_db_lobs(db,verbose);
            }
        }
    }
    return nerr;
}

static int
check_empty(MDBM* db)
{
    int i;
    int empty = 1;

    for (i = 0; i <= db->db_max_dirbit; i++) {
        mdbm_page_t* page;
        if ((page = pagenum_to_page(db,i,MDBM_PAGE_NOALLOC,MDBM_PAGE_NOMAP))) {
            if (page->p.p_num_entries > 0) {
                empty = 0;
            }
            if (MDBM_IS_WINDOWED(db)) {
                release_window_page(db,page);
            }
            if (!empty) {
                break;
            }
        }
    }
    return empty;
}

#if 0
static void
print_dir(MDBM* db, mdbm_page_t* page)
{
    int i;
    int base = MDBM_ENTRY(page,0)->e_offset;

    for (i = 0; i <= page->p.p_num_entries; i++) {
        mdbm_entry_t* ep = MDBM_ENTRY(page,i);
        printf(" [%d]%s%d(%d)",i,ep->e_key.key.len ? "" : "*",ep->e_offset,base - ep->e_offset);
    }
    printf(" (num=%d free=%d)\n",page->p.p_num_entries,MDBM_PAGE_FREE_BYTES(page));
}
#endif

static int
find_free_chunk(MDBM* db, int npages, int n0, int n1, int* tot_free_pages)
{
    int n;
    int best_alloc = 0;
    int best_alloc_prev = 0;
    int best_alloc_pages = db->db_num_pages;
    int num_free = 0;
    int prev = 0;

    n = db->db_hdr->h_first_free;
    while (n > 0) {
        mdbm_page_t* page = MDBM_PAGE_PTR(db,n);
        if (!n0 || n+page->p_num_pages <= n0 || n > n1) {
            if (page->p_num_pages == npages) {
                alloc_free_chunk(db,npages,n,prev);
                return n;
            }
            num_free += page->p_num_pages;

            if (page->p_num_pages > npages) {
                if (page->p_num_pages < best_alloc_pages) {
                    best_alloc_prev = prev;
                    best_alloc = n;
                    best_alloc_pages = page->p_num_pages;
                }
            }
            if (page->p.p_next_free == n) {
                mdbm_log(LOG_CRIT, "%s: invalid next_free (%d vs %d) in find_free_chunk",
                         db->db_filename, page->p.p_next_free, n);
                abort();
            }
        }
        prev = n;
        n = page->p.p_next_free;
    }
    if (best_alloc) {
        alloc_free_chunk(db,npages,best_alloc,best_alloc_prev);
        return best_alloc;
    }
    *tot_free_pages = num_free;
    return 0;
}

static int
fixup_lob_pointer(MDBM* db, int data_page, int lob_page, int new_lob_page)
{
    mdbm_page_t* page = pagenum_to_page(db,data_page,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
    mdbm_entry_t* ep;

    for (ep = MDBM_ENTRY(page,0); ep->e_key.match != MDBM_TOP_OF_PAGE_MARKER; ep++) {
        if (ep->e_key.match && MDBM_ENTRY_LARGEOBJ(ep)) {
            mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
            if (lp->l_pagenum == lob_page) {
                lp->l_pagenum = new_lob_page;
                return 0;
            }
        }
    }
    return -1;
}

static int
clear_pages(MDBM* db, int p0, int npages)
{
    int n;
    mdbm_page_t* page;
    int p1 = p0 + npages - 1;
    int prev = 0;

    /* fprintf(stderr, "=============> clear_pages(%p, %d, %d)\n", (void*)db, p0, npages); */
    if (p1 >= db->db_num_pages) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "clear_pages failed, p1=%u, db_num_pages=%u",
                 p1, db->db_num_pages);
#endif
        errno = ENOMEM;
        return -1;
    }

    for (n = p0; n <= p1 && n <= db->db_hdr->h_last_chunk;) {
        page = MDBM_PAGE_PTR(db,n);
        if (page->p_type != MDBM_PTYPE_FREE) {
            mdbm_page_t* new_page;
            int new_pagenum;
            int prev_num_pages;

            page = MDBM_MAPPED_PAGE_PTR(db,n);
            if ((new_page = alloc_chunk(db,page->p_type,page->p_num_pages,p0,p1,MDBM_PAGE_MAP,0))
                == NULL)
            {
                return -1;
            }

            new_pagenum = new_page->p_num;
            prev_num_pages = new_page->p_prev_num_pages;
            memcpy(new_page,page,page->p_num_pages * db->db_pagesize);
            new_page->p_prev_num_pages = prev_num_pages;
            if (new_page->p_type == MDBM_PTYPE_DATA) {
/* fprintf(stderr, "=============> clear_pages(%p, %d, %d) n=%d->%d DATA\n", (void*)db, p0, npages, n, new_pagenum); */
/*              printf("move data page %d: %d -> %d\n", */
/*                     page->p_num, */
/*                     n, */
/*                     new_pagenum); */
                MDBM_SET_PAGE_INDEX(db,page->p_num, new_pagenum);
            } else if (new_page->p_type == MDBM_PTYPE_LOB) {
/* fprintf(stderr, "=============> clear_pages(%p, %d, %d) n=%d->%d LOB\n", (void*)db, p0, npages, n, new_pagenum); */
/*              printf("move lob page %d: %d -> %d\n", */
/*                     page->p_num, */
/*                     n, */
/*                     new_pagenum); */
                fixup_lob_pointer(db,page->p_num,n,new_pagenum);
            }
            n = free_chunk(db,n,&prev);
            if (n < p0) {
                p0 = n;
            }
            page = MDBM_PAGE_PTR(db,n);
        }
        n += page->p_num_pages;
    }

    if (p0 > db->db_hdr->h_last_chunk) {
        mdbm_page_t* last_page = MDBM_PAGE_PTR(db,db->db_hdr->h_last_chunk);
        assert(p0 == db->db_hdr->h_last_chunk + last_page->p_num_pages);
        page = MDBM_PAGE_PTR(db,p0);
        /* this is a placeholder type designation to keep the sanity checks happy */
        page->p_type = MDBM_PTYPE_DATA;
        page->p_num_pages = npages;
        page->p_prev_num_pages = last_page->p_num_pages;
        db->db_hdr->h_last_chunk = p0;
    } else {
        alloc_free_chunk(db,npages,p0,-1);
        page = MDBM_PAGE_PTR(db,p0);
        /* this is a placeholder type designation to keep the sanity checks happy */
        page->p_type = MDBM_PTYPE_DATA;
        page->p_num_pages = npages;
    }

    CHECK_DB(db);

    return p0;
}

static int
find_defrag_free_chunk(MDBM* db, int npages)
{
    int n;
    int best_moved = db->db_num_pages;
    int best_maxmove = db->db_num_pages;
    int best_n0 = 0;

/*     printf("defrag npages=%d dbpages=%d\n",npages,db->db_num_pages); */

    {
        int got;

        got = 0;
        n = MDBM_PAGE_PTR(db,0)->p_num_pages;
        while (got < npages && n <= db->db_hdr->h_last_chunk) {
            mdbm_page_t* p = MDBM_PAGE_PTR(db,n);
            int num = p->p_num_pages;
            if (p->p_type != MDBM_PTYPE_FREE && num > 1) {
                got = 0;
            } else {
                if (!got) {
                    best_n0 = n;
                }
                got += num;
            }
            n += num;
        }

        if (got >= npages) {
            if ((n = clear_pages(db,best_n0,npages)) > 0) {
                return n;
            }
        }
    }

    n = db->db_hdr->h_first_free;
    while (n > 0) {
        mdbm_page_t* n0p;
        int n0, n1, got, moved, maxmove, next;

        got = 0;
        n0 = n1 = n;
        n0p = MDBM_PAGE_PTR(db,n);

/*      printf("  n=%d\n",n); */

        maxmove = moved = 0;
        while (got < npages) {
            mdbm_page_t* n0pnew;
            mdbm_page_t* n1pnew;
            if (n0p && n0p->p_type != MDBM_PTYPE_DIR) {
                n0pnew = MDBM_PAGE_PTR(db,n0 - n0p->p_prev_num_pages);
            } else {
                n0pnew = NULL;
            }
            if (n1 <= db->db_hdr->h_last_chunk) {
                n1pnew = MDBM_PAGE_PTR(db,n1);
            } else {
                if (n1 < db->db_num_pages) {
                    got += db->db_num_pages - n1;
                    n1 = db->db_num_pages;
                    if (got >= npages) {
                        break;
                    }
                }
                n1pnew = NULL;
            }
/*          printf("   n0:%d-%d=%d n1:%d+%d=%d got=%d\n", */
/*                 n0,n0pnew ? n0p->p_prev_num_pages : 0, */
/*                 n0pnew ? n0 - n0p->p_prev_num_pages : 0, */
/*                 n1,n1pnew ? n1pnew->p_num_pages : 0, */
/*                 n1pnew ? n1 + n1pnew->p_num_pages : 0, */
/*                 got); */
            if (n0pnew && (!n1pnew
                           || n0pnew->p_type == MDBM_PTYPE_FREE
                           || (got + n1pnew->p_num_pages > npages
                               && n0pnew->p_num_pages < n1pnew->p_num_pages)
                           || (n0pnew->p_num_pages >= n1pnew->p_num_pages)))
            {
                n0p = n0pnew;
                n0 -= n0p->p_num_pages;
                got += n0p->p_num_pages;
                if (n0p->p_type != MDBM_PTYPE_FREE) {
                    if (n0p->p_num_pages > maxmove) {
                        maxmove = n0p->p_num_pages;
                    }
                    moved += n0p->p_num_pages;
                }
            } else if (n1pnew) {
                got += n1pnew->p_num_pages;
                n1 += n1pnew->p_num_pages;
                if (n1pnew->p_type != MDBM_PTYPE_FREE) {
                    if (n1pnew->p_num_pages > maxmove) {
                        maxmove = n1pnew->p_num_pages;
                    }
                    moved += n1pnew->p_num_pages;
                }
            }
            if (!n0pnew && !n1pnew) {
                break;
            }
        }
        if (got >= npages
            && (maxmove < best_maxmove
                || (maxmove == best_maxmove
                    && moved < best_moved)))
        {
            best_moved = moved;
            best_maxmove = maxmove;
            best_n0 = n0;
            if (moved == 1 || got == npages) {
                break;
            }
        }
        next = MDBM_PAGE_PTR(db,n)->p.p_next_free;
        assert(next > n || !next);
        n = next;
    }

    if (!best_n0) {
        return -1;
    }

    if ((n = clear_pages(db,best_n0,npages)) < 0) {
        return -1;
    }

/*     printf("defrag n0=%d npages=%d: n=%d\n",best_n0,npages,n); */
    CHECK_DB(db);

    return n;
}

static int
map(MDBM* db, void* start_addr, off_t start_offset, size_t len, char** base, size_t* base_len)
{
    void* m;
    int prot = MDBM_IS_RDONLY(db) ? PROT_READ : (PROT_READ|PROT_WRITE);
    int mapflags = MAP_SHARED;
    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
        mapflags |= MAP_ANON;
    }
#ifdef FREEBSD
    mapflags |= MAP_NOCORE;
    if ((db->db_flags & (MDBM_DBFLAG_ASYNC|MDBM_DBFLAG_FSYNC)) == 0) {
        mapflags |= MAP_NOSYNC;
    }
#endif
    len = (len+db->db_sys_pagesize-1) & ~((size_t)db->db_sys_pagesize-1);
    if (MAP_FAILED == (m = mmap(start_addr,len, prot, mapflags,db->db_fd,start_offset))) {
        mdbm_logerror(LOG_ERR,0,"%s: mmap failure mmap(%p, %llu, %d, %d, %d, %lld)",
            db->db_filename,
            (void*)start_addr, (unsigned long long)len, prot, mapflags, db->db_fd,
            (long long)start_offset);
        return -1;
    }
    /*fprintf(stderr, "map::mmap(%p, %d, %d, %d, %d, %d) = %p\n", start_addr, len, prot, */
    /*              mapflags,db->db_fd,(int)start_offset, m); */

    *base = m;
    *base_len = len;
    return 0;
}

static int
protect_dir(MDBM* db, int protect)
{
    int dir_size;
    int used;
    int total;

    if (db->db_flags & MDBM_DBFLAG_HDRONLY) {
        /* No extra space when mapped hdronly */
        return 0;
    }
    dir_size = MDBM_DB_NUM_DIR_BYTES(db);

    /* Round `used' up to a system page size. */
    used = (dir_size + (db->db_sys_pagesize - 1)) & ~(db->db_sys_pagesize - 1);

    /* Round `total' up to an mdbm page size. */
    total = dir_size + (db->db_pagesize - 1);

    /* Reduce the total size if the mdbm page size is greater then the system's page size. */
    if (db->db_pagesize > db->db_sys_pagesize) {
        total -= total % db->db_pagesize;
    }
    total -= total % db->db_sys_pagesize;
    if (total > used) {
        mdbm_log(LOG_DEBUG,
                 "protect_dir,"
                 " db_pagesize=%d, db_syspage_size=%d, dir_size=%d,"
                 " used=%d, total=%d, db_base=%p, db_base+used=%p, total - used=%d"
                 " mprotect start=%p, mprotect end=%p",
                 db->db_pagesize, db->db_sys_pagesize, dir_size,
                 used, total, db->db_base, db->db_base + used, total - used,
                 db->db_base + used, db->db_base + used + total - used - 1);
        if (mprotect(db->db_base + used,
                     total - used,
                     (protect
                      ? PROT_NONE
                      : (MDBM_IS_RDONLY(db) ? PROT_READ : (PROT_READ|PROT_WRITE)))) < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: mprotect failure",db->db_filename);
            return -1;
        }
    }
    return 0;
}

void
sync_dir(MDBM* db, mdbm_hdr_t* hdr)
{
    if (!hdr) {
        hdr = db->db_hdr;
    }
    if (db->db_dir) {
        free(db->db_dir);
    }
    /* HDRONLY means map only 1st page, so the entire directory may not be there (e.g. big MDBMs).
     * That's why we don't copy the directory, and data should not be accessed with HDRONLY */
    if (db->db_flags & MDBM_DBFLAG_HDRONLY) {
        db->db_dir = NULL;
    } else {
        db->db_dir = (mdbm_dir_t*)malloc(MDBM_DIR_SIZE(hdr->h_dir_shift));
        memcpy(db->db_dir,MDBM_DIR_PTR(db),MDBM_DIR_SIZE(hdr->h_dir_shift));
        db->db_dir_gen = hdr->h_dir_gen;
        db->db_dir_flags = hdr->h_dbflags & MDBM_HFLAG_PERFECT;
    }
}

static void
init(MDBM* db, mdbm_hdr_t* hdr)
{
    db->db_pagesize = hdr->h_pagesize;
    db->db_cache_mode = hdr->h_cache_mode;
    db->db_num_pages = hdr->h_num_pages;
    db->db_align_mask = hdr->h_dbflags & MDBM_ALIGN_MASK;
    db->db_dir_shift = hdr->h_dir_shift;
    db->db_max_dir_shift = hdr->h_max_dir_shift;
    db->db_max_dirbit = MDBM_HASH_MASK(db->db_dir_shift);
    sync_dir(db,hdr);
    db->db_ptable = MDBM_PTABLE_PTR(MDBM_DIR_PTR(db),db->db_dir_shift);
    db->db_hashfunc = mdbm_hash_funcs[hdr->h_hash_func];
    db->db_spillsize = (hdr->h_dbflags & MDBM_HFLAG_LARGEOBJ) ? hdr->h_spill_size : 0;
}

int
mdbm_internal_remap(MDBM *db, size_t dbsize, int flags)
{
    mdbm_hdr_t hdr;
    int got_hdr = 0;

    /* Save a copy of the header, then unmap current db. */
    if (db->db_base) {
        assert(sizeof(hdr) <= db->db_base_len);
        memcpy(&hdr,db->db_base+MDBM_PAGE_T_SIZE,sizeof(hdr));
        got_hdr = 1;
        if (munmap(db->db_base,db->db_base_len) < 0) {
            mdbm_logerror(LOG_ERR,0,"munmap(%p,%llu)",
                          db->db_base,(unsigned long long)db->db_base_len);
        }
        /*fprintf(stderr, "remap::munmap(%p,%u)\n", db->db_base,(unsigned)db->db_base_len); */
        db->db_base = NULL;
    }

    if (!dbsize || !got_hdr) {
        /* There's no header, and requested size is 0, so create a minmal map
         * based on the system page size. */
        uint64_t s;
        char* p;

        mdbm_log(LOG_DEBUG,"%s: remap, using default system page size for header",
                 db->db_filename);
        assert(sizeof(hdr) <= db->db_sys_pagesize);

        if ((p = (char*)mmap(0,db->db_sys_pagesize,PROT_READ,MAP_SHARED,db->db_fd,0))
            == MAP_FAILED)
        {
            mdbm_logerror(LOG_ERR,0,"%s: mdbm header mmap failure",db->db_filename);
            return -1;
        }
        /*fprintf(stderr, "remap::mmap(%p, %u, %d, %d, %d, %d) = %p\n", (void*)0,(unsigned)db->db_sys_pagesize,PROT_READ,MAP_SHARED,db->db_fd,0, p); */

        memcpy(&hdr,p+MDBM_PAGE_T_SIZE,sizeof(hdr));
        munmap(p,db->db_sys_pagesize);
        /*fprintf(stderr, "remap::munmap(%p,%u)\n", p,(unsigned)db->db_sys_pagesize); */

        if (check_db_header(db,&hdr,1) > 0) {
            errno = EBADF;
            return -1;
        }

        s = MDBM_HDR_DB_SIZE_64(&hdr);
#ifndef __x86_64__
        if (s > UINT_MAX && !MDBM_IS_WINDOWED(db)) {
            mdbm_log(LOG_ERR,
                     "%s: mdbm too large to open in 32-bit mode",
                     db->db_filename);
            errno = EFBIG;
            return -1;
        }
#endif
        if (!dbsize) {
            dbsize = s;
        }
    }

    if (MDBM_IS_WINDOWED(db)) {
#ifdef __linux__
        int syspagesz = db->db_sys_pagesize;
        int dbpagesz = hdr.h_pagesize;
        /*int ndbpages = hdr.h_num_pages; // db-sized pages */
        /* this provides enough space for the header, dir-bits, and ptable... */
        int ndirpages = MDBM_NUM_DIR_PAGES(dbpagesz,hdr.h_dir_shift);
        size_t mapsz = (size_t)ndirpages*dbpagesz;

        /* ensure mapsz is a multiple of db_page_size rounded up to sys_page_size */
        mapsz = (size_t)(MDBM_NUM_PAGES_ROUNDED(dbpagesz,  mapsz))*dbpagesz;
        mapsz = (size_t)(MDBM_NUM_PAGES_ROUNDED(syspagesz, mapsz))*syspagesz;

        /* map the header/dir/ptable and extra space */
        if (map(db,0,0,mapsz,&db->db_base,&db->db_base_len) < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: map failure",db->db_filename);
            return -1;
        }

        db->db_hdr = (mdbm_hdr_t*)(db->db_base + MDBM_PAGE_T_SIZE);
        if (check_db_header(db,db->db_hdr,1) > 0) {
            errno = EBADF;
            return -1;
        }
        init(db,db->db_hdr);

        if (!db->db_window.base) {
            static const int MDBM_DEFAULT_WINDOW_PAGES = 8;
            if (mdbm_set_window_size_internal(db,MDBM_DEFAULT_WINDOW_PAGES * dbpagesz) < 0) {
                return -1;
            }
        }
#endif
    } else {
        size_t mapsz = (flags & MDBM_HEADER_ONLY) ? (size_t)sizeof(hdr) : dbsize;
        if (map(db,NULL,0,mapsz,&db->db_base,&db->db_base_len) < 0) {
            return -1;
        }

        if (flags & MDBM_HEADER_ONLY) {
            db->db_flags |= MDBM_DBFLAG_HDRONLY;
        }
        if (db->db_flags & MDBM_DBFLAG_LOCK_PAGES) {
            pin_pages(db);   /* Pin pages into memory, if necessary */
        }

        db->db_hdr = (mdbm_hdr_t*)(db->db_base + MDBM_PAGE_T_SIZE);
        if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
            memcpy(db->db_hdr, &hdr,sizeof(hdr));
        }

        if (check_db_header(db,db->db_hdr,1) > 0) {
            errno = EBADF;
            return -1;
        }
        init(db,db->db_hdr);
    }
    protect_dir(db,1);
    if (db->db_dup_info) {
        update_dup_map_gen(db);
    }

    return 0;
}

/**
 * Resizes a db.  Adjusts the header, page directory, and page table appropriately.
 *
 * \param[in,out] db mdbm handle
 * \param[in] npages total number of pages for the db
 * \return return code
 * \retval 0 success
 * \retval -1 failure, errno set
 */
static int
resize_db(MDBM* db, int npages)
{
    size_t dbsize;
    int prev_npages;

#ifndef __x86_64__
    if (npages > UINT_MAX/db->db_pagesize) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "Cannot remap over 4GB, npages=%d", npages);
#endif
        errno = ENOMEM;
        return -1;
    }
#endif

    if (npages > MDBM_NUMPAGES_MAX) {
        mdbm_log(LOG_ERR, "MDBM cannot grow to %d pages, max=%d", npages, MDBM_NUMPAGES_MAX);
        errno = ENOSPC;
        return -1;
    }

    dbsize = (size_t)npages * db->db_pagesize;
    prev_npages = db->db_num_pages;
    db->db_num_pages = db->db_hdr->h_num_pages = npages;

    if (!(db->db_flags & (MDBM_DBFLAG_MEMONLYCACHE|MDBM_DBFLAG_HUGEPAGES))) {
        if (ftruncate(db->db_fd,dbsize) < 0) {
            db->db_num_pages = db->db_hdr->h_num_pages = prev_npages;
            mdbm_logerror(LOG_ERR,0,"%s: ftruncate failure",db->db_filename);
            return -1;
        }
    }

    if (mdbm_internal_remap(db,dbsize,0) < 0) {
        /* remap failed, db is currently unmapped.          */
        /* restore the original number of pages for the db. */
        dbsize = (size_t)prev_npages * db->db_pagesize;
        db->db_num_pages = prev_npages;
        /* This remap restores original size, so should mostly succeed, and failure means
         * no MDBM data is mapped in. */
        if (mdbm_internal_remap(db,dbsize,0) < 0) {
            mdbm_log(LOG_ERR, "remap cannot restore size to %d pages, MDBM handle invalid",
                     prev_npages);
            errno = ENOSPC;
            return -1;
        }
        db->db_hdr->h_num_pages = prev_npages;
        init(db,db->db_hdr);
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "remap cannot resize, npages=%d", npages);
#endif
        errno = ENOMEM;
        return -1;
    }

    return 0;
}

static int resize(MDBM* db, int new_dirshift, int new_numpages);

static void*
alloc_chunk(MDBM* db, int type, int npages, int n0, int n1, int map, int lock)
{
    int n;
    mdbm_page_t* page;
    int lock_upgraded = 0;

    CHECK_LOCK_INTERNAL_ISOWNED;

/*     printf("**alloc_chunk npages=%d\n",npages); */

    /* find a free chunk */
    for (;;) {
        int last_chunk, last_chunk_pages;
        int tot_free_pages;

        if ((n = find_free_chunk(db,npages,n0,n1,&tot_free_pages)) != 0) {
            break;
        }

        /* nothing big enough; how about unused space? */
        last_chunk = db->db_hdr->h_last_chunk;
        page = MDBM_PAGE_PTR(db,last_chunk);
        last_chunk_pages = page->p_num_pages;

        if (last_chunk+last_chunk_pages+npages <= db->db_hdr->h_num_pages) {
            n = last_chunk + last_chunk_pages;
            page = MDBM_PAGE_PTR(db,n);
            page->p_prev_num_pages = last_chunk_pages;
            db->db_hdr->h_last_chunk = n;
            break;
        }

        if (n0) {
            page = NULL;
            goto alloc_ret;
        }

        if (resize(db,0,last_chunk + last_chunk_pages + npages) < 0) {
            if (tot_free_pages >= npages) {
                /* Need db lock in order to defrag */
                if (!mdbm_isowned(db)) {
                    if (lock != MDBM_ALLOC_CAN_UNLOCK) {
                        page = NULL;
                        goto alloc_ret;
                    }
                    mdbm_internal_unlock(db);
                    if (mdbm_lock(db) != 1) {
                        return NULL;
                    }
                    if (mdbm_internal_lock(db) < 0) {
                        mdbm_unlock(db);
                        return NULL;
                    }
                    lock_upgraded = 1;
                }
                if ((n = find_defrag_free_chunk(db,npages)) >= 0) {
                    break;
                }
            }
            page = NULL;
            goto alloc_ret;
        }
    }

    page = MDBM_CHUNK_PTR(db,n,npages);
    page->p_num = n;
    page->p_type = type;
    page->p_flags = 0;
    page->p_num_pages = npages;
    page->p_r0 = 0;
    page->p_r1 = 0;
    page->p.p_data = 0;

/*     printf("alloc_chunk npages=%d => page %d\n",npages,n); */
    CHECK_DB_PARTIAL(db,1);

 alloc_ret:
    if (!page && lock == MDBM_ALLOC_CAN_UNLOCK) {
        mdbm_internal_unlock(db);
    }
    if (lock_upgraded) {
        mdbm_unlock(db);
    }

    return page;
}

static int
free_chunk(MDBM* db, int pagenum, int* prevp)
{
    int npages;
    mdbm_page_t* page;
    int prevprev, prev, next, p1;

    CHECK_LOCK_INTERNAL_ISOWNED;

    page = MDBM_PAGE_PTR(db,pagenum);
    npages = page->p_num_pages;

#ifdef DEBUG
     printf("**free_chunk p=%d npages=%d (prev npages=%d)\n",
        pagenum,npages,page->p_prev_num_pages);
#endif

    if (pagenum == 0) {
        mdbm_log(LOG_CRIT, "%s: invalid attempt to free chunk 0",db->db_filename);
        abort();
    }

    page->p_type = MDBM_PTYPE_FREE;
    page->p_num = pagenum;

    /* Find 2 previous free chunks and next free chunk */
    prevprev = 0;
    prev = 0;
    next = db->db_hdr->h_first_free;
    if (prevp && *prevp) {
        prevprev = *prevp;
        prev = MDBM_PAGE_PTR(db,prevprev)->p.p_next_free;
        next = MDBM_PAGE_PTR(db,prev)->p.p_next_free;
    }
    while (next && next < pagenum) {
        prevprev = prev;
        prev = next;
        next = MDBM_PAGE_PTR(db,next)->p.p_next_free;
    }

    if (next == pagenum) {
        mdbm_log(LOG_CRIT, "%s: attempt to free chunk (%d) on free-list (npages=%d prev=%d)",
                 db->db_filename,pagenum,npages,prev);
        abort();
    }

    /* p1 is page number of (possibly-merged) free chunk */
    p1 = pagenum;

    if (prev) {
        mdbm_page_t* pprev = MDBM_PAGE_PTR(db,prev);
        if (prev + pprev->p_num_pages == pagenum) {
            /* previous free chunk adjoins: merge chunks */
            assert(pprev->p_type == MDBM_PTYPE_FREE);
            assert(pagenum - page->p_prev_num_pages == prev);
            pprev->p_num_pages += npages;
            npages = 0; /* signal that we've merged */
            if (pagenum < db->db_hdr->h_last_chunk) {
                /* merged chunk is not the last chunk, so update back-pointer */
                MDBM_PAGE_PTR(db,prev + pprev->p_num_pages)->p_prev_num_pages = pprev->p_num_pages;
            } else {
                /* chunk being freed was last chunk */
                assert(pagenum == db->db_hdr->h_last_chunk);
                db->db_hdr->h_last_chunk = prev;
            }
            p1 = prev; /* update page number to new merged chunk */
            prev = prevprev;
        }
    }
    if (next) {
        mdbm_page_t* pp1 = MDBM_PAGE_PTR(db,p1);
        assert(pp1->p_type == MDBM_PTYPE_FREE);
        if (p1 + pp1->p_num_pages == next) {
            /* following free chunk adjoins: merge chunks */
            mdbm_page_t* pnext = MDBM_PAGE_PTR(db,next);
            assert(next - pnext->p_prev_num_pages == p1);
            pp1->p_num_pages += pnext->p_num_pages;
            pp1->p.p_next_free = pnext->p.p_next_free;
            npages = 0;
            if (db->db_hdr->h_first_free == next) {
                /* following free chunk was head of free list; merged chunk is new head */
                assert(prev == 0);
                db->db_hdr->h_first_free = p1;
            } else if (prev != 0) {
                /* update next pointer in previous free list chunk */
                MDBM_PAGE_PTR(db,prev)->p.p_next_free = p1;
            }

            /* last chunk should never be free, so following free chunk shouldn't be last */
            assert(db->db_hdr->h_last_chunk != next);

            /* update back-pointer on following chunk */
            MDBM_PAGE_PTR(db,p1 + pp1->p_num_pages)->p_prev_num_pages = pp1->p_num_pages;
        }
    }
    if (npages) {
        /* wasn't merged, so add to free list */
        if (prev) {
            mdbm_page_t* pprev = MDBM_PAGE_PTR(db,prev);
            page->p.p_next_free = pprev->p.p_next_free;
            pprev->p.p_next_free = pagenum;
        } else {
            page->p.p_next_free = db->db_hdr->h_first_free;
            db->db_hdr->h_first_free = pagenum;
        }
    }
    if (p1 == db->db_hdr->h_last_chunk) {
        /* we're the last chunk, but last chunk can't be free; adjust */
        if (db->db_hdr->h_first_free == p1) {
            assert(prev == 0);
            db->db_hdr->h_first_free = 0;
        } else {
            assert(prev != 0);
            MDBM_PAGE_PTR(db,prev)->p.p_next_free = 0;
        }
        /* update last chunk to previous chunk */
        db->db_hdr->h_last_chunk = p1 - MDBM_PAGE_PTR(db,p1)->p_prev_num_pages;
    }

    CHECK_DB(db);

    if (prevp) {
        if (prevprev) {
            *prevp = prevprev;
        } else {
            *prevp = 0;
        }
    }

    return p1;
}

static void
free_large_object_chunks(MDBM* db)
{
    int pno = 0;
    int prev = 0;
    while (pno <= db->db_hdr->h_last_chunk) {
        mdbm_page_t* page = MDBM_PAGE_PTR(db,pno);
        if (page->p_type == MDBM_PTYPE_LOB) {
            free_chunk(db,pno,&prev);
        }
        pno += page->p_num_pages;
    }
}

/* Expand an existing chunk to at least 'npages' size.
 * 'pagenum' is the physical page number in units of db-page-size.
 * 'npages' is the desired number of (contiguous) pages in units of db-page-size.
 */
static int
grow_chunk(MDBM* db, int pagenum, int npages)
{
    mdbm_page_t* page = MDBM_PAGE_PTR(db,pagenum);

    if (pagenum + npages > db->db_num_pages) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "grow_chunk failed, pagenum=%d, npages=%d, db_num_pages=%u",
                 pagenum, npages, db->db_num_pages);
#endif
        errno = ENOMEM;
        return -1;
    }
    if (page->p_num_pages >= npages) {
        mdbm_log(LOG_WARNING,
            "grow_chunk() redundant, pagenum=%d, npages=%d (already %d), db_num_pages=%u",
            pagenum, npages, page->p_num_pages, db->db_num_pages);
        return 0;
    }
    if (pagenum == db->db_hdr->h_last_chunk) {
        page->p_num_pages = npages;
    } else {
        if (clear_pages(db,pagenum+page->p_num_pages,npages-page->p_num_pages) < 0) {
            return -1;
        }
        page->p_num_pages = npages;
        MDBM_PAGE_PTR(db,pagenum + npages)->p_prev_num_pages = page->p_num_pages;
    }

    CHECK_DB(db);

    return 0;
}

static void
shrink_page(MDBM* db, mdbm_page_t* page)
{
    int pagenum = page->p_num;
    int p = MDBM_GET_PAGE_INDEX(db,pagenum);
    MDBM_SET_PAGE_INDEX(db,pagenum, 0);
    free_chunk(db,p,NULL);

    pagenum_to_page(db,pagenum,MDBM_PAGE_ALLOC,MDBM_PAGE_NOMAP);
    CHECK_DB(db);
}

static inline int
MDBM_MAX_PGMAP_ENTRIES(const MDBM* db)
{
    return ((db->db_pagesize-MDBM_PAGE_T_SIZE)/MDBM_ENTRY_T_SIZE) - 1;
}

static mdbm_page_t*
pagenum_to_page(MDBM* db, int pagenum, int alloc, int map)
{
    mdbm_page_t* page;
    int p;
#ifdef MDBM_SANITY_CHECKS
    if (pagenum < 0 || pagenum > db->db_max_dirbit) {
        mdbm_log(LOG_CRIT, "%s: invalid pagenum (%d/%d) in pagenum_to_page",
                 db->db_filename,pagenum,db->db_max_dirbit);
        abort();
    }
#endif
    if ((p = MDBM_GET_PAGE_INDEX(db,pagenum)) != 0) {
        page = MDBM_PAGE_PTR(db,p);
        if (MDBM_IS_WINDOWED(db)
            && (map || (page->p.p_num_entries+1 >= MDBM_MAX_PGMAP_ENTRIES(db))))
        {
            /* force a remap if it's larger than a single page */
            page = get_window_page(db,page,0,0,0,0);
            if (!page) {
              return NULL;
            }
        }
#ifdef MDBM_SANITY_CHECKS
        if (p > db->db_num_pages || page->p_type != MDBM_PTYPE_DATA) {
            mdbm_log(LOG_CRIT, "%s: invalid page (%d/%d type:%d vs %d) in pagenum_to_page",
                     db->db_filename,p,db->db_num_pages, page->p_type, MDBM_PTYPE_DATA);
            abort();
        }
#endif
        return page;
    }

#ifdef MDBM_SANITY_CHECKS
    if (alloc == MDBM_PAGE_EXISTS) {
        mdbm_log(LOG_CRIT, "%s: alloc==MDBM_PAGE_EXISTS in pagenum_to_page",
                 db->db_filename);
        abort();
    }
#endif
    if (alloc == MDBM_PAGE_NOALLOC) {
        return NULL;
    }

    if (mdbm_internal_lock(db) < 0) {
        return NULL;
    }

    if ((page = alloc_chunk(db,MDBM_PTYPE_DATA,1,0,0,MDBM_PAGE_NOMAP,MDBM_ALLOC_CAN_UNLOCK))
        == NULL)
    {
        /* alloc_chunk releases internal lock if allocation fails */
        return NULL;
    }

    MDBM_INIT_TOP_ENTRY(MDBM_ENTRY(page,0),db->db_pagesize);
    MDBM_SET_PAGE_INDEX(db, pagenum, page->p_num);
    page->p_num = pagenum;
    mdbm_internal_unlock(db);

    if (map && MDBM_IS_WINDOWED(db)) {
        page = get_window_page(db,page,0,0,0,0);
    }

    return page;
}

static mdbm_entry_t*
first_entry(mdbm_page_t* page, mdbm_entry_data_t* e)
{
    e->e_page = page;
    e->e_index = 0;
    if (page->p.p_num_entries > 0) {
        e->e_entry = MDBM_ENTRY(e->e_page,e->e_index);
    } else {
        e->e_entry = NULL;
    }
    return e->e_entry;
}

static inline mdbm_entry_t*
set_entry(MDBM* db, mdbm_pagenum_t pagenum, int index, mdbm_entry_data_t* e)
{
    if ((e->e_page = pagenum_to_page(db,pagenum,MDBM_PAGE_NOALLOC,MDBM_PAGE_MAP)) == NULL
        || index >= e->e_page->p.p_num_entries)
    {
        e->e_entry = NULL;
        e->e_index = -1;
    } else {
        e->e_index = index;
        e->e_entry = MDBM_ENTRY(e->e_page,index);
    }
    return e->e_entry;
}

static inline mdbm_entry_t*
next_entry(mdbm_entry_data_t* e)
{
    if (++e->e_index < e->e_page->p.p_num_entries) {
        e->e_entry++;
    } else {
        e->e_entry = NULL;
    }
    return e->e_entry;
}

static void
get_kv0 (MDBM* db, mdbm_page_t* p, mdbm_entry_t* ep, datum* key, datum* val,
         struct mdbm_fetch_info* info, int upd_cache, uint32_t lob_alloclen)
{
    key->dptr = MDBM_KEY_PTR1(p,ep);
    key->dsize = MDBM_KEY_LEN1(ep);
    if (val != NULL) {
        char* v = MDBM_VAL_PTR1(db,p,ep);
        int vlen = MDBM_VAL_LEN1(db,ep);
        if (MDBM_DB_CACHEMODE(db)) {
            mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)v;
            v += MDBM_CACHE_ENTRY_T_SIZE;
            vlen -= MDBM_CACHE_ENTRY_T_SIZE;
            if (upd_cache && !MDBM_IS_RDONLY(db)) {
                switch (MDBM_DB_CACHEMODE(db)) {
                case MDBM_CACHEMODE_LFU:
                case MDBM_CACHEMODE_LRU:
                    c->c_num_accesses++;
                    c->c_dat.access_time = get_time_sec();
                    break;

                case MDBM_CACHEMODE_GDSF:
                    c->c_num_accesses++;
                    c->c_dat.priority = (float)c->c_num_accesses / (key->dsize + vlen);
                    break;
                }
            }
            if (info != NULL) {
                info->flags = ((!(db->db_flags & MDBM_DBFLAG_NO_DIRTY) && MDBM_ENTRY_DIRTY(ep))
                               ? MDBM_FETCH_FLAG_DIRTY
                               : 0);
                info->cache_num_accesses = c->c_num_accesses;
                switch (MDBM_DB_CACHEMODE(db)) {
                case MDBM_CACHEMODE_LFU:
                case MDBM_CACHEMODE_LRU:
                    info->cache_access_time = c->c_dat.access_time;
                    break;

                case MDBM_CACHEMODE_GDSF:
                    info->cache_access_time = 0;
                    break;
                }
            }
        }
        if (MDBM_ENTRY_LARGEOBJ(ep)) {
            uint32_t lob_pages, vallen;
            mdbm_page_t* lob;
            mdbm_entry_lob_t l = *((mdbm_entry_lob_t*)v);

            if (!lob_alloclen) {
                MDBM_ENTRY_LOB_ALLOC_LEN1(db,p,ep,&vallen,&lob_alloclen);
            }
            lob_pages = lob_alloclen / db->db_pagesize;
            lob = MDBM_CHUNK_PTR(db,l.l_pagenum,lob_pages);
            val->dptr = MDBM_LOB_VAL_PTR(lob);
            val->dsize = MDBM_LOB_VAL_LEN(lob);
        } else {
            val->dptr = v;
            val->dsize = vlen;
        }
    }
}

static inline void
get_kv(MDBM* db, mdbm_entry_data_t* e, datum* key, datum* val)
{
    get_kv0(db,e->e_page,e->e_entry,key,val,NULL,0,0);
}

static inline void
get_kv1 (MDBM* db, mdbm_page_t* p, mdbm_entry_t* ep, datum* key, datum* val,
         struct mdbm_fetch_info* info)
{
    get_kv0(db,p,ep,key,val,info,1,0);
}

static void
get_kv2 (MDBM* db, mdbm_page_t* p, mdbm_entry_t* ep, datum* key, datum* val)
{
    get_kv0(db,p,ep,key,val,NULL,0,0);
}

static mdbm_entry_t*
find_entry(MDBM* db, mdbm_page_t* page, const datum* key, mdbm_hashval_t hash,
           datum* k, datum* v, struct mdbm_fetch_info* info)
{
    /* requires page lock */

    mdbm_entry_t test;
    mdbm_entry_t* ep;
    unsigned int i;
    mdbm_page_t* rpage = page;
    int lob_alloclen = 0;

    test.e_key.key.len = key->dsize;
    test.e_key.key.hash = hash >> 16;

    ep = MDBM_ENTRY(page,0);
    i = 0;
    for (;;) {
        char* kp;

        while (ep[i].e_key.match != test.e_key.match && i < page->p.p_num_entries) {
        /* fprintf(stderr, "find_entry() page:%p, entry:%d key:[%s]\n", (void*)page, i, ((char*)page)+MDBM_ENTRY_OFFSET(db,ep+i)); */
        /* fprintf(stderr, "find_entry() match: %d vs %d\n", ep[i].e_key.match, test.e_key.match );*/
            i++;
        }
        if (i == page->p.p_num_entries) {
            return NULL;
        }

        if (MDBM_IS_WINDOWED(db)) {
            int ent_off = MDBM_ENTRY_OFFSET(db,ep+i);
            int ent_len = MDBM_ENTRY_LEN(db,ep+i);
            rpage = get_window_page(db,page,0,0, ent_off, ent_len);
            if (!rpage) {
              return NULL;
            }
        }
        kp = MDBM_KEY_PTR1(rpage,ep+i);
        if (memcmp(kp,key->dptr,key->dsize)) {
            i++;
            continue;
        }
        break;
    }

    if (MDBM_IS_WINDOWED(db) && MDBM_ENTRY_LARGEOBJ(ep+i)) {
        uint32_t vallen, lob_alloclen;
        int n;

        MDBM_ENTRY_LOB_ALLOC_LEN1(db,rpage,ep+i,&vallen,&lob_alloclen);
        n = 2 + lob_alloclen/db->db_pagesize;
        if (db->db_window.num_pages < n) {
            mdbm_log(LOG_ERR,
                     "%s: window size too small for large object (need at least %d bytes)",
                     db->db_filename,n * db->db_pagesize);
            return NULL;
        }
    }

    get_kv0(db,rpage,ep+i,k,v,info,1,lob_alloclen);
    return ep+i;
}

static int
del_entry(MDBM* db, mdbm_page_t* page, mdbm_entry_t* ep)
{
    /* requires page lock */

    int index = MDBM_ENTRY_INDEX(page,ep);
    int offset;

    if (MDBM_ENTRY_LARGEOBJ(ep)) {
        mdbm_page_t* rpage = page;
        mdbm_entry_lob_t*  lp;

        if (MDBM_IS_WINDOWED(db)) {
            int ent_off = MDBM_ENTRY_OFFSET(db,ep);
            int ent_len = MDBM_ENTRY_LEN(db,ep);
            rpage = get_window_page(db,page,0,0, ent_off, ent_len);
            if (!rpage) {
              return -1;
            }
        }
        lp = MDBM_LOB_PTR1(db,rpage,ep);
        if (mdbm_internal_lock(db) < 0) {
            return -1;
        }
        free_chunk(db,lp->l_pagenum,NULL);
        mdbm_internal_unlock(db);
    }

    offset = ep[0].e_offset + MDBM_ALIGN_LEN(db,ep[0].e_key.key.len);
    ep->e_key.match = 0;
    ep->e_offset = offset;

    if (index == page->p.p_num_entries-1) {
        while (index > 0 && !ep[-1].e_key.match) {
            index--;
            ep--;
        }
        page->p.p_num_entries = index;
        MDBM_INIT_TOP_ENTRY(ep,ep->e_offset);
        if (!index && page->p_num_pages > 1) {
            /* Free oversized page. */
            if (mdbm_internal_lock(db) == 1) {
                shrink_page(db,page);
                mdbm_internal_unlock(db);
            }
        }
    } else {
        if (index > 0 && !ep[-1].e_key.match) {
            offset = ep[-1].e_offset;
        }
        while (!ep->e_key.match) {
            ep->e_offset = offset;
            ep++;
        }
    }
    return 0;
}

static void
wring_page(MDBM* db, mdbm_page_t* page)
{
    /* requires page lock */

    mdbm_entry_data_t e;
    mdbm_entry_t* ep;
    int index = -1;
    int offset = 0;

    int num = 0;
    for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
        if (ep->e_key.match) {
            num++;
            if (offset) {
                mdbm_entry_t* move_ep = MDBM_ENTRY(page,index);
                int ksize = MDBM_ALIGN_LEN(db,ep->e_key.key.len);
                int vsize = ep[0].e_offset -
                            ep[1].e_offset -
                            MDBM_ALIGN_LEN(db,ep[1].e_key.key.len);
                int kvsize = ksize + vsize;

                offset -= kvsize;
                move_ep->e_key.match = ep->e_key.match;
                move_ep->e_flags = ep->e_flags;
                move_ep->e_offset = offset + vsize;
                memmove((char*)page + offset,MDBM_VAL_PTR2(page,ep->e_offset,vsize),kvsize);

                index++;
            }
        } else {
            if (!offset) {
                index = e.e_index;
                offset = e.e_entry->e_offset;
            }
        }
    }
    if (offset) {
        MDBM_INIT_TOP_ENTRY(MDBM_ENTRY(page,num),offset);
        page->p.p_num_entries = num;
    }
    /* printf("wring page=%d: before=%d after=%d\n",page->p_num,n,MDBM_PAGE_FREE_BYTES(page)); */
}

static int
resize(MDBM* db, int new_dirshift, int new_num_pages)
{
    /* requires db lock */

    int old_num_pages = db->db_num_pages;
    int old_dirpages;
    int old_dirshift = db->db_dir_shift;
    int old_dirsize;
    int old_ptsize;
    int new_dirpages;
    int new_dirwidth;
    int new_dirsize;
    int new_ptsize;
    mdbm_ptentry_t* new_ptable;

    if (!new_dirshift) {
        new_dirshift = old_dirshift;
    }
    if (!new_num_pages) {
        new_num_pages = old_num_pages + MDBM_NUM_DIR_PAGES(db->db_pagesize,new_dirshift);
    }
    new_dirwidth = MDBM_DIR_WIDTH(new_dirshift);
    if (new_dirwidth > new_num_pages) {
        new_num_pages = new_dirwidth;
    }
    if (new_dirshift <= old_dirshift && new_num_pages <= old_num_pages) {
        return 0;
    }
    if (new_num_pages > old_num_pages) {
        if ((db->db_hdr->h_max_pages && new_num_pages > db->db_hdr->h_max_pages)
                || resize_db(db,new_num_pages) < 0) {
            mdbm_log(LOG_DEBUG,
                 "resize db failure,"
                 " old_num_pages=%d, new_num_pages=%d,"
                 " dirwidth=%d, new_dirwidth=%d)\n",
                 old_num_pages, new_num_pages,
                 MDBM_DB_DIR_WIDTH(db), new_dirwidth);
            errno = ENOSPC;
            return -1;
        }
        new_num_pages = db->db_num_pages;
    }
    if (new_dirshift == old_dirshift) {
        return 0;
    }

    old_dirpages = MDBM_DB_NUM_DIR_PAGES(db);
    old_dirsize = MDBM_DB_DIR_SIZE(db);
    old_ptsize = MDBM_DB_PTABLE_SIZE(db);

    new_dirpages = MDBM_NUM_DIR_PAGES(db->db_pagesize,new_dirshift);
    new_dirsize = MDBM_DIR_SIZE(new_dirshift);
    new_ptsize = MDBM_PTABLE_SIZE(new_dirshift);

    protect_dir(db,0);

    if (new_dirpages > old_dirpages) {
        if (grow_chunk(db,0,new_dirpages) < 0) {
            protect_dir(db,1);
            return -1;
        }
    }

    if (new_dirsize > old_dirsize) {
        /* Initialize new directory and page table */
        new_ptable = MDBM_PTABLE_PTR(MDBM_DIR_PTR(db),new_dirshift);
        mdbm_log(LOG_DEBUG,
                 "move page table,"
                 " db_dir=%p, old_dirsize=%d, new_dirsize=%d,"
                 " old_ptable=%p, old_ptsize=%d,"
                 " new_ptable=%p, new_ptsize=%d,"
                 " directory size increase=%d\n",
                 db->db_dir,
                 old_dirsize,
                 new_dirsize,
                 (void*)db->db_ptable,
                 old_ptsize,
                 (void*)new_ptable,
                 new_ptsize,
                 new_dirsize - old_dirsize);

        memmove(new_ptable,db->db_ptable,old_ptsize);
        db->db_ptable = new_ptable;
        /* Clear the added portion at the end of the new page directory. */
        memset(MDBM_DIR_PTR(db)+old_dirsize,0,new_dirsize - old_dirsize);
        /* NOTE: this gets set again below, but sync_dir allocates memory based on it. */
        db->db_hdr->h_dir_shift = (uint8_t)new_dirshift;
        sync_dir(db,NULL);
    }
    /* Clear the added portion at the end of the new page table. */
    mdbm_log(LOG_DEBUG,
             "Clear new page table, db_ptable=%p, db_ptable+old_ptsize=%p, size=%d",
             (void*)db->db_ptable, (void*)(db->db_ptable+old_ptsize), new_ptsize - old_ptsize);
    memset((char*)db->db_ptable+old_ptsize,0,new_ptsize - old_ptsize);

    db->db_dir_shift = db->db_hdr->h_dir_shift = (uint8_t)new_dirshift;
    db->db_max_dirbit = MDBM_HASH_MASK(db->db_dir_shift);

    protect_dir(db,1);

    return 0;
}

static mdbm_page_t*
split_page(MDBM* db, mdbm_hashval_t hashval)
{
    int dirbit;
    int hashbit;
    mdbm_pagenum_t pagenum, newpagenum;
    mdbm_page_t* page;
    mdbm_page_t* newpage;
    mdbm_entry_data_t e;
    mdbm_entry_t* ep;
    mdbm_hashval_t hvbit;
    mdbm_hashval_t hv;
    int new_index;
    int new_offset;
    int num;

    hv = hashval;
    dirbit = 0;
    hashbit = 0;
    while (dirbit < db->db_max_dirbit && MDBM_DIR_BIT(db,dirbit)) {
        dirbit = (dirbit << 1) + (hv & 1) + 1;
        hashbit++;
        hv >>= 1;
    }

    pagenum = (hashval & MDBM_HASH_MASK(hashbit));
    newpagenum = pagenum | (1<<hashbit);

    if (newpagenum > db->db_max_dirbit
        && db->db_max_dir_shift
        && db->db_dir_shift >= db->db_max_dir_shift) {
        /* can't split beyond capped size */
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "Cannot split page, pagenum=%u, newnum=%u, dir_shift=%u",
                 pagenum, newpagenum, db->db_dir_shift);
#endif
        errno = ENOMEM;
        return NULL;
    }

    if (!db_is_owned(db)) {
        errno = EPERM;
        return NULL;
    }

    mdbm_log(LOG_DEBUG, "split page, pagenum=%d, newpagenum=%d\n", pagenum, newpagenum);

    if (newpagenum > db->db_max_dirbit) {
        mdbm_log(LOG_DEBUG,
                 "resizing page directory for page split,"
                 " db_dir_shift=%d, db_dir_shift+1=%d, db_max_dir_shift=%d,"
                 " pagenum=%d, newpagenum=%d\n",
                 db->db_dir_shift, db->db_dir_shift + 1, db->db_max_dir_shift,
                 pagenum, newpagenum);
        if ((db->db_max_dir_shift && db->db_dir_shift >= db->db_max_dir_shift)
            || resize(db,db->db_dir_shift+1,0) < 0) {
            db->db_max_dir_shift = db->db_dir_shift;
            return NULL;
        }
    }

    newpage = pagenum_to_page(db,newpagenum,MDBM_PAGE_ALLOC,MDBM_PAGE_MAP);
    if (!newpage) {
        return NULL;
    }

    page = pagenum_to_page(db,pagenum,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
    if (page->p_num_pages > 1) {
        return NULL;
    }

    hvbit = (1<<hashbit);
    new_index = newpage->p.p_num_entries;
    new_offset = MDBM_DATA_PAGE_BOTTOM_OF_DATA(newpage);
    num = 0;
    for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
        if (ep->e_key.match) {
            mdbm_hashval_t h = MDBM_HASH_VALUE(db,MDBM_KEY_PTR(&e),ep->e_key.key.len);
            if (h & hvbit) {
                mdbm_entry_t* new_ep = MDBM_ENTRY(newpage,new_index);
                int ksize = MDBM_ALIGN_LEN(db,ep[0].e_key.key.len);
                int vsize = (ep[0].e_offset -
                             ep[1].e_offset -
                             MDBM_ALIGN_LEN(db,ep[1].e_key.key.len));
                int kvsize = ksize + vsize;

                new_ep->e_offset = new_offset - ksize;
                new_ep->e_key.match = ep->e_key.match;
                new_ep->e_flags = ep->e_flags;
                memcpy(MDBM_VAL_PTR2(newpage,new_offset,kvsize),
                       MDBM_VAL_PTR2(page,ep->e_offset,vsize),
                       kvsize);
                new_offset -= kvsize;
                if (MDBM_ENTRY_LARGEOBJ(ep)) {
                    mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
                    mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
                    lob->p_num = newpagenum;
                }
                ep->e_flags &= ~MDBM_EFLAG_LARGEOBJ;
                del_entry(db,page,ep);
                ++new_index;
            }
        }
        num++;
    }
    newpage->p.p_num_entries = new_index;
    mdbm_log(LOG_DEBUG,
             "split page=%d, newpage=%d, moved(new_index)=%d of num=%d\n",
             pagenum, newpagenum, new_index, num);
    MDBM_INIT_TOP_ENTRY(MDBM_ENTRY(newpage,new_index),new_offset);

    MDBM_SET_DIR_BIT(db,dirbit);
    db->db_hdr->h_dbflags &= ~MDBM_HFLAG_PERFECT;
    db->db_hdr->h_dir_gen++;
    sync_dir(db,NULL);

    return (hashval & hvbit) ? newpage : page;
}

static int
shake_page(MDBM* db, mdbm_page_t* page,
            const datum* key, const datum* val,
            int free_bytes, int needed_bytes)
{
    /* requires page lock */

    int recycle_bytes = 0;
    int ver = MDBM_DBFLAG_SHAKE_VER(db);

    if (ver == MDBM_DBFLAG_SHAKE_V3) {
        struct mdbm_shake_data_v3 od;
        int i, num;
        mdbm_entry_data_t e;
        mdbm_entry_t* ep;
        kvpair null_kv;

        od.page_num = page->p_num;
        od.page_begin = MDBM_DATA_PAGE_BASE(page);
        od.page_end = od.page_begin + db->db_pagesize;
        od.page_free_space = free_bytes;
        od.space_needed = needed_bytes;
        num = page->p.p_num_entries;
        od.page_num_items = num;
        od.page_items = (kvpair*)malloc(num * sizeof(od.page_items[0]));
        od.page_item_sizes = (uint32_t*)malloc(num * sizeof(od.page_item_sizes[0]));
        od.user_data = db->db_shake_data;

        memset(&null_kv,0,sizeof(null_kv));
        for (ep = first_entry(page,&e), i = 0; ep; ep = next_entry(&e), i++) {
            if (ep->e_key.match) {
                get_kv(db,&e,&od.page_items[i].key,&od.page_items[i].val);
                od.page_item_sizes[i] = MDBM_ENTRY_SIZE(db,ep);
            } else {
                od.page_items[i] = null_kv;
                od.page_item_sizes[i] = 0;
            }
        }

        if (((mdbm_shake_func_v3)db->db_shake_func)(db,key,val,&od)) {
            for (i = 0; i < num; i++) {
                if (od.page_item_sizes[i] && od.page_items[i].key.dsize == 0) {
                    del_entry(db,page,MDBM_ENTRY(page,i));
                    recycle_bytes += od.page_item_sizes[i];
                }
            }
        }

        free(od.page_items);
        free(od.page_item_sizes);
    }
    if (needed_bytes <= free_bytes + recycle_bytes) {
        return 0;
    }

    errno = ENOMEM;
    return -1;
}

static int
cache_evict(MDBM* db, mdbm_page_t* page,
            const datum* key, const datum* val,
            int free_bytes, int needed_bytes, int want_large)
{
    int clean = (db->db_cache_mode & MDBM_CACHEMODE_EVICT_CLEAN_FIRST) != 0;
    int quit = 0;
    uint64_t t0 = 0;
    uint64_t nerror = 0;

    if (db->db_flags & MDBM_DBFLAG_NO_DIRTY) {
        clean = 0;
    }

    if (MDBM_DO_STAT_TIME(db)) {
        t0 = db->db_get_usec();
    }

    {   /* Reset markers for entries that failed db_clean_func. */
        mdbm_entry_t* ep;
        mdbm_entry_data_t e;
        for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
            ep->e_flags &= ~MDBM_EFLAG_SYNC_ERROR;
        }
    }
    while (free_bytes < needed_bytes) {
        mdbm_entry_t* ep;
        mdbm_entry_data_t e;
        mdbm_entry_t* evict = NULL;
        uint32_t evict_num_accesses = 0;
        uint32_t evict_access_time = 0;
        uint32_t evict_bytes = 0;
        float evict_priority = 0;

        switch (MDBM_DB_CACHEMODE(db)) {
        case MDBM_CACHEMODE_LFU:
            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                if (clean && (ep->e_flags & MDBM_EFLAG_SYNC_ERROR)) {
                    continue; /* skip entries that failed db_clean_func */
                }
                if (ep->e_key.match
                    && (!clean || !MDBM_ENTRY_DIRTY(ep))
                    && (!want_large || MDBM_ENTRY_LARGEOBJ(ep)))
                {
                    mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)MDBM_VAL_PTR(db,&e);
                    if (!evict
                        || (c->c_num_accesses < evict_num_accesses)
                        || (c->c_num_accesses == evict_num_accesses
                            && c->c_dat.access_time < evict_access_time))
                    {
                        evict = ep;
                        evict_num_accesses = c->c_num_accesses;
                        evict_access_time = c->c_dat.access_time;
                        if (want_large) {
                            uint32_t vallen;
                            MDBM_ENTRY_LOB_ALLOC_LEN(db,&e,&vallen,&evict_bytes);
                        } else {
                            evict_bytes = MDBM_VAL_LEN(db,&e);
                        }
                    }
                }
            }
            break;

        case MDBM_CACHEMODE_LRU:
            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                if (clean && (ep->e_flags & MDBM_EFLAG_SYNC_ERROR)) {
                    continue; /* skip entries that failed db_clean_func */
                }
                if (ep->e_key.match
                    && (!clean || !MDBM_ENTRY_DIRTY(ep))
                    && (!want_large || MDBM_ENTRY_LARGEOBJ(ep)))
                {
                    mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)MDBM_VAL_PTR(db,&e);
                    if (!evict
                        || (c->c_dat.access_time < evict_access_time)
                        || (c->c_dat.access_time == evict_access_time
                            && c->c_num_accesses < evict_num_accesses))
                    {
                        evict = ep;
                        evict_num_accesses = c->c_num_accesses;
                        evict_access_time = c->c_dat.access_time;
                        if (want_large) {
                            uint32_t vallen;
                            MDBM_ENTRY_LOB_ALLOC_LEN(db,&e,&vallen,&evict_bytes);
                        } else {
                            evict_bytes = MDBM_VAL_LEN(db,&e);
                        }
                    }
                }
            }
            break;

        case MDBM_CACHEMODE_GDSF:
            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                if (clean && (ep->e_flags & MDBM_EFLAG_SYNC_ERROR)) {
                    continue; /* skip entries that failed db_clean_func */
                }
                if (ep->e_key.match
                    && (!clean || !MDBM_ENTRY_DIRTY(ep))
                    && (!want_large || MDBM_ENTRY_LARGEOBJ(ep)))
                {
                    mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)MDBM_VAL_PTR(db,&e);
                    float priority = c->c_dat.priority;
                    uint32_t vallen, lob_alloclen;
                    if (want_large) {
                        MDBM_ENTRY_LOB_ALLOC_LEN(db,&e,&vallen,&lob_alloclen);
                        priority = c->c_num_accesses*(lob_alloclen - vallen) - priority;
                    }
                    if (!evict || priority < evict_priority) {
                        evict = ep;
                        evict_num_accesses = c->c_num_accesses;
                        evict_priority = priority;
                        if (want_large) {
                            evict_bytes = lob_alloclen;
                        } else {
                            evict_bytes = MDBM_VAL_LEN(db,&e);
                        }
                    }
                }
            }
            break;
        }

        if (!evict) {
            /* clean_func didn't free enough, retry with normal cache preferences */
            if (clean) {
                clean = 0;
                continue;
            }
            return free_bytes;
        }

        if (clean && db->db_clean_func && MDBM_ENTRY_DIRTY(evict)) {
            datum k, v;
            get_kv2(db,page,evict,&k,&v);
            if (!db->db_clean_func(db,&k,&v,db->db_clean_data,&quit)) {
                evict->e_flags |= MDBM_EFLAG_SYNC_ERROR;
                nerror++;
                if (quit) {
                    return free_bytes;
                }
                continue;
            }
        }
        if (MDBM_DB_CACHEMODE(db) == MDBM_CACHEMODE_GDSF) {
            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                if (ep->e_key.match) {
                    mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)MDBM_VAL_PTR(db,&e);
                    c->c_dat.priority -= evict_priority;
                }
            }
        }
        free_bytes += evict_bytes;
        del_entry(db,page,evict);
    }

    if (MDBM_DO_STAT_TIME(db)) {
      mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->cache_evict : NULL;
      MDBM_ADD_STAT(db,rstats_val,(db->db_get_usec() - t0), MDBM_STAT_TAG_CACHE_EVICT);
      MDBM_ADD_STAT_ERROR_COUNTER(db, rstats_val, nerror, MDBM_STAT_TAG_CACHE_EVICT);
    } else if (MDBM_DO_STATS(db)) {
      MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_CACHE_EVICT);
      MDBM_ADD_STAT_ERROR_COUNTER(db, NULL, nerror, MDBM_STAT_TAG_STORE);
    }
    return free_bytes;
}

static int
expand_page(MDBM* db, mdbm_page_t* page, int pagenum)
{
    /* requires page lock; acquires internal lock */

    mdbm_page_t* newpage;
    int newpagenum;
    int oldpagenum;
    int tod;
    mdbm_entry_t* ep;
    int i;
    int prev_numpages;

    if (mdbm_internal_lock(db) < 0) {
        return -1;
    }

    if ((newpage = alloc_chunk(db,MDBM_PTYPE_DATA,page->p_num_pages+1,0,0,MDBM_PAGE_MAP,
                               MDBM_ALLOC_CAN_UNLOCK)) == NULL)
    {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "expand_page failed in alloc_chunk, pagenum=%d", pagenum);
#endif
        /* alloc_chunk releases internal lock if allocation fails */
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "expand_page failed in alloc_chunk, pagenum=%d", pagenum);
#endif
        errno = ENOMEM;
        return -1;
    }

    newpagenum = newpage->p_num;
    prev_numpages = newpage->p_prev_num_pages;

    page = pagenum_to_page(db,pagenum,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
    tod = MDBM_DATA_PAGE_BOTTOM_OF_DATA(page);

    memcpy(newpage,page,MDBM_PAGE_T_SIZE + (page->p.p_num_entries+1)*MDBM_ENTRY_T_SIZE);
    memcpy((char*)newpage + tod + db->db_pagesize,
           (char*)page + tod,
           page->p_num_pages*db->db_pagesize - tod);
    ep = MDBM_ENTRY(newpage,0);
    for (i = 0; i <= newpage->p.p_num_entries; i++, ep++) {
        ep->e_offset += db->db_pagesize;
    }

    newpage->p_num = pagenum;
    newpage->p_num_pages++;
    newpage->p_prev_num_pages = prev_numpages;

    oldpagenum = MDBM_GET_PAGE_INDEX(db,pagenum);
    MDBM_SET_PAGE_INDEX(db, pagenum, newpagenum);
    free_chunk(db,oldpagenum,NULL);

    mdbm_internal_unlock(db);
    return 0;
}

int
mdbm_internal_replace(MDBM* db)
{
    if (MDBM_IS_REPLACED(db)) {
        struct stat st1;
        struct stat st2;
        int newfd, oldfd;
        int wsize;

        if (fstat(db->db_fd,&st1) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm replace: fstat(fd=%d)",db->db_fd);
            return 0;
        }
        if (stat(db->db_filename,&st2) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm replace: stat(%s)",db->db_filename);
            return 0;
        }
        if (st1.st_dev == st2.st_dev && st1.st_ino == st2.st_ino) {
            db->db_hdr->h_dbflags &= ~MDBM_HFLAG_REPLACED;
            mdbm_log(LOG_NOTICE,
                     "mdbm replace: Reset stale replace flag: %s",
                     db->db_filename);
            return 1;
        }

        newfd = open(db->db_filename,MDBM_IS_RDONLY(db) ? MDBM_O_RDONLY : MDBM_O_RDWR,0);
        if (newfd < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm replace: %s",db->db_filename);
            return -1;
        }

        oldfd = db->db_fd;
        db->db_fd = newfd;

        if (mdbm_internal_remap(db,0,0) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm replace unable to map %s",db->db_filename);
            return -1;
        }

        wsize = db->db_window.num_pages * db->db_pagesize;
        mdbm_set_window_size_internal(db,wsize);

        close(oldfd);
    }
    return 0;
}


struct mdbm_walk {
    MDBM* db;
    int dirbit;
    int level;
    int min_level;
    int max_level;
    int num_nodes;
};


void
walk_dir(struct mdbm_walk* w)
{
    w->dirbit <<= 1;
    w->level++;
    if (w->level >= w->db->db_dir_shift
        || (!MDBM_DIR_BIT(w->db,w->dirbit+1) && !MDBM_DIR_BIT(w->db,w->dirbit+2)))
    {
        if (w->min_level == -1) {
            w->min_level = w->level;
        }
        if (w->level > w->max_level) {
            w->max_level = w->level;
        }
    } else {
        w->dirbit++;
        walk_dir(w);
        w->dirbit++;
        walk_dir(w);
        w->dirbit -= 2;
    }
    w->dirbit >>= 1;
    w->level--;
    w->num_nodes++;
}

static int
truncate_db(MDBM* db, int npages, int pagesize, int flags)
{
    int dir_shift, n;
    int dir_pages, tot_pages;
    size_t dbsize;
    mdbm_page_t* page;
    mdbm_hdr_t* hdr;
    size_t mapsz;
    int ret;

    /* Compute dir bit shift. */
    for (dir_shift = 0, n = 1; (n<<1) <= npages; dir_shift++, n <<= 1);
    dir_pages = MDBM_NUM_DIR_PAGES(pagesize,dir_shift);

    tot_pages = npages;
    if (n == npages) {
        tot_pages += dir_pages;
    }

    dbsize = (size_t)tot_pages * pagesize;
    mapsz = (size_t)dir_pages * pagesize;

    /* memory-only: don't want to ftruncate to make the file big */
    if (!(db->db_flags & (MDBM_DBFLAG_HUGEPAGES|MDBM_DBFLAG_MEMONLYCACHE))) {
        if (ftruncate(db->db_fd,dbsize) < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: ftruncate failure",db->db_filename);
            ERROR();
            return -1;
        }
    }

    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
        mapsz = dbsize;
    }


    if (map(db,NULL,0,mapsz,&db->db_base,&db->db_base_len) < 0) {
        ERROR();
        return -1;
    }

    memset(db->db_base,0,dir_pages * pagesize);

    page = (mdbm_page_t*)db->db_base;
    page->p_num = 0;
    page->p_type = MDBM_PTYPE_DIR;
    page->p_flags = 0;
    page->p_num_pages = dir_pages;
    page->p_r0 = 0;
    page->p_prev_num_pages = 0;
    page->p_r1 = 0;
    page->p.p_data = _MDBM_MAGIC_NEW2;

    hdr = (mdbm_hdr_t*)(db->db_base + MDBM_PAGE_T_SIZE);
    hdr->h_magic = _MDBM_MAGIC_NEW2;
    hdr->h_dbflags = 0;
    hdr->h_pagesize = pagesize;
    hdr->h_num_pages = tot_pages;
    hdr->h_max_pages = 0;
    hdr->h_dir_shift = dir_shift;
    hdr->h_last_chunk = 0;
    hdr->h_hash_func = MDBM_DEFAULT_HASH;
    hdr->h_first_free = 0;

    if (mdbm_internal_remap(db,dbsize,flags) < 0) {
        ERROR();
        return -1;
    }


    ret = mdbm_pre_split(db,npages);
    hdr = (mdbm_hdr_t*)(db->db_base + MDBM_PAGE_T_SIZE); /* db was remaped */
    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
        /* set max_pages since we would lose all our data if it remapped memory */
        hdr->h_max_pages = db->db_num_pages;
    }
    return ret;
}

static void
get_key(datum* key, char* buf, int buflen)
{
    int nonprint = 0;
    int bp = 0;
    int i;

    for (i = 0; i < key->dsize; ++i) {
        if (isprint(key->dptr[i])) {
            if (bp >= buflen-1) {
                break;
            }
            buf[bp++] = key->dptr[i];
        } else {
            if (bp >= buflen-4) {
                break;
            }
            bp += snprintf(buf+bp, buflen-bp, "\\x%02x",key->dptr[i]);
            ++nonprint;
        }
    }
    if (nonprint > key->dsize/4) {
        bp = 0;
        for (i = 0; i < key->dsize; ++i) {
            if (bp >= buflen-2) {
                break;
            }
            bp += snprintf(buf+bp, buflen-bp, "%02x",key->dptr[i]);
        }
    }
    buf[bp] = 0;
}

/**
 * \brief Accesses a key in this MDBM.
 * \param[in,out] db database handle
 * \param[in] key key to access
 * \param[out] value value pointing to accessed value (if any)
 * \param[out] buf buffer containing copy of accessed value (if any)
 * \param[out] info information about the fetch
 * \param[out] iter initialized iterator for next entry
 * \return status code for success of lock
 * \retval -1 error
 * \retval  0 success
 */
static int
access_entry(MDBM* db, datum* key, datum* val, datum* buf, struct mdbm_fetch_info* info,
             MDBM_ITER* iter)
{
    mdbm_hashval_t hashval;
    mdbm_pagenum_t pagenum;
    mdbm_page_t* page;
    mdbm_entry_t* ep;
    int do_del = !val;
    int locked = 0;
#ifdef MDBM_BSOPS
    int bserr = 0;
#endif
    uint64_t t0 = 0;

    if (MDBM_DO_STAT_TIME(db)) {
        t0 = db->db_get_usec();
    }

    /*fprintf(stderr, "access_entry key %p\n", key); */
    if (!key || !key->dsize) {
        errno = EINVAL;
        return -1;
    }

    if (check_guard_padding(db, 1) != 0) {
        return -1;
    }

    /* All accesses must lock when deleting a key, copying out a key value, or
     * if an mdbm replacement occurred.
     */
    if (do_del || (buf != NULL) || MDBM_REPLACED_OR_CHANGED(db)) {
        if (mdbm_internal_do_lock(db,
                    do_del ? MDBM_LOCK_WRITE : MDBM_LOCK_READ,
                    MDBM_LOCK_WAIT,
                    key,
                    &hashval,
                    &pagenum) < 0) {
            /*print_lock_state(db); */
            goto access_error;
        } else {
            locked = 1;
        }
    } else {
        hashval = hash_value(db,key);
        pagenum = hashval_to_pagenum(db,hashval);
    }

#ifdef MDBM_BSOPS
    if (do_del && db->db_bsops) {
        if (db->db_bsops->bs_delete(db->db_bsops_data,key,0) < 0) {
            bserr = (errno == ENOENT) ? ENOENT : EIO;
        }
    }
#endif

    page = pagenum_to_page(db,pagenum,MDBM_PAGE_NOALLOC,MDBM_PAGE_NOMAP);
    /* fprintf(stderr, "access_entry() key %s pagenum:%d page:%p\n", key->dptr, pagenum, (void*)page); */
    if (!page || (ep = find_entry(db,page,key,hashval,key,val,info)) == NULL) {
        errno = ENOENT;
#ifdef MDBM_BSOPS
        if (db->db_bsops) {
            if (do_del) {
                if (!bserr) {
                    if (locked) {
                        mdbm_internal_do_unlock(db,key);
                    }
                    if (MDBM_DO_STAT_TIME(db)) {
                        mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->remove : NULL;
                        MDBM_ADD_STAT_WAIT(db,rstats_val,(db->db_get_usec() - t0), MDBM_STAT_TAG_DELETE);
                    } else if (MDBM_DO_STATS(db)) {
                        MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_DELETE);
                    }
                    if (MDBM_DO_STAT_PAGE(db)) {
                          MDBM_PAGE_STAT(db, pagenum, MDBM_STAT_TAG_PAGE_DELETE);
                    }
                    return 0;
                }
                errno = bserr;
            } else if (db->db_bsops->bs_fetch(db->db_bsops_data,key,val,buf,0) < 0) {
                if (errno != ENOENT) {
                    mdbm_logerror(LOG_ERR,0,"mdbm backing store fetch error");
                }
            } else {
                datum k = *key;
                datum v = *val;
                if (mdbm_store_r(db,&k,&v,MDBM_REPLACE|MDBM_CLEAN|MDBM_CACHE_ONLY,NULL) < 0) {
                    char kbuf[128];
                    get_key(key,kbuf,sizeof(kbuf));
                    mdbm_logerror((errno != ENOMEM && errno != EINVAL)
                                  ? LOG_ERR : LOG_DEBUG,
                                  0,"mdbm backing store cache refill error (key=%s)",kbuf);
                }
                if (locked) {
                    mdbm_internal_do_unlock(db,key);
                }
                if (MDBM_DO_STAT_TIME(db)) {
                    uint64_t dt = db->db_get_usec() - t0;
                    mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->fetch : NULL;
                    MDBM_ADD_STAT_WAIT(db,rstats_val,dt,MDBM_STAT_TAG_FETCH);
                    rstats_val = (db->db_rstats) ? &db->db_rstats->fetch_uncached : NULL;
                    MDBM_ADD_STAT_WAIT(db,rstats_val,dt,MDBM_STAT_TAG_FETCH_UNCACHED);
                } else if (MDBM_DO_STATS(db)) {
                    MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_FETCH);
                    MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_FETCH_UNCACHED);
                }
                return 0;
            }
        }
#endif
        goto access_error;
    }

    if (!do_del) {
        if (buf) {
            if (val->dsize > buf->dsize) {
                buf->dptr = (char*)realloc(buf->dptr,val->dsize);
                buf->dsize = val->dsize;
            }
            memcpy(buf->dptr,val->dptr,val->dsize);
            val->dptr = buf->dptr;
        }
        if (MDBM_DO_STAT_TIME(db)) {
            mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->fetch : NULL;
            MDBM_ADD_STAT_WAIT(db,rstats_val,(db->db_get_usec() - t0), MDBM_STAT_TAG_FETCH);
        } else if (MDBM_DO_STATS(db)) {
            MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_FETCH);
        }
    } else {
        if (!bserr || bserr == ENOENT) {
            del_entry(db,page,ep);
            if (MDBM_DO_STAT_TIME(db)) {
                mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->remove : NULL;
                MDBM_ADD_STAT_WAIT(db,rstats_val,(db->db_get_usec() - t0), MDBM_STAT_TAG_DELETE);
            } else if (MDBM_DO_STATS(db)) {
                MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_DELETE);
            }
            if (MDBM_DO_STAT_PAGE(db)) {
                  MDBM_PAGE_STAT(db, pagenum, MDBM_STAT_TAG_PAGE_DELETE);
            }
        }
    }

    if (iter) {
        iter->m_pageno = pagenum;
        iter->m_next = -3 - 2*MDBM_ENTRY_INDEX(page,ep);
    }

    if (locked) {
        mdbm_internal_do_unlock(db,key);
    }

    return 0;

  access_error:
    if (MDBM_DO_STATS(db)) {
        if (do_del) {
            mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->remove : NULL;
            MDBM_INC_STAT_ERROR_COUNTER(db, rstats_val, MDBM_STAT_TAG_DELETE_FAILED);
        } else {
            mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->fetch : NULL;
            if (errno == ENOENT) {
                MDBM_INC_STAT_ERROR_COUNTER(db, rstats_val, MDBM_STAT_TAG_FETCH_NOT_FOUND);
            } else {
                MDBM_INC_STAT_ERROR_COUNTER(db, rstats_val, MDBM_STAT_TAG_FETCH_ERROR);
            }
        }
    }
    if (locked) {
        mdbm_internal_do_unlock(db,key);
    }
    if (iter) {
        iter->m_pageno = MDBM_DB_DIR_WIDTH(db);
        iter->m_next = -1;
    }
    key->dptr = NULL;
    key->dsize = 0;
    if (val) {
        *val = *key;
    }
    return -1;
}


/* EXTERNAL INTERFACES */

int 
mdbm_delete_lockfiles(const char* dbname)
{
  return do_delete_lockfiles(dbname);
}


int
mdbm_plock(MDBM *db, const datum *key, int flags)
{
    mdbm_hashval_t hashval;
    mdbm_pagenum_t pagenum;

    if (key) {
        return mdbm_internal_do_lock(db,MDBM_LOCK_READ,MDBM_LOCK_WAIT,key,&hashval,&pagenum);
    }
    return mdbm_lock(db);
}

int
mdbm_tryplock(MDBM *db, const datum *key, int flags)
{
    mdbm_hashval_t hashval;
    mdbm_pagenum_t pagenum;

    if (key) {
        return mdbm_internal_do_lock(db,MDBM_LOCK_READ,MDBM_LOCK_NOWAIT,key,&hashval,&pagenum);
    }
    return mdbm_trylock(db);
}

int
mdbm_punlock(MDBM *db, const datum *key, int flags)
{
    return mdbm_internal_do_unlock(db,key);
}

int
mdbm_trylock(MDBM *db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_EXCLUSIVE,MDBM_LOCK_NOWAIT,NULL,NULL,NULL);
}

int
mdbm_trylock_shared(MDBM *db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_SHARED,MDBM_LOCK_NOWAIT,NULL,NULL,NULL);
}

uint32_t mdbm_get_lockmode(MDBM *db) {
  return db_get_lockmode(db);
}

int
mdbm_lock(MDBM *db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_EXCLUSIVE,MDBM_LOCK_WAIT,NULL,NULL,NULL);
}

int
mdbm_lock_shared(MDBM *db)
{
    return mdbm_internal_do_lock(db,MDBM_LOCK_SHARED,MDBM_LOCK_WAIT,NULL,NULL,NULL);
}

int
mdbm_islocked(MDBM *db)
{
  return db_is_locked(db) ? 1 : 0;
}

/* NOTE: despite the documentation, this only checks ownership */
/* by the calling process, instead of ownership by *any* process. */
int
mdbm_isowned(MDBM *db)
{
  return db_is_owned(db) ? 1 : 0;
}

int
mdbm_unlock(MDBM *db)
{
    return mdbm_internal_do_unlock(db,NULL);
}


/**
 * Return whether to enable the protect feature based on a regular expression
 * pattern match with an mdbm path.
 * \param[in] filename mdbm path
 * \param[in] envvar environment variable containing regex pattern
 * \return 1 if the filename matches. Otherwise, return 0
 */
static int
protect_regex_match(const char *filename, const char *envvar)
{
    regex_t regex;
    char *pattern = getenv(envvar);
    int rc;
    int matched;

    mdbm_log(LOG_DEBUG, "%s: protect_regex_match, envvar=%s", filename, envvar);
    if (pattern == NULL) {
        return 0;
    }
    if (strlen(pattern) == 0) {
        mdbm_log(LOG_ERR,
                 "%s: protect_regex_match environment variable defined but it has an empty value"
                 ", envvar=%s", filename, envvar);
        return 0;
    }

    mdbm_log(LOG_DEBUG, "%s: protect_regex_match, pattern=%s", filename, pattern);
    rc = regcomp(&regex, pattern, REG_EXTENDED | REG_NOSUB);
    if (rc) {
        mdbm_logerror(LOG_ERR, 0,
                      "%s: protect_regex_match cannot compile regular expresion"
                      ", envvar=%s"
                      ", pattern=%s",
                      filename, envvar, pattern);
        return 0;
    }

    rc = regexec(&regex, filename, 0, NULL, 0);
    matched = (rc == 0 ? 1 : 0);
    regfree(&regex);
    mdbm_log(LOG_DEBUG, "%s: protect_regex_match, matched=%d", filename, matched);
    return matched;
}


/**
 * Return whether to enable the protect feature if a .protect file is present.
 * \param[in] filename mdbm path
 * \return 1 if the protect file exists. Otherwise, return 0
 */
static int
protect_file_exists(const char *filename)
{
    struct stat st;
    char protect_path[MAXPATHLEN+1];
    int size = 0;
    int found = 0;

    if (!(filename && filename[0])) { return 0; }

    size = snprintf(protect_path, sizeof(protect_path), "%s.protect", filename);

    if (size < sizeof(protect_path)) {
        /* appease ynetdblib, it erroneously relies on errno=0 after mdbm_open() */
        int errno_save=errno;
        if (lstat(protect_path, &st) < 0) {
            if (errno != ENOENT) {
                mdbm_logerror(LOG_ERR, 0, "%s", protect_path);
            }
        } else {
            /* Removing restriction that .protect file must be owned by root to  simplify
               testing and it is no longer useful for avoiding unintended uses in production. */
            /*if (st.st_uid) { */
            /*    mdbm_log(LOG_ERR, "%s: WARNING: must be owned by root", protect_path); */
            if (!S_ISLNK(st.st_mode)) { /* Make sure it is a symbolic link */
                mdbm_log(LOG_ERR, "%s: WARNING: must be a symbolic link", protect_path);
            } else {
                found = 1;
            }
        }
        errno=errno_save;
    } else {
        mdbm_log(LOG_ERR, "%s.protect: pathname too long", filename);
    }

    mdbm_log(LOG_DEBUG, "%s: protect_file_exists, found=%d", filename, found);
    return found;
}

/**
 * Determines whether a file is on a hugetlbfs.
 * This function has been moved out mdbm_open for profiling purposes.
 *
 * \param[in] filename - mdbm path
 * \param[in] fd file descriptor of opened \a filename
 * \param[out] hugep - predicate result for whether fd is on a hugetlbfs
 *             If the return code is 0, hugep is returned.
 * \param[out] page_size - if \a hugep, the optimal transfer block size
 *             If the return code is 0, page_size is returned.
 * \return whether the check were successful
 * \retval 1 platform cannot have a hugetblfs
 * \retval 0 check success
 * \retval -1 check failed (fstatfs)
 */
static int
check_hugetlbfs(const char *filename, int fd, int *hugep, uint32_t *page_size)
{
    int rc;
#ifdef __linux__
    {
        struct statfs fs;
        if (fstatfs(fd, &fs) < 0) {
            mdbm_logerror(LOG_ERR, 0, "%s: mdbm fstatfs failure", filename);
            rc = -1;
        } else {
            rc = 0;
            *hugep = (fs.f_type == MDBM_HUGETLBFS_MAGIC);
            if (page_size != NULL) {
                *page_size = (uint32_t)fs.f_bsize; /* Optimal transfer block size. */
            }
        }
    }
#else
    rc = 1;
    *hugep = 0;
    *page_size = 0;
#endif
    return rc;
}

static int
is_hugetlbfs(const char *filename, int fd)
{
    int hugep = 0;
    (void)check_hugetlbfs(filename, fd, &hugep, NULL);
    return hugep;
}

/**
 * Gets the magic number from an mdbm by mmap instead of pread.
 * This is needed for older Linux kernel versions (2.6.18) that don't support
 * hugetlbfs file-based read-write operations (i.e., pread).  hugtlbfs
 * read-write operations aren't supported until kernel version 2.6.25.
 *
 * Currently (Linux kernel 2.6.18), hugetlbfs files cannot be mmap'd for just
 * the header size, or the typical system page size (4096) because mmap fails
 * with errno=EINVAL.  A hugetlbfs mdbm has to be mmap's according to its
 * system page size (ex., 2MB).
 *
 * \param[in] filename - mdbm path
 * \param[in] fd file descriptor of opened \a filename
 * \param[in] db_sys_pagesize - file-system page page size.
 *            Normally, the same as system page size, unless the
 *            the mdbm is on hugetlbfs, where it must be the
 *            huge page size.
 * \param[out] magic - mdbm magic number (internal version identifier)
 * \param[in] log_error - non-zero value for whether to log an error.
 *            Normally, an error should be emitted.  However, during an
 *            mdbm_open when a file is being created, there is no data
 *            and the caller will not want to emit an expected error.
 * \return whether a magic number is returned
 * \retval  0 magic number returned in \a magic
 * \retval -1 file is truncated (0 bytes)
 * \retval -2 file size is smaller than magic number
 * \retval -3 mmap failed
 * \retval -4 munmap failed
 * \retval -5 file size invalid (truncated)
 */
static int
get_magic_number_mmap(const char *filename, int fd, uint32_t db_sys_pagesize,
                      uint32_t *magic, int log_error)
{
    mdbm_hdr_t* hp = NULL;
    off_t len;

    mdbm_log(LOG_DEBUG, "%s: get_magic_number_mmap", filename);
    len = lseek(fd, 0, SEEK_END);
    if (len == 0) {
        return -1;
    } else {
        /* The above lseek might've failed, catch it here. */
        if (len < sizeof(hp->h_magic)) {
            mdbm_logerror(LOG_ERR, 0,
                          "%s: mdbm header is smaller than the size of a magic number, "
                          "header_len=%ld, magic_size=%ld",
                          filename, (long)len, (long)sizeof(hp->h_magic));
            return -2;
        } else {
            /* Ideally, we only want to map the header, but hugetlbfs mmap
             * operations fail if something other than that file-system's page
             * size is used (ex., sizes 4096 and sizeof(*hp) fail). */
            if (db_sys_pagesize < sizeof(*hp)) {
                /* Display an error message, but allow to continue.  In the
                 * future, it might be OK to not use an integral page size for
                 * hugetlbfs mmap. */
                mdbm_log(LOG_ERR,
                         "%s: get_magic_number_mmap map size is less than header size, "
                         "header_size=%ld, map_size=%d",
                         filename, (long)sizeof(*hp), db_sys_pagesize);
            }
            hp = (mdbm_hdr_t*)mmap(0, db_sys_pagesize, PROT_READ, MAP_SHARED, fd, 0);
            if (hp == (mdbm_hdr_t*)-1) {
                mdbm_logerror(LOG_ERR, 0,
                              "%s: mdbm header map failure, len=%lld, db_sys_pagesize=%d",
                              filename, (long long)len, db_sys_pagesize);
                return -3;
            }
            *magic = hp->h_magic;

            if (munmap(hp, db_sys_pagesize)) {
                mdbm_logerror(LOG_ERR, 0, "%s: mdbm header unmap failure, size=%d",
                              filename, db_sys_pagesize);
                return -4;
            }

            if (*magic == _MDBM_MAGIC_NEW2 && (len < db_sys_pagesize)) {
                mdbm_logerror(LOG_ERR, 0, "%s: mdbm header is truncated, len=%lld",
                              filename, (long long)len);
                return -5;
            }
        }
    }

    return 0;
}

/**
 * Gets the magic number from an mdbm using pread.  Using pread is much faster
 * than using mmap as done in get_magic_number_mmap.  pread is used for
 * non-hugetlbfs file-systems.
 *
 * \param[in] filename - mdbm path
 * \param[in] fd file descriptor of opened \a filename
 * \param[out] magic - mdbm magic number (internal version identifier)
 * \param[in] log_error - non-zero value for whether to log an error.
 *            Normally, an error should be emitted.  However, during an
 *            mdbm_open when a file is being created, there is no data
 *            and the caller will not want to emit an expected error.
 * \return whether a magic number is returned
 * \retval  0 magic number returned in \a magic
 * \retval -1 cannot read file
 * \retval -2 file is truncated (0 bytes)
 * \retval -3 cannot read all of magic number
 */
static int
get_magic_number_pread(const char* filename, int fd, uint32_t *magic, int log_error)
{
    uint32_t magic_local;
    ssize_t bytes_expected = sizeof(magic_local);
    off_t offset = offsetof(mdbm_hdr_t, h_magic);
    ssize_t bytes_read = pread(fd, &magic_local, bytes_expected, offset);

#ifdef DEBUG
    mdbm_log(LOG_DEBUG, "%s: get_magic_number_pread", filename);
#endif
    if (bytes_read < 0) {
        if (log_error) {
            mdbm_logerror(LOG_ERR, 0,
                          "%s: mdbm header read failure, "
                          "bytes_expected=%ld, bytes_read=%ld, offset=%ld",
                          filename, (long)bytes_expected, (long)bytes_read, (long)offset);
        }
        return -1;
    } else if (bytes_read == 0) {
        if (log_error) {
            mdbm_logerror(LOG_ERR, 0,
                          "%s: mdbm header read failure, file is truncated, "
                          "bytes_expected=%ld, bytes_read=%ld, offset=%ld",
                          filename, (long)bytes_expected, (long)bytes_read, (long)offset);
        }
        return -2;
    } else if (bytes_read != bytes_expected) {
        if (log_error) {
            mdbm_logerror(LOG_ERR, 0,
                          "%s: mdbm header read failure, "
                          "bytes_expected=%ld, bytes_read=%ld, offset=%ld",
                          filename, (long)bytes_read, (long)bytes_expected, (long)offset);
        }
        return -3;
    } else {
        *magic = magic_local;
        return 0;
    }
}

/**
 * Gets the magic number from an mdbm.
 *
 * \param[in] filename - mdbm path
 * \param[in] fd - file descriptor of opened \a filename
 * \param[in] db_sys_pagesize - file-system page page size.
 *            Normally, the same as system page size, unless the
 *            the mdbm is on hugetlbfs, where it must be the
 *            huge page size.
 * \param[out] magic - mdbm magic number (internal version identifier)
 * \param[in]  log_error - non-zero value for whether to log an error.
 *             Normally, an error should be emitted.  However, during an
 *             mdbm_open when a file is being created, there is no data
 *             and the caller will not want to emit an expected error.
 * \return whether a magic number is returned
 * \retval  0 magic number returned in \a magic
 * \retval -1 file (\a fd) is an empty file
 * \retval -2 get magic failed on hugetlbfs
 * \retval -3 get magic failed on non-hugetlbfs
 */
int
get_magic_number(const char* filename, int fd, uint32_t db_sys_pagesize,
                 uint32_t *magic, int log_error)
{
    int rc;

    /* If this mdbm is on a hugetlbfs, use the slower get_magic_number_mmap
     * function because file-based read-write operations are not supported in
     * older version of the kernel.
     *
     * TBD: if the kernel version is 2.6.25 or later, read-write operations
     * are supported and get_magic_number_pread should be used.
     */
    if (is_hugetlbfs(filename, fd)) {
        rc = get_magic_number_mmap(filename, fd, db_sys_pagesize, magic, log_error);
        if ((rc != 0) && (rc != -1)) {
            /* If rc is 0, or -1, return that.  Otherwise, convert the rc code. */
            if (log_error) {
                mdbm_logerror(LOG_ERR, 0,
                              "%s: get_magic_number failed on hugetlbfs, rc=%d",
                              filename, rc);
            }
            rc = -2;
        }
    } else {
        rc = get_magic_number_pread(filename, fd, magic, log_error);
        /* If rc is 0, return that.  Otherwise, convert the rc code. */
        switch (rc) {
        case 0:
            break;

        case -2:
            rc = -1;
            break;

        default:
            if (log_error) {
                mdbm_logerror(LOG_ERR, 0,
                              "%s: get_magic_number failed on non-hugetlbfs, rc=%d",
                              filename, rc);
            }
            rc = -3;
        }
    }

    return rc;
}

/**
 * Gets the magic number from an mdbm.
 *
 * \param[in] db mdbm handle
 * \param[out] magic - mdbm magic number (internal version identifier)
 * \return whether a magic number is returned
 * \retval 0 magic number returned in \a magic
 * \retval -1 get magic number failed on hugetlbfs
 * \retval -2 get magic number failed on non-hugetlbfs
 */
int
mdbm_get_magic_number(MDBM *db, uint32_t *magic)
{
    if (db == NULL) {
        return -1;
    }
    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
        return _MDBM_MAGIC_NEW2;
    }
    return get_magic_number(db->db_filename, db->db_fd, db->db_sys_pagesize, magic, 1);
}


void*
mdbm_allocate_handle()
{
    /* NOTE: this assumes a V3 handle is larger than V2 (currently true).
     * we need to allocate enough space for either. */
    void* ret = calloc(1,sizeof(MDBM)); /* auto-zero's the memory */

    struct mdbm* db = (struct mdbm *) ret;
    if (db) {
      db->guard_padding_1 = db->guard_padding_2 = MDBM_GUARD_PADDING; 
      db->guard_padding_3 = db->guard_padding_4 = MDBM_GUARD_PADDING;
    }

    return ret;
}

void
mdbm_free_handle(MDBM* db)
{
    free(db);
}


/* extern int mdbm_convert_legacy_mdbm(const char* filename); */

MDBM*
mdbm_open_inner(const char *filename, int flags, int mode, int pagesize, int dbsize, int retry)
{
    MDBM *db = NULL;
    int do_check = 0;
    int err;
    int openflags = 0;
    int fd = -1;
    uint32_t magic;
    /*uint32_t page_size = 0; */
    int db_flags = 0;
    uint32_t db_sys_pagesize = getpagesize();
    char pathname[MAXPATHLEN+1];
    mode_t umask_save;
    char memonly_fname[MAXPATHLEN];
    char *tempfilename;

    if (filename == NULL) {
        /* user requested a memory only db */
        if (pagesize == 0) {
            pagesize = MDBM_PAGESIZ;
        }
        if (pagesize < MDBM_MINPAGE) {
            mdbm_logerror(LOG_ERR, 0,"mem-only: mdbm open memory-only cache failure,"
                          " invalid page size specified=%d", pagesize);
            errno = EINVAL;
            return NULL;
        }

        /* dbsize is a required parameter */
        if ((dbsize < (2 * pagesize)) || (dbsize < (MDBM_MINPAGE * 3))) {
            mdbm_logerror(LOG_ERR, 0,"mem-only: mdbm open memory-only failure, "
                          "invalid dbsize specified=%d", dbsize);
            errno = EINVAL;
            return NULL;
        }

        db_flags = MDBM_DBFLAG_MEMONLYCACHE;

        /* sanity check flags that make sense for memory-only cache */
        if (flags & (MDBM_OPEN_WINDOWED|MDBM_O_FSYNC|MDBM_HEADER_ONLY|MDBM_O_RDONLY)) {
            mdbm_logerror(LOG_ERR, 0,"mem-only: mdbm open memory-only failure, "
                          "conflicting flags(%x) specified", flags);
            errno = EINVAL;
            return NULL;
        }
        flags |= MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR | MDBM_CREATE_V3;

        /* create a unique tmp name for filename */
        tempfilename = tempnam("/tmp", "memoryonly");
        snprintf(memonly_fname, sizeof(memonly_fname),"%s",tempfilename);
        filename = memonly_fname;
        free(tempfilename);
    }


    /* force V3 */
    flags |= MDBM_CREATE_V3;
    /* unset V2 */
    if (flags & MDBM_CREATE_V2) {
      mdbm_logerror(LOG_ERR,0,"%s: MDBM V4 doesn't support MDBM_CREATE_V2", filename);
      errno = EINVAL;
      return NULL;
    }

    /* open_locks_inner() handles MDBM_ANY_LOCKS for now... */
    if (flags & MDBM_ANY_LOCKS) {
      db_flags |= MDBM_ANY_LOCKS;
    }
    if (flags & MDBM_SINGLE_ARCH) {
      db_flags |= MDBM_SINGLE_ARCH;
    }


    /* fprintf(stderr, "MDBM V3 header size is %d bytes\n", sizeof(mdbm_hdr_t)); */

    if ((flags & MDBM_O_ACCMODE) != MDBM_O_RDONLY) {
        openflags = O_RDWR;
    } else {
        openflags = O_RDONLY;
        /* remap_file_pages() fails when opening RDONLY */
        if (flags & MDBM_OPEN_WINDOWED) {
            mdbm_logerror(LOG_ERR,0,"%s: cannot use MDBM_OPEN_WINDOWED in read-only mode",filename);
            ERROR();
            goto open_error;
        }
    }
    if (flags & MDBM_O_CREAT) {
        openflags |= O_CREAT;
    }

    mdbm_log(LOG_DEBUG, "%s: mdbm_open, openflags=0x%x, mode=0%o",
             filename, openflags, mode);

    if (db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
        fd = -1;
    } else {
        umask_save = umask(0);      /* Allow all permissions to be set */
        fd = open(filename,openflags,mode);
        umask(umask_save);          /* Restore umask */
        if (fd < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: mdbm file open failure",filename);
            ERROR();
            goto open_error;
        }

      /* Check for a hugetlbfs, and update config if appropriate. */
      {
          int hugep = 0; /* Boolean for whether filename is on a hugetlbfs */
          uint32_t page_size = 0;

          if (check_hugetlbfs(filename, fd, &hugep, &page_size) < 0) {
              ERROR();
              goto open_error;
          } else {
              if (hugep) {
                  db_flags |= MDBM_DBFLAG_HUGEPAGES;
                  db_sys_pagesize = page_size;
              }
          }
      }
    }

    /* Verify the magic number in the header. */
    magic = 0;
    if (!(flags & MDBM_O_TRUNC)) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "%s: mdbm_open, check for magic number", filename);
        int rc = get_magic_number(filename, fd, db_sys_pagesize, &magic, 1);
        mdbm_log(LOG_DEBUG, "%s: mdbm_open, magic number=0x%x, rc=%d",
                 filename, magic, rc);
#else
        int rc = get_magic_number(filename, fd, db_sys_pagesize, &magic, 0);
#endif
        /* Either the mdbm exists and it has a valid magic number, or a new mdbm
         * is being created (and no data for a magic number could be read). */
        if (rc) {
            if (rc == -1) {
                /* mdbm was empty */
                if (!(flags & MDBM_O_CREAT)) {
                    mdbm_logerror(LOG_ERR,0,"%s: mdbm header is truncated",filename);
                    ERROR();
                    goto open_error;
                }
            } else {
              mdbm_logerror(LOG_ERR,0,"%s: can't get mdbm magic number (fd:%d, rc:%d)",
                            filename, fd, rc);
              goto open_error;
            }
        }
    }

#ifdef DEBUG
    mdbm_log(LOG_DEBUG, "%s: mdbm_open, magic number=0x%x", filename, magic);
#endif
    /* If there is no magic number (we're creating an mdbm), set it according
     * to the mdbm version desired. */
    if (!magic) {
        magic = _MDBM_MAGIC_NEW2;
    }
    if (magic != _MDBM_MAGIC_NEW2) {
        if (magic == _MDBM_MAGIC || magic == _MDBM_MAGIC_NEW) {
          mdbm_log(LOG_ERR,"%s: V2 mdbm is not supported",filename);
          errno = EBADF;
          return NULL;

          /*
          //if (!retry) {
          //  mdbm_log(LOG_ERR,"%s: V2 mdbm is not supported, and autoconvert failed",filename);
          //  errno = EBADF;
          //  return NULL;
          //}
          //close(fd);
          // // log error
          //mdbm_log(LOG_ERR,"%s: V2 mdbm is not supported, attempting to autoconvert",filename);
          //mdbm_log(LOG_ERR,"%s: conversion will go away completely in the future",filename);
          // // try upgrade...
          //if (mdbm_convert_legacy_mdbm(filename)) {
          //  errno = EBADF;
          //  return NULL;
          //}
          //return mdbm_open_inner(filename, flags, mode, pagesize, dbsize, 0);
          */

        } else {
          close(fd);
          mdbm_log(LOG_ERR,"%s: mdbm header magic number is invalid",filename);
          ERROR();
          errno = EBADF;
          return NULL;
        }
    }

    if (pagesize && (pagesize < MDBM_MINPAGE)) {
        mdbm_logerror(LOG_ERR,0,"%s: Page size %d is too small", filename, pagesize);
        close(fd);
        errno = EINVAL;
        return NULL;
    }

    if (pagesize && (pagesize > MDBM_MAXPAGE)) {
        mdbm_logerror(LOG_ERR,0,"%s: Page size %d is too large", filename, pagesize);
        close(fd);
        errno = EINVAL;
        return NULL;
    }

    if ((flags & MDBM_HEADER_ONLY) && (flags & MDBM_O_CREAT)) {
        mdbm_logerror(LOG_ERR,0,
                      "%s: cannot use MDBM_HEADER_ONLY with MDBM_O_CREAT",filename);
        close(fd);
        errno = EINVAL;
        return NULL;
    }

    if (!filename || !filename[0] || strlen(filename) > MAXPATHLEN
#ifndef __x86_64__
        || ((flags & (MDBM_DBSIZE_MB|MDBM_DBSIZE_MB_OLD)) && dbsize > 2048)
#endif
        || ((flags & (MDBM_CREATE_V2|MDBM_CREATE_V3)) == (MDBM_CREATE_V2|MDBM_CREATE_V3)))
        {
            mdbm_logerror(LOG_ERR,0,"%s: invalid path, size=%ld",
                          filename, (filename ? (long)strlen(filename) : 0L) );
            close(fd);
            errno = EINVAL;
            return NULL;
        }

#ifdef FREEBSD
    if (flags & MDBM_OPEN_WINDOWED) {
        ERROR();
        close(fd);
        errno = EINVAL;
        return NULL;
    }
#endif
    if ((flags & MDBM_OPEN_WINDOWED) && pagesize && (0 != (pagesize % db_sys_pagesize))) {
        ERROR();
        mdbm_logerror(LOG_ERR,0,"%s: Page size %d must be a multiple of system page size %d", filename, pagesize, db_sys_pagesize);
        close(fd);
        errno = EINVAL;
        return NULL;
    }

/*
    //if ((flags & MDBM_OPEN_WINDOWED) && remap_is_limited(db_sys_pagesize)) {
    //    mdbm_logerror(LOG_ERR,0,"%s: OS does not support windowed mode", filename);
    //    close(fd);
    //    errno = EINVAL;
    //    return NULL;
    //}
*/

    if (!(db_flags & MDBM_DBFLAG_MEMONLYCACHE)) {
        if (!realpath(filename,pathname)) {
          mdbm_logerror(LOG_ERR,0,"Unable to resolve real path for mdbm: %s",filename);
          ERROR();
          return NULL;
        }
    } else {
      pathname[0] = '\0';
    }

    db = (MDBM*)mdbm_allocate_handle();
    if (db == NULL) {
        ERROR();
        errno = ENOMEM;
        return NULL;
    }
    db->db_ver_flag = MDBM_DBVER_3;
    db->db_fd = fd;
    db->db_flags |= db_flags;
    db->db_sys_pagesize = db_sys_pagesize;
    /*init_locks(db); */

    db->db_filename[0] = 0;
    if (filename[0] == '/') {
        strncat(db->db_filename,filename,sizeof(db->db_filename)-1);
    } else {
        strncat(db->db_filename,pathname,sizeof(db->db_filename)-1);
    }
    db->m_stat_cb = 0;
    db->m_stat_cb_flags = 0;
    db->m_stat_cb_user = 0;
    db->db_get_usec = &fast_time_usec;

    if ((flags & MDBM_O_ACCMODE) == MDBM_O_RDONLY) {
        db->db_flags |= MDBM_DBFLAG_RDONLY;
    }
    if (flags & MDBM_NO_DIRTY) {
        db->db_flags |= MDBM_DBFLAG_NO_DIRTY;
    }
    if (flags & MDBM_OPEN_NOLOCK) {
        db->db_flags |= MDBM_DBFLAG_NOLOCK;
    } else {
        int lockmode = 0;
        int lflags = (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE);
        if (mdbm_internal_open_locks(db,flags|lflags,1,&do_check) < 0) {
            ERROR();
            goto open_error;
        }
        lockmode = db_get_lockmode(db);
        /* fprintf(stderr, "SETTING db_flags FROM db_get_lockmode %x ON %s\n",
             lockmode, db->db_filename); */
        /* correct db_flags with actual locking mode */
        db->db_flags |= lockmode;
    }

    if (flags & MDBM_STAT_OPERATIONS) {
        if (mdbm_enable_stat_operations(db, MDBM_STATS_BASIC | MDBM_STATS_TIMED) != 0) {
#if 0
/* deprecated */
        if (flags & MDBM_O_RDWR) {
            db->db_flags |= MDBM_DBFLAG_STAT_OPERATIONS;
        } else {  /* Must have write access: keeping stats on fetches still writes to MDBM */
             mdbm_log(LOG_ERR,
                 "%s: STAT_OPERATIONS (even of fetches only) require Read/Write access", filename);
#endif
            goto open_error;
        }
    }

    if ((flags & MDBM_PROTECT) ||
        (protect_file_exists(pathname)) ||
        (protect_regex_match(pathname, "MDBM_PROTECT_PATH_REGEX"))) {
        mdbm_log(LOG_WARNING,
                 "%s: protect feature enabled -- performance might be degraded", filename);
        db->db_flags |= MDBM_DBFLAG_PROTECT;
    }

    if (flags & MDBM_OPEN_WINDOWED) {
        db->db_flags |= MDBM_DBFLAG_WINDOWED;
    }
#ifdef DEBUG
    mdbm_log(LOG_DEBUG, "%s: mdbm_open, magic number=0x%x", filename, magic);
#endif

    /* If there is a create flag, and the file is empty, treat it like a
     * truncation.  The truncation will initialize the header (magic number,
     * etc.) */
    if (flags & MDBM_O_CREAT) {
        if (lseek(db->db_fd,0,SEEK_END) == 0) {
            flags |= MDBM_O_TRUNC;
        }
    }
    if (flags & MDBM_O_ASYNC) {
        db->db_flags |= MDBM_DBFLAG_ASYNC;
    }
    if (flags & MDBM_O_FSYNC) {
        db->db_flags |= MDBM_DBFLAG_FSYNC;
    }
    if (flags & MDBM_O_TRUNC) {
        int npages;

        if (!pagesize) {
            pagesize = MDBM_PAGESIZ; /* go with default size */
        } else if (pagesize < MDBM_MINPAGE) {
            pagesize = MDBM_MINPAGE;
        } else if (pagesize > MDBM_MAXPAGE) {
            pagesize = MDBM_MAXPAGE;
        } else {
            pagesize = (pagesize + (MDBM_PAGE_ALIGN-1)) & ~(MDBM_PAGE_ALIGN-1);
        }

        if (dbsize) {
            size_t ldbsize = (unsigned int)dbsize;
            if (flags & (MDBM_DBSIZE_MB|MDBM_DBSIZE_MB_OLD)) {
                ldbsize *= (1024*1024);
            }
            npages = (ldbsize + pagesize - 1) / pagesize;
        } else {
            npages = 1;
        }
        if (truncate_db(db,npages,pagesize,flags) < 0) {
            ERROR();
            goto open_error;
        }
        if (flags & MDBM_LARGE_OBJECTS) {
            db->db_hdr->h_dbflags |= MDBM_HFLAG_LARGEOBJ;
            db->db_spillsize = db->db_hdr->h_spill_size = pagesize/2 + pagesize/4;
        }
    } else {
        if (mdbm_internal_remap(db,0,flags) < 0) {
            ERROR();
            goto open_error;
        }
        if (do_check) {
            if ((db->db_flags & MDBM_DBFLAG_HDRONLY) || MDBM_IS_WINDOWED(db)) {
                mdbm_log(LOG_NOTICE,
                         "%s: previous lock owner died; use mdbm_check to test db integrity",
                         db->db_filename);
            } else if (mdbm_check(db,3,1) > 0) {
                mdbm_log(LOG_ERR,"%s: mdbm_open db integrity check failure",filename);
                ERROR();
                errno = EFAULT;
                goto open_error;
            }
        }
    }
    if (!MDBM_NOLOCK(db)) {
        mdbm_internal_do_unlock(db,NULL);
    }

    if ((flags & MDBM_OPEN_WINDOWED)){
        if (0 != (db->db_pagesize % db_sys_pagesize)) {
            mdbm_logerror(LOG_ERR,0,"%s: Page size %d must be a multiple of system page size %d", 
                filename, pagesize, db_sys_pagesize);
            ERROR();
            errno = EINVAL;
            goto open_error;
        }
    }

#ifdef MDBM_SANITY_CHECKS
    if (getenv("MDBM_SANITY_CHECK")) {
        mdbm_sanity_check = 1;
    }
#endif

    if (check_guard_padding(db, 1) != 0) {
        goto open_error;
    }

    return db;

  open_error:
    err = errno;
    if (db) {
        mdbm_internal_close_locks(db);
        if (db->db_base) {
            if (munmap(db->db_base,db->db_base_len) < 0) {
                mdbm_logerror(LOG_ERR,0,"munmap(%p,%llu)",
                              db->db_base,(unsigned long long)db->db_base_len);
            }
            /*fprintf(stderr, "open_error::munmap(%p,%u)\n", db->db_base,(unsigned)db->db_base_len); */
        }
        if (db->db_dir) {
            free(db->db_dir);
        }
        if (db->db_window.buckets) {
            free(db->db_window.buckets);
        }
        if (db->db_window.wpages) {
            free(db->db_window.wpages);
        }
        if (db->db_fd != -1) {
            close(db->db_fd);
        }
        mdbm_free_handle(db);
    } else {
        if (fd != -1) {
            close(fd);
        }
    }
    errno = err;
    return NULL;
}


MDBM*
mdbm_open(const char *filename, int flags, int mode, int pagesize, int dbsize)
{
#ifdef DEBUG
    {
        /* DEBUG: manually enable debugging for testing.
         * Specify a numeric log level:
         *
         * 0: emerg   1: alert  2: crit   3: err     4: warn
         * 5: notice  6: info   7: debug  8: debug2  9: debug3
         */
        char *mdbm_level = getenv("MDBM_LOG_LEVEL");
        if ((mdbm_level != NULL)) {
            int level = atoi(mdbm_level);
            if ((level >= LOG_EMERG) && (level <= LOG_DEBUG)) {
              mdbm_log_minlevel(level);
            } else {
                mdbm_log(LOG_ERR, "%s: mdbm log level must in numeric and in range %d,%d, level=%s",
                         filename, LOG_EMERG, LOG_DEBUG, mdbm_level);
            }
        }
    }
#endif

    /* Add code to handle opening RDONLY|CREAT by allowing us to create an empty V3 */
    /* MDBM (which requires read+write access) then closing it and reopening it RDONLY */
    /* Created mdbm_open_internal out of the old mdbm_open() to avoid recursive calls. */
    if ((flags & MDBM_CREATE_V3) && (flags & MDBM_O_CREAT) && !(flags & MDBM_O_RDWR) && (filename != NULL)) {
        int errno_save = errno;
        struct stat stat_info;
        if ((stat(filename, &stat_info) == 0) && (stat_info.st_size != 0) ) {
            return mdbm_open_inner(filename, flags, mode, pagesize, dbsize, 1);
        } else {
            MDBM *db =  NULL;
            int cflags = flags | MDBM_O_RDWR | MDBM_O_TRUNC;
            errno = 0;
            db =  mdbm_open_inner(filename, cflags, mode, pagesize, dbsize, 1);
            if (db != NULL) {
                mdbm_close(db);
                errno = errno_save;
            } else {
                mdbm_logerror(LOG_ERR,0,"%s: cannot truncate and reopen",filename);
                errno = errno_save;
                return NULL;
            }
        }
    }

    return mdbm_open_inner(filename, flags, mode, pagesize, dbsize, 1);
}


void
mdbm_close(MDBM *db)
{
    int zrefs;

    if (!db) {
        return;
    }

    zrefs = !db->db_dup_info || (dec_dup_ref(db) == 0);
    if (zrefs && db->db_rstats) {
        mdbm_close_rstats(db->db_rstats_mem);
        db->db_rstats = NULL;
        db->db_rstats_mem = NULL;
    }

#ifdef MDBM_BSOPS
    if (db->db_bsops) {
        db->db_bsops->bs_term(db->db_bsops_data,0);
        db->db_bsops_data = NULL;
        db->db_bsops = NULL;
    }
#endif

    mdbm_set_window_size_internal(db,0);

    if (zrefs && db->db_base) {
        if (!MDBM_IS_RDONLY(db)) {
            if (db->db_flags & MDBM_DBFLAG_FSYNC) {
                msync(db->db_base,MDBM_DB_MAP_SIZE(db),MS_ASYNC);
            }
        }
        if (munmap(db->db_base,db->db_base_len) < 0) {
            mdbm_logerror(LOG_ERR,0,"munmap(%p,%llu)",
                          db->db_base,(unsigned long long)db->db_base_len);
        }
        /*fprintf(stderr, "close::munmap(%p,%u)\n", db->db_base,(unsigned)db->db_base_len); */
        db->db_base = NULL;
        db->db_hdr = NULL;
    }
    mdbm_internal_close_locks(db);
    if (zrefs && db->db_fd >= 0) {
        close(db->db_fd);
        db->db_fd = -1;
    }
    if (zrefs && db->db_dup_info) {
        free(db->db_dup_info);
        db->db_dup_info = NULL;
    }
    if (db->db_dir) {
        free(db->db_dir);
        db->db_dir = NULL;
    }
    check_guard_padding(db, 1);
    mdbm_free_handle(db);
}

int
mdbm_set_cachemode(MDBM* db, int cachemode)
{
    int err = 0;

    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return -1;
    }
    if ((cachemode & ~MDBM_CACHEMODE_BITS)
        || (MDBM_CACHEMODE(cachemode) > MDBM_CACHEMODE_MAX))
    {
        errno = EINVAL;
        return -1;
    }

    if (lock_db(db) < 0) {
        return -1;
    }
    if (!check_empty(db)) {
        err = EINVAL;
    } else {
        db->db_cache_mode = db->db_hdr->h_cache_mode = cachemode;
    }
    unlock_db(db);

    if (err) {
        errno = err;
        return -1;
    }
    return 0;
}

int
mdbm_get_cachemode(MDBM* db)
{
    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return -1;
    }
    return db->db_cache_mode;
}

const char*
mdbm_get_cachemode_name(int cachemode)
{
    static char* s_cache_modes[] =
        { "none",
          "LFU",
          "LRU",
          "GDSF",
          "unknown"
        };

    assert(sizeof(s_cache_modes) / sizeof(s_cache_modes[0]) > MDBM_CACHEMODE_MAX);
    return (cachemode > MDBM_CACHEMODE_MAX
            ? s_cache_modes[MDBM_CACHEMODE_MAX + 1]
            : s_cache_modes[cachemode]);
}

int
mdbm_get_stat_counter(MDBM *db, mdbm_stat_type type, mdbm_counter_t *value)
{
    int rc = 0;

#ifdef __MDBM_V2_COMPAT__
    if ((db == NULL) || (db->db_ver_flag != MDBM_DBVER_3)) {
        return -1;
    }
#endif

    switch ((int)type) {
    case MDBM_STAT_TYPE_FETCH:
        *value =  mdbm_hdr_stats(db)->s_fetches;
        break;

    case MDBM_STAT_TYPE_STORE:
        *value =  mdbm_hdr_stats(db)->s_stores;
        break;

    case MDBM_STAT_TYPE_DELETE:
        *value =  mdbm_hdr_stats(db)->s_deletes;
        break;

    default:
        rc = -1;
    }

    return rc;
}

int
mdbm_get_stat_time(MDBM *db, mdbm_stat_type type, time_t *value)
{
    int rc = 0;

#ifdef __MDBM_V2_COMPAT__
    if ((db == NULL) || (db->db_ver_flag != MDBM_DBVER_3)) {
        return -1;
    }
#endif

    switch ((int)type) {
    case MDBM_STAT_TYPE_FETCH:
        *value =  (time_t) mdbm_hdr_stats(db)->s_last_fetch;
        break;

    case MDBM_STAT_TYPE_STORE:
        *value =  (time_t) mdbm_hdr_stats(db)->s_last_store;
        break;

    case MDBM_STAT_TYPE_DELETE:
        *value =  (time_t) mdbm_hdr_stats(db)->s_last_delete;
        break;

    default:
        rc = -1;
    }

    return rc;
}

inline static void
fetch_increment(MDBM *db)
{
    if (MDBM_GET_STAT_OPERATIONS_OPS(db)) {
        MDBM_INC_STAT_64(&mdbm_hdr_stats(db)->s_fetches);
    }
    if (MDBM_GET_STAT_OPERATIONS_TIMES(db)) {
        mdbm_hdr_stats(db)->s_last_fetch = get_time_sec();
    }
}

inline static void
store_increment(MDBM *db)
{
    if (MDBM_GET_STAT_OPERATIONS_OPS(db)) {
        MDBM_INC_STAT_64(&mdbm_hdr_stats(db)->s_stores);
    }
    if (MDBM_GET_STAT_OPERATIONS_TIMES(db)) {
        mdbm_hdr_stats(db)->s_last_store = get_time_sec();
    }
}

inline static void
delete_increment(MDBM *db)
{
    if (MDBM_GET_STAT_OPERATIONS_OPS(db)) {
        MDBM_INC_STAT_64(&mdbm_hdr_stats(db)->s_deletes);
    }
    if (MDBM_GET_STAT_OPERATIONS_TIMES(db)) {
        mdbm_hdr_stats(db)->s_last_delete = get_time_sec();
    }
}

void
mdbm_reset_stat_operations(MDBM *db)
{
#ifdef __MDBM_V2_COMPAT__
    if ((db == NULL) || (db->db_ver_flag != MDBM_DBVER_3)) {
        return;
    }
#endif

    mdbm_hdr_stats(db)->s_fetches = 0ULL;
    mdbm_hdr_stats(db)->s_stores  = 0ULL;
    mdbm_hdr_stats(db)->s_deletes = 0ULL;

    mdbm_hdr_stats(db)->s_last_fetch  = 0ULL;
    mdbm_hdr_stats(db)->s_last_store  = 0ULL;
    mdbm_hdr_stats(db)->s_last_delete = 0ULL;
}

datum
mdbm_fetch(MDBM *db, datum key)
{
    datum val;

    fetch_increment(db);
    if (access_entry(db,&key,&val,NULL,NULL,NULL) < 0) {
        val.dptr = NULL;
        val.dsize = 0;
    }
    return val;
}

int
mdbm_fetch_r(MDBM *db, datum *key, datum *val, MDBM_ITER *iter)
{
    fetch_increment(db);
    return access_entry(db,key,val,NULL,NULL,iter);
}

int
mdbm_fetch_buf(MDBM *db, datum *key, datum *val, datum *buf, int flags)
{
    fetch_increment(db);
    return access_entry(db,key,val,buf,NULL,NULL);
}

static kvpair
next_r(MDBM *db, MDBM_ITER *iter, int one_page);

int
mdbm_fetch_dup_r(MDBM *db, datum *key, datum *val, MDBM_ITER *iter)
{
    mdbm_pagenum_t pagenum;
    kvpair kv;

    pagenum = iter->m_pageno;
    if (iter->m_pageno == 0 && iter->m_next == -1) {
        /* Called with an uninitialized iterator.
         * Just lookup first entry.
         */
        return mdbm_fetch_r(db,key,val,iter);
    }

    fetch_increment(db);

    /* Look for next occurrence of this key in the same page.  Continue
     * looking for this key until I run out of keys on this page; or
     * until I find it.
     */

    do {
        kv = next_r(db,iter,1);
    } while (kv.val.dptr                        /* Still data in the db */
             && iter->m_pageno == pagenum       /* Still on same page */
             && (kv.key.dsize != key->dsize     /* Key not same length -OR- */
                 || kv.key.dptr[0] != key->dptr[0] /* Key not same value - look again */
                 || memcmp(kv.key.dptr,key->dptr,key->dsize)));

    /* ASSERT: kv.val.dptr is NULL [end of db];
     *         pageno is different [moved to next page];
     *         or we found the appropriate key.
     */
    if (kv.val.dptr && (iter->m_pageno == pagenum)) {
        /* found something. */
        *val = kv.val;
        return 0;
    }

    val->dptr = NULL;
    val->dsize = 0;
    errno = ENOENT;
    return -1;
}

int
mdbm_fetch_info(MDBM *db, datum *key, datum *val, datum *buf,
                struct mdbm_fetch_info* info,
                MDBM_ITER* iter)
{
    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return -1;
    }

    fetch_increment(db);
    return access_entry(db,key,val,buf,info,iter);
}

char*
mdbm_fetch_str(MDBM *db, const char *key)
{
    datum k;

    k.dptr = (char *)key;
    k.dsize = strlen(key) + 1;
    return mdbm_fetch(db,k).dptr;
}

int
mdbm_delete(MDBM *db, datum key)
{
    delete_increment(db);
    return access_entry(db,&key,NULL,NULL,NULL,NULL);
}

int
mdbm_delete_r(MDBM *db, MDBM_ITER *iter)
{
    int ret;
    mdbm_entry_data_t e;
    mdbm_entry_t* ep;
    mdbm_pagenum_t pagenum;
    int index;

    if (!iter) {
        errno = EINVAL;
        return -1;
    }

    pagenum = iter->m_pageno;
    index = (iter->m_next+3)/-2;

    if (pagenum > db->db_max_dirbit || index < 0) {
        errno = EINVAL;
        return -1;
    }

    if (mdbm_internal_do_lock(db,MDBM_LOCK_WRITE,MDBM_LOCK_WAIT,NULL,NULL,&pagenum) < 0) {
        return -1;
    }

    if ((ep = set_entry(db,pagenum,index,&e)) == NULL || !ep->e_key.match) {
        delete_increment(db);
        errno = ENOENT;
        ret = -1;
    } else {
        del_entry(db,e.e_page,ep);
        ret = 0;
    }

    mdbm_internal_do_unlock(db,NULL);

    return ret;
}

int
mdbm_delete_str(MDBM *db, const char *key)
{
    MDBM_ITER iter;
    datum k;


    delete_increment(db);
    k.dptr = (char *)key;
    k.dsize = strlen(key) + 1;
    return access_entry(db,&k,NULL,NULL,NULL,&iter);
}



int
mdbm_store_r(MDBM *db, datum* key, datum* val, int flags, MDBM_ITER* iter)
{
    int ret = 0;
    mdbm_hashval_t hashval;
    mdbm_pagenum_t pagenum;
    int want_large, is_large;
    mdbm_page_t* page;
    int pass, ksize, vsize, kvsize, esize, maxesize;
    mdbm_entry_t* ep = NULL;
    mdbm_entry_t* freep;
    int free_index = -1;
    kvpair kv;
    mdbm_pagenum_t lob_pagenum = 0;
    char* v;
    uint64_t t0 = 0;
    int stored = 0;
    int cache_stored = 0;
    int db_locked = 0;
    int key_locked = 0;
    int tries = 0;
    static const int MAX_LOCK_TRIES = 8;

    store_increment(db);

    if (MDBM_DO_STAT_TIME(db)) {
        t0 = db->db_get_usec();
    }

    if (!key || !val || key->dsize < 1 || key->dsize > MDBM_KEYLEN_MAX
        || val->dsize < 0 || (!db->db_spillsize && val->dsize > MDBM_VALLEN_MAX))
    {
        errno = EINVAL;
        return -1;
    }

    if (check_guard_padding(db, 1) != 0) {
        return -1;
    }

    if (MDBM_IS_WINDOWED(db) && db->db_spillsize && val->dsize >= db->db_spillsize) {
        int n = 2+MDBM_DB_NUM_PAGES_ROUNDED(db,val->dsize+MDBM_PAGE_T_SIZE);
        if (db->db_window.num_pages < n) {
            mdbm_log(LOG_ERR,
                     "%s: window size too small for large object (need at least %d bytes)",
                     db->db_filename,
                     n*db->db_pagesize);
            errno = ENOMEM;
            return -1;
        }
    }

    want_large = 0;
    ksize = MDBM_ALIGN_LEN(db,key->dsize);
    vsize = MDBM_ALIGN_LEN(db,val->dsize);
    if (MDBM_DB_CACHEMODE(db)) {
        vsize += MDBM_CACHE_ENTRY_T_SIZE;
    }
    kvsize = ksize + vsize;
    esize = kvsize + MDBM_ENTRY_T_SIZE;
    maxesize = db->db_pagesize-MDBM_PAGE_T_SIZE-MDBM_ENTRY_T_SIZE;

    if (esize > maxesize || (db->db_spillsize && MDBM_LOB_ENABLED(db) && vsize >= db->db_spillsize)) {
        if (db->db_spillsize) {
            want_large = 1;
            vsize = MDBM_ALIGN_LEN(db,MDBM_ENTRY_LOB_T_SIZE);
            if (MDBM_DB_CACHEMODE(db)) {
                vsize += MDBM_CACHE_ENTRY_T_SIZE;
            }
            kvsize = ksize + vsize;
            esize = kvsize + MDBM_ENTRY_T_SIZE;
        }
        if (esize > maxesize) {
            errno = EINVAL;
            ret = -1;
            goto store_error_unlocked;
        }
    }

 store_retry_lock:
    if (mdbm_internal_do_lock(db,MDBM_LOCK_WRITE,MDBM_LOCK_WAIT,key,&hashval,&pagenum) < 0) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "Cannot write lock when storing");
#endif
        ret = -1;
        goto store_error_unlocked;
    }
    key_locked = 1;

#ifdef MDBM_BSOPS
    if (!(flags & MDBM_CACHE_ONLY) && db->db_bsops) {
        if (db->db_bsops->bs_store(db->db_bsops_data,key,val,flags) < 0) {
            int err = errno;
            mdbm_internal_do_unlock(db,key);
            if (err == EPERM) {
              mdbm_log(LOG_NOTICE,
                       "Retrying store to backing-store after possible"
                       " page-split locking conflict");
              if (++tries < MAX_LOCK_TRIES) {
                goto store_retry_lock;
              }
            }
            errno = err;
            return -1;
        }
        flags |= MDBM_CLEAN;
        flags = flags & ~MDBM_STORE_MASK;
        if (MDBM_CACHE_UPDATE_MODE(flags) == MDBM_CACHE_MODIFY) {
            flags |= MDBM_MODIFY;
        } else {
            flags |= MDBM_REPLACE;
        }
        stored = 1;
    }
#endif

    MDBM_SIG_DEFER;
    if ((page = pagenum_to_page(db,pagenum,MDBM_PAGE_ALLOC,MDBM_PAGE_MAP)) == NULL) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "pagenum_to_page inside store failed, pagenum=%d", pagenum);
#endif
        errno = ENOMEM;
        ret = -1;
        goto store_ret;
    }

    if (MDBM_STORE_MODE(flags) != MDBM_INSERT_DUP
        && ((ep = find_entry(db,page,key,hashval,&kv.key,&kv.val,NULL)) != NULL))
    {
        if (iter) {
            iter->m_pageno = pagenum;
            iter->m_next = -3 - 2*MDBM_ENTRY_INDEX(page,ep);
        }
        if (MDBM_STORE_MODE(flags) == MDBM_INSERT) {
            *key = kv.key;
            *val = kv.val;
            ret = MDBM_STORE_ENTRY_EXISTS;
            goto store_ret;
        }
        is_large = MDBM_ENTRY_LARGEOBJ(ep);
        if (is_large == want_large) {
            if (is_large) {
                mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
                mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
                if (MDBM_DB_NUM_PAGES_ROUNDED(db,lob->p.p_vallen+MDBM_PAGE_T_SIZE)
                    == MDBM_DB_NUM_PAGES_ROUNDED(db,val->dsize+MDBM_PAGE_T_SIZE))
                {
                    lob = MDBM_PAGE_TO_CHUNK_PTR(db,lob);
                    if (!(flags & MDBM_RESERVE)) {
                        memcpy(MDBM_LOB_VAL_PTR(lob),val->dptr,val->dsize);
                    }
                    kv.val.dsize = lob->p.p_vallen = val->dsize;
                    *key = kv.key;
                    *val = kv.val;
                    ret = 0;
                    cache_stored = 1;
                    goto store_ret;
                }
            } else {
                /* If the new value fits in the same aligned space, return that space. */
                if (MDBM_ALIGN_LEN(db,kv.val.dsize) == MDBM_ALIGN_LEN(db,val->dsize)) {
                    MDBM_SET_PAD_BYTES(ep,MDBM_ALIGN_PAD_BYTES(db,val->dsize));
                    if (!(flags & MDBM_RESERVE)) {
                        memcpy(kv.val.dptr,val->dptr,val->dsize);
                    }
                    *key = kv.key;
                    *val = kv.val;
                    ret = 0;
                    cache_stored = 1;
                    goto store_ret;
                }
            }
        }
        del_entry(db,page,ep);
    } else if (MDBM_STORE_MODE(flags) == MDBM_MODIFY) {
        if (stored) {
            ret = 0;
        } else {
            errno = ENOENT;
            ret = -1;
        }
        goto store_ret;
    }

    pass = 1;
    while (esize > MDBM_PAGE_FREE_BYTES(page)) {
        mdbm_page_t* newpage;
        int del_bytes = 0;
        int free_size = 0;
        int free_bytes;
        int free_dir_space = MDBM_PAGE_FREE_BYTES(page) >= MDBM_ENTRY_T_SIZE;
        int i;

        /* Look for a deleted entry. */
        for (i = 0; i < page->p.p_num_entries; i++) {
            ep = MDBM_ENTRY(page,i);
            if (!ep->e_key.match) {
                int size = MDBM_ENTRY_SIZE(db,ep);
                del_bytes += size;
                if (size == esize) {
                    /* Exact size deleted entry found. Finshed looking. */
                    free_index = i;
                    free_size = size;
                    break;
                }
                if (free_dir_space && size > esize && (!free_size || size < free_size)) {
                    /* Bigger deleted size found.  Continue looking. */
                    free_index = i;
                    free_size = size;
                }
            }
        }
        if (free_size) {
            if (free_size > esize) {
                int move_count = page->p.p_num_entries - free_index;
                if (move_count > 0) {
                    /* Shuffle the entries above free_index up 1 index entry,
                     * and use this entry. */
                    freep = MDBM_ENTRY(page,free_index);
                    memmove(freep+2,freep+1,move_count*MDBM_ENTRY_T_SIZE);
                    freep[1].e_key.match = 0;
                    freep[1].e_flags = 0;
                    freep[1].e_offset = freep[0].e_offset - kvsize;
                }
                page->p.p_num_entries++; /* BUG? if move count <= 0 */
            }
            break;
        }
        free_bytes = MDBM_PAGE_FREE_BYTES(page) + del_bytes;
        if (esize <= free_bytes) {
            wring_page(db,page);
            free_index = -1;
            break;
        }
        if (pass > 1) {
            errno = ENOMEM;
            ret = -1;
            goto store_ret;
        }

        if ((newpage = split_page(db,hashval)) == NULL) {
            if (errno == EPERM) {
                int lock_tries = 0;
                static const int MAX_STORE_LOCK_TRIES = 2;
                /* Must upgrade to db lock because split page will almost always be
                   in a different partition. */
                for (;;) {
                    if ((key_locked && trylock_db(db) == 1)
                        || (!key_locked && lock_db(db) ==1))
                    {
                        if ((newpage = split_page(db,hashval)) == NULL) {
                            unlock_db(db);
                        } else {
                            db_locked = 1;
                        }
                        break;
                    }
                    if (errno != EDEADLK
                        || db_multi_part_locked(db)
                        || ++lock_tries >= MAX_STORE_LOCK_TRIES)
                    {
                        break;
                    }
                    if (key_locked) {
                        mdbm_internal_do_unlock(db,key);
                        key_locked = 0;
                    }
                    {
                        struct timespec tv = { 0, 1000000 };
                        nanosleep(&tv,NULL);
                    }
                    mdbm_log(LOG_NOTICE,
                             "%s: retrying store after page-split deadlock avoidance (page=%d)",
                             db->db_filename,pagenum);
                }
            }
        }
        if (!newpage) {
            page = pagenum_to_page(db,pagenum,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
            if ((!db->db_shake_func || shake_page(db,page,key,val,free_bytes,esize) < 0)
                && (!MDBM_DB_CACHEMODE(db)
                    || cache_evict(db,page,key,val,free_bytes,esize,0) < esize)
                && expand_page(db,page,pagenum) < 0)
            {
                errno = ENOMEM;
                ret = -1;
                goto store_ret;
            }
            page = pagenum_to_page(db,pagenum,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
            pass++;
        } else {   /* newpage!=NULL, so new page has been allocated */
            page = newpage;
            pagenum = newpage->p_num;
        }
    }

    if (want_large) {
        mdbm_page_t* alloc_lob = NULL;
        uint32_t alloc_npages = MDBM_DB_NUM_PAGES_ROUNDED(db,val->dsize+MDBM_PAGE_T_SIZE);
        int ntries = 0;
        static const int MAX_TRIES = 100;

        if (mdbm_internal_lock(db) < 0) {
#ifdef DEBUG
        mdbm_log(LOG_DEBUG, "Cannot mdbm_internal_lock when storing large object");
#endif
            ret = -1;
            goto store_ret;
        }
        while (ntries < MAX_TRIES) {
            if ((alloc_lob = (mdbm_page_t*)alloc_chunk(db,MDBM_PTYPE_LOB,alloc_npages,
                                                       0,0,MDBM_PAGE_MAP,MDBM_ALLOC_CAN_UNLOCK))
                != NULL)
            {
                break;
            }
            /* alloc_chunk releases internal lock if allocation fails */
            if (mdbm_internal_lock(db) < 0) {
#ifdef DEBUG
                mdbm_log(LOG_DEBUG, "Cannot mdbm_internal_lock after alloc_chunk");
#endif
                ret = -1;
                goto store_ret;
            }

            if (MDBM_DB_CACHEMODE(db)) {
                mdbm_page_t* p = page;
                mdbm_hashval_t h = hashval;
                mdbm_pagenum_t pnum = pagenum;
                uint32_t alloc_bytes = alloc_npages*db->db_pagesize;
                uint32_t free_bytes = 0;

                while (ntries < MAX_TRIES) {
                    ++ntries;
                    if ((free_bytes = cache_evict(db,p,key,val,free_bytes,alloc_bytes,1))
                        >= alloc_bytes)
                    {
                        break;
                    }
                    h = MDBM_HASH_VALUE(db,&h,sizeof(h));
                    pnum = hashval_to_pagenum(db,h);
                    p = pagenum_to_page(db,pnum,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
                }
                if (free_bytes >= alloc_bytes) {
                    continue;
                }
            }

            mdbm_internal_unlock(db);
#ifdef DEBUG
            mdbm_log(LOG_DEBUG, "Cannot find space for storing large object");
#endif
            errno = ENOMEM;
            ret = -1;
            goto store_ret;
        }
        page = pagenum_to_page(db,pagenum,MDBM_PAGE_EXISTS,MDBM_PAGE_MAP);
                        /* may have remapped db */
        lob_pagenum = alloc_lob->p_num;

        alloc_lob->p_num = pagenum;
        alloc_lob->p.p_vallen = val->dsize;
        if (!(flags & MDBM_RESERVE)) {
            memcpy(MDBM_LOB_VAL_PTR(alloc_lob),val->dptr,val->dsize);
        }
    }

    if (free_index == -1) {
        free_index = page->p.p_num_entries++;
        freep = MDBM_ENTRY(page,free_index);
        MDBM_INIT_TOP_ENTRY(freep+1,freep[0].e_offset - kvsize);
    } else {
        freep = MDBM_ENTRY(page,free_index);
    }

    freep->e_offset -= MDBM_ALIGN_LEN(db,key->dsize);
    freep->e_flags = 0;
    freep->e_key.key.len = key->dsize;
    freep->e_key.key.hash = hashval >> 16;

    memcpy(MDBM_KEY_PTR1(page,freep),key->dptr,key->dsize);
    MDBM_SET_PAD_BYTES(freep,MDBM_ALIGN_PAD_BYTES(db,val->dsize));
    v = MDBM_VAL_PTR1(db,page,freep);
    if (MDBM_DB_CACHEMODE(db)) {
        mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)v;
        c->c_num_accesses = 0;
        c->c_dat.access_time = 0;
        v += MDBM_CACHE_ENTRY_T_SIZE;
    }
    if (want_large) {
        mdbm_entry_lob_t* lp = (mdbm_entry_lob_t*)v;
        lp->l_pagenum = lob_pagenum;
        lp->l_flags = 0;
        lp->l_vallen = val->dsize;
        freep->e_flags |= MDBM_EFLAG_LARGEOBJ;
        mdbm_internal_unlock(db);
    } else {
        if (!(flags & MDBM_RESERVE)) {
            memcpy(v,val->dptr,val->dsize);
        }
    }
    get_kv1(db,page,freep,key,val,NULL);
    if (iter) {
        iter->m_pageno = pagenum;
        iter->m_next = -3 - 2*MDBM_ENTRY_INDEX(page,freep);
    }
    ep = freep;
    cache_stored = 1;

 store_ret:
    if (!ret) {
        if (ep && !(db->db_flags & MDBM_DBFLAG_NO_DIRTY)) {
            if ((flags & MDBM_CLEAN)) {
                ep->e_flags &= ~MDBM_EFLAG_DIRTY;
            } else {
                ep->e_flags |= MDBM_EFLAG_DIRTY;
            }
        }
    }

    MDBM_SIG_ACCEPT;
    if (db_locked) {
        unlock_db(db);
    }
    if (key_locked) {
        mdbm_internal_do_unlock(db,key);
    }

store_error_unlocked:
    if (ret<0) {
        mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->store : NULL;
        MDBM_INC_STAT_ERROR_COUNTER(db, rstats_val, MDBM_STAT_TAG_STORE_ERROR);
    } else {
      if (MDBM_DO_STAT_TIME(db)) {
        uint64_t now = db->db_get_usec();
        if (!(flags & MDBM_CACHE_ONLY)) {
            mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->store : NULL;
            MDBM_ADD_STAT_WAIT(db,rstats_val,now - t0, MDBM_STAT_TAG_STORE);
        }
        if (cache_stored && (!db->db_rstats || db->db_rstats->version == MDBM_RSTATS_VERSION)
              && db->db_bsops) {
            mdbm_rstats_val_t* rstats_val = (db->db_rstats) ? &db->db_rstats->cache_store : NULL;
            MDBM_ADD_STAT_WAIT(db,rstats_val,now - t0, MDBM_STAT_TAG_CACHE_STORE);
        }
      } else if (MDBM_DO_STATS(db)) {
          if (!(flags & MDBM_CACHE_ONLY)) {
            MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_STORE);
          }
          if (cache_stored && db->db_bsops) {
            MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_CACHE_STORE);
          }
      }
      if (MDBM_DO_STAT_PAGE(db)) {
            MDBM_PAGE_STAT(db, pagenum, MDBM_STAT_TAG_PAGE_STORE);
      }
    }


    if (stored && ret < 0 && (errno == ENOMEM || errno == EINVAL)) {
        ret = 0;
    }

    return ret;
}

int
mdbm_store(MDBM *db, datum key, datum val, int flags)
{
    return mdbm_store_r(db,&key,&val,flags,NULL);
}

int
mdbm_store_str(MDBM *db, const char *key, const char *val, int flags)
{
    datum k, v;

    k.dptr = (char *)key;
    k.dsize = strlen(key) + 1;
    v.dptr = (char *)val;
    v.dsize = strlen(val) + 1;
    return mdbm_store_r(db,&k,&v,flags,NULL);
}

static int
count_entries(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
    uint64_t *count_ptr = (uint64_t *)user;
    *count_ptr += info->i_page.page_active_entries;
    return 0;
}

uint64_t
mdbm_count_records(MDBM *db)
{
    uint64_t db_entry_count = 0;

    if (mdbm_iterate(db, -1, count_entries, 0, &db_entry_count) < 0) {
        mdbm_logerror(LOG_ERR, 0, "%s: entry count with mdbm_iterate failed", db->db_filename);
        return -1;
    }

    return db_entry_count;
}


kvpair
mdbm_first(MDBM *db)
{
    return mdbm_first_r(db,&db->db_iter);
}

kvpair
mdbm_first_r(MDBM *db, MDBM_ITER *iter)
{
    iter->m_pageno = 0;
    iter->m_next = -1;
    return mdbm_next_r(db,iter);
}

datum
mdbm_firstkey(MDBM *db)
{
    kvpair kv;

    kv = mdbm_first_r(db,&db->db_iter);
    return kv.key;
}

datum
mdbm_firstkey_r(MDBM *db, MDBM_ITER *iter)
{
    kvpair kv;

    kv = mdbm_first_r(db,iter);
    return kv.key;
}

kvpair mdbm_next(MDBM *db)
{
    return mdbm_next_r(db,&db->db_iter);
}

static kvpair
next_r(MDBM *db, MDBM_ITER *iter, int one_page)
{
    mdbm_pagenum_t pagenum;
    int index;
    kvpair kv;

    pagenum = iter->m_pageno;
    index = (iter->m_next+1)/-2;

    if (index < 0) {
        index = 0;
    }
    while (pagenum <= db->db_max_dirbit) {
        mdbm_entry_data_t e;
        mdbm_entry_t* ep;

        if (index == 0) {
            if (db_is_multi_lock(db) && !mdbm_isowned(db)) {
                if (mdbm_internal_do_lock(db,MDBM_LOCK_READ,MDBM_LOCK_WAIT,NULL,NULL,&pagenum) != 1) {
                    memset(&kv,0,sizeof(kv));
                    return kv;
                }
            }
        } else {
            if (MDBM_IS_WINDOWED(db)) {
                if ((ep = set_entry(db,pagenum,index-1,&e)) != NULL) {
                    if (MDBM_ENTRY_LARGEOBJ(ep)) {
                        get_kv(db,&e,&kv.key,&kv.val);
                        release_window_page(db,kv.val.dptr);
                    }
                }
            }
        }

        if ((ep = set_entry(db,pagenum,index,&e)) != NULL) {
            while (ep) {
                if (ep->e_key.match) {
                    get_kv(db,&e,&kv.key,&kv.val);
                    iter->m_pageno = pagenum;
                    iter->m_next = -3 - 2*e.e_index;
                    return kv;
                }
                ep = next_entry(&e);
            }
        }

        if (MDBM_IS_WINDOWED(db)) {
            release_window_page(db,e.e_page);
        }

        if (one_page) {
            memset(&kv,0,sizeof(kv));
            iter->m_pageno = pagenum+1;
            iter->m_next = 0;
            return kv;
        }

        if (db_is_multi_lock(db) && !mdbm_isowned(db)) {
            mdbm_internal_do_unlock(db,NULL);
        }

        pagenum++;
        index = 0;
    }
    memset(&kv,0,sizeof(kv));
    iter->m_pageno = db->db_max_dirbit+1;
    iter->m_next = -1;
    return kv;
}

kvpair
mdbm_next_r(MDBM *db, MDBM_ITER *iter)
{
    return next_r(db,iter,0);
}

datum
mdbm_nextkey(MDBM *db)
{
    kvpair kv;

    kv = mdbm_next_r(db,&db->db_iter);
    return kv.key;
}

datum
mdbm_nextkey_r(MDBM *db, MDBM_ITER *iter)
{
    kvpair kv;

    kv = mdbm_next_r(db,iter);
    return kv.key;
}

int
mdbm_pre_split(MDBM* db, mdbm_ubig_t pages)
{
    int dir_shift, n, p;
    int init_bytes, init_bits;
    int npages, extra;
    mdbm_dir_t* dir;

    if (mdbm_count_records(db) > 0) {   /* pre_split requires DB to be empty */
        mdbm_log(LOG_ERR, "%s: mdbm_pre_split requires an empty DB", db->db_filename);
        errno = EFBIG;
        return -1;
    }
    if (pages > MDBM_NUMPAGES_MAX || pages < 1) {
        mdbm_log(LOG_ERR, "%s: mdbm_pre_split invalid page count (%u vs %u max)", 
            db->db_filename, (unsigned int) pages, MDBM_NUMPAGES_MAX);
        errno = EFBIG;
        return -1;
    }

    if (lock_db(db) < 0) {
        ERROR();
        return -1;
    }

    for (dir_shift = 0, n = 1; n <= pages; dir_shift++, n <<= 1);
    if (dir_shift > 0) {
        dir_shift--;
    }
    extra = MDBM_NUM_DIR_PAGES(db->db_pagesize,dir_shift);
    pages += extra;
    /* fprintf(stderr, "mdbm_pre_split() pages extra:%d total:%d \n", extra, pages); */
    if (resize(db,dir_shift,pages) < 0) {
        unlock_db(db);
        ERROR();
        return -1;
    }

#define MDBM_BITS_PER_BYTE      8
    init_bytes = db->db_max_dirbit / MDBM_BITS_PER_BYTE;
    init_bits = db->db_max_dirbit % MDBM_BITS_PER_BYTE;

    dir = MDBM_DIR_PTR(db);
    if (init_bytes) {
        memset(dir,0xff,init_bytes);
    }
    if (init_bits) {
        int i;

        dir[init_bytes] = 0;
        for (i = 0; i < init_bits; i++) {
            mdbm_dir_t* d = dir + init_bytes;
            *d = (mdbm_dir_t)(*d | (1 << i));
        }
    }
    db->db_hdr->h_dbflags |= MDBM_HFLAG_PERFECT;
    ++db->db_hdr->h_dir_gen;
    sync_dir(db,NULL);

    p = db->db_hdr->h_last_chunk;
    npages = MDBM_PAGE_PTR(db,p)->p_num_pages;
    p += npages;
    /* fprintf(stderr, "mdbm_pre_split() max_dirbit:%d \n", db->db_max_dirbit); */
    for (n = 0; n <= db->db_max_dirbit; n++) {
        mdbm_page_t* page;
        if (MDBM_GET_PAGE_INDEX(db,n)) {
            /* fprintf(stderr, "mdbm_pre_split() init(skip) page %d / %d p:%d \n", n, pages, p); */
            continue;
        }
        /* fprintf(stderr, "mdbm_pre_split() init page %d / %d p:%d \n", n, pages, p); */
        page = MDBM_PAGE_PTR(db,p);
        MDBM_SET_PAGE_INDEX(db, n, p);
        page->p_num = n;
        page->p_type = MDBM_PTYPE_DATA;
        page->p_flags = 0;
        page->p_num_pages = 1;
        page->p_prev_num_pages = npages;
        page->p_r0 = 0;
        page->p_r1 = 0;
        page->p.p_data = 0;
        npages = 1;
        MDBM_INIT_TOP_ENTRY(MDBM_ENTRY(page,0),db->db_pagesize);
        if (MDBM_IS_WINDOWED(db)) {
            release_window_page(db,page);
        }
        p++;
    }
    db->db_hdr->h_last_chunk = p-1;

    unlock_db(db);
    return 0;
}

int
mdbm_set_alignment(MDBM* db, int align)
{
    if (lock_db(db) != 1) {
        return -1;
    }
    if ((align != MDBM_ALIGN_8_BITS
         && align != MDBM_ALIGN_16_BITS
         && align != MDBM_ALIGN_32_BITS
         && align != MDBM_ALIGN_64_BITS)
        || !check_empty(db))
    {
        unlock_db(db);
        errno = EINVAL;
        return -1;
    }

    db->db_hdr->h_dbflags = db->db_hdr->h_dbflags | (uint16_t)align;
    db->db_align_mask = align;
    unlock_db(db);
    return 0;
}


static int
mdbm_limit_size_new_common(MDBM* db,
                           mdbm_ubig_t pages,
                           mdbm_shake_func_v3 shake,
                           void *data)
{
    int dir_shift;
    int npages;
    int want_pages;

    for (dir_shift = 0, npages = 1; 2*npages <= pages; dir_shift++, npages <<= 1);
    want_pages = pages;
    pages += MDBM_NUM_DIR_PAGES(db->db_pagesize,dir_shift);

    if (lock_db(db) < 0) {
        return -1;
    }
    if (db->db_hdr->h_num_pages > pages) {
        unlock_db(db);
        mdbm_log(LOG_ERR,
                 "%s: existing number of pages is greater than new limit"
                 ", existing pages=%d"
                 ", new limit pages=%d",
                 db->db_filename, db->db_hdr->h_num_pages, pages);
        errno = EINVAL;
        return -1;
    }

    db->db_hdr->h_max_pages = pages;
    unlock_db(db);
    db->db_flags = (db->db_flags & ~MDBM_DBFLAG_SHAKE_MASK) | MDBM_DBFLAG_SHAKE_V2;
    db->db_shake_func = shake;
    db->db_shake_data = data;

    if (db->db_spillsize
        && (!db->db_max_dir_shift || db->db_max_dir_shift >= dir_shift)
        && (want_pages == npages || want_pages - npages < want_pages/20))
    {
        mdbm_log(LOG_WARNING,
                 "WARNING: %s: size limit leaves %s%dK available for large-object storage",
                 db->db_filename,
                 (want_pages > npages) ? "just " : "",
                 (want_pages - npages) * db->db_pagesize / 1024);
    }

    return 0;
}


/* FIX: There shouldn't be version-specific mdbm_limit_size functions.  */
int
mdbm_limit_size_v3 (MDBM* db, mdbm_ubig_t pages,
                    mdbm_shake_func_v3 shake,
                    void *data)
{
    if (db->db_ver_flag != MDBM_DBVER_3) {
        mdbm_log(LOG_ERR, "%s: mdbm_limit_size_v3 may only be used with v3 mdbms.",
                 db->db_filename);
        errno = EINVAL;
        return -1;
    }

    /*if (mdbm_limit_size_new_common(db,pages,(mdbm_shake_func)shake,data) < 0) { */
    if (mdbm_limit_size_new_common(db,pages,shake,data) < 0) {
        return -1;
    }
    db->db_flags = (db->db_flags & ~MDBM_DBFLAG_SHAKE_MASK) | MDBM_DBFLAG_SHAKE_V3;
    return 0;
}

int
mdbm_limit_dir_size(MDBM *db, int pages)
{
    int dir_shift;
    int npages;

    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = EINVAL;
        return -1;
    }

    if (pages<=0 || pages>MDBM_NUMPAGES_MAX) {
        errno = EINVAL;
        return -1;
    }

    for (dir_shift = 0, npages = 1; 2*npages <= pages; dir_shift++, npages <<= 1);

    if (lock_db_ex(db) < 0) {
        return -1;
    }

    if (db->db_hdr->h_dir_shift > dir_shift) {
        unlock_db(db);
        errno = EINVAL;
        return -1;
    }

    db->db_max_dir_shift = db->db_hdr->h_max_dir_shift = dir_shift;
    /* XXX should this be calling sync_dir() ? */

    unlock_db(db);
    return 0;
}

void dump_mdbm_header(MDBM* db) {
    fprintf(stderr ,"db_filename     %s\n",           db->db_filename             );
    fprintf(stderr ,"h_magic       0x%x\n", (unsigned)db->db_hdr->h_magic         );
    fprintf(stderr ,"h_dbflags       %u\n", (unsigned)db->db_hdr->h_dbflags       );
    fprintf(stderr ,"h_cache_mode    %u\n", (unsigned)db->db_hdr->h_cache_mode    );
    fprintf(stderr ,"h_pad1          %u\n", (unsigned)db->db_hdr->h_pad1          );
    fprintf(stderr ,"h_dir_shift     %u\n", (unsigned)db->db_hdr->h_dir_shift     );
    fprintf(stderr ,"h_hash_func     %u\n", (unsigned)db->db_hdr->h_hash_func     );
    fprintf(stderr ,"h_max_dir_shift %u\n", (unsigned)db->db_hdr->h_max_dir_shift );
    fprintf(stderr ,"h_dir_gen       %u\n", (unsigned)db->db_hdr->h_dir_gen       );
    fprintf(stderr ,"h_pagesize      %u\n", (unsigned)db->db_hdr->h_pagesize      );
    fprintf(stderr ,"h_num_pages     %u  ", (unsigned)db->db_hdr->h_num_pages     );
    fprintf(stderr ,"db_num_pages    %u\n", (unsigned)db->db_num_pages            );
    fprintf(stderr ,"h_max_pages     %u\n", (unsigned)db->db_hdr->h_max_pages     );
    fprintf(stderr ,"h_spill_size    %u\n", (unsigned)db->db_hdr->h_spill_size    );
    fprintf(stderr ,"h_last_chunk    %u\n", (unsigned)db->db_hdr->h_last_chunk    );
    fprintf(stderr ,"h_first_free    %u\n", (unsigned)db->db_hdr->h_first_free    );
    /*uint32_t            h_pad4[8];       // unused padding (future expansion) */
    /*mdbm_hdr_stats_t    h_stats;         // store/fetch/delete statistics */
}

/* Debugging function. Walks from chunk to chunk, dumping info about each one. */
void dump_chunk_headers(MDBM* db) {
  int pg = 0;
  fprintf(stderr, "DUMPING CHUNK HEADERS, pages:%d|%d first_free:%d last_chunk:%d...\n",
      db->db_num_pages, db->db_hdr->h_num_pages, db->db_hdr->h_first_free,
      db->db_hdr->h_last_chunk);
  /* while (pg < db->db_num_pages) { */
  while (pg < db->db_hdr->h_num_pages) {
    mdbm_page_t* p = MDBM_PAGE_PTR(db, pg);
    const char* typ = "????";
    switch (p->p_type) {
      case MDBM_PTYPE_FREE: typ = "FREE"; break;
      case MDBM_PTYPE_DIR : typ = "DIR "; break;
      case MDBM_PTYPE_DATA: typ = "DATA"; break;
      case MDBM_PTYPE_LOB : typ = "LOB "; break;
      default: break;
    }

    fprintf(stderr, "  Page:%4.4d (%p) type:%s sz:%d, prev_sz:%d p.*:%u \n", pg, (void*)p,
        typ, p->p_num_pages, p->p_prev_num_pages,  (unsigned)p->p.p_data);

    pg += p->p_num_pages;
    if (p->p_num_pages < 1) {
      fprintf(stderr, " INVALID PAGE COUNT, BREAKING...\n");
      break;
    }
  }
}

/* De-fragments a db, by moving DATA/LOB chunks down, and merging FREE chunks upward. */
static int
compact_db(MDBM* db) {
    int ret = 0;
    int merge = 0; /* Could there be adjacent free-pages to merge from previous iteration. */
    int compacted = 0; /* Has free space been created (to truncate) at the end of the DB? */
    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
      mdbm_log(LOG_WARNING,"%s: compact_db() doesn't support MEMORY-ONLY",
          db->db_filename);
      return -1;
    }
    if (MDBM_IS_WINDOWED(db)) {
      mdbm_log(LOG_WARNING,"%s: compact_db() doesn't support WINDOWED MODE",
          db->db_filename);
      return -1;
    }

    mdbm_lock(db);

    /* compact mdbm by moving occupied pages down (low VMA), so the free ones 
     *   are highest (high VMA) (patching the page_table as we go).
     *   Note: LOB/OVERSIZE chunks must move as a contiguous block. */
    MDBM_SIG_DEFER;
    while (1) {
      /* free list should be sorted, lowest page first, based on free_chunk() */
      uint32_t cur = db->db_hdr->h_first_free, next = 0, nextnext = 0;
      mdbm_page_t *curp = NULL, *nextp = NULL, *nextnextp = NULL;
      uint32_t cur_pages = 0;

      if (!cur) { /* no free pages */
        break;
      }

      curp = MDBM_PAGE_PTR(db, cur);
      cur_pages = curp->p_num_pages;
      if (cur+cur_pages >= db->db_num_pages) { /* free_list completely compacted */
        compacted = 1;
        break;
      }
      /* NOTE: this is simple and fairly safe, but potentially slow.
       * It bubbles down one DATA/LOB page at a time. i.e.
       *   |--page A --|-- free --|--page B   --|-- free --|
       * becomes
       *   |--page A --|--page B   --|-- free  ...  ...  --|
       * At each step:
       *   an occupied page is copied downward, 
       *   page headers are patched (to maintain the implicit linked list)
       *   the page-table entry is updated
       *   adjacent free-pages are coalesced
       *
       * Moving the highest DATA/LOB page down might be faster,
       * but breaks when things don't fit in the available gap.
       * Also, updating page-headers, etc for all affected entries is a mess. 
       */
      next = cur + cur_pages;
      nextp = MDBM_PAGE_PTR(db, next);

      if (nextp->p_type == MDBM_PTYPE_FREE) {
        /* fprintf(stderr, "MERGING %d and %d\n", cur, next); */
        /* if we've done some compaction, try to merge any adjacent free pages */
        if (next > db->db_hdr->h_last_chunk && 0 == nextp->p_num_pages) {
          /* FIXME MDBM seems to often leave a "phantom" page at the end of the db.
           * It's a zero-length, free page, which is not on the free list.
           * Ignore it for now, but we should try to understand why it's present later. */
          compacted = 1;
          break;
        }
        if (!merge) {
          /* adjacent free pages should always be coalesced */
          mdbm_logerror(LOG_ERR, 0, "compact_db(%s) unexpected unmerged page on the freelist"
              "pagenum=%u page=%p\n", db->db_filename, next, (void*)nextp);
          errno = EINVAL;
          ret = -1;
          break;
        }
        /* merge-able */
        /* fprintf(stderr, " old-size:%d (+=%d) \n", curp->p_num_pages, nextp->p_num_pages); */
        curp->p_num_pages += nextp->p_num_pages;
        /* fprintf(stderr, " new-size:%d \n", curp->p_num_pages); */
        curp->p.p_next_free = nextp->p.p_next_free;
        /* curp->p_prev_num_pages shouldn't change, but the *new* next chunk one should */
        nextnext = next + nextp->p_num_pages;
        if (nextnext >= db->db_num_pages) {
          compacted = 1;
          break; /* last free chunk... we're done */
        }
        nextnextp = MDBM_PAGE_PTR(db, nextnext);
        nextnextp->p_prev_num_pages = curp->p_num_pages;
        merge = 0;
      } else if ((nextp->p_type == MDBM_PTYPE_DATA) || (nextp->p_type == MDBM_PTYPE_LOB)) {
        int is_lob = (nextp->p_type == MDBM_PTYPE_LOB) ? 1 : 0;
        uint32_t next_count = nextp->p_num_pages;
        uint32_t next_sz = db->db_pagesize * next_count;
        nextnext = next + next_count;
        nextnextp = NULL;
        if (nextnext < db->db_num_pages) {
          nextnextp = MDBM_PAGE_PTR(db, nextnext);
        }
        /* fprintf(stderr, " SWAP(%d): nextnext:%d size:%d  pages(db:%d h:%d)\n", __LINE__,
            nextnext, nextnextp?nextnextp->p_num_pages:-1, db->db_num_pages,
            db->db_hdr->h_num_pages);
        mdbm_check(db, 3, 3); */
        {
          /* stash old-page header to patch freelist */
          mdbm_page_t old_free_h = *curp;
          mdbm_page_t old_data_h = *nextp;
          /* DB page index of the resulting free page (cur and next may be different sizes) */
          uint32_t nu = cur +  next_count; /* the "new" page location */
          mdbm_page_t* nup  = MDBM_PAGE_PTR(db, nu);
          /* fprintf(stderr, "SWAPPING %d (sz:%d) and %d (sx:%d), nu=%d next_free:%d\n",
               cur, cur_pages, next, next_count, nu, curp->p.p_next_free);
             fprintf(stderr, " SWAP: nextnext:%d size:%d \n",
               nextnext, (nextnextp ? nextnextp->p_num_pages : -1)); */
          /* copy it down to lowest free page */
          memmove((void*)curp, (void*)nextp, next_sz);
          /* recreate free header at new location */
          *nup = old_free_h;
          nup->p_num = nu; /* gratuitous */
          /* patch freelist (we're working at the front) */
          if (db->db_hdr->h_first_free == cur) {
            db->db_hdr->h_first_free = nu;
          }
          /* update the backwards chain */;
          curp->p_prev_num_pages = old_free_h.p_prev_num_pages;
          nup->p_prev_num_pages = old_data_h.p_num_pages;

          /* fprintf(stderr, "SWAPPED cur=%d (sz:%d) and nu=%d (sz:%d)\n", */
          /*    cur, curp->p_num_pages, nu, nup->p_num_pages);*/
          /* update backwards chain for the chunk after cur and next */
          if (nextnextp && nextnextp->p_num_pages != 0) {
            /* fprintf(stderr," SWAP: nextnext:%d size:%d (nu->next_free:%d nxnx->next_free:%d)\n",
                nextnext, nextnextp->p_num_pages, nup->p.p_next_free, nextnextp->p.p_next_free); */
            nextnextp->p_prev_num_pages = old_free_h.p_num_pages;
          }

          if (is_lob) {
            /* LOB_page->p_num should be the DATA page index that contains the key */
            fixup_lob_pointer(db, old_data_h.p_num, next, cur);
          } else {
            /* patch page-table */
            MDBM_SET_PAGE_INDEX(db, old_data_h.p_num, cur);
          }
          if (db->db_hdr->h_last_chunk == next) {
            /* db->db_hdr->h_last_chunk = cur; */
            db->db_hdr->h_last_chunk = nu;
          }
          merge = 1;
        }
      } else if (nextp->p_type == MDBM_PTYPE_DIR) {
        /* broken */
        mdbm_logerror(LOG_ERR,0, "compact_db(%s) encountered a dir-page on the freelist"
            "pagenum=%u page=%p\n", db->db_filename, next, (void*)nextp);
        errno = EINVAL;
        ret = -1;
        break;
      } else {
        /* horribly broken */
        mdbm_logerror(LOG_ERR,0, "compact_db(%s) encountered an unknown-page on the freelist"
            "pagenum=%u page=%p type=%d\n", db->db_filename, next, (void*)nextp, nextp->p_type);
        errno = EINVAL;
        ret = -1;
        break;
      }
    }
    MDBM_SIG_ACCEPT;

    if (compacted && db->db_hdr->h_first_free && !ret) {
      /* NOTE: we could verify that there is only one free-list chunk */
      uint32_t last_page = db->db_hdr->h_first_free;
      uint32_t last_unfree = 0;
      int syspagesz = db->db_sys_pagesize;
      size_t sys_pages = MDBM_NUM_PAGES_ROUNDED(syspagesz, last_page*db->db_pagesize);
      size_t compact_size = sys_pages * syspagesz;
      mdbm_page_t *freep = MDBM_PAGE_PTR(db, last_page);

      last_unfree = last_page - freep->p_prev_num_pages;
      /* truncate and munmap trailing pages, */
      if (ftruncate(db->db_fd,compact_size) < 0) {
          mdbm_logerror(LOG_ERR,0,"%s: ftruncate failure in compact_db()",db->db_filename);
          ret = -1;
      } else {
        /* fprintf(stderr, "compact munmapping(%p, %d) compact_size:%d last_page:%d "
            "sys_pages:%d old_len:%d\n",
            db->db_base+compact_size, db->db_base_len-compact_size, compact_size, last_page,
            sys_pages, db->db_base_len); */
        if (munmap(db->db_base+compact_size,db->db_base_len-compact_size) < 0) {
          mdbm_logerror(LOG_ERR,0,"%s: munmap() base:%p, len:%llu new:%llu "
              "failure in compact_db()",
              db->db_filename, db->db_base,(unsigned long long)db->db_base_len,
              (unsigned long long)compact_size);
          ret = -1;
          /* fall thru to do damage control.. we've already truncated the file */
        }
        /* truncated and unmapped. update db, db->db_hdr,  */
        db->db_num_pages = db->db_hdr->h_num_pages = last_page;
        db->db_base_len = compact_size;
        db->db_hdr->h_first_free = 0;
        db->db_hdr->h_last_chunk = last_unfree;
        /* sync_dir(), is there a better way to notify other users to adjust their map? */
        db->db_hdr->h_dir_gen++;
        sync_dir(db,NULL);
      }
    }

    mdbm_unlock(db);
    return ret;
}

/* Copies an existing entry to a new page.
 * 'page' is the destination page. 
 * 'key' and 'val' are the key and value of the entry to be copied.
 * 'hashval' is it's pre-calculated hash-value.
 * 'entry' is a pointer to the pre-existing entry on the old page.
 * 'oldpage' is the page where the entry already exists.
 * On entry, the DB should be locked, and signals deferred. 
 */
int
copy_page_entry(MDBM *db, mdbm_page_t* page, mdbm_hashval_t hashval, const datum* key, const datum* val, mdbm_entry_t* entry, mdbm_page_t* oldpage)
{
    int ksize, vsize, kvsize, esize;
    mdbm_entry_t* freep = NULL;
    int free_index = -1;
    char* v;
    if (page->p_type != MDBM_PTYPE_DATA) {
      mdbm_log(LOG_ERR, "%s copy_page_entry() FAIL bad page-type %d (hash:%d)\n",
          db->db_filename, page->p_type, hashval);
      /* ensure data pagetype */
      errno=EINVAL;
      return -1;
    }
    if (page->p_num_pages != 1) {
      mdbm_log(LOG_ERR, "%s copy_page_entry() FAIL oversize page %d sub-pages (hash:%d)\n",
          db->db_filename, page->p_num_pages, hashval);
      /* punt for oversize pages */
      errno=EINVAL;
      return -1;
    }

    /* check fit, include alignment bytes */
    ksize = MDBM_ALIGN_LEN(db,key->dsize);
    vsize = MDBM_ALIGN_LEN(db,val->dsize);
    if (MDBM_DB_CACHEMODE(db)) {
        vsize += MDBM_CACHE_ENTRY_T_SIZE;
    }
    kvsize = ksize + vsize;
    esize = kvsize + MDBM_ENTRY_T_SIZE;
    if (MDBM_ENTRY_LARGEOBJ(entry)) {
      int delta = sizeof(mdbm_entry_lob_t) - vsize;
      /* mdbm_entry_lob_t *lob_ent = MDBM_LOB_PTR1(db, oldpage, entry);
         fprintf(stderr, "copy_page_entry() (page %d->%d) copying LOB key.size:%d, lob_page:%d\n",
            oldpage->p_num, page->p_num, val->dsize, lob_ent->l_pagenum); */
      /* adjust size, actual LOB data goes into another chunk of pages */
      esize += delta;
      kvsize += delta;
    }
    if (esize > MDBM_PAGE_FREE_BYTES(page)) {
      mdbm_log(LOG_ERR, "%s copy_page_entry() FAIL insufficient space (%d vs %d) (hash:%d)\n",
          db->db_filename, esize, MDBM_PAGE_FREE_BYTES(page), hashval);
      errno=ENOMEM;
      return -1;
    }

    free_index = page->p.p_num_entries++;
    freep = MDBM_ENTRY(page,free_index);
    MDBM_INIT_TOP_ENTRY(freep+1,freep[0].e_offset - kvsize);

    freep->e_offset -= MDBM_ALIGN_LEN(db,key->dsize);
    freep->e_flags = 0;
    freep->e_key.key.len = key->dsize;
    freep->e_key.key.hash = hashval;

    /* fprintf(stderr, "copy_page_entry() adding at index:%d (hash:%d (%d:%d), size:%d)\n", free_index, freep->e_key.key.hash, hashval, hashval>>16, freep->e_key.key.len); */

    memcpy(MDBM_KEY_PTR1(page,freep),key->dptr,key->dsize);
    MDBM_SET_PAD_BYTES(freep,MDBM_ALIGN_PAD_BYTES(db,val->dsize));
    v = MDBM_VAL_PTR1(db,page,freep);
    if (MDBM_ENTRY_LARGEOBJ(entry)) {
      mdbm_entry_lob_t *newlob, *oldlob;
      mdbm_page_t *lob_chunk;
      /* set flags and handle value specially */
      freep->e_flags = entry->e_flags;
      newlob = MDBM_LOB_PTR1(db, page, freep);
      oldlob = MDBM_LOB_PTR1(db, oldpage, entry);
      *newlob = *oldlob;
      lob_chunk = MDBM_PAGE_PTR(db, oldlob->l_pagenum);
      /* patch data page index on lob chunk starting page */
      lob_chunk->p_num = page->p_num;
    } else if (MDBM_DB_CACHEMODE(db)) {
      memcpy(v,val->dptr-MDBM_CACHE_ENTRY_T_SIZE,vsize);
    } else {
      memcpy(v,val->dptr,val->dsize);
    }
    return 0;
}


/* compress_tree data structure for merging pages (source entries copied to destination) */
typedef struct mdbm_merge_data {
  MDBM* db;               /* The database being iterated over */
  mdbm_page_t* src_page;  /* Pointer to the beginning of the source page */
  mdbm_page_t* dst_page;  /* Pointer to the beginning of the destination page */
  int src_index;          /* Index of the source page */
  int dst_index;          /* Index of the source page */
  int retcode;            /* return value, 0 for success */
} mdbm_merge_data_t;

static int
merge_page_iter (void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
  int ret;
  mdbm_merge_data_t* data = (mdbm_merge_data_t*) user;
  mdbm_entry_t* entry = MDBM_ENTRY(data->src_page, info->i_entry.entry_index);

  /* MDBM* db = data->db;
     mdbm_hashval_t h = MDBM_HASH_VALUE(db,kv->key.dptr,kv->key.dsize);
     int pagenum = hashval_to_pagenum(db, h);
     int realpage = MDBM_GET_PAGE_INDEX(db, pagenum);
     fprintf(stderr, "merge_page_iter() moving key [%s] hash:%d(%d) from %di to %di "
     "(expected %di->%dp) \n", kv->key.dptr, entry->e_key.key.hash, (int)h,
     data->src_index, data->dst_index, pagenum, realpage); */
  /* assert(realpage == data->dst_index) */

  ret = copy_page_entry(data->db, data->dst_page, entry->e_key.key.hash, &kv->key, &kv->val,
      entry, data->src_page);
  if (ret) {
    data->retcode = ret;
  }
  return ret;
}

/* Internal function for compress_tree. 
 * Joins two DATA pages into a single page, if possible, by coping entry-by-entry
 * from the page at src_index to the page at dst_index.
 * If the merge fails, we unroll by restoring the old entry-count on the destination page.
 * NOTE: On entry signals should be deferred, the DB locked, and the destination
 * page should be un-fragmented. 
 */
int
merge_page(MDBM *db, int dest_index, int src_index)
{
    mdbm_page_t* srcpg;
    mdbm_page_t* dstpg;
    mdbm_merge_data_t data  = {NULL, NULL, NULL, -1, -1, 0};
    int ret = 0;
    int need, avail;

    srcpg = MDBM_PAGE_PTR(db, MDBM_GET_PAGE_INDEX(db,src_index));
    dstpg = MDBM_PAGE_PTR(db, MDBM_GET_PAGE_INDEX(db,dest_index));
    if (!srcpg) {
        mdbm_log(LOG_ERR, "%s: compress_tree() merge_page srcpg is NULL, src:%d dst:%d\n",
            db->db_filename, src_index, dest_index);
        /* fprintf(stderr, "compress_tree() merge_page FAILED, bad src (%d,%d)\n",
           src_index, dest_index); */
        errno = ENOMEM;
        return -1;
    }
    if (!dstpg) {
        mdbm_log(LOG_ERR, "%s: compress_tree() merge_page dstpg is NULL, src:%d dst:%d\n",
            db->db_filename, src_index, dest_index);
        /* fprintf(stderr, "compress_tree() merge_page FAILED, bad dst (%d,%d)\n",
           src_index, dest_index); */
        errno = ENOMEM;
        return -1;
    }

    if (srcpg != dstpg) { /* ensure the pages are actually different */
      /* check for fit first */
      avail = MDBM_PAGE_FREE_BYTES(dstpg);
      need = db->db_pagesize - MDBM_PAGE_FREE_BYTES(srcpg);
      if (need <= avail) {
        uint32_t old_entry_count = srcpg->p.p_num_entries;
        data.db=db;
        data.src_page=srcpg;
        data.dst_page=dstpg;
        data.src_index=src_index;
        data.dst_index=dest_index;
        mdbm_iterate(db,src_index,merge_page_iter,MDBM_ITERATE_ENTRIES,&data);
        ret = data.retcode;
        if (ret) {
          /* "delete" the newly added entries. */
          srcpg->p.p_num_entries = old_entry_count;
          mdbm_log(LOG_ERR, "%s: compress_tree() merge_page action FAILED (%d,%d)\n",
              db->db_filename, src_index, dest_index);
        }

      } else {
        mdbm_log(LOG_ERR, "%s: compress_tree() merge_page FAILED, "
            "insufficient space:%d,%d  pages:%d,%d\n",
            db->db_filename, need, avail, src_index, dest_index);
        ret = -1;
      }
    } else {
      /* left and right is the same page, log error */
      mdbm_log(LOG_ERR, "%s: compress_tree() merge_page src_pg == dst_pg (%d,%d)\n",
          db->db_filename, src_index, dest_index);
    }

    return ret;
}


/* 
 * This routine attempts to recursively fold DATA pages from the current size to the
 * next lower power-of-two size.
 * At each step, it pre-calculates whether the folds will succeed in an attempt
 * to avoid needing to unroll changes.
 * If we succeed in folding, then we attempt to de-fragment the resulting 
 * free-pages via compact_db().
 * */
int
merge_all_pages(MDBM *db)
{
    int bits = db->db_max_dirbit;
    int lvl, left, right, lp, rp;
    /*int rh, lh;*/
    int pages, halfpages;
    int cur, ret = 0;
    int do_sync = 0;
    int do_compact = 0;
    int can_fold = 0;

    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
      mdbm_log(LOG_WARNING,"%s: merge_all_pages() doesn't support MEMORY-ONLY",
          db->db_filename);
      return -1;
    }
    if (MDBM_IS_WINDOWED(db)) {
      mdbm_log(LOG_WARNING,"%s: merge_all_pages() doesn't support WINDOWED MODE",
          db->db_filename);
      return -1;
    }

    mdbm_lock(db);
    for (lvl=1; lvl<=((bits)>>1); lvl<<=1) {}
    /* for (lvl = bits; (lvl>1) && (ret==0); lvl>>=1) { */
    for (; (lvl>1) && (ret==0); lvl>>=1) {
        /* Try to iteratively fold down number of pages by powers of 2. */
        pages = lvl<<1;
        halfpages = pages/2;
        /* fprintf(stderr, "compress_tree() level:%d (bits:%d) "
            "pages:%d half:%d num_pages:%d max_dirbit:%d\n",
            lvl, bits, pages, halfpages, db->db_num_pages, db->db_max_dirbit); */
        /* verify fit for all pages on this level */
        can_fold = 1;
        for (left=0; left<halfpages; ++left) {
            mdbm_page_t *srcpg, *dstpg;
            right = left + halfpages;
            lp = MDBM_GET_PAGE_INDEX(db,left);
            rp = MDBM_GET_PAGE_INDEX(db,right);
            srcpg = MDBM_PAGE_PTR(db,rp);
            dstpg = MDBM_PAGE_PTR(db,lp);
            if (srcpg && dstpg) {
              if (srcpg->p_num_pages != 1 || dstpg->p_num_pages != 1 ) {
                /* punt for any oversize pages */
                can_fold = 0;
                break;
              } else {
                /* pack all entries on the pages first */
                wring_page(db, srcpg);
                wring_page(db, dstpg);
                {
                  int avail = MDBM_PAGE_FREE_BYTES(dstpg);
                  int need = db->db_pagesize - MDBM_PAGE_FREE_BYTES(srcpg);
                  if (need >= avail || need<0 || avail<0) {
                    /* fprintf(stderr, "compress_tree() CAN'T FOLD (space %d vs %d) "
                        "level:%d (bits:%d) pages:%d half:%d num_pages:%d max_dirbit:%d\n",
                        need, avail,
                        lvl, bits, pages, halfpages, db->db_num_pages, db->db_max_dirbit); */
                    can_fold = 0;
                    break;
                  }
                }
              }
            } else {
              can_fold = 0;
              break;
            }
        }
        if (!can_fold) {
          break;
        }
        for (left=0; left<halfpages; ++left) {
            right = left + halfpages;
            /* skip if not split */
            /* lh = hashval_to_pagenum(db, left); rh = hashval_to_pagenum(db, right); */
            lp = MDBM_GET_PAGE_INDEX(db,left);
            rp = MDBM_GET_PAGE_INDEX(db,right);
            /* fprintf(stderr, "compress_tree() attempting to merge %d+%d (lp:%d, rp:%d)\n",
               left, right, lp, rp); */
            if (lp == rp) {  /* they map to the same physical page already */
              /* fprintf(stderr, "compress_tree() MERGE SKIPPED %d+%d (lp:%d, rp:%d)\n",
                 left, right, lp, rp); */
              continue;
            }
            if (rp >= db->db_num_pages || right>db->db_max_dirbit) { /* not really in use */
              /* fprintf(stderr, "compress_tree() MERGE SKIPPED2 %d+%d (lp:%d, rp:%d) "
                "num_pages:%d max_dirbit:%d\n", left, right, lp, rp,
                db->db_num_pages, db->db_max_dirbit); */

              continue;
            }
            /* Don't allow signals to interrupt mid merge. */
            MDBM_SIG_DEFER;
            cur = merge_page(db, left, right);
            if (cur) {
              mdbm_log(LOG_ERR, "compress_tree() MERGE FAILED pages:%d+%d (lp:%d, rp:%d)\n",
                  left, right, lp, rp);
              ret = -1;
            } else {
               /* fprintf(stderr, "compress_tree() MERGE SUCCEEDED %d+%d (lp:%d, rp:%d)\n",
                  left, right, lp, rp); */
              do_sync = 1;
              MDBM_SET_PAGE_INDEX(db, right, lp);
              /* clear dirbits, so we don't have to re-split if resize() fails */
              MDBM_CLEAR_DIR_BIT(db, right);
              db->db_dir_flags &= ~MDBM_HFLAG_PERFECT;
              /* page is merged out, release it. */
              free_chunk(db, rp, NULL);
              assert(MDBM_DIR_BIT(db, left));
            }
            MDBM_SIG_ACCEPT;
        }
        if (ret) {
          mdbm_log(LOG_ERR, "%s compress_tree() LEVEL %d FAILED \n", db->db_filename, lvl);
          break;
        } else {
          int new_ptsize;
          mdbm_ptentry_t* old_ptable = MDBM_PTABLE_PTR(MDBM_DIR_PTR(db),db->db_dir_shift);
          MDBM_SIG_DEFER;
          /* fprintf(stderr, "compress_tree() LEVEL %d SUCCEEDED \n", lvl); */
          /* DROP one directory level */
          do_compact = 1;
          db->db_dir_flags |= MDBM_HFLAG_PERFECT;

          /* adjust handle */
          db->db_dir_shift -= 1;
          /*db->db_max_dir_shift */
          db->db_max_dirbit >>= 1;

          /* adjust header */
          db->db_hdr->h_dir_shift -= 1;
          /*db->db_hdr->h_max_dir_shift */

          /* copy down page-table */
          new_ptsize = MDBM_PTABLE_SIZE(db->db_dir_shift);
          db->db_ptable = MDBM_PTABLE_PTR(MDBM_DIR_PTR(db),db->db_dir_shift);
          memmove(db->db_ptable, old_ptable, new_ptsize);

          ++db->db_hdr->h_dir_gen;
          sync_dir(db, db->db_hdr);

          /* NOTE: could free trailing dir_page(s) if we're using less */
          MDBM_SIG_ACCEPT;
        }
    }

    if (do_sync) {
      ++db->db_hdr->h_dir_gen;
      sync_dir(db,NULL);
    }

    if (do_compact) {
      int rret=0;
      /* fprintf(stderr, "DOING COMPACTION\n");*/
      /* NOTE: we don't compact the directory or pagetable.
       * Ideally, " resize(db, lvl-1, halfpages); "
       * would do this, but it doesn't work for shrinking.
       */
      rret = compact_db(db);
      if (rret) {
        mdbm_logerror(LOG_ERR,0, "%s: compact_db() failed", db->db_filename);
        errno = ENOMEM;
        ret = -1;
      }
    } else {
      /* fprintf(stderr, "SKIPPING COMPACTION\n");*/
    }


    CHECK_DB(db);
    mdbm_unlock(db);
    return ret;
}



void
mdbm_compress_tree(MDBM *db)
{
    /* XXX
    mdbm_log(LOG_CRIT, "%s: unimplemented mdbm_compress_tree",
             db->db_filename);
    abort();
    */
    int ret = merge_all_pages(db);
    if (ret) {
      mdbm_logerror(LOG_ERR,0, "%s: mdbm_compress_tree failed: merge_all_pages() returned %d\n", db->db_filename, ret);
    }

}

void
mdbm_stat_header(MDBM *db)
{
    /* XXX */
    mdbm_log(LOG_CRIT, "%s: unimplemented mdbm_stat_header",
             db->db_filename);
    abort();
}

void
mdbm_stat_all_page(MDBM *db)
{
    /* XXX */
    mdbm_log(LOG_CRIT, "%s: unimplemented mdbm_stat_all_page",
             db->db_filename);
    abort();
}

void
mdbm_chk_error(MDBM* db, int pagenum, int mapped_pagenum, int index)
{
    /* XXX */
}

int
mdbm_chk_page(MDBM *db, int pno)
{
    if (!db || pno > db->db_max_dirbit) {
      errno = EINVAL;
      return -1;
    }

    if (lock_db_ex(db) != 1) {
        return -1;
    }
    if (check_db_page(db,pno,1,3) > 0) {
        unlock_db(db);
        errno = EFAULT;
        return -1;
    }
    unlock_db(db);
    return 0;
}

int
mdbm_chk_all_page(MDBM *db)
{
    int i;
    int nerr = 0;

    if (lock_db_ex(db) < 0) {
        return -1;
    }
    for (i = 0; i <= db->db_max_dirbit; i++) {
        if (check_db_page(db,i,1,3) > 0) {
            nerr++;
        }
    }
    unlock_db(db);
    if (nerr) {
        errno = EFAULT;
        return -1;
    }
    return 0;
}

mdbm_ubig_t
mdbm_get_page(MDBM* db, const datum* key)
{
    mdbm_hashval_t hashval;
    mdbm_pagenum_t pagenum;

    if (!db || !key || (key->dsize == 0) || (key->dptr == NULL)) {
      return -1;
    }

    hashval = hash_value(db,key);
    if (mdbm_internal_do_lock(db,MDBM_LOCK_READ,MDBM_LOCK_WAIT,key,&hashval,&pagenum) < 0) {
        return -1;
    }
    mdbm_internal_do_unlock(db,key);
    return pagenum;
}

int
mdbm_sync(MDBM *db)
{
    if (!db) {
        errno = EINVAL;
        return -1;
    }

    if (MDBM_IS_RDONLY(db)) {
        return 0;
    } else {
      int ret = msync(db->db_base,MDBM_DB_MAP_SIZE(db),MS_ASYNC);
      if (MDBM_DO_STAT_PAGE(db)) {
            MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_SYNC);
      }

      return ret;
    }
}

int
mdbm_fsync(MDBM *db)
{
    int ret, e;

    if (!db) {
        errno = EINVAL;
        return -1;
    }

    if (MDBM_IS_RDONLY(db) || (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE)) {
        return 0;
    }

    if (lock_db(db) < 0) {
        return -1;
    }
    ret = fsync(db->db_fd);
    if (MDBM_DO_STAT_PAGE(db)) {
          MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_SYNC);
    }
    e = errno;
    unlock_db(db);
    if (ret < 0) {
        errno = e;
    }
    return ret;
}

void
mdbm_close_fd(MDBM *db)
{
    if (db->db_fd >= 0) {
        close(db->db_fd);
        db->db_fd = -1;
    }
}

void
mdbm_truncate(MDBM *db)
{
    const char *TRUNC_WARN_MSG = 
        "mdbm_truncate will result in the loss of the setting.\n"
        "If you'd like to retain this setting, use mdbm_purge() !";

    if (lock_db(db) < 0) {
        return;
    }

    errno = 0;   /* Reset errno so it won't show while printing the following warnings */

    /* Look at header to generate warn msgs, since you can mdbm_open() w/o LARGE_OBJ flag */
    if (db->db_hdr->h_dbflags & MDBM_HFLAG_LARGEOBJ) {
        mdbm_logerror(LOG_ERR,0, "%s: Large Object Support will no longer be set: %s",
                      db->db_filename, TRUNC_WARN_MSG);
    }

    if (db->db_hdr->h_spill_size) {  /* Set spillsize indicates likely large object support */
        mdbm_logerror(LOG_ERR,0, "%s: Spill Size will no longer be set to %d: %s",
                      db->db_filename, db->db_hdr->h_spill_size, TRUNC_WARN_MSG);
    }

    if (db->db_hdr->h_hash_func != MDBM_DEFAULT_HASH) {
        uint8_t hash = db->db_hdr->h_hash_func;
        mdbm_logerror(LOG_ERR,0, "%s: Hashing method will no longer be %s: %s",
                      db->db_filename,
                      (hash < MDBM_HASH_MAX) ? MDBM_HASH_FUNCNAMES[hash] : "?",
                      TRUNC_WARN_MSG);
    }

    if (db->db_hdr->h_max_pages) {
        mdbm_logerror(LOG_ERR,0, "%s: MDBM Size Limit will no longer be set to %u: %s",
                      db->db_filename, db->db_hdr->h_max_pages, TRUNC_WARN_MSG);
    }

    if (truncate_db(db,1,db->db_pagesize,0) < 0) {
    }

    unlock_db(db);
}

int
mdbm_set_hash(MDBM* db, int hash)
{
    if (!db) {
        errno = EINVAL;
        return -1;
    }

    if (hash < 0 || hash > MDBM_MAX_HASH || !mdbm_hash_funcs[hash]) {
        errno = EINVAL;
        return -1;
    }
    if (lock_db(db) != 1) {
        return -1;
    }
    if (!check_empty(db)) {
        unlock_db(db);
        errno = EINVAL;
        return -1;
    }
    db->db_hdr->h_hash_func = (uint8_t)hash;
    db->db_hashfunc = mdbm_hash_funcs[hash];
    unlock_db(db);
    return 0;
}

int
mdbm_sethash(MDBM* db, int hash)
{
  int ret = mdbm_set_hash(db, hash);
  return (ret < 0) ? ret : 1;
}

int
mdbm_replace_db(MDBM* db, const char* newfile)
{
    MDBM* new_db = NULL;
    struct stat st;
    struct stat new_st;

    if (!db) {
        errno = EINVAL;
        return -1;
    }

    if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
        errno = EINVAL;
        return -1;
    }

    new_db = mdbm_open(newfile,MDBM_O_RDWR|MDBM_HEADER_ONLY|MDBM_ANY_LOCKS,0,0,0);
    if (!new_db) {
        goto replace_error;
    }

    if ((db->db_ver_flag == MDBM_DBVER_3) != (new_db->db_ver_flag == MDBM_DBVER_3)) {
        mdbm_logerror(LOG_ERR,0,
                      "%s: mdbm_replace_db(%s) are different db formats",
                      db->db_filename,newfile);
        goto replace_error;
    }

    if (fstat(db->db_fd,&st) == -1) {
        mdbm_logerror(LOG_ERR,0,
                      "%s: mdbm_replace_db unable to stat existing db file",
                      db->db_filename);
        goto replace_error;
    }

    if (fstat(new_db->db_fd,&new_st) == -1) {
        mdbm_logerror(LOG_ERR,0,
                      "%s: mdbm_replace_db unable to stat new db file",
                      newfile);
        goto replace_error;
    }

    if (st.st_dev != new_st.st_dev) {
      /* Must both be on the same device for rename() to work. */
        errno = EXDEV;
        mdbm_logerror(LOG_ERR,0,
                      "%s: mdbm_replace_db cannot use %s",
                      db->db_filename,newfile);
        goto replace_error;
    }

    if (mdbm_lock(db) != 1) {
        if (errno != EFAULT) {
            mdbm_logerror(LOG_ERR,0,
                          "%s: mdbm_replace_db unable to lock existing db",
                          db->db_filename);
            goto replace_error;
        }
    }

    if (mdbm_lock(new_db) != 1) {
        if (errno != EFAULT) {
            mdbm_logerror(LOG_ERR,0,
                          "%s: mdbm_replace_db unable to lock new db",
                          newfile);
            mdbm_unlock(db);
            goto replace_error;
        }
    }

    if (rename(newfile,db->db_filename) == -1) {
        mdbm_logerror(LOG_ERR,0,
                      "%s: mdbm_replace_db rename (from %s) failure",
                      newfile,db->db_filename);
        mdbm_unlock(new_db);
        mdbm_unlock(db);
        goto replace_error;
    }

    mdbm_unlock(new_db);
    mdbm_close(new_db);

    db->db_hdr->h_dbflags |= MDBM_HFLAG_REPLACED;
    mdbm_unlock(db);

    return 0;

 replace_error:
    if (new_db) {
        mdbm_close(new_db);
    }
    return -1;
}

int
get_version(const char *file, uint32_t *version)
{
    MDBM *db;
    if ((db = mdbm_open(file, MDBM_O_RDWR|MDBM_HEADER_ONLY, 0, 0, 0)) == NULL) {
        return -1;
    }

    *version = mdbm_get_version(db);
    mdbm_close(db);

    return 0;
}


int
mdbm_replace_file(const char* oldfile, const char* newfile)
{
    int ret;
    MDBM* db;

    if ((db = mdbm_open(oldfile,MDBM_O_RDWR|MDBM_HEADER_ONLY|MDBM_ANY_LOCKS,0,0,0)) == NULL) {
        return -1;
    }
    ret = mdbm_replace_db(db,newfile);
    mdbm_close(db);
    return ret;
}

int
mdbm_setspillsize(MDBM* db, int size)
{
    if (!db) {
        errno = EINVAL;
        return -1;
    }
    if (size <= 1) {
        mdbm_log(LOG_ERR, "%s: mdbm_setspillsize invalid size %u ", db->db_filename, size);
        errno = EINVAL;
        return -1;
    }
    if (!MDBM_LOB_ENABLED(db)) {
        mdbm_log(LOG_ERR, "%s: mdbm_setspillsize on db without large object support ", db->db_filename);
        errno = EINVAL;
        return -1;
    }
    if (lock_db(db) != 1) {
        return -1;
    }
    if (size > db->db_pagesize) {
        unlock_db(db);
        errno = EINVAL;
        return -1;
    }
    db->db_spillsize = db->db_hdr->h_spill_size = size;
    unlock_db(db);
    return 0;
}

void
mdbm_prune(MDBM* db, int(*prune) (MDBM* db, datum, datum, void *), void *param)
{
    int i;

    if (lock_db(db) < 0) {
        return;
    }
    for (i = 0; i <= db->db_max_dirbit; i++) {
        if (MDBM_GET_PAGE_INDEX(db,i)) {
            mdbm_page_t* page = MDBM_MAPPED_PAGE_PTR(db,MDBM_GET_PAGE_INDEX(db,i));
            mdbm_entry_t* ep;
            mdbm_entry_data_t e;

            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                if (ep->e_key.match) {
                    datum k, v;

                    get_kv(db,&e,&k,&v);
                    if (prune(db,k,v,param)) {
                        del_entry(db,page,ep);
                    }
                }
            }
            if (MDBM_IS_WINDOWED(db)) {
                release_window_page(db,page);
            }
        }
    }
    unlock_db(db);
}

void
mdbm_purge(MDBM* db)
{
    int i;

    if (lock_db(db) < 0) {
        return;
    }
    for (i = 0; i <= db->db_max_dirbit; i++) {
        if (MDBM_GET_PAGE_INDEX(db,i)) {
            mdbm_page_t* page = MDBM_PAGE_PTR(db,MDBM_GET_PAGE_INDEX(db,i));
            page->p.p_num_entries = 0;
            MDBM_INIT_TOP_ENTRY(MDBM_ENTRY(page,0),db->db_pagesize);
            if (MDBM_IS_WINDOWED(db)) {
                release_window_page(db,page);
            }
        }
    }
    if (db->db_spillsize) {
        free_large_object_chunks(db);
    }
    unlock_db(db);
}

int
mdbm_save(MDBM *db, const char *file, int flags, int mode, int compressionLevel)
{
    /* XXX */
    errno = ENOSYS;
    return -1;
}

int
mdbm_restore(MDBM *db, const char *file)
{
    /* XXX */
    errno = ENOSYS;
    return -1;
}

uint64_t
mdbm_get_size(MDBM *db)
{
    return MDBM_DB_SIZE(db);
}

uint64_t
mdbm_get_limit_size(MDBM *db)
{
    return ((uint64_t)db->db_hdr->h_max_pages) * db->db_pagesize;
}

int
mdbm_get_page_size(MDBM *db)
{
    return db->db_pagesize;
}

int
mdbm_get_hash(MDBM *db)
{
    return db->db_hdr->h_hash_func;
}

int
mdbm_get_alignment(MDBM *db)
{
    return db->db_hdr->h_dbflags & MDBM_ALIGN_MASK;
}

int
mdbm_get_errno(MDBM *db)
{
    return db->db_errno;
}

#define MDBM_UINT32_VAL(v)      (((v) <= UINT_MAX) ? v : 0)


int
mdbm_get_stats(MDBM* db, mdbm_stats_t* s, size_t stats_size)
{
    mdbm_db_info_t info;
    mdbm_stat_info_t stats;

    if (stats_size < sizeof(*s)) {
        errno = ENOMEM;
        return -1;
    }

    if (mdbm_get_db_stats(db,&info,&stats,0) < 0) {
        return -1;
    }

    s->s_size = MDBM_UINT32_VAL((uint64_t)info.db_page_size * info.db_num_pages);
    s->s_page_size = info.db_page_size;
    s->s_page_count = info.db_num_pages;
    s->s_pages_used = stats.num_active_pages;
    s->s_bytes_used = MDBM_UINT32_VAL(stats.sum_key_bytes
                                      + stats.sum_lob_val_bytes
                                      + stats.sum_normal_val_bytes);
/*fprintf(stderr,"NUM_ACTIVE_ENTRIES: %llu \n", (unsigned long long)stats.num_active_entries); */
/*fprintf(stderr,"S_NUM_ENTRIES: %lu\n", (unsigned long)s->s_num_entries); */
    s->s_num_entries = MDBM_UINT32_VAL(stats.num_active_entries);
/*uint32_t ne = s->s_num_entries; */
/*fprintf(stderr,"S_NUM_ENTRIES: %llu\n", (unsigned long long)s->s_num_entries); */
/*fprintf(stderr,"S_NUM_ENTRIES: %lu\n", ne); */
    s->s_min_level = info.db_dir_min_level;
    s->s_max_level = info.db_dir_max_level;
    s->s_large_page_size = info.db_page_size;
    s->s_large_page_count = stats.num_lob_pages;
    s->s_large_threshold = db->db_spillsize;
    s->s_large_pages_used = stats.num_lob_pages;
    s->s_large_num_free_entries = 0;
    s->s_large_max_free = 0;
    s->s_large_num_entries = MDBM_UINT32_VAL(stats.num_active_lob_entries);
    s->s_large_bytes_used = MDBM_UINT32_VAL(stats.sum_lob_val_bytes);
    s->s_large_min_size = stats.min_lob_bytes;
    s->s_large_max_size = stats.max_lob_bytes;
    s->s_cache_mode = info.db_cache_mode;

    return 0;
}

int
mdbm_chunk_iterate(MDBM* db, mdbm_chunk_iterate_func_t func, int flags, void* user)
{
    int pno;
    mdbm_chunk_info_t info;

    if (!(flags & MDBM_ITERATE_NOLOCK)) {
        if (do_read_lock(db) < 0) {
            return -1;
        }
    }

    pno = 0;
    while (pno < db->db_num_pages) {
        mdbm_page_t* page = MDBM_PAGE_PTR(db,pno);
        info.page_num = pno;
        info.dir_page_num = page->p_num;
        info.page_type = page->p_type;
        info.num_pages = page->p_num_pages;
        info.page_data = page->p.p_data;
        info.page_size = db->db_pagesize;
        info.prev_num_pages = page->p_prev_num_pages;
        if (page->p_type == MDBM_PTYPE_DATA) {
            info.num_entries = page->p.p_num_entries;
            info.used_space = info.num_pages*info.page_size - MDBM_PAGE_FREE_BYTES(page);
        } else if (page->p_type == MDBM_PTYPE_LOB) {
            info.num_entries = 1;
            info.used_space = page->p.p_vallen;
        } else {
            info.num_entries = 0;
            info.used_space = 0;
        }
        if (MDBM_IS_WINDOWED(db)) {
            release_window_page(db,page);
        }
        if (func(user,&info)) {
            if (!(flags & MDBM_ITERATE_NOLOCK)) {
                mdbm_internal_do_unlock(db,NULL);
            }
            return 1;
        }
        if (pno == db->db_hdr->h_last_chunk) {
            break;
        }
        pno += page->p_num_pages;
    }
    if (!(flags & MDBM_ITERATE_NOLOCK)) {
        mdbm_internal_do_unlock(db,NULL);
    }
    return 0;
}

int
mdbm_iterate(MDBM* db, int pagenum, mdbm_iterate_func_t func, int flags, void* user)
{
    int i;
    mdbm_iterate_info_t info;

    if (!(flags & MDBM_ITERATE_NOLOCK)) {
        if (do_read_lock(db) < 0) {
            return -1;
        }
    }
    if (pagenum > db->db_max_dirbit) {
        if (!(flags & MDBM_ITERATE_NOLOCK)) {
            mdbm_internal_do_unlock(db,NULL);
        }
        errno = EINVAL;
        return -1;
    }
    for (i = (pagenum < 0) ? 0 : pagenum; i <= db->db_max_dirbit; i++) {
        if (MDBM_GET_PAGE_INDEX(db,i)) {
            mdbm_page_t* page;
            mdbm_entry_t* ep;
            mdbm_entry_data_t e;

            memset(&info,0,sizeof(info));

            info.i_page.page_num = i;
            info.i_page.mapped_page_num = MDBM_GET_PAGE_INDEX(db,i);
            page = MDBM_MAPPED_PAGE_PTR(db,MDBM_GET_PAGE_INDEX(db,i));
            info.i_page.page_addr = (intptr_t)page;
            info.i_page.page_size = page->p_num_pages * db->db_pagesize;
            info.i_page.page_bottom_of_data = MDBM_DATA_PAGE_BOTTOM_OF_DATA(page);
            info.i_page.page_free_space = MDBM_PAGE_FREE_BYTES(page);
            info.i_page.page_active_entries = 0;
            info.i_page.page_active_space = 0;
            info.i_page.page_deleted_entries = 0;
            info.i_page.page_deleted_space = 0;
            info.i_page.page_min_entry_size = 0;
            info.i_page.page_max_entry_size = 0;
            info.i_page.page_overhead_space = MDBM_PAGE_T_SIZE + MDBM_ENTRY_T_SIZE;

            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                int ksize, vsize, lob_size, size;

                ksize = MDBM_KEY_LEN1(ep);

                vsize = MDBM_VAL_LEN1(db,ep);
                size = ksize + vsize;
                if (!ep->e_key.match) {
                    /*fprintf(stderr,"DELETED ENTRY: k:%d v:%d\n", ksize, vsize); */
                    info.i_page.page_deleted_entries++;
                    info.i_page.page_deleted_space += size;
                    info.i_page.page_overhead_space += MDBM_ENTRY_T_SIZE;
                } else {
                    if (MDBM_ENTRY_LARGEOBJ(ep)) {
                        /*fprintf(stderr,"ACTIVE LOB ENTRY: k:%d v:%d\n", ksize, vsize); */
                        mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
                        mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
                        vsize = 0;
                        lob_size = lob->p.p_vallen;
                        /* lob_space = lob->p_num_pages * db->db_pagesize; */
                        size = ksize + lob_size;
                    } else {
                        /*fprintf(stderr,"ACTIVE ENTRY: k:%d v:%d\n", ksize, vsize); */
                        lob_size = 0;
                    }
                    info.i_page.page_active_entries++;
                    info.i_page.page_overhead_space += MDBM_ENTRY_T_SIZE;
                    info.i_page.page_active_space += ksize+vsize;
                    info.i_page.page_active_key_space += ksize;
                    if (!info.i_page.page_min_entry_size || size < info.i_page.page_min_entry_size)
                    {
                        info.i_page.page_min_entry_size = size;
                    }
                    if (!info.i_page.page_max_entry_size || size > info.i_page.page_max_entry_size)
                    {
                        info.i_page.page_max_entry_size = size;
                    }
                    if (!info.i_page.page_max_key_size || ksize > info.i_page.page_max_key_size) {
                        info.i_page.page_max_key_size = ksize;
                    }
                    if (!info.i_page.page_min_key_size || ksize < info.i_page.page_min_key_size) {
                        info.i_page.page_min_key_size = ksize;
                    }
                    if (lob_size) {
                        info.i_page.page_active_lob_entries++;
                        info.i_page.page_overhead_space += MDBM_ENTRY_LOB_T_SIZE;
                        info.i_page.page_active_lob_space += lob_size;
                        if (!info.i_page.page_min_lob_size
                            || lob_size < info.i_page.page_min_lob_size)
                        {
                            info.i_page.page_min_lob_size = lob_size;
                        }
                        if (!info.i_page.page_max_lob_size
                            || lob_size > info.i_page.page_max_lob_size)
                        {
                            info.i_page.page_max_lob_size = lob_size;
                        }
                    } else {
                        info.i_page.page_active_val_space += vsize;
                        if (!info.i_page.page_min_val_size
                            || vsize < info.i_page.page_min_val_size)
                        {
                            info.i_page.page_min_val_size = vsize;
                        }
                        if (!info.i_page.page_max_val_size
                            || vsize > info.i_page.page_max_val_size)
                        {
                            info.i_page.page_max_val_size = vsize;
                        }
                    }
                }
            }
            if (flags & MDBM_ITERATE_ENTRIES) {
                for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                    kvpair kv;
                    char* page_base = MDBM_DATA_PAGE_BASE(page);
                    char* v;
                    int vlen;

                    memset(&info.i_entry,0,sizeof(info.i_entry));
                    if (!ep->e_key.match) {
                        info.i_entry.entry_flags |= MDBM_ENTRY_DELETED;
                    }
                    info.i_entry.entry_index = e.e_index;
                    info.i_entry.entry_offset = (char*)ep - page_base;

                    kv.key.dptr = MDBM_KEY_PTR(&e);
                    kv.key.dsize = MDBM_KEY_LEN(&e);
                    info.i_entry.key_offset = kv.key.dptr - page_base;

                    if (ep->e_key.match) {
                        v = MDBM_VAL_PTR(db,&e);
                        vlen = MDBM_VAL_LEN(db,&e);
                        if (MDBM_DB_CACHEMODE(db)) {
                            mdbm_cache_entry_t* c = (mdbm_cache_entry_t*)v;
                            v += MDBM_CACHE_ENTRY_T_SIZE;
                            vlen -= MDBM_CACHE_ENTRY_T_SIZE;

                            info.i_entry.cache_num_accesses = c->c_num_accesses;
                            if (MDBM_DB_CACHEMODE(db) == MDBM_CACHEMODE_GDSF) {
                                info.i_entry.cache_priority = c->c_dat.priority;
                            } else {
                                info.i_entry.cache_access_time = c->c_dat.access_time;
                            }
                        }
                        if (MDBM_ENTRY_LARGEOBJ(ep)) {
                            mdbm_entry_lob_t* lp = (mdbm_entry_lob_t*)v;
                            mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
                            info.i_entry.entry_flags |= MDBM_ENTRY_LARGE_OBJECT;
                            info.i_entry.lob_page_num = lp->l_pagenum;
                            info.i_entry.lob_num_pages = lob->p_num_pages;
                            info.i_entry.lob_page_addr = (intptr_t)lob;
                            kv.val.dptr = MDBM_LOB_VAL_PTR(lob);
                            kv.val.dsize = MDBM_LOB_VAL_LEN(lob);
                        } else {
                            kv.val.dptr = v;
                            kv.val.dsize = vlen;
                        }
                        info.i_entry.val_offset = v - page_base;
                    }
                    if (func(user,&info,&kv)) {
                        if (!(flags & MDBM_ITERATE_NOLOCK)) {
                            mdbm_internal_do_unlock(db,NULL);
                        }
                        if (MDBM_IS_WINDOWED(db)) {
                            release_window_page(db,page);
                        }
                        return 1;
                    }
                }
            } else {
                if (func(user,&info,NULL)) {
                    if (!(flags & MDBM_ITERATE_NOLOCK)) {
                        mdbm_internal_do_unlock(db,NULL);
                    }
                    if (MDBM_IS_WINDOWED(db)) {
                        release_window_page(db,page);
                    }
                    return 1;
                }
            }
            if (MDBM_IS_WINDOWED(db)) {
                release_window_page(db,page);
            }
        }
        if (pagenum >= 0) {
            break;
        }
    }
    if (!(flags & MDBM_ITERATE_NOLOCK)) {
        mdbm_internal_do_unlock(db,NULL);
    }
    return 0;
}

const char*
MDBM_HASH_FUNCNAMES[] = {
    "CRC-32",
    "EJB",
    "Phong",
    "OZ",
    "Torek",
    "FNV",
    "STL",
    "MD5",
    "SHA-1",
    "Jenkins",
    "Hsieh"
};


int
get_db_info(MDBM* db, mdbm_db_info_t* info, int flags)
{
    struct mdbm_walk w;

    if (!(flags & MDBM_STAT_NOLOCK)) {
        if (do_read_lock(db) < 0) {
            return -1;
        }
    }

    w.db = db;
    w.dirbit = 0;
    w.level = 0;
    w.min_level = -1;
    w.max_level = -1;
    w.num_nodes = 0;
    if (!MDBM_DIR_BIT(w.db,w.dirbit)) {
        w.min_level = w.max_level = 1;
    } else {
        walk_dir(&w);
    }

    info->db_page_size = db->db_pagesize;
    info->db_num_pages = db->db_num_pages;
    info->db_max_pages = db->db_hdr->h_max_pages;
    info->db_num_dir_pages = MDBM_DB_NUM_DIR_PAGES(db);
    info->db_dir_width = MDBM_DB_DIR_WIDTH(db);
    info->db_max_dir_shift = db->db_max_dir_shift;
    info->db_dir_min_level = w.min_level;
    info->db_dir_max_level = w.max_level;
    info->db_dir_num_nodes = w.num_nodes;
    info->db_hash_func = db->db_hdr->h_hash_func;
    info->db_hash_funcname = MDBM_HASH_FUNCNAMES[db->db_hdr->h_hash_func];
    info->db_spill_size = db->db_spillsize;
    info->db_cache_mode = db->db_cache_mode;

    if (!(flags & MDBM_STAT_NOLOCK)) {
        mdbm_internal_do_unlock(db,NULL);
    }
    return 0;
}

int
mdbm_get_db_info(MDBM* db, mdbm_db_info_t* info)
{
    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = EBADF;
        return -1;
    }

    return get_db_info(db,info,0);
}

typedef struct mdbm_stat_iterate {
    mdbm_db_info_t* dbinfo;
    mdbm_stat_info_t* dbstats;
} mdbm_stat_iterate_t;


static int
stat_dump(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
    mdbm_stat_iterate_t* si = (mdbm_stat_iterate_t*)user;
    int used = info->i_page.page_active_space + info->i_page.page_overhead_space;
    int free = info->i_page.page_size - used;
    /* Risk overflow, but otherwise, we get bucket > MDBM_STAT_BUCKETS. */
    int bucket = (used * MDBM_STAT_BUCKETS) / si->dbstats->max_page_used_space;

    assert(bucket <= MDBM_STAT_BUCKETS);
    /*fprintf(stderr, "active: %d  overhead %d\n", info->i_page.page_active_space, info->i_page.page_overhead_space); */
    /*fprintf(stderr, "used: %d  max_p_u_s %d\n", used, si->dbstats->max_page_used_space); */
    /*fprintf(stderr, "Bucket %d / %d\n", bucket, MDBM_STAT_BUCKETS); */
    si->dbstats->buckets[bucket].num_pages++;
    if (si->dbstats->buckets[bucket].num_pages == 1
        || used < si->dbstats->buckets[bucket].min_bytes)
    {
        si->dbstats->buckets[bucket].min_bytes = used;
    }
    if (used > si->dbstats->buckets[bucket].max_bytes) {
        si->dbstats->buckets[bucket].max_bytes = used;
    }
    si->dbstats->buckets[bucket].sum_entries += info->i_page.page_active_entries;
    si->dbstats->buckets[bucket].sum_bytes += used;
    /*fprintf(stderr,"PAGE_ACTIVE_ENTRIES: %d (acc:%llu) \n", info->i_page.page_active_entries, (long long unsigned)si->dbstats->num_active_entries); */
    si->dbstats->num_active_entries += info->i_page.page_active_entries;
    si->dbstats->num_active_lob_entries += info->i_page.page_active_lob_entries;
    si->dbstats->sum_key_bytes += info->i_page.page_active_key_space;
    si->dbstats->sum_normal_val_bytes += info->i_page.page_active_val_space;
    si->dbstats->sum_lob_val_bytes += info->i_page.page_active_lob_space;
    si->dbstats->sum_overhead_bytes += info->i_page.page_overhead_space;
    si->dbstats->sum_overhead_bytes += info->i_page.page_active_lob_entries * MDBM_PAGE_T_SIZE;

    if (free > si->dbstats->buckets[bucket].max_free_bytes) {
        si->dbstats->buckets[bucket].max_free_bytes = free;
    }
    if (si->dbstats->buckets[bucket].num_pages == 1
        || free < si->dbstats->buckets[bucket].min_free_bytes)
    {
        si->dbstats->buckets[bucket].min_free_bytes = free;
    }
    si->dbstats->buckets[bucket].sum_free_bytes += free;

#define MM(which) \
if (!si->dbstats->min_##which##_bytes \
    || info->i_page.page_min_##which##_size < si->dbstats->min_##which##_bytes) { \
    si->dbstats->min_##which##_bytes = info->i_page.page_min_##which##_size; \
} \
if (!si->dbstats->max_##which##_bytes \
    || info->i_page.page_max_##which##_size > si->dbstats->max_##which##_bytes) { \
    si->dbstats->max_##which##_bytes = info->i_page.page_max_##which##_size; \
}

    MM(key)
    MM(val)
    MM(entry)
    MM(lob)

    return 0;
}

static int
stat_chunk(void* user, const mdbm_chunk_info_t* info)
{
    mdbm_stat_iterate_t* si = (mdbm_stat_iterate_t*)user;
    switch (info->page_type) {
    case MDBM_PTYPE_FREE:
        si->dbstats->num_free_pages += info->num_pages;
        break;

    case MDBM_PTYPE_DATA:
        if (info->num_pages > si->dbstats->max_data_pages) {
            si->dbstats->max_data_pages = info->num_pages;
        }
        if (info->used_space > si->dbstats->max_page_used_space) {
            si->dbstats->max_page_used_space = info->used_space;
        }
        si->dbstats->num_active_pages += info->num_pages;
        if (info->num_pages == 1) {
            si->dbstats->num_normal_pages++;
        } else {
            si->dbstats->num_oversized_pages++;
        }
        if (info->num_entries > si->dbstats->max_page_entries) {
            si->dbstats->max_page_entries = info->num_entries;
        }
        if (si->dbstats->num_normal_pages+si->dbstats->num_oversized_pages == 1
            || info->num_entries < si->dbstats->min_page_entries)
        {
            si->dbstats->min_page_entries = info->num_entries;
        }
        break;

    case MDBM_PTYPE_DIR:
        break;

    case MDBM_PTYPE_LOB:
        si->dbstats->num_lob_pages += info->num_pages;
        break;
    }
    return 0;
}

int
mdbm_get_db_stats(MDBM* db, mdbm_db_info_t* dbinfo, mdbm_stat_info_t* dbstats, int flags)
{
    int ret = 0;
    int iflags = 0;
    mdbm_stat_iterate_t si;

    if (!(flags & MDBM_STAT_NOLOCK)) {
        if (do_read_lock(db) < 0) {
            return -1;
        }
        iflags = MDBM_ITERATE_NOLOCK;
    }

    memset(dbinfo,0,sizeof(*dbinfo));
    memset(dbstats,0,sizeof(*dbstats));

    si.dbinfo = dbinfo;
    si.dbstats = dbstats;
    if (get_db_info(db,dbinfo,flags) < 0
        || mdbm_chunk_iterate(db,stat_chunk,iflags,&si) < 0
        || mdbm_iterate(db,-1,stat_dump,iflags,&si) < 0)
    {
        ret = -1;
    }
    dbstats->sum_overhead_bytes += dbinfo->db_num_dir_pages * dbinfo->db_page_size;
    dbstats->num_free_pages +=
        db->db_num_pages - (db->db_hdr->h_last_chunk
                            + MDBM_PAGE_PTR(db,db->db_hdr->h_last_chunk)->p_num_pages);

    if (!(flags & MDBM_STAT_NOLOCK)) {
        mdbm_internal_do_unlock(db,NULL);
    }
    return ret;
}

mdbm_hdr_t*
mdbm_get_header(MDBM* db)
{
    return db->db_hdr;
}

uint32_t
mdbm_get_version(MDBM* db)
{
    return (db->db_ver_flag == MDBM_DBVER_3) ? 3 : 2;
}

const char* mdbm_get_filename(MDBM *db)
{
    return db->db_filename;
}

int mdbm_get_fd(MDBM *db)
{
    return db->db_fd;
}


int
mdbm_check(MDBM* db, int level, int verbose)
{
    return check_db(db,level,10,verbose);
}

int
mdbm_clean(MDBM* db, int pnum, int flags)
{
    int i;
    int quit = 0;
    int ncleaned = 0;
    int pagenum = pnum;
    mdbm_pagenum_t page_mpt = (mdbm_pagenum_t)pnum;

    if (pagenum > db->db_max_dirbit || (db->db_flags & MDBM_DBFLAG_NO_DIRTY)) {
        if (pagenum > db->db_max_dirbit) {
          mdbm_log(LOG_ERR, "%s: mdbm_clean() pagenum:%d > dirbit:%d\n", 
              db->db_filename, pnum, db->db_max_dirbit);
        }
        errno = EINVAL;
        return -1;
    }
    if (!db->db_clean_func) {
          mdbm_log(LOG_ERR, "%s: mdbm_clean(), No clean function set! \n", db->db_filename);
        errno = EINVAL;
        return -1;
    }
    if (mdbm_internal_do_lock(db,MDBM_LOCK_WRITE,MDBM_LOCK_WAIT,NULL,NULL,(pagenum >= 0) ? &page_mpt : NULL) < 0)
    {
        mdbm_log(LOG_ERR, "%s: mdbm_clean(), internal lock failed! \n", db->db_filename);
        return -1;
    }
    for (i = (pagenum < 0) ? 0 : pagenum; i <= db->db_max_dirbit; i++) {
        mdbm_page_t* page;
        if ((page = pagenum_to_page(db,i,MDBM_PAGE_NOALLOC,MDBM_PAGE_MAP))) {
            mdbm_entry_data_t e;
            mdbm_entry_t* ep;

            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                /*mdbm_log(LOG_ERR, "%s: mdbm_clean(), page %d entry %p\n", db->db_filename, i, (void*)ep);*/
                if (ep->e_key.match && MDBM_ENTRY_DIRTY(ep)) {
                    datum key, val;

                    get_kv2(db,page,ep,&key,&val);
                    if (db->db_clean_func(db,&key,&val,db->db_clean_data,&quit)) {
                        ep->e_flags &= ~MDBM_EFLAG_DIRTY;
                        ncleaned++;
                    }
                    if (quit) {
                        break;
                    }
                }
            }
            if (MDBM_IS_WINDOWED(db)) {
                release_window_page(db,page);
            }
        }
        if (pagenum >= 0) {
            break;
        }
    }
    mdbm_internal_do_unlock(db,NULL);

    if (quit) {
        errno = EINTR;
        return -1;
    }
    return ncleaned;
}

int
mdbm_set_cleanfunc(MDBM* db, mdbm_clean_func func, void* data)
{
    if (!db || !func) {
        errno = EINVAL;
        return -1;
    }

    db->db_clean_func = func;
    db->db_clean_data = data;
    return 0;
}

/* Copy the MDBM header into a new file */
int
fcopy_header(MDBM* db, int fd, int flags)
{
    int ret = 0;
    int nbytes;
    int npages;
    char *buf;
    uint32_t pgsz = db->db_pagesize;

    /* Make sure we're writing at the start of the file */
    if (lseek(fd,0,SEEK_SET) < 0) {
        return -1;
    }

    /* Grab the db lock to copy the db directory */
    if (lock_db_ex(db) < 0) {
        return -1;
    }

    nbytes = MDBM_DB_NUM_DIR_BYTES(db);
    if (write(fd,db->db_base,nbytes) < nbytes) {
        ret = -1;
    }
    /* Write out page padding separately because the actual memory pages are mapped
       as inaccessible.
     */
    npages = MDBM_DB_NUM_DIR_PAGES(db);
    nbytes = npages * pgsz - nbytes;
    if (nbytes > 0) {
        buf = malloc(nbytes);
        memset(buf,0,nbytes);
        if (write(fd,buf,nbytes) < nbytes) {
            ret = -1;
        }
        free(buf);
    }

    /* Drop the db lock if we're not locking the entire DB */
    if (!(flags & MDBM_COPY_LOCK_ALL)) {
        unlock_db(db);
    }
    return ret;
}

/* Copy the MDBM body into a new file.  Should not be called w/o calling fcopy_header first */
int
fcopy_body(MDBM* db, int fd, int flags)
{
    int ret = 0;
    int nbytes;
    char *buf;
    uint32_t pgsz = db->db_pagesize;
    mdbm_pagenum_t n;
    uint32_t pnum;
    ssize_t len;
    uint8_t dir_shift_start = db->db_hdr->h_dir_shift;   /* Size of directory, in bits */
    uint32_t num_pages_start = db->db_hdr->h_num_pages;  /* Number of pages at the beginning of fcopy */

    /* For each db page */
    for (n = 0; n <= db->db_max_dirbit; n++) {
        mdbm_page_t* page;

        if (!(flags & MDBM_COPY_LOCK_ALL)) {
            if (mdbm_internal_do_lock(db,MDBM_LOCK_READ,MDBM_LOCK_WAIT,NULL,NULL,&n) < 0) {
                ret = -1;
                break;
            }
        }

        if (pagenum_to_page(db,n,MDBM_PAGE_NOALLOC,MDBM_PAGE_MAP)) {
            mdbm_entry_data_t e;
            mdbm_entry_t* ep;
            off_t offst;
            pnum = db->db_ptable[n].pt_pagenum;

            page = MDBM_PAGE_PTR(db, pnum);
            nbytes = page->p_num_pages * pgsz;
            offst = ((off_t) pnum) * pgsz;
            if ((len = pwrite(fd, page, nbytes, offst)) < nbytes) {
                ret = -1;
                break;
            }

            for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
                if (MDBM_ENTRY_LARGEOBJ(ep)) {
                    mdbm_entry_lob_t* lp = MDBM_LOB_PTR1(db,page,ep);
                    mdbm_page_t* lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
                    off_t offset = (char*)lob - db->db_base;

                    nbytes = lob->p_num_pages * pgsz;
                    if ((len = pwrite(fd,lob,nbytes,offset)) < nbytes) {
                        ret = -1;
                        break;
                    }
                }
            }

            if (MDBM_IS_WINDOWED(db)) {
                release_window_page(db,page);
            }
            if (ret < 0) {
                break;
            }
        }
        if (!(flags & MDBM_COPY_LOCK_ALL)) {
            unlock_db(db);
        }
    }  /* for each page */

    /* Write out free list - partition locking cannot be used w/o data */
    pnum = 0;
    buf = malloc(pgsz);
    memset(buf, 0, pgsz);
    while ((ret >= 0) && (pnum < db->db_hdr->h_num_pages)) {
        mdbm_page_t* page = MDBM_PAGE_PTR(db,pnum);
        if (page->p_type == MDBM_PTYPE_FREE) {
            if (!(flags & MDBM_COPY_LOCK_ALL)) {
                if (MDBM_RWLOCKS(db)) {
                    mdbm_lock_shared(db);
                } else {
                    lock_db_ex(db);
                }
            }
            memcpy(buf, page, sizeof(mdbm_page_t));  /* write page hdr */
            if (!(flags & MDBM_COPY_LOCK_ALL)) {
                unlock_db(db);
            }
            if ((len = pwrite(fd, buf, pgsz, ((off_t) pnum) * pgsz)) < pgsz) {
                ret = -1;
                break;
            }
        }
        if (page->p_num_pages) {
            pnum += page->p_num_pages;
        } else {
            ++pnum;
        }
    }
    free(buf);

    if (flags & MDBM_COPY_LOCK_ALL) {
        unlock_db(db);
    }

    if ((ret == -1) || (dir_shift_start != db->db_hdr->h_dir_shift)
         || (num_pages_start != db->db_hdr->h_num_pages)) {
        errno = EAGAIN;
        ftruncate(fd, 0);
        ret = -1;
    } else {
        ret = ftruncate(fd,MDBM_DB_SIZE(db));
    }

    return ret;
}

/* Perform a copy of an MDBM into a new file.  Retry "tries" times, returning 1
 * if after copying we discovered that a retry is needed, which can happen when
 * an MDBM split during the copying process.
 */ 
int
mdbm_internal_fcopy(MDBM* db, int fd, int flags, int tries)
{
    int ret = 0;
    int i = 0;

    do {
        errno = 0;
        ret = fcopy_header(db, fd, flags);
        if (ret == 0) {
            ret = fcopy_body(db, fd, flags);
        }
    } while ((ret != 0) && (++i < tries));

    return ret;
}

int
mdbm_fcopy(MDBM* db, int fd, int flags)
{
    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return -1;
    }

    if (flags & ~MDBM_COPY_LOCK_ALL) {
        errno = EINVAL;
        return -1;
    }

    return mdbm_internal_fcopy(db, fd, flags, 1);
}

int
mdbm_reset_rstats(struct mdbm_rstats* rs)
{
    memset(rs,0,sizeof(*rs));
    rs->version = MDBM_RSTATS_VERSION;
    return 0;
}

struct mdbm_rstats_mem {
    mdbm_shmem_t* mem;
};


int
mdbm_open_rstats(const char* dbfilename, int flags,
                 struct mdbm_rstats_mem** m, struct mdbm_rstats** r)
{
    char path[MAXPATHLEN+1];
    int f, init;
    struct mdbm_rstats_mem* mem;
    mdbm_rstats_t* rs;
    size_t sz;

    if (snprintf(path,sizeof(path),"%s.stats",dbfilename) >= sizeof(path)) {
        errno = EINVAL;
        return -1;
    }

    if ((flags & O_ACCMODE) == O_RDONLY) {
        f = MDBM_SHMEM_RDONLY;
    } else {
        f = MDBM_SHMEM_RDWR|MDBM_SHMEM_CREATE|MDBM_SHMEM_TRUNC;
    }
    mem = (struct mdbm_rstats_mem*)malloc(sizeof(*mem));

    sz = sizeof(*rs);

#ifdef __linux__
    {
        struct statfs fs;
        int ret;
        if ((ret = statfs(path,&fs)) < 0) {
            if (errno == ENOENT && (f & MDBM_SHMEM_CREATE)) {
                int fd;
                if ((fd = open(path,O_RDWR|O_CREAT,0666)) >= 0) {
                    ret = fstatfs(fd,&fs);
                    close(fd);
                }
            }
        }
        if (ret < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: mdbm statfs failure",path);
        } else if (fs.f_type == MDBM_HUGETLBFS_MAGIC) {
            sz = (sz + fs.f_bsize-1) & ~(fs.f_bsize-1);
        }
    }
#endif

    if ((mem->mem = mdbm_shmem_open(path,f,sz,&init)) == NULL) {
        free(mem);
        return -1;
    }

    rs = (mdbm_rstats_t*)mem->mem->base;
    if (init) {
        if (rs->version == MDBM_RSTATS_VERSION_0) {
            memset(&rs->cache_store,0,sizeof(rs->cache_store));
            rs->version = MDBM_RSTATS_VERSION;
        } else if (rs->version != MDBM_RSTATS_VERSION) {
            mdbm_reset_rstats(rs);
        }
        mdbm_shmem_init_complete(mem->mem);
    }

    *m = mem;
    *r = rs;
    return 0;
}

int
mdbm_close_rstats(struct mdbm_rstats_mem* mem)
{
    mdbm_shmem_close(mem->mem,0);
    free(mem);
    return 0;
}

int
mdbm_init_rstats(MDBM* db, int flags)
{
    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return -1;
    }

    if (mdbm_open_rstats(db->db_filename,O_RDWR,&db->db_rstats_mem,&db->db_rstats) < 0) {
        return -1;
    }
    if (flags & MDBM_RSTATS_THIST) {
        db->db_flags |= MDBM_DBFLAG_RSTATS_THIST;
    }
    return 0;
}

static void
diff_rstats_val(mdbm_rstats_val_t* base, mdbm_rstats_val_t* sample, mdbm_rstats_val_t* diff,
                mdbm_rstats_val_t* new_base)
{
    int i;

    diff->num = sample->num - base->num;
    diff->sum_usec = sample->sum_usec - base->sum_usec;
    diff->num_error = sample->num_error - base->num_error;
    diff->num_lock_wait = sample->num_lock_wait - base->num_lock_wait;
    diff->sum_lock_wait_usec = sample->sum_lock_wait_usec - base->sum_lock_wait_usec;
    for (i = 0; i < MDBM_RSTATS_THIST_MAX; i++) {
        diff->thist[i] = sample->thist[i] - base->thist[i];
    }

    if (new_base) {
        new_base->num = sample->num;
        new_base->sum_usec = sample->sum_usec;
        new_base->num_error = sample->num_error;
        new_base->num_lock_wait = sample->num_lock_wait;
        new_base->sum_lock_wait_usec = sample->sum_lock_wait_usec;
        for (i = 0; i < MDBM_RSTATS_THIST_MAX; i++) {
            new_base->thist[i] = sample->thist[i];
        }
    }
}

#define DIFF_RSTATS_VAL(f)      \
        diff_rstats_val(&base->f,&sample->f,&diff->f,new_base ? &new_base->f : NULL)

void
mdbm_diff_rstats(mdbm_rstats_t* base, mdbm_rstats_t* sample, mdbm_rstats_t* diff,
                 mdbm_rstats_t* new_base)
{
    diff->version = sample->version;
    diff->flags = sample->flags;
    if (new_base) {
        new_base->version = sample->version;
        new_base->flags = sample->flags;
    }

    DIFF_RSTATS_VAL(fetch);
    DIFF_RSTATS_VAL(fetch_uncached);
    DIFF_RSTATS_VAL(store);
    DIFF_RSTATS_VAL(remove);
    DIFF_RSTATS_VAL(getpage);
    DIFF_RSTATS_VAL(getpage_uncached);
    DIFF_RSTATS_VAL(cache_evict);
    if (sample->version >= MDBM_RSTATS_VERSION) {
        DIFF_RSTATS_VAL(cache_store);
    }
}

int
mdbm_protect(MDBM* db, int protect)
{
    int prot;
    int ret;

    if (((protect & MDBM_PROT_ACCESS) && (protect & ~MDBM_PROT_ACCESS))
        || (protect & ~(MDBM_PROT_ACCESS|MDBM_PROT_READ|MDBM_PROT_WRITE))
        || ((protect & MDBM_PROT_WRITE) && MDBM_IS_RDONLY(db)))
    {
        errno = EINVAL;
        return -1;
    }

    if (protect & MDBM_PROT_ACCESS) {
        prot = MDBM_IS_RDONLY(db) ? PROT_READ : (PROT_READ|PROT_WRITE);
    } else {
        prot = 0;
        if (protect & MDBM_PROT_READ) {
            prot |= PROT_READ;
        }
        if (protect & MDBM_PROT_WRITE) {
            prot |= PROT_WRITE;
        }
    }

    ret = mprotect(db->db_base,db->db_base_len,prot);
    if (!ret && MDBM_IS_WINDOWED(db)) {
        ret = mprotect(db->db_window.base,db->db_window.base_len,prot);
    }
    return ret;
}

int
mdbm_set_window_size_internal(MDBM* db, size_t wsize)
{
    int i;

    /* ensure wsize is a multiple of db (rounded up to system) page size */
    int syspagesz = db->db_sys_pagesize;
    int dbpagesz = db->db_pagesize;
    wsize = ((size_t)MDBM_NUM_PAGES_ROUNDED(dbpagesz, wsize))*dbpagesz;
    wsize = ((size_t)MDBM_NUM_PAGES_ROUNDED(syspagesz, wsize))*syspagesz;


    if (db->db_window.base) {
        if (munmap(db->db_window.base,db->db_window.base_len) < 0) {
            mdbm_logerror(LOG_ERR,0,"munmap(%p,%llu)",
                          db->db_window.base,(unsigned long long)db->db_window.base_len);
        }
        /*fprintf(stderr, "set_window_size::munmap(%p,%u)\n", db->db_window.base,(unsigned)db->db_window.base_len); */
        db->db_window.base = NULL;
    }
    if (db->db_window.buckets) {
        free(db->db_window.buckets);
        db->db_window.buckets = NULL;
    }
    if (db->db_window.wpages) {
        free(db->db_window.wpages);
        db->db_window.wpages = NULL;
    }
    if (!wsize) {
        return 0;
    }

    if (wsize / db->db_pagesize < 2) {
        wsize = db->db_pagesize * 2;
    }
    if (map(db,0,0,wsize,&db->db_window.base,&db->db_window.base_len) < 0) {
        return -1;
    }
    db->db_window.num_pages = wsize / db->db_pagesize;

    db->db_window.num_buckets = db->db_window.num_pages < 16 ? 4 : db->db_window.num_pages / 4;
    db->db_window.buckets = (mdbm_wpage_head_t*)malloc(db->db_window.num_buckets
                                                       * sizeof(mdbm_wpage_head_t));

    db->db_window.wpages = (mdbm_wpage_t*)calloc(db->db_window.num_pages,sizeof(mdbm_wpage_t));
    for (i = 0; i < db->db_window.num_pages; i++) {
        db->db_window.wpages[i].pagenum = -1;
        db->db_window.wpages[i].num_pages = 1;
    }

    release_window_pages(db);
    return 0;
}

int
mdbm_set_window_size(MDBM* db, size_t wsize)
{
  int dbpagesz = db->db_pagesize;
  if (wsize < 2*dbpagesz) {
    mdbm_log(LOG_ERR,"%s: mdbm_set_window_size() wsize should be at least 2 pages\n", db->db_filename);
    errno=EINVAL;
    return -1;
  }
  return mdbm_set_window_size_internal(db, wsize);
}

int
mdbm_get_window_stats(MDBM* db, mdbm_window_stats_t* s, size_t s_size)
{
    if (!s) {
      errno = EINVAL;
      return -1;
    }
    if (s_size < offsetof(mdbm_window_stats_t,w_window_size)) {
        errno = ENOMEM;
        return -1;
    }
    s->w_num_reused = db->db_window.num_reused;
    s->w_num_remapped = db->db_window.num_remapped;

    if (s_size >= sizeof(*s)) {
        s->w_window_size = db->db_window.num_pages * db->db_pagesize;
        s->w_max_window_used = db->db_window.max_first_free * db->db_pagesize;
    }
    return 0;
}


#ifdef __linux__

#ifndef TAILQ_FOREACH

#define TAILQ_FOREACH(var, head, field)                                 \
        for ((var) = TAILQ_FIRST((head));                               \
            (var);                                                      \
            (var) = TAILQ_NEXT((var), field))

#endif

#define TAILQ_FOREACH_SAFE(var, head, field, tvar)                      \
        for ((var) = TAILQ_FIRST((head));                               \
            (var) && ((tvar) = TAILQ_NEXT((var), field), 1);            \
            (var) = (tvar))

#ifndef TAILQ_FOREACH_REVERSE

#define TAILQ_FOREACH_REVERSE(var, head, headname, field)               \
        for ((var) = TAILQ_LAST((head), headname);                      \
            (var);                                                      \
            (var) = TAILQ_PREV((var), headname, field))

#endif

#define TAILQ_FOREACH_REVERSE_SAFE(var, head, headname, field, tvar)    \
        for ((var) = TAILQ_LAST((head), headname);                      \
            (var) && ((tvar) = TAILQ_PREV((var), headname, field), 1);  \
            (var) = (tvar))


#define TAILQ_FIRST(head)       ((head)->tqh_first)

#define TAILQ_LAST(head, headname)                                      \
        (*(((struct headname *)((head)->tqh_last))->tqh_last))

#define TAILQ_NEXT(elm, field) ((elm)->field.tqe_next)

#define TAILQ_PREV(elm, headname, field)                                \
        (*(((struct headname *)((elm)->field.tqe_prev))->tqh_last))


static inline int
match_mapped_window_page(mdbm_wpage_t* w, uint32_t off, uint32_t len)
{
    return (w->mapped_off <= off && w->mapped_off+w->mapped_len >= off+len);
}

#undef MDBM_WSTATUS

#ifdef MDBM_WSTATUS
static void
wstatus(MDBM* db, const char* where)
{
    int i;
    mdbm_log(LOG_ERR,"%s: window.num_pages=%d window.first_free=%d",
             where,db->db_window.num_pages,db->db_window.first_free);
    for (i = 0; i < db->db_window.first_free; ++i) {
        mdbm_wpage_t* w = db->db_window.wpages+i;
        mdbm_log(LOG_ERR,"%s: [%4d] pagenum=%-6d num_pages=%-6d\n",
                 where,i,w->pagenum,w->num_pages);
    }
}
#endif

void
release_window_pages(MDBM* db)
{
    int i;
    /*fprintf(stderr, "release_window_pages()\n"); */

    db->db_window.first_free = 0;
    for (i = 0; i < db->db_window.num_buckets; i++) {
        TAILQ_INIT(db->db_window.buckets+i);
    }

#ifdef MDBM_WSTATUS
    wstatus(db,"released");
#endif
}

void
release_window_page(MDBM* db, void* p)
{
    int wi;
    mdbm_wpage_t* w;
    int pnum;
    int npages;
    int h;
    int i;

    if ((char*)p < db->db_window.base
        || (char*)p >= db->db_window.base+db->db_window.base_len)
    {
        return;
    }

    wi = ((char*)p - db->db_window.base) / db->db_pagesize;
    w = db->db_window.wpages+wi;

    while (!w->num_pages) {
        if (wi == 0) {
            mdbm_log(LOG_CRIT, "%s: wi==0 in release_window_page",
                     db->db_filename);
            abort();
        }
        --wi;
        --w;
    }

    pnum = w->pagenum;
    npages = w->num_pages;
    for (i = 0; i < npages; ++i) {
        w->pagenum = -1;
        w->num_pages = 1;
    }

    h = pnum % db->db_window.num_buckets;
    TAILQ_REMOVE(db->db_window.buckets+h,w,hash_link);

    if (wi + npages == db->db_window.first_free) {
        db->db_window.first_free = wi;
    }
}


extern int
remap_window_bytes(MDBM* db, char* mem_base, int mem_offset, uint32_t len, uint64_t file_offset, int flags) {
    int sys_pg_sz = db->db_sys_pagesize;
    /* db page size in units of system pages */
    int map_syspg_unit = MDBM_NUM_PAGES_ROUNDED(sys_pg_sz,db->db_pagesize);
    /* (rounded) db page size in bytes */
    int map_unit = map_syspg_unit * sys_pg_sz;
    int map_count = len / map_unit;
    int i;
    int pgoff = file_offset / sys_pg_sz;

    /*ASSERT_MULTIPLE(file_offset, sys_pg_sz); */
    ASSERT_MULTIPLE(mem_offset, sys_pg_sz);
    ASSERT_MULTIPLE(len, sys_pg_sz);

    /*
      fprintf(stderr, "remap_window_bytes(%p, %p, %d, %u, %d, %d) "
          "map_unit=%d db_page=%d, sys_page=%d\n",
          db, mem_base, mem_offset, (unsigned)len, file_offset, flags,
          map_unit, db->db_pagesize, db->db_sys_pagesize);
    */

    /* loop in units of map_unit to keep the exact same fragmentation of the window,  */
    /* as RHEL6 can't handle coalescing map fragments */
    for (i=0; i<map_count; ++i) {
        char* mem_ptr = mem_base+mem_offset+map_unit*i;
        ssize_t pg_offset = pgoff + map_syspg_unit*i;
        /*fprintf(stderr, "remap_file_pages(%p,%d,0,%llu,0) idx:%d db_base:%p window_base:%p\n", */
        /*        mem_ptr,map_unit,(long long unsigned)pg_offset, i, db->db_base, db->db_window.base); */
        if (remap_file_pages(mem_ptr,map_unit,0,pg_offset,len ? 0 : MAP_NONBLOCK) < 0) {
            mdbm_logerror(LOG_ERR,0, "remap_file_pages(%p,%llu,0,%llu,0); ",
                          mem_ptr,(unsigned long long)map_unit,(unsigned long long)pg_offset);
            errno = ENOMEM;
            return -1;
        }
    }
    return 0;
}

/* npages is in db_page_size */
extern mdbm_page_t*
get_window_page(MDBM* db, mdbm_page_t* page, int pagenum, int npages, uint32_t off, uint32_t len)
{
    int pnum;
    int h;
    mdbm_wpage_t* w;
    int wi = -1;
    char* p;
    uint64_t t0 = 0;
    uint32_t ulen;
    int i;

    /*fprintf(stderr, "get_window_page() page:%p, pagenum:%d, off:%u len:%u\n", page, pagenum, off, len); */
    if (npages > db->db_window.num_pages) {
        mdbm_log(LOG_ALERT,"%s: window size too small (need at least %d bytes)",
                 db->db_filename,(2+npages)*db->db_pagesize);
    }

    if (MDBM_DO_STAT_TIME(db)) {
        t0 = db->db_get_usec();
    }
    if (page && page->p_num_pages > npages) {
        npages = page->p_num_pages;
    }

    if (!npages) { npages = 1; }

    /* if length isn't specified, map a whole page */
    ulen = len ? len : (npages * db->db_pagesize);
    if (page) {
        if ((char*)page >= db->db_window.base
            && (char*)page < db->db_window.base+db->db_window.base_len)
        {
            /* already window-mapped */
            wi = ((char*)page - db->db_window.base) / db->db_pagesize;
            w = db->db_window.wpages + wi;
            if (match_mapped_window_page(w,off,ulen)) {
                db->db_window.num_reused++;
                if (MDBM_DO_STAT_TIME(db)) {
                  mdbm_rstats_val_t* val = (db->db_rstats) ? &db->db_rstats->getpage : NULL;
                  MDBM_ADD_STAT(db,val,db->db_get_usec() - t0, MDBM_STAT_TAG_GETPAGE);
                } else if (MDBM_DO_STATS(db)) {
                  MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_GETPAGE);
                }
                return page;
            }
            pnum = w->pagenum;
        } else {
            /* FIXME THIS IS HORRIBLY BROKEN. But seems to not be used. */
            /* FIXME   we may have to look it up by scanning the window map list */
            mdbm_logerror(LOG_ERR,0, "%s: get_window_page MDBM_PAGE_INDEX() not implemented",
                     db->db_filename);
            abort();
            /*pnum = MDBM_PAGE_INDEX(db,page); */
        }
    } else {
        pnum = pagenum;
    }
    if (pnum >= db->db_num_pages) {
        mdbm_log(LOG_CRIT, "%s: invalid pagenum (%d/%d) in get_window_page",
                 db->db_filename, pnum, db->db_num_pages);
        abort();
    }

#ifdef MDBM_WSTATUS
    {
        char buf[32];
        sprintf(buf,"gwp p=%d n=%d",pnum,npages);
        wstatus(db,buf);
    }
#endif

    /* loop over the buckets looking for one that contains the desired range */
    h = pnum % db->db_window.num_buckets;
    TAILQ_FOREACH(w,db->db_window.buckets+h,hash_link) {
        if (w->pagenum == pnum && match_mapped_window_page(w,off,ulen)) {
            wi = w - db->db_window.wpages;
            p = db->db_window.base + (uint64_t)wi*db->db_pagesize;
            db->db_window.num_reused++;
            if (MDBM_DO_STAT_TIME(db)) {
              mdbm_rstats_val_t* val = (db->db_rstats) ? &db->db_rstats->getpage : NULL;
              MDBM_ADD_STAT(db,val,db->db_get_usec() - t0, MDBM_STAT_TAG_GETPAGE);
            } else if (MDBM_DO_STATS(db)) {
              MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_GETPAGE);
            }
            return (mdbm_page_t*)p;
        }
    }

    if (db->db_window.num_pages - db->db_window.first_free < npages) {
        /* first free chunk isn't large enough, see if any later ones are. */
        int nfree = 0;
        for (i = 0; i < db->db_window.first_free;) {
            if (db->db_window.wpages[i].pagenum < 0) {
                if (!nfree) {
                    wi = i;
                }
                if (++nfree == npages) {
                    break;
                }
                ++i;
            } else {
                nfree = 0;
                i += db->db_window.wpages[i].num_pages;
            }
        }
        if (nfree < npages) {
            if (nfree + (db->db_window.num_pages - db->db_window.first_free) >= npages) {
                db->db_window.first_free += npages - nfree;
            } else {
                mdbm_log(LOG_ERR,
                         "%s: Unable to allocate %d window page(s): need a larger window size",
                         db->db_filename,npages);
#ifdef MDBM_WSTATUS
                wstatus(db,"");
#endif
                abort();
            }
        }
    } else {
        /* first free chunk is big enough... */
        wi = db->db_window.first_free;
        db->db_window.first_free += npages;
    }

    w = db->db_window.wpages+wi;

    w->pagenum = pnum;
    w->num_pages = npages;
    for (i = 1; i < npages; ++i) {
        db->db_window.wpages[wi+i].pagenum = pnum;
        db->db_window.wpages[wi+i].num_pages = 0;
    }
    TAILQ_INSERT_HEAD(db->db_window.buckets+h,w,hash_link);

    if (db->db_window.first_free > db->db_window.max_first_free) {
        db->db_window.max_first_free = db->db_window.first_free;
    }

    p = db->db_window.base + (uint64_t)wi*db->db_pagesize;
    {
        /* db page size in units of system pages */
        /*int map_syspg_unit = MDBM_NUM_PAGES_ROUNDED(db->db_sys_pagesize,db->db_pagesize); */
        /* db page size in bytes */
        /*int map_unit = map_syspg_unit * db->db_sys_pagesize; */
        int nsyspages = MDBM_NUM_PAGES_ROUNDED(db->db_sys_pagesize,npages*db->db_pagesize);
        uint64_t foff = ((uint64_t)pnum)*db->db_pagesize;
        size_t pgoff, flen;

        if (len > 0) {
            /* force pgoff and flen to be multiples of sys pagesize */
            /* this trick assumes/requires sys pagesize is a power of two */
            pgoff = off & ~(db->db_sys_pagesize-1);
            flen = ((off + len + db->db_sys_pagesize-1) & ~(db->db_sys_pagesize-1)) - pgoff;
        } else {
            pgoff = 0;
            flen = nsyspages*db->db_sys_pagesize;
        }
        w->mapped_off = pgoff;
        w->mapped_len = flen;

        /* size_t pgindex = (foff+pgoff) / db->db_sys_pagesize; */
        /*fprintf(stderr, "pseudo_remap_file_pages(%p,%llu,0,%llu,0); %d:%u/%u\n", */
        /*              p+pgoff,(unsigned long long)flen,(unsigned long long)pgindex, */
        /*              pnum,off,len); */
        if (remap_window_bytes(db, p, pgoff, flen, foff+pgoff, len ? 0 : MAP_NONBLOCK)) {
            errno = ENOMEM;
            return NULL;
        }
    }
    if (MDBM_DO_STAT_TIME(db)) {
        mdbm_rstats_val_t* val = (db->db_rstats) ? &db->db_rstats->getpage : NULL;
        MDBM_ADD_STAT(db,val,db->db_get_usec() - t0, MDBM_STAT_TAG_GETPAGE);
        val = (db->db_rstats) ? &db->db_rstats->getpage_uncached : NULL;
        MDBM_ADD_STAT(db,val,db->db_get_usec() - t0, MDBM_STAT_TAG_GETPAGE_UNCACHED);
    } else if (MDBM_DO_STATS(db)) {
        MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_GETPAGE);
        MDBM_ADD_STAT(db,NULL,0, MDBM_STAT_TAG_GETPAGE_UNCACHED);
    }
    db->db_window.num_remapped++;
#ifdef MDBM_WSTATUS
    wstatus(db,"gwp done");
#endif
    return (mdbm_page_t*)p;
}
#endif

#ifdef FREEBSD
static mdbm_page_t*
get_window_page(MDBM* db, mdbm_page_t* page, int pagenum, int npages, uint32_t off, uint32_t len)
{
    return NULL;
}
void
release_window_pages(MDBM* db)
{
}
void
release_window_page(MDBM* db, void* p)
{
}
#endif

#ifdef MDBM_BSOPS

typedef struct mdbm_bsop_file {
    MDBM* db;
    char path[MAXPATHLEN+1];
    char filename[MAXPATHLEN+1];
    int flags;
    int fd;
    int depth;
    int width;
    datum buf;

    int db_fd;
} mdbm_bsop_file_t;

static const char* MDBM_BSOP_FILE_PATH = "/tmp/mdbm_filebs";
static const char* MDBM_BSOP_FILE_DB = "/__DB__";

/* parameter opt is the filename (and not a directory) of the file-based backingstore being created */
void*
mdbm_bsop_file_init(MDBM* db, const char* filename, void* opt, int flags)
{
    mdbm_bsop_file_t* d;
    char* path;

    if (strlen(filename) > MAXPATHLEN) {
        errno = EINVAL;
        return NULL;
    }
    d = (mdbm_bsop_file_t*)malloc(sizeof(*d));
    if (d == NULL) {
        errno = EINVAL;
        return NULL;
    }

    if (opt == NULL) {
        struct stat st;
        char buf[MAXPATHLEN+16];

        strncpy(d->path, MDBM_BSOP_FILE_PATH, sizeof(d->path));
        d->path[sizeof(d->path)-1] = '\0';
        snprintf(d->filename, sizeof(d->filename), "%s%s", MDBM_BSOP_FILE_PATH, MDBM_BSOP_FILE_DB);
        snprintf(buf, sizeof(buf), "mkdir -p %s", d->path);
        system(buf);   /* Create directory, if necessary */
        if ((stat(d->path, &st) != 0) || !S_ISDIR(st.st_mode)) {
            errno = EBADF;
            free(d);
            return NULL;
        }
    } else if ((path = strrchr(opt, '/')) != NULL) {
        const char *opt_char_ptr = (const char *) opt;

        strncpy(d->path, opt, path - opt_char_ptr);
        d->path[path - opt_char_ptr] = '\0';
        strncpy(d->filename, opt, sizeof(d->filename));
    } else {
        errno = EINVAL;
        free(d);
        return NULL;
    }
    d->filename[sizeof(d->filename)-1] = '\0';

    d->db = db;
    d->fd = -1;
    d->depth = 2;
    d->width = 256;
    d->buf.dptr = NULL;
    d->buf.dsize = 0;
    d->flags = flags;

    if ((d->db_fd = open(d->filename,flags & (O_RDWR|O_CREAT|O_TRUNC),0660)) < 0) {
        free(d);
        return NULL;
    }

    return d;
}

int
mdbm_bsop_file_term(void* data, int flags)
{
    mdbm_bsop_file_t* d = (mdbm_bsop_file_t*)data;

    close(d->db_fd);
    if (d->fd >= 0) {
        close(d->fd);
    }
    if (d->buf.dptr) {
        free(d->buf.dptr);
    }
    free(d);
    return 0;
}

static char HEX[] = "0123456789abcdef";

static int
mdbm_bsop_file_open(mdbm_bsop_file_t* d, const datum* key, int flags)
{
    uint32_t h;
    int buflen = sizeof(d->filename);
    char* s;
    unsigned char* p;
    int i;
    int keylen;
    int do_mkdir = 0;

    for (;;) {
        h = htonl(d->db->db_hashfunc((const unsigned char*)key->dptr,key->dsize));

        strcpy(d->filename,d->path);
        s = d->filename + strlen(d->filename);

        if (do_mkdir && mkdir(d->filename,0775) < 0) {
            if (errno != EEXIST) {
                return -1;
            }
        }

        for (i = 0; i < d->depth; i++) {
            unsigned int v = h % d->width;
            int rem = buflen - (s - d->filename);
            int n;

            if ((n = snprintf(s,rem,"/%02x",v)) >= rem) {
                errno = EINVAL;
                return -1;
            }
            h /= d->width;
            s += n;
            if (do_mkdir && mkdir(d->filename,0775) < 0) {
                if (errno != EEXIST) {
                    return -1;
                }
            }
        }

        *(s++) = '/';
        keylen = key->dsize;
        for (p = (unsigned char*)key->dptr; keylen > 0; keylen--) {
            if (s >= d->filename+buflen-4) {
                errno = EINVAL;
                return -1;
            }
            if (*p == '/' || *p == '%' || !isprint(*p)) {
                *(s++) = '%';
                *(s++) = HEX[*p >> 4];
                *(s++) = HEX[*p & 0xf];
            } else {
                *(s++) = *p;
            }
            p++;
        }
        *s = 0;

        if (d->flags & MDBM_O_DIRECT) {
            flags |= O_DIRECT;
        }
        if ((d->fd = open(d->filename,flags,0664)) < 0
            && errno == ENOENT
            && !do_mkdir
            && (flags & O_CREAT))
        {
            do_mkdir = 1;
        } else {
            break;
        }
    }
    return d->fd;
}

int
mdbm_bsop_file_lock(void* data, const datum* key, int flags)
{
    mdbm_bsop_file_t* d = (mdbm_bsop_file_t*)data;
    int fd;
#define MDBM_BSOP_FILE_USE_FCNTL
#ifdef MDBM_BSOP_FILE_USE_FCNTL
    struct flock lock;
#endif
    int f;

    if (key && (flags == MDBM_LOCK_WRITE || flags == MDBM_LOCK_EXCLUSIVE)) {
        f = O_RDWR|O_CREAT;
    } else {
        f = O_RDONLY;
    }
    if (mdbm_bsop_file_open(d,key,f) < 0) {
        return -1;
    }

    if (flags == MDBM_LOCK_EXCLUSIVE || flags == MDBM_LOCK_SHARED) {
        fd = d->db_fd;
    } else {
        fd = d->fd;
    }

#ifdef MDBM_BSOP_FILE_USE_FCNTL
    lock.l_type = (flags == MDBM_LOCK_EXCLUSIVE || flags == MDBM_LOCK_WRITE) ? F_WRLCK : F_RDLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = 0;
    lock.l_len = 0;
    lock.l_pid = 0;
    return fcntl(fd,F_SETLKW,&lock);
#endif
#ifdef MDBM_BSOP_FILE_USE_FLOCK
    return flock(fd,
                 (flags == MDBM_LOCK_EXCLUSIVE || flags == MDBM_LOCK_WRITE) ? LOCK_EX : LOCK_SH);
#endif
}

int
mdbm_bsop_file_unlock(void* data, const datum* key)
{
    mdbm_bsop_file_t* d = (mdbm_bsop_file_t*)data;
    int old_fd;

    if (d->fd < 0) {
        errno = EBADF;
        return -1;
    }

    old_fd = d->fd;
    d->fd = -1;
    return close(old_fd);
}

static const int MDBM_FILEBUF_ALIGN = 512;

static off_t
mdbm_bsop_file_align_size(off_t sz)
{
    return (sz + MDBM_FILEBUF_ALIGN-1) & ~(MDBM_FILEBUF_ALIGN-1);
}

static char*
mdbm_bsop_file_align_ptr(char* p)
{
    return (char*)(((intptr_t)p+MDBM_FILEBUF_ALIGN-1) & ~(MDBM_FILEBUF_ALIGN-1));
}

int
mdbm_bsop_file_alloc_buf(mdbm_bsop_file_t* d, datum* buf, off_t sz)
{
    int misalign = (intptr_t)buf->dptr & (MDBM_FILEBUF_ALIGN-1);
    int pad = misalign ? MDBM_FILEBUF_ALIGN-misalign : 0;

    if (sz + pad > buf->dsize) {
        int old = buf->dsize;
        buf->dsize = sz+MDBM_FILEBUF_ALIGN-1;
        if ((buf->dptr = (char*)realloc(buf->dptr,buf->dsize)) == NULL) {
            errno = ENOMEM;
            return -1;
        }
        /* make valgrind happy about uninitialized padding */
        memset(buf->dptr+old, 0, buf->dsize - old);
       
    }

    return 0;
}

int
mdbm_bsop_file_fetch(void* data, const datum* key, datum* val, datum* buf, int flags)
{
    mdbm_bsop_file_t* d = (mdbm_bsop_file_t*)data;
    struct stat st;
    off_t sz;
    char* p;

    if (d->fd < 0) {
        errno = EBADF;
        return -1;
    }

    if (!buf) {
        buf = &d->buf;
    }

    if (fstat(d->fd,&st) < 0) {
        mdbm_logerror(LOG_ERR,0,"fstat(%d)",d->fd);
        return -1;
    }

    sz = mdbm_bsop_file_align_size(st.st_size);
    if (mdbm_bsop_file_alloc_buf(d,buf,sz) < 0) {
        return -1;
    }
    p = mdbm_bsop_file_align_ptr(buf->dptr);
    if (pread(d->fd,p,sz,0) < st.st_size) {
        mdbm_logerror(LOG_ERR,0,"pread(%d,%p,%lld,0)",d->fd,p,(long long)sz);
        return -1;
    }
    val->dptr = p;
    val->dsize = st.st_size;
    return 0;
}

int
mdbm_bsop_file_store(void* data, const datum* key, const datum* val, int flags)
{
    mdbm_bsop_file_t* d = (mdbm_bsop_file_t*)data;
    char* p;
    off_t sz;

    if (d->fd < 0) {
        errno = EBADF;
        return -1;
    }

    sz = mdbm_bsop_file_align_size(val->dsize);
    if ((intptr_t)val->dptr & (MDBM_FILEBUF_ALIGN-1)) {
        if (mdbm_bsop_file_alloc_buf(d,&d->buf,sz) < 0) {
            return -1;
        }
        p = mdbm_bsop_file_align_ptr(d->buf.dptr);
        memcpy(p,val->dptr,val->dsize);
    } else {
        p = val->dptr;
    }

    if (pwrite(d->fd,p,sz,0) < sz) {
        mdbm_logerror(LOG_ERR,0,"pwrite(%d,%p,%lld,0)",d->fd,p,(long long)sz);
        return -1;
    }

    if (sz > val->dsize) {
        if (ftruncate(d->fd,val->dsize) < 0) {
            mdbm_logerror(LOG_ERR,0,"ftruncate(%d,%lld)",d->fd,(long long)val->dsize);
            return -1;
        }
    }

    return 0;
}

int
mdbm_bsop_file_delete(void* data, const datum* key, int flags)
{
    mdbm_bsop_file_t* d = (mdbm_bsop_file_t*)data;

    if (d->fd < 0) {
        errno = EBADF;
        return -1;
    }

    return unlink(d->filename);
}

void*
mdbm_bsop_file_dup(MDBM* db, MDBM* newdb, void* data)
{
    mdbm_bsop_file_t* new_d = (mdbm_bsop_file_t*)malloc(sizeof(*new_d));
    memcpy(new_d,data,sizeof(*new_d));
    new_d->db = newdb;
    new_d->fd = -1;
    return new_d;
}

mdbm_bsops_t mdbm_bsops_file = {
    mdbm_bsop_file_init,
    mdbm_bsop_file_term,
    mdbm_bsop_file_lock,
    mdbm_bsop_file_unlock,
    mdbm_bsop_file_fetch,
    mdbm_bsop_file_store,
    mdbm_bsop_file_delete,
    mdbm_bsop_file_dup
};

typedef struct mdbm_bsop_mdbm {
    MDBM* db;
    MDBM* bsdb;
} mdbm_bsop_mdbm_t;

void*
mdbm_bsop_mdbm_init(MDBM* db, const char* filename, void* opt, int flags)
{
    mdbm_bsop_mdbm_t* d;

    d = (mdbm_bsop_mdbm_t*)malloc(sizeof(*d));
    d->db = db;
    d->bsdb = (MDBM*)opt;

    return d;
}

int
mdbm_bsop_mdbm_term(void* data, int flags)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;

    if (d->bsdb) {
        mdbm_close(d->bsdb);
    }
    free(d);
    return 0;
}

int
mdbm_bsop_mdbm_lock(void* data, const datum* key, int flags)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;
    int ret;

    if (MDBM_RWLOCKS(d->bsdb)) {
        if (flags == MDBM_LOCK_WRITE) {
            ret = mdbm_lock(d->bsdb);
        } else {
            ret = mdbm_lock_shared(d->bsdb);
        }
    } else if (key) {
        ret = mdbm_plock(d->bsdb,key,0);
    } else {
        ret = mdbm_lock(d->bsdb);
    }
    return (ret == 1) ? 0 : -1;
}

int
mdbm_bsop_mdbm_unlock(void* data, const datum* key)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;
    int ret;

    if (key) {
        ret = mdbm_punlock(d->bsdb,key,0);
    } else {
        ret = mdbm_unlock(d->bsdb);
    }
    return (ret == 1) ? 0 : -1;
}

int
mdbm_bsop_mdbm_fetch(void* data, const datum* key, datum* val, datum* buf, int flags)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;
    datum k;
    MDBM_ITER iter;

    k = *key;
    return mdbm_fetch_r(d->bsdb,&k,val,&iter);
}

int
mdbm_bsop_mdbm_store(void* data, const datum* key, const datum* val, int flags)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;
    datum k, v;
    MDBM_ITER iter;

    k = *key;
    v = *val;
    return mdbm_store_r(d->bsdb,&k,&v,flags,&iter);
}

int
mdbm_bsop_mdbm_delete(void* data, const datum* key, int flags)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;

    return mdbm_delete(d->bsdb,*key);
}

void*
mdbm_bsop_mdbm_dup(MDBM* db, MDBM* newdb, void* data)
{
    mdbm_bsop_mdbm_t* d = (mdbm_bsop_mdbm_t*)data;
    mdbm_bsop_mdbm_t* new_d = (mdbm_bsop_mdbm_t*)malloc(sizeof(*d));

    new_d->db = newdb;
    new_d->bsdb = mdbm_dup_handle(d->bsdb,0);
    return new_d;
}

mdbm_bsops_t mdbm_bsops_mdbm = {
    mdbm_bsop_mdbm_init,
    mdbm_bsop_mdbm_term,
    mdbm_bsop_mdbm_lock,
    mdbm_bsop_mdbm_unlock,
    mdbm_bsop_mdbm_fetch,
    mdbm_bsop_mdbm_store,
    mdbm_bsop_mdbm_delete,
    mdbm_bsop_mdbm_dup
};


int
mdbm_set_backingstore(MDBM* db, const mdbm_bsops_t* bsops, void* opt, int flags)
{
    /* Validity check: make sure cache and BS are not the same */
    if ((db == opt) ||
        ((bsops == MDBM_BSOPS_MDBM) && strcmp(db->db_filename,((MDBM *)opt)->db_filename) == 0)) {

        errno = EINVAL;
        return -1;
    }

    if (bsops == MDBM_BSOPS_FILE) {
        bsops = &mdbm_bsops_file;
    } else if (bsops == MDBM_BSOPS_MDBM) {
        bsops = &mdbm_bsops_mdbm;
    } else if (bsops == NULL) {
        errno = EINVAL;
        return -1;
    }

    if ((db->db_bsops_data = bsops->bs_init(db,db->db_filename,opt,flags)) == NULL) {
        return -1;
    }
    db->db_bsops = bsops;
    return 0;
}


#if MDBM_VALIDATE_NOTYET

int
mdbm_validate_r(MDBM* db, MDBM_ITER* iter)
{
    int ret;
    mdbm_entry_data_t e;
    mdbm_entry_t* ep;
    mdbm_pagenum_t pagenum;
    int index;

    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return -1;
    }

    if (!iter || !db->db_bsops) {
        errno = EINVAL;
        return -1;
    }

    pagenum = iter->m_pageno;
    index = (iter->m_next+3)/-2;

    if (pagenum > db->db_max_dirbit || index < 0) {
        errno = EINVAL;
        return -1;
    }

    if (mdbm_internal_do_lock(db,MDBM_LOCK_WRITE,MDBM_LOCK_WAIT,NULL,&pagenum) < 0) {
        return -1;
    }

    if ((ep = set_entry(db,pagenum,index,&e)) == NULL || !ep->e_key.match) {
        errno = ENOENT;
        ret = -1;
    } else {
        datum key, val;
        datum v;

        ret = 0;
        get_kv(db,&e,&key,&val);
        if (db->db_bsops->bs_lock(db->db_bsops_data,&key,MDBM_BSOPS_LOCK_READ) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm backing store locking error");
            ret = -1;
        } else {
            if (db->db_bsops->bs_fetch(db->db_bsops_data,&key,&v,NULL,0) < 0) {
                if (errno == ENOENT) {
                    del_entry(db,e.e_page,ep);
                } else {
                    mdbm_logerror(LOG_ERR,0,"mdbm backing store fetch error");
                    ret = -1;
                }
            } else if (v.dsize != val.dsize || memcmp(v.dptr,val.dptr,v.dsize)) {
                datum k = key;
                if (mdbm_store_r(db,&k,&v,MDBM_REPLACE|MDBM_CLEAN|MDBM_CACHE_ONLY,iter) < 0
                    && errno != ENOMEM && errno != EINVAL))
                {
                    mdbm_logerror(LOG_ERR,0,"mdbm backing store cache refill error");
                    ret = -1;
                }
            }
            db->db_bsops->bs_unlock(db->db_bsops_data,&key);
        }
    }

    mdbm_internal_do_unlock(db);

    return ret;
}

int
mdbm_validate_db(MDBM* db)
{
    int ret = 0;
    MDBM_ITER iter;
    kvpair kv;

    kv = mdbm_first_r(db,&iter);
    while (kv.key.dptr) {
        if (mdbm_validate_r(db,&iter) < 0) {
            ret = -1;
            break;
        }
        kv = mdbm_next_r(db,&iter);
    }

    return ret;
}

#endif

#endif


MDBM*
mdbm_dup_handle(MDBM* db, int flags)
{
    MDBM* newdb;
    int wsize;

    if (db->db_ver_flag != MDBM_DBVER_3) {
        errno = ENOSYS;
        return NULL;
    }

    if (lock_db_ex(db) < 0) {
        return NULL;
    }

    newdb = (MDBM*)malloc(sizeof(*newdb));
    memcpy(newdb,db,sizeof(*newdb));

    /*init_locks(newdb); */

    if (!MDBM_NOLOCK(db)) {
        int need_check = 0;
        int lflags =
            db_is_multi_lock(db)
            ? (MDBM_RWLOCKS(db) ? MDBM_RW_LOCKS : MDBM_PARTITIONED_LOCKS)
            : 0;
        lflags |= MDBM_ANY_LOCKS;
        lflags |= (flags&MDBM_LOCK_MASK);
        if (db->db_flags & MDBM_SINGLE_ARCH) {
          lflags |= MDBM_SINGLE_ARCH;
        }
        if (db->db_flags & MDBM_DBFLAG_MEMONLYCACHE) {
          lflags |= MDBM_DBFLAG_MEMONLYCACHE;
        }
        if (mdbm_internal_open_locks(newdb,lflags,0,&need_check) < 0) {
            goto dup_error;
        }
    } else {
      newdb->db_flags |= MDBM_DBFLAG_NOLOCK;
    }

    newdb->db_dir = NULL;
    newdb->db_errno = 0;

#ifdef MDBM_BSOPS
    if (newdb->db_bsops) {
        newdb->db_bsops_data = newdb->db_bsops->bs_dup(db,newdb,newdb->db_bsops_data);
    }
#endif

    if (!db->db_dup_info) {
        newdb->db_dup_info = db->db_dup_info = (mdbm_dup_info_t*)malloc(sizeof(*db->db_dup_info));
        newdb->db_dup_map_gen = db->db_dup_map_gen = db->db_dup_info->dup_map_gen = 1;
        db->db_dup_info->nrefs = 1;
    }

    sync_dir(newdb,NULL);

    inc_dup_ref(newdb);

    unlock_db(db);

    wsize = newdb->db_window.num_pages * db->db_pagesize;
    memset(&newdb->db_window,0,sizeof(newdb->db_window));
    if (wsize) {
        mdbm_set_window_size_internal(newdb,wsize);
    }
    return newdb;

 dup_error:
    unlock_db(db);
    mdbm_internal_close_locks(newdb);
    free(newdb);
    return NULL;
}

void
update_dup_map_gen(MDBM* db)
{
    uint64_t map_gen = dup_map_gen_start(db);
    db->db_dup_info->dup_fd = db->db_fd;
    db->db_dup_info->dup_base = db->db_base;
    db->db_dup_info->dup_base_len = db->db_base_len;
    dup_map_gen_end(db,map_gen);
}

void
sync_dup_map_gen(MDBM* db)
{
    if (db->db_dup_map_gen != db->db_dup_info->dup_map_gen) {
        /* This is a spin-lock to wait for dup_map_gen_end() */
        while (db->db_dup_info->dup_map_gen_marker != db->db_dup_info->dup_map_gen) {
            /* Note: this should probably be
             *   __asm__ __volatile__ ("pause" : : : "memory");
             * since we're waiting for a structure to change. */
            atomic_pause();
        }
        db->db_fd = db->db_dup_info->dup_fd;
        db->db_base = db->db_dup_info->dup_base;
        db->db_base_len = db->db_dup_info->dup_base_len;
        db->db_hdr = (mdbm_hdr_t*)(db->db_base + MDBM_PAGE_T_SIZE);
        init(db,db->db_hdr);
        db->db_dup_map_gen = db->db_dup_info->dup_map_gen;
    }
}


extern void print_lock_state(MDBM* db);


static void
print_data_bytes(datum d, int print_all)
{
    int i, len = d.dsize;
    int max = 16;
    const char* s;

    if (!print_all) {
        s = getenv("MDBM_DUMP_BYTES");
        if (s) { max = atoi(s); }
        if (len > max) { len = max; }
    }
    s = d.dptr;
    putchar('\n');
    while (len > 0) {
        fputs("    ",stdout);
        for (i = 0; i < 16; i++) {
            if (i == 8) { putchar(':'); }
            if (i < len) {
                printf("%02x:",(unsigned char)(s[i]));
            } else {
                fputs("   ",stdout);
            }
        }
        fputs("  ",stdout);
        for (i = 0; i < 16; i++) {
            if (i < len) {
                if (s[i] > ' ' && s[i] < 0x7f) {
                    putchar(s[i]);
                } else {
                    putchar('.');
                }
            }
        }
        s += 16;
        len -= 16;
        putchar('\n');
    }
}



typedef struct mdbm_v3_dump_data {
  int max_page;
  int last_page;
  MDBM* db;
} mdbm_v3_dump_data_t;

static int
dump_v3_page (void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
  mdbm_v3_dump_data_t* v3dd = (mdbm_v3_dump_data_t*) user;
  MDBM* db = v3dd->db;
  const mdbm_page_info_t* pi = &info->i_page;
  const mdbm_entry_info_t* ei = &info->i_entry;
  int pno = pi->page_num;
  int idx = ei->entry_index;
  datum key = kv->key;
  datum val = kv->val;
  int deleted=0;
  if ((v3dd->max_page >= 0) && (v3dd->max_page < pno)) {
    return 1; /* passed our single page, stop iterating */
  }

  if (v3dd->last_page != pno) {
    /* start-of-page, print page header */
    int avg = 0;
    if (info->i_page.page_active_entries) {
        avg = (info->i_page.page_active_space / info->i_page.page_active_entries);
    }
    printf("PAGE=%u addr=%p entries=%u (%u bytes),"
           " free=%u deleted=%u sizes=%u/%u/%u\n",
        pno, (void*)pi->page_addr,pi->page_active_entries, info->i_page.page_active_space,
        pi->page_free_space, pi->page_deleted_space,
        pi->page_min_entry_size, avg, pi->page_max_entry_size
        );
    v3dd->last_page = pno;
  }


  printf("  KEY[%u] sz=%-5u pg+off=%u+%u off=%-10ld",
         idx, key.dsize, pno, ei->key_offset, (long)(key.dptr-db->db_base));
  if (key.dsize) {
      printf(" addr=%p = ", key.dptr);
      print_data_bytes(key,0);
  /*} else if (PNO(db,pag) == 0 && i == -1) { */
  /*    printf(" [header]\n"); */
  } else if (ei->entry_flags & MDBM_ENTRY_DELETED) {
      printf(" [deleted]\n");
      deleted = 1;
  } else {
      printf(" [unknown]\n");
  }

  printf("  VAL[%u] sz=%-5u pg+off=%u+%u addr=%p = ",
         idx, (deleted ? 0 : val.dsize), pno, ei->val_offset, (deleted ? NULL : val.dptr));
  if (key.dsize) {
      print_data_bytes(val,0);
  } else if (ei->entry_flags & MDBM_ENTRY_DELETED) {
      printf(" [deleted]\n");
  } else if (ei->entry_flags) {
      printf(" [unknown]\n");
  } else {
      printf(" [empty]\n");
  }

  return 0; /* continue iterating */
}

void
mdbm_dump_all_page(MDBM *db)
{
    /* XXX */
    mdbm_v3_dump_data_t v3dd = {-1, -1, NULL};
    v3dd.db=db;
    mdbm_iterate(db,-1,dump_v3_page,MDBM_ITERATE_ENTRIES,&v3dd);
}

void
mdbm_dump_page(MDBM *db, int pno)
{
    /* XXX */
    mdbm_v3_dump_data_t v3dd = {-1, -1, NULL};
    v3dd.max_page=pno;
    v3dd.db=db;
    mdbm_iterate(db,pno,dump_v3_page,MDBM_ITERATE_ENTRIES,&v3dd);
}


void
mdbm_lock_dump(MDBM* db)
{
  print_lock_state(db);
}

int
mdbm_lock_reset(const char* dbfilename, int flags)
{
  return do_lock_reset(dbfilename, flags);
}


/* Returns the partition number for a key.
 *
 * \param[in] db mdbm handle
 * \param[in] db key datum containing key
 * \return partition number or -1 upon error
 */
int
mdbm_get_partition_number(const MDBM* db, datum key)
{
    mdbm_hashval_t hashValue;
    mdbm_pagenum_t pageNumber;
    int partitionNumber;

    if (db == NULL || key.dptr == NULL || key.dsize == 0) {
        errno = EINVAL;
        return -1;
    }

    if (db_get_lockmode((MDBM*)db) != MDBM_PARTITIONED_LOCKS) {
        mdbm_logerror(LOG_ERR,0,"%s: wrong lockmode.  Partition locking is required.", db->db_filename);
        errno = EINVAL;
        return -1;
    }

    hashValue       = MDBM_HASH_VALUE(db, key.dptr, key.dsize);
    pageNumber      = hashval_to_pagenum(db, hashValue);
    partitionNumber = MDBM_PAGENUM_TO_PARTITION((MDBM*)db, pageNumber);
    return partitionNumber;
}

/*
 * Given a hash function code, get the hash value for the given key.
 *
 * \param[in] db key datum containing key
 * \param[in] hashFunctionCode for a valid hash function, ex: MDBM_HASH_MD5, MDBM_HASH_SHA_1, etc.
 * \param[out] hashValue is calculated for the specified key and hash function
 * \return 0 upon success or -1 upon error
 */
int
mdbm_get_hash_value(datum key, int hashFunctionCode, uint32_t *hashValue)
{
    mdbm_hash_t hashfunc;
    if (hashFunctionCode < 0 || hashFunctionCode > MDBM_MAX_HASH
        || mdbm_hash_funcs[hashFunctionCode] == NULL || hashValue == NULL) {
        errno = EINVAL;
        return -1;
    }

    hashfunc   = mdbm_hash_funcs[hashFunctionCode];
    *hashValue = hashfunc((uint8_t*)key.dptr, key.dsize);
    return 0;
}


/* mdbm_lock_smart()
 *
 * mdbm_open(MDBM_PARTITIONED_LOCKS) means mdbm_lock_smart calls mdbm_plock,
 * mdbm_open(MDBM_RW_LOCKS) means mdbm_lock_smart calls mdbm_lock_shared for reads, and
 * mdbm_open() for all other locking flags or for writes, mdbm_lock_smart will call mdbm_lock.
 */

int
mdbm_lock_smart(MDBM *db, const datum *key, int flags)
{
    if (db == NULL) {
        return -1;
    } else {
      uint32_t mode = mdbm_get_lockmode(db);

      if (mode == MDBM_PARTITIONED_LOCKS) {
          return mdbm_plock(db, key, flags);
      } else if (mode == MDBM_RW_LOCKS) {
          if (!(flags & MDBM_O_RDWR)) {
              return mdbm_lock_shared(db);   /* Shared if performing a read */
          } else {
              return mdbm_lock(db);          /* exclusive lock for writes */
          }
      } else if (mode == 0) {   /* Exclusive lock */
          return mdbm_lock(db);
      }

      return 1;   /* NOLOCK */
    }
}

/* mdbm_trylock_smart()
 *
 * mdbm_open(MDBM_PARTITIONED_LOCKS) means mdbm_trylock_smart calls mdbm_tryplock,
 * mdbm_open(MDBM_RW_LOCKS) means mdbm_trylock_smart calls mdbm_trylock_shared for reads, and
 * mdbm_open() for all other locking flags or for writes, mdbm_trylock_smart will call mdbm_trylock.
 */

int
mdbm_trylock_smart(MDBM *db, const datum *key, int flags)
{
    if (db == NULL) {
        return -1;
    } else {
      uint32_t mode = mdbm_get_lockmode(db);

      if (mode == MDBM_PARTITIONED_LOCKS) {
          return mdbm_tryplock(db, key, flags);
      } else if (mode == MDBM_RW_LOCKS) {
          if (!(flags & MDBM_O_RDWR)) {
              return mdbm_trylock_shared(db);   /* Shared if performing a read */
          } else {
              return mdbm_trylock(db);          /* exclusive trylock for writes */
          }
      } else if (mode == 0) {   /* Exclusive trylock */
          return mdbm_trylock(db);
      }

      return 1;   /* NOLOCK */
    }
}

/* mdbm_unlock_smart()
 *
 * mdbm_open(MDBM_PARTITIONED_LOCKS) means mdbm_lock_smart calls mdbm_punlock,
 * otherwise calls mdbm_unlock
 */

int mdbm_unlock_smart(MDBM *db, const datum *key, int flags)
{
    if (db == NULL) {
        return -1;
    } else {
      uint32_t mode = mdbm_get_lockmode(db);

      if (mode == MDBM_PARTITIONED_LOCKS) {
          return mdbm_punlock(db, key, flags);
      } else if ((mode == MDBM_RW_LOCKS) || (mode == 0)) {
          return mdbm_unlock(db);   /* To unlock shared and exclusive locks */
      }

      return 1;   /* NOLOCK */
    }
}


#ifdef FREEBSD
int
remap_is_limited(uint32_t sys_pagesize)
{
    return 1;   /* Windowed mode is not supported by FreeBSD */
}
#else    /* Not FreeBSD (RHEL) */

/* open_tmp_test_file is a helper used by remap_is_limited to open the test file. */
/* Returns file descriptor if successful, -1 otherwise */

int
open_tmp_test_file(const char *file, uint32_t siz, char *buf)
{
    int created = 0, fd = -1;

    snprintf(buf, MAXPATHLEN, "%s%d.map", file, gettid()); /* assuming buf is big enough */

    fd = open(buf, O_RDWR|O_NOATIME);
    if (fd < 0) {
        created = 1;
        fd = open(buf, O_RDWR|O_CREAT|O_TRUNC|O_NOATIME, 0664);
        if (fd < 0) {
            return -1;
        }
    }

    /* expand the file to the proper size: write a "\0" byte to force the expansion */
    if (created) {
        if ((lseek(fd, siz-1, SEEK_SET) < 0) || (write(fd, "", 1) != 1)) {
            mdbm_logerror(LOG_ERR,0,"%s: Windowing remap test, cannot expand",buf);
            close(fd);
            return -1;
        }
    }
    return fd;
}


static inline void
close_unmap_unlink(uint8_t *map, uint32_t size, int fd, char *fname)
{
    if ((map != NULL) && (munmap(map, size) != 0)) {
        mdbm_logerror(LOG_ERR,0,"%s: Could not munmap", fname);
    }
    if (close(fd) != 0) {
        mdbm_logerror(LOG_ERR,0,"%s: Could not close", fname);
    }
    if (unlink(fname) != 0) {
        mdbm_logerror(LOG_ERR,0,"%s: Could not unlink", fname);
    }
}

/* for use by remap_is_limited().
 * -1 means unknown, 0 means not limited (RHEL4), 1 means limited(RHEL6)
 */
static sig_atomic_t remap_limited_status = -1;


/* remap_is_limited() - for internal use by mdbm_open_internal
 *
 * Tests whether MDBM V3 is running on RHEL 6 (in a 4-on-6 yroot).
 * Needed because we have to fail mdbm_open if opened with WINDOWED_MODE on RHEL6.
 *
 * Returns 1 if remap_file_pages is limited (RHEL6), or 0 if not (RHEL4).
 *
 * remap_limited_status: -1 means unknown, 0 means not limited (RHEL4), 1 means limited(RHEL6)
 */

int
remap_is_limited(uint32_t sys_pagesize)
{
    int ret, fd = -1;
    uint8_t* map = NULL;
    char *fname = "/tmp/remaptest";
    uint32_t size = sys_pagesize + sys_pagesize; /* 2 pages needed in total */
    char fnbuf[MAXPATHLEN];  /* File name buffer */

    if (remap_limited_status >= 0) {
        return remap_limited_status;   /* remap test previously performed: return result */
    }

    /* remap_limited_status < 0 - create test file */

    if ((fd = open_tmp_test_file(fname, size, fnbuf)) < 0) {
        fname = "/tmp/remaptest2";
        if ((fd = open_tmp_test_file(fname, size, fnbuf)) < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: Windowing remap test, cannot open", fname);
            return 0;   /* Things are probably very broken, so something else will fail soon */
        }
    }

    map = (uint8_t*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        mdbm_logerror(LOG_ERR,0,"%s: Windowing remap test, cannot mmap (%d)", fnbuf, errno);
        close_unmap_unlink(NULL, 0, fd, fnbuf);
        return 1;  /* shouldn't fail: return "limited" but don't update remap_limited_status */
    }

    /*int remap_file_pages(void *addr, size_t size, int prot, ssize_t pgoff, int flags); */
    if (remap_file_pages((void*)map, size, 0, 0, 0) != 0) {   /* map 2 pages first */
        mdbm_logerror(LOG_ERR,0,"%s: Windowing remap test, cannot remap first 2 pages",
                      fnbuf);
        close_unmap_unlink(map, size, fd, fnbuf);
        return 1;  /* return "limited" but don't update remap_limited_status */
    }

    /* map 2nd file-page to offset 0 */
    if (remap_file_pages((void*)map, sys_pagesize, 0, 1, 0) != 0) {
        mdbm_logerror(LOG_ERR,0,"%s: Windowing remap test, cannot remap second page",
                      fnbuf);
        close_unmap_unlink(map, size, fd, fnbuf);
        return 1;  /* return "limited" but don't update remap_limited_status */
    }

    /* Now try remapping both pages again - fails under RHEL6 but not RHEL4 */
    ret = remap_file_pages((void*)map, size, 0, 0, 0);

    remap_limited_status = (ret == 0) ?
        0 : /* Success means ret=0 (RHEL4) - remap_file_pages is not limited */
        1; /* RHEL6 - remap_file_pages is limited */

    close_unmap_unlink(map, size, fd, fnbuf);
    return remap_limited_status;
}

#endif  /* not FREEBSD (RHEL) */

static int
page_counter_func(void *user, const mdbm_chunk_info_t *info)
{
    mdbm_ubig_t *counter = (mdbm_ubig_t *) user;

    switch (info->page_type) {
        case MDBM_PTYPE_DATA:
        case MDBM_PTYPE_DIR:
        case MDBM_PTYPE_LOB:
            *counter += info->num_pages;
            break;
        default:
            break;
    }
    return 0;
}

mdbm_ubig_t
mdbm_count_pages(MDBM *db)
{
    mdbm_ubig_t used_pages = 0;

    if (mdbm_chunk_iterate(db, page_counter_func, 0, &used_pages) < 0) {
        mdbm_logerror(LOG_ERR, 0, "%s: page count with mdbm_chunk_iterate failed", db->db_filename);
        return -1;
    }

    return used_pages;
}



void
print_page_table(MDBM *db)
{
    int i, pnum;

    printf("\nPrinting MDBM Page Table:\n");

    for (i = 0; i <= db->db_max_dirbit; i++) {
        if ((pnum = db->db_ptable[i].pt_pagenum) != 0) {
            if (pnum <= db->db_hdr->h_last_chunk) {
                mdbm_page_t* page = MDBM_PAGE_PTR(db,pnum);
                if ((page->p_type == MDBM_PTYPE_DATA) && (page->p_num == i)) {
                     printf("Page table entry %d is data page %d, size=%d at %p\n",
                            i, pnum, page->p_num_pages, (void*)page);
                }
            }
        }
    }
}

void
mdbm_internal_print_free_list(MDBM *db)
{
    int pnum;

    printf("\nPrinting MDBM Free Page List:\n");

    pnum = 0;
    while (pnum < db->db_hdr->h_num_pages) {
        mdbm_page_t* page = MDBM_PAGE_PTR(db,pnum);
        if (page->p_type == MDBM_PTYPE_FREE) {
            printf("Free Page %d is at address %p\n", pnum, (void*)page);
        }
        if (page->p_num_pages) {
            pnum += page->p_num_pages;
        } else {
            ++pnum;
        }
    }
}

/* mdbm_preload () - for performance inmprovement
 * Read every 4k bytes to force all pages into memory
 * Returns 0 on success  return -1 on fail
 */
int mdbm_preload(MDBM* db)
{
    mdbm_page_t* page;
    mdbm_page_t* lob;
    mdbm_entry_lob_t* lp;
    mdbm_entry_t* ep;
    mdbm_entry_data_t e;
    int i = 0;
    int pagesize, pagesizeTemp, lobpagesize;
    volatile int dummy=0;
    (void)dummy; /* suppress unused variable warning. */

    if (db == NULL) {
        mdbm_logerror(LOG_ERR,0,"database is NULL");
        return -1;
    }

    if (MDBM_IS_WINDOWED(db)) {
        mdbm_logerror(LOG_ERR,0,"operation does not support windowed mode");
        return -1;
    }
    pagesize = db->db_pagesize;
    for (i=0; i <= db->db_max_dirbit; i++) {
        page = pagenum_to_page(db,i,MDBM_PAGE_NOALLOC,MDBM_PAGE_NOMAP);
        if (page == NULL) {
           continue;
        }
        if (pagesize <= db->db_sys_pagesize){
           dummy = *(int*)(char*)page;
        }
        else {
           /* read every 4K pages */
           pagesizeTemp = 0;
           while (pagesizeTemp < pagesize) {
              dummy = *(int*)((char*)page + pagesizeTemp);
              pagesizeTemp = pagesizeTemp + db->db_sys_pagesize;
           }
        }
        /* large object memory read */
        for (ep = first_entry(page,&e); ep; ep = next_entry(&e)) {
           if (!ep->e_key.match) {
              /* Deleted entry. */
           } else if (MDBM_ENTRY_LARGEOBJ(ep)) {
              lp = MDBM_LOB_PTR1(db,page,ep);
              lob = MDBM_PAGE_PTR(db,lp->l_pagenum);
              lobpagesize = lob->p_num_pages * pagesize;
              if (lobpagesize <= db->db_sys_pagesize) {
                 dummy = *(int*) (char*)lob;
              }
              else {
                 pagesizeTemp = 0;
                 while (pagesizeTemp < lobpagesize) {
                    dummy = *(int*)((char*)lob + pagesizeTemp);
                    pagesizeTemp = pagesizeTemp + db->db_sys_pagesize;
                 }
              }
           }
        }
    }
    return 0;
}

/* mlock() does all the rounding of sizes to system page size internally */

static int
pin_pages(MDBM *db)
{
    int ret = 0;
    uint32_t pgsz = db->db_pagesize;
    size_t hdr_size, db_size, rounded;
    struct rlimit rlim;

    /* Need a separate mlock() for hdr since for short hdrs and longer pages such as 8K or more,
     * mlock() only allocates 4K for the hdr, and that cannot be allocated continguously with
     * the MDBM data pages */
    hdr_size = MDBM_DB_NUM_DIR_BYTES(db);
    if (mlock(db->db_base, hdr_size) < 0) {
        mdbm_logerror(LOG_ERR, 0, "Cannot mlock header (%p,%llu)",
                      db->db_base, (unsigned long long) hdr_size);
        return -1;
    }

    /* Round hdr size to next MDBM pagesize */
    rounded = (MDBM_NUM_PAGES_ROUNDED(pgsz, hdr_size)) * pgsz;
    db_size = MDBM_DB_MAP_SIZE(db) - rounded;

    rlim.rlim_cur = RLIM_INFINITY;
    rlim.rlim_max = RLIM_INFINITY;
    if ((setrlimit(RLIMIT_MEMLOCK, &rlim) < 0) && (getrlimit(RLIMIT_MEMLOCK, &rlim) == 0)) {
        uint32_t dbpg = db->db_sys_pagesize;
        size_t avail = rlim.rlim_cur -
                       (MDBM_NUM_PAGES_ROUNDED(dbpg, MDBM_DB_NUM_DIR_BYTES(db)) * dbpg);
        if (avail <= 0) {
            mdbm_log(LOG_WARNING,
                     "mdbm_lock_pages cannot mlock MDBM due to a resource limit of %lluK",
                     (unsigned long long) rlim.rlim_cur / 1024);
            return 0;
        }

        if (db_size > avail) {
            mdbm_log(LOG_WARNING,
                "mdbm_lock_pages is unable to mlock entire MDBM, locking only %lluK out of %lluK",
                (unsigned long long) avail / 1024, (unsigned long long) avail / 1024);
            db_size = avail;
        }
    }

    if ((ret = mlock(MDBM_PAGE_PTR(db, 1), db_size)) < 0) {  /* skip 1st page: hdr */
        mdbm_logerror(LOG_ERR, 0, "Cannot mlock MDBM data (%p,%llu)",
                      (void*)MDBM_PAGE_PTR(db, 1), (unsigned long long) db_size);
    }

    return ret;
}

static int
unlock_pages(MDBM *db)
{
    int ret = 0;
    uint32_t pgsz = db->db_pagesize;
    size_t hdr_size, db_size, rounded;

    if (!(db->db_flags & MDBM_DBFLAG_LOCK_PAGES)) {
        return 0;
    }

    hdr_size = MDBM_DB_NUM_DIR_BYTES(db);
    if ((ret = munlock(db->db_base, hdr_size)) < 0) {
        mdbm_logerror(LOG_ERR, 0, "Cannot munlock hdr(%p,%llu)",
                      db->db_base, (unsigned long long) hdr_size);
    }

    /* Round hdr size to next MDBM pagesize */
    rounded = (MDBM_NUM_PAGES_ROUNDED(pgsz, hdr_size)) * pgsz;
    db_size = MDBM_DB_MAP_SIZE(db) - rounded;
    if ((ret = munlock(MDBM_PAGE_PTR(db, 1), db_size)) < 0) {  /* skip 1st page: hdr */
        mdbm_logerror(LOG_ERR, 0, "Cannot munlock page(%p,%llu)",
                      (void*)MDBM_PAGE_PTR(db, 1), (unsigned long long) db_size);
    }

    return ret;
}

int
mdbm_lock_pages(MDBM *db)
{
    if (db == NULL) {
        return -1;
    }

    if (MDBM_IS_WINDOWED(db)) {   /* Won't fit */
        mdbm_log(LOG_WARNING,
                 "mdbm_lock_pages does not support windowed mode");
        return -1;
    }

    db->db_flags |= MDBM_DBFLAG_LOCK_PAGES;
    return pin_pages(db);
}

int
mdbm_unlock_pages(MDBM *db)
{
    int ret;

    if (db == NULL) {
        return -1;
    }

    ret = unlock_pages(db);
    db->db_flags &= ~MDBM_DBFLAG_LOCK_PAGES;

    return ret;
}

/* Library constructor of the timestamp counter performance measurement code */
void __attribute__ ((constructor)) mdbm_perf_tsc_init(void);

void mdbm_perf_tsc_init(void)
{
    last_usec = get_gtod_usec();
    hi_tsc = last_tsc = rdtsc();
    next_gtod_tsc = tsc_per_usec = 0;
}

