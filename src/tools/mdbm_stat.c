/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_util.h"

#define FREE_PAGE_FLAG     "freepg"    /* Option to the "-i" command line arg */
#define ENTRY_COUNT_FLAG   "ecount"    /* Option to the "-i" command line arg */
#define RESIDEN_COUNT_FLAG "resident"  /* Option to the "-i" command line arg */

typedef struct {
    uint32_t data;
    uint32_t dir;
    uint32_t free;
    uint32_t lob;
} page_type_chunk_counters_t;

/* For each oversized "size" (oversized=2,3,4... pages), figure out the following information: */
typedef struct {
    uint32_t size;   /* How large, in pages, is this set of oversized pages */
    uint32_t count;  /* How many such oversized pages were found */
    uint32_t entry_count;      /* Running counter, for all pages with oversized=2,3,4... */
    uint32_t min_used_bytes;
    uint32_t max_used_bytes;
    uint64_t total_used_bytes;   /* Running total, for all pages with oversized=2,3,4... */
    uint32_t min_free_bytes;
    uint32_t max_free_bytes;
    uint64_t total_free_bytes;   /* Running total, for all pages with oversized=2,3,4... */
} oversized_info_t;

static const char*
kmg(char* buf, uint64_t val)
{
    static const char* SUFF[] = {"","K","M","G","T","P"};
    double v = val;
    int i = 0;

    while (v > 1024) {
        v /= 1024;
        i++;
    }
    if ((double)(int)v == v) {
        sprintf(buf,"%d%s",(int)v,SUFF[i]);
    } else {
        sprintf(buf,"%.1f%s",v,SUFF[i]);
    }
    return buf;
}


static void
usage(int exit_code)
{
    fprintf(stderr,
"Usage: mdbm_stat [Options] <db filename>\n"
"Options:\n"
"        -H            Show db header\n"
"        -h            This help information\n"
"        -i " FREE_PAGE_FLAG "     Show total number of free pages and free-list pages\n"
"        -i " ENTRY_COUNT_FLAG "     Print the number of entries\n"
"        -i " RESIDEN_COUNT_FLAG "   Print memory-resident page info\n"
"        -l mode       Lock mode\n"
lockstr_to_flags_usage("                          ")
"        -L            Do not lock the DB\n"
"        -o            Show oversized pages distribution\n"
"        -u            Show db utilization statistics\n"
"        -R            Show page memory-residency information\n"
"        -w  <size>    Open db in windowed mode\n"
    );
    exit(exit_code);
}


static const int UTIL[] = {0,10,25,50,75,90,95,98,99};
#define NUM_UTIL 9

struct mdbm_stat_util
{
    uint32_t db_pagesize;
    uint32_t num_dir_pages;
    uint32_t num_used_pages;
    uint32_t num_oversize_pages;
    uint32_t num_pages_util[NUM_UTIL];
    int32_t min_page_free;
    uint64_t page_bytes_total;
    uint64_t page_bytes_used;
    uint64_t data_bytes_used;
    uint64_t page_bytes_overhead;
    uint64_t key_bytes_used;
    uint64_t val_bytes_used;
    uint32_t lob_pages_used;
    uint64_t lob_bytes_total;
    uint64_t lob_bytes_used;
    uint64_t lob_bytes_overhead;
    uint32_t num_entries;
    uint32_t num_val_entries;
    uint32_t num_lob_entries;
    uint32_t min_entry_size;
    uint32_t max_entry_size;
    uint32_t min_key_size;
    uint32_t max_key_size;
    uint32_t min_val_size;
    uint32_t max_val_size;
    uint32_t min_lob_size;
    uint32_t max_lob_size;
};


static int
page_util(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
    struct mdbm_stat_util* su = (struct mdbm_stat_util*)user;
    uint32_t nused, nfree;
    double util;
    int i;

    nused = info->i_page.page_overhead_space + info->i_page.page_active_space;
    util = (double)nused * 100 / info->i_page.page_size;
    nfree = info->i_page.page_size - nused;
    for (i = 0; i < NUM_UTIL; i++) {
        if (!info->i_page.page_active_entries || util < UTIL[i]) {
            break;
        }
        su->num_pages_util[i]++;
    }
    if (!su->num_used_pages || nfree < (uint32_t)su->min_page_free) {
        su->min_page_free = nfree;
    }
    if (info->i_page.page_size > su->db_pagesize) {
        su->num_oversize_pages++;
    }
    su->page_bytes_total += info->i_page.page_size;
    su->page_bytes_used += nused;
    su->data_bytes_used += info->i_page.page_active_space + info->i_page.page_active_lob_space;
    su->page_bytes_overhead += info->i_page.page_overhead_space;

    if (info->i_page.page_active_entries) {
        su->key_bytes_used += info->i_page.page_active_key_space;
        if (!su->num_entries || info->i_page.page_min_entry_size < su->min_entry_size) {
            su->min_entry_size = info->i_page.page_min_entry_size;
        }
        if (info->i_page.page_max_entry_size > su->max_entry_size) {
            su->max_entry_size = info->i_page.page_max_entry_size;
        }
        if (!su->num_entries || info->i_page.page_min_key_size < su->min_key_size) {
            su->min_key_size = info->i_page.page_min_key_size;
        }
        if (info->i_page.page_max_key_size > su->max_key_size) {
            su->max_key_size = info->i_page.page_max_key_size;
        }
        if (info->i_page.page_active_entries > info->i_page.page_active_lob_entries) {
            su->val_bytes_used += info->i_page.page_active_val_space;
            if (!su->num_val_entries || info->i_page.page_min_val_size < su->min_val_size) {
                su->min_val_size = info->i_page.page_min_val_size;
            }
            if (info->i_page.page_max_val_size > su->max_val_size) {
                su->max_val_size = info->i_page.page_max_val_size;
            }
            su->num_val_entries += info->i_page.page_active_entries
                - info->i_page.page_active_lob_entries;
        }
        if (info->i_page.page_active_lob_entries) {
            if (!su->num_lob_entries || info->i_page.page_min_lob_size < su->min_lob_size) {
                su->min_lob_size = info->i_page.page_min_lob_size;
            }
            if (info->i_page.page_max_lob_size > su->max_lob_size) {
                su->max_lob_size = info->i_page.page_max_lob_size;
            }
            su->num_lob_entries += info->i_page.page_active_lob_entries;
        }
    }
    su->num_entries += info->i_page.page_active_entries;
    su->num_used_pages += info->i_page.page_size / su->db_pagesize;

    return 0;
}

static int
chunk_util(void* user, const mdbm_chunk_info_t* info)
{
    struct mdbm_stat_util* su = (struct mdbm_stat_util*)user;

    switch (info->page_type) {
    case MDBM_PTYPE_DIR:
        su->num_dir_pages += info->num_pages;
        break;

    case MDBM_PTYPE_LOB:
        su->lob_pages_used += info->num_pages;
        su->lob_bytes_total += info->num_pages * info->page_size;
        su->lob_bytes_used += info->used_space;
        su->lob_bytes_overhead += info->num_pages*info->page_size - info->used_space;
        break;
    }
    return 0;
}

static int
time_string(time_t time_val, char *out, uint32_t out_len)
{
    struct tm result;
    size_t length;


    if (out_len == 0) {
        return -1;
    }

    if (gmtime_r(&time_val, &result) == NULL) {
        out[0] = '\0';
        return -2;
    }

    if (asctime_r(&result, out) == NULL) {
        return -3;
    }

    /* Remove the trailing newline. */
    length = strlen(out);
    if (out && (out[length - 1] == '\n')) {
        out[length - 1] = '\0';
    }

    return 0;
}

static int
page_chunk_counter(void* user, const mdbm_chunk_info_t* info)
{
    page_type_chunk_counters_t *page_count_ptr = (page_type_chunk_counters_t *) user;

    switch (info->page_type) {
        case MDBM_PTYPE_DATA:
            page_count_ptr->data += info->num_pages;
            break;
        case MDBM_PTYPE_DIR:
            page_count_ptr->dir += info->num_pages;
            break;
        case MDBM_PTYPE_LOB:
            page_count_ptr->lob += info->num_pages;
            break;
        case MDBM_PTYPE_FREE:
            page_count_ptr->free += info->num_pages;
            break;
    }
    return 0;
}

static void
print_free_pages(MDBM *db, int flags)
{
    page_type_chunk_counters_t page_counts;
    mdbm_db_info_t dbinfo;

    if (mdbm_get_db_info(db, &dbinfo) < 0) {
        mdbm_logerror(LOG_ERR,0,"print_free_pages: mdbm_get_db_info failed");
        return;
    }

    memset(&page_counts, 0, sizeof(page_counts));
    if (mdbm_chunk_iterate(db, page_chunk_counter, flags, &page_counts) < 0) {
        mdbm_logerror(LOG_ERR, 0, "print_free_pages: mdbm_chunk_iterate() failed");
        return;
    }

    /* Total number of free pages is the total number of pages, minus all the allocated pages:
     * total-(data+directory+large_obj).
     */
    fprintf(stdout, "free-pages           = %8u\n", 
           dbinfo.db_num_pages - (page_counts.data + page_counts.dir + page_counts.lob) );

    /* Also print count free list pages: pages with MDBM_PTYPE_FREE */
    fprintf(stdout, "free-list-pages      = %8u\n", page_counts.free);
}

/* Find the entry in the array, while keeping it sorted by order of oversized page size */
static oversized_info_t*
find_oversized(uint32_t size_in_pages, oversized_info_t* oversized_info)
{
    uint32_t i, max_count = oversized_info->entry_count;
    ++oversized_info;   /* Skip: first array element being used to pass data */
    for (i=0; i < max_count; ++i, ++oversized_info) {
        /* Struct array is sorted, with the last structs are unused and therefore have count==0 */
        if ((oversized_info->count == 0) || (oversized_info->size == size_in_pages)) {
            break; 
        } else if (size_in_pages < oversized_info->size) {
            /* Shuffle down to insert smaller oversized entry to keep it sorted 
 *              * by order of oversized page size */
            memmove(oversized_info + 1, oversized_info, sizeof(oversized_info_t) * (max_count-i-1));
            memset(oversized_info, 0, sizeof(oversized_info_t));    /* re-initialize */
            break;
        }
    }
    return oversized_info;
}
    
static int
oversized_page_handler(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
    oversized_info_t* cur_oversized;
    oversized_info_t* oversized_info = (oversized_info_t *) user;
    uint32_t num_entries = info->i_page.page_active_entries;
    uint32_t used_bytes = info->i_page.page_overhead_space + info->i_page.page_active_space;
    uint32_t free_bytes = info->i_page.page_size - used_bytes;
    uint32_t size_in_pages = info->i_page.page_size / oversized_info->size;

    if (size_in_pages < 2) {
        return 0;
    }

    cur_oversized = find_oversized(size_in_pages, oversized_info);
    if (cur_oversized->size == 0) {    /* If using an empty entry */
        ++oversized_info->count;
        cur_oversized->size = size_in_pages;
        cur_oversized->min_used_bytes = used_bytes;  /* init minimum used */
        cur_oversized->min_free_bytes = free_bytes;  /* init minimum free */
    }
    ++cur_oversized->count;
    if (!num_entries) {
        return 0;
    }
    cur_oversized->entry_count += num_entries;
    if (used_bytes < cur_oversized->min_used_bytes) {
        cur_oversized->min_used_bytes = used_bytes;
    }
    if (used_bytes > cur_oversized->max_used_bytes) {
        cur_oversized->max_used_bytes = used_bytes;
    }
    cur_oversized->total_used_bytes += used_bytes;
    if (free_bytes < cur_oversized->min_free_bytes) {
        cur_oversized->min_free_bytes = free_bytes;
    }
    if (free_bytes > cur_oversized->max_free_bytes) {
        cur_oversized->max_free_bytes = free_bytes;
    }
    cur_oversized->total_free_bytes += free_bytes;
    return 0;
}

static void
print_oversized_pages(MDBM *db, uint32_t db_page_size, uint32_t num_oversized_pages)
{
    uint32_t i, total_count;
    /* Use the first entry of oversized_info to store common data (more below) */
    /* oversized_info_t oversized_info[num_oversized_pages + 1]; */
    oversized_info_t *oversized_info = 
      (oversized_info_t*)malloc((num_oversized_pages + 1)*sizeof(oversized_info_t));

    memset(oversized_info, 0, (num_oversized_pages + 1)*sizeof(oversized_info_t));
    oversized_info[0].count = 0;             /* count = current count of array entries */
    oversized_info[0].size = db_page_size;   /* size = MDBM page size */
    oversized_info[0].entry_count = num_oversized_pages;   /* entry_count = # of oversized pages */
    if (mdbm_iterate(db, -1, oversized_page_handler, 0, oversized_info) < 0) {
        mdbm_logerror(LOG_ERR, 0, "print_oversized_pages: mdbm_iterate() failed");
        free(oversized_info);
        return;
    }

    total_count = oversized_info[0].count;
    if (total_count == 0) {   /* Nothing to print, so return */
        free(oversized_info);
        return;
    }

    fprintf(stdout, "\n  %d Oversized Page(s), with distribution:\n    ", oversized_info[0].count);
    fprintf(stdout, "Size   Count    Average-Entries min/avg/max-Used-Bytes min/avg/max-Free-Bytes\n");
    for (i=1; i <= oversized_info[0].count; ++i) {
        uint32_t count = oversized_info[i].count;
        uint64_t avg_used, avg_free;
        if (!count) {
            break;
        }
        avg_used = oversized_info[i].total_used_bytes / count;
        avg_free = oversized_info[i].total_free_bytes / count;
        fprintf(stdout, "   %4u   %4u         %8u  %8u/%u/%u     %8u/%u/%u\n",
            oversized_info[i].size, count, oversized_info[i].entry_count / count,
            oversized_info[i].min_used_bytes, (uint32_t)avg_used, oversized_info[i].max_used_bytes,
            oversized_info[i].min_free_bytes, (uint32_t)avg_free, oversized_info[i].max_free_bytes
              );
    }
    free(oversized_info);
}

static void
print_residency_info(MDBM *db)
{
    mdbm_ubig_t pages_in=0, pages_out=0;
    mdbm_check_residency(db, &pages_in, &pages_out);
    fprintf(stdout, "pages_resident = %llu, bytes_resident = %llu\n", 
        (unsigned long long) pages_in, ((unsigned long long)pages_in)*sysconf(_SC_PAGESIZE));
    fprintf(stdout, "pages_nonresident = %llu, bytes_nonresident = %llu\n", 
        (unsigned long long) pages_out, ((unsigned long long)pages_out)*sysconf(_SC_PAGESIZE));
}

static void
print_entry_count(MDBM *db)
{
    fprintf(stdout, "entries    = %llu\n", (unsigned long long) mdbm_count_records(db));
}

#define str_append(buf, suffix, len) \
    strncat(buf,suffix,len);         \
    len -= sizeof(suffix);


int
main(int argc, char** argv)
{
    MDBM* db;
    mdbm_db_info_t dbinfo;
    mdbm_stat_info_t dbstats;
    mdbm_hdr_t* hdr;
    int oflags = 0;
    int flags = 0;
    int opt;
    int header = 0;
    int util = 0;
    uint64_t winsize = 0;
    char fetch_last_time[32];
    char store_last_time[32];
    char delete_last_time[32];
    int get_free_pages = 0;
    int get_oversized_pages = 0;
    int get_entry_count = 0;
    int show_residency = 0;

    while ((opt = getopt(argc,argv,"Hhi:Ll:ouw:")) != -1) {
        switch (opt) {
        case 'H':
            header = 1;
            break;

        case 'h':
            usage(0);

        case 'i':
            if (strncasecmp(FREE_PAGE_FLAG, optarg, strlen(FREE_PAGE_FLAG)) == 0) {
                get_free_pages = 1;
            } else if (strncasecmp(ENTRY_COUNT_FLAG, optarg, strlen(ENTRY_COUNT_FLAG)) == 0) {
                get_entry_count = 1;
            } else if (strncasecmp(RESIDEN_COUNT_FLAG, optarg, strlen(RESIDEN_COUNT_FLAG)) == 0) {
                show_residency = 1;
            } else {
                fprintf(stderr, "-i invalid option (%s)\n", optarg);
                usage(1);
            }
            break;

        case 'l':
            {
              int lock_flags = 0;
              if (mdbm_util_lockstr_to_flags(optarg, &lock_flags)) {
                fprintf(stderr, "Invalid locking argument, argument=%s, ignoring\n", optarg);
                usage(1);
              }
              oflags |= lock_flags;
            }
            if (oflags & MDBM_OPEN_NOLOCK) {
                flags |= MDBM_STAT_NOLOCK;
            }
            break;

        case 'L':
            oflags |= MDBM_OPEN_NOLOCK;
            flags |= MDBM_STAT_NOLOCK;
            break;

        case 'u':
            util = 1;
            break;

        case 'o':
            get_oversized_pages = 1;
            break;

        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
            if (winsize == 0) {
                fprintf(stderr, "Window size must be positive, size=%llu\n\n",
                        (unsigned long long)winsize);
                usage(1);
            }
            oflags |= (MDBM_OPEN_WINDOWED | MDBM_O_RDWR);  /* Windowed mode fails with read-only access */
            break;
        }
    }


    if (optind+1 != argc) {
        usage(1);
    }

    if (!winsize) {
        oflags |= MDBM_O_RDONLY;
    }

    if (!(oflags&MDBM_OPEN_NOLOCK)) {
      oflags |= MDBM_ANY_LOCKS;
    }

    db = mdbm_open(argv[optind],oflags,0,0,0);
    if (!db) {
        perror(argv[1]);
        return 2;
    }

    if (mdbm_get_version(db) != 3) {
        mdbm_stat_header(db);
        mdbm_stat_all_page(db);
        mdbm_close(db);
        return 0;
    }

    if (winsize) {
        errno = 0;
        if (mdbm_set_window_size(db, winsize) != 0) {
            fprintf(stderr, "Unable to set window size to %llu (%s), exiting\n",
                (unsigned long long) winsize, strerror(errno));
            mdbm_close(db);
            exit(1);
        }
    }

    if (get_entry_count) {
        print_entry_count(db);
        mdbm_close(db);
        return 0;
    }

    if (get_free_pages) {
        print_free_pages(db, flags);
        mdbm_close(db);
        return 0;
    }

    if (show_residency) {
        print_residency_info(db);
        mdbm_close(db);
        return 0;
    }


    hdr = mdbm_get_header(db);

    if (header) {
        char flags_buf[128];
        int left = sizeof(flags_buf);
        const char* magic;
        int align = hdr->h_dbflags & MDBM_ALIGN_MASK;
        time_t fetch_last_val = 0;
        time_t store_last_val = 0;
        time_t delete_last_val = 0;

        flags_buf[0] = 0;
        if (align == MDBM_HFLAG_ALIGN_2) {
            str_append(flags_buf," align-16",left);
        } else if (align == MDBM_HFLAG_ALIGN_4) {
            str_append(flags_buf," align-32",left);
        } else if (align == MDBM_HFLAG_ALIGN_8) {
            str_append(flags_buf," align-64",left);
        }

        if (hdr->h_dbflags & MDBM_HFLAG_PERFECT) {
            str_append(flags_buf," perfect",left);
        }

        if (hdr->h_dbflags & MDBM_HFLAG_REPLACED) {
            str_append(flags_buf," replaced",left);
        }

        if (hdr->h_dbflags & MDBM_HFLAG_LARGEOBJ) {
            str_append(flags_buf," largeobj",left);
        }

        if (hdr->h_magic == _MDBM_MAGIC_NEW2) {
            magic = "db version 3";
        } else if (hdr->h_magic == _MDBM_MAGIC_NEW) {
            magic = "db version 2";
        } else if (hdr->h_magic == _MDBM_MAGIC) {
            magic = "db version 1";
        } else {
            magic = "invalid";
        }

        fetch_last_time[0] = store_last_time[0] = delete_last_time[0] = '\0';
        if (mdbm_get_stat_time(db, MDBM_STAT_TYPE_FETCH, &fetch_last_val) == 0) {
            time_string(fetch_last_val, fetch_last_time, sizeof(fetch_last_time));
        }
        if (mdbm_get_stat_time(db, MDBM_STAT_TYPE_STORE, &store_last_val) == 0) {
            time_string(store_last_val, store_last_time, sizeof(store_last_time));
        }
        if (mdbm_get_stat_time(db, MDBM_STAT_TYPE_DELETE, &delete_last_val) == 0) {
            time_string(delete_last_val, delete_last_time, sizeof(delete_last_time));
        }

        fprintf(stdout, "\
  Magic        = %08lx (%s)\n\
  Flags        = %04x%s\n\
  Dir shift    = %8u\n\
  Hash func    = %8u (%s)\n\
  Max dirshift = %8u\n\
  Dir gen      = %8u\n\
  Page size    = %8u\n\
  Num pages    = %8u\n\
  Max pages    = %8u\n\
  Spill size   = %8u\n\
  Last chunk   = %8u\n\
  First free   = %8u\n\
  Fetches      = %16llu, Last time: %lu (%s UTC)\n\
  Stores       = %16llu, Last time: %lu (%s UTC)\n\
  Removes      = %16llu, Last time: %lu (%s UTC)\n",
               (unsigned long)hdr->h_magic,magic,
               hdr->h_dbflags,
               flags_buf,
               hdr->h_dir_shift,
               hdr->h_hash_func,
               (hdr->h_hash_func < MDBM_HASH_MAX) ? MDBM_HASH_FUNCNAMES[hdr->h_hash_func] : "?",
               hdr->h_max_dir_shift,
               hdr->h_dir_gen,
               hdr->h_pagesize,
               hdr->h_num_pages,
               hdr->h_max_pages,
               hdr->h_spill_size,
               hdr->h_last_chunk,
               hdr->h_first_free,

               (unsigned long long)hdr->h_stats.s_fetches,
               (unsigned long)fetch_last_val,
               fetch_last_time,

               (unsigned long long)hdr->h_stats.s_stores,
               (unsigned long)store_last_val,
               store_last_time,

               (unsigned long long)hdr->h_stats.s_deletes,
               (unsigned long)delete_last_val,
               delete_last_time);
    }
    if (util) {
        struct mdbm_stat_util su;
        mdbm_db_info_t dbinfo;
        int i;

        if (mdbm_get_db_info(db,&dbinfo) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm_get_db_info(%s)",argv[optind]);
            return 1;
        }

        memset(&su,0,sizeof(su));
        su.db_pagesize = dbinfo.db_page_size;
        if (mdbm_iterate(db,-1,page_util,flags,&su) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm_iterate(%s)",argv[optind]);
            return 1;
        }
        if (mdbm_chunk_iterate(db,chunk_util,flags,&su) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm_chunk_iterate(%s)",argv[optind]);
            return 1;
        }
        fprintf(stdout, "\
cache-mode           = %8d (%s)\n\
total-pages          = %8u\n\
used-pages           = %8u\n\
free-pages           = %8u\n\
oversize-pages       = %8u\n\
page-size            = %8u\n\
dir-width            = %8u\n\
max-pages            = %8u\n\
min-free-page-bytes  = %8u\n\
empty-pages          = %8u\n",
               dbinfo.db_cache_mode, mdbm_get_cachemode_name(dbinfo.db_cache_mode),
               dbinfo.db_num_pages,
               su.num_used_pages,
               dbinfo.db_num_pages - su.num_used_pages - su.num_dir_pages - su.lob_pages_used,
               su.num_oversize_pages,
               dbinfo.db_page_size,
               dbinfo.db_dir_width,
               dbinfo.db_max_pages,
               su.min_page_free,
               dbinfo.db_dir_width - su.num_pages_util[0]);
        for (i = 1; i < NUM_UTIL; i++) {
            fprintf(stdout, "pages-%02d%%-filled     = %8u\n",UTIL[i],su.num_pages_util[i]);
        }
        fprintf(stdout, "empty-pages-%%        = %8.2f\n",
               dbinfo.db_dir_width - su.num_pages_util[0]
               ? (double)(dbinfo.db_dir_width - su.num_pages_util[0]) * 100 /  dbinfo.db_dir_width
               :0);
        for (i = 1; i < NUM_UTIL; i++) {
            fprintf(stdout, "pages-%02d%%-filled-%%   = %8.2f\n",UTIL[i],
                   su.num_pages_util[i]
                   ? (double)su.num_pages_util[i] * 100 / dbinfo.db_dir_width
                   : 0);
        }
        fprintf(stdout, "\
db-space-util%%       = %8.2f\n\
data-space-util%%     = %8.2f\n\
data-space-overhead%% = %8.2f\n\
lob-overhead%%        = %8.2f\n\
num-entries          = %8u\n\
num-val-entries      = %8u\n\
num-lob-entries      = %8u\n\
min-key-size         = %8u\n\
mean-key-size        = %8u\n\
max-key-size         = %8u\n\
min-val-size         = %8u\n\
mean-val-size        = %8u\n\
max-val-size         = %8u\n\
min-entry-size       = %8u\n\
mean-entry-size      = %8u\n\
max-entry-size       = %8u\n\
min-lob-size         = %8u\n\
mean-lob-size        = %8u\n\
max-lob-size         = %8u\n",
               (double)(su.page_bytes_used+su.lob_bytes_used) * 100 / mdbm_get_size(db),
               su.page_bytes_total ? (double)su.page_bytes_used * 100 / su.page_bytes_total : 0,
               su.page_bytes_total ? (double)su.page_bytes_overhead * 100 / su.page_bytes_total :0,
               su.lob_bytes_total ? (double)su.lob_bytes_overhead * 100 / su.lob_bytes_total : 0,
               su.num_entries,
               su.num_val_entries,
               su.num_lob_entries,
               su.min_key_size,
               (uint32_t)(su.num_entries ? su.key_bytes_used / su.num_entries : 0),
               su.max_key_size,
               su.min_val_size,
               (uint32_t)(su.num_val_entries ? su.val_bytes_used / su.num_val_entries : 0),
               su.max_val_size,
               su.min_entry_size,
               (uint32_t)(su.num_entries ? su.data_bytes_used / su.num_entries : 0),
               su.max_entry_size,
               su.min_lob_size,
               (uint32_t)(su.num_lob_entries ? su.lob_bytes_used / su.num_lob_entries : 0),
               su.max_lob_size);
    }
    if (header || util) {
        mdbm_close(db);
        return 0;
    }

    if (mdbm_get_db_stats(db,&dbinfo,&dbstats,flags) < 0) {
        mdbm_logerror(LOG_ERR,0,"mdbm_get_db_stats(%s)",argv[optind]);
        mdbm_close(db);
        return 1;
    }

    {
        char page_size_buf[32];
        char max_pages_buf[32];
        char db_size_buf[32];
        char bytes_buf[32];
        char normal_bytes_buf[32];
        char lob_bytes_buf[32];
        char overhead_bytes_buf[32];
        char unused_bytes_buf[32];
        uint64_t normal, lob, tot, overhead, unused, dbsize;

        normal = dbstats.sum_key_bytes+dbstats.sum_normal_val_bytes;
        lob = dbstats.sum_lob_val_bytes;
        tot = normal + lob;
        overhead = dbstats.sum_overhead_bytes;
        dbsize = (uint64_t)dbinfo.db_page_size *dbinfo.db_num_pages;
        unused = dbsize - tot - overhead;

        fprintf(stdout, "\
  Page size    = %10s\t\t  Num pages    = %10d\n\
  Hash func    = %10u %-8s\t    used       = %10u (%5.2f%%)\n\
  Max dir size = %10u\t\t    free       = %10u\n\
  Max pages    = %10u %-8s\t  Db size      = %11s\n\
  Spill size   = %10u\n\
  Cache mode   = %10u %s\n\
\n\
  Directory\t\t\t\t  Data\n\
   width       = %10d\t\t   entries     = %10llu\n\
   pages       = %10d (%5.2f%%)\t     normal    = %10llu\n\
   min level   = %10d\t\t     large-obj = %10llu\n\
   max level   = %10d\t\t   bytes       = %11s (%5.2f%% utilization)\n\
   nodes       = %10d\t\t     normal    = %11s (%5.2f%%)\n\
\t\t\t\t\t     large-obj = %11s (%5.2f%%)\n\
  Chunks       = %10u\t\t     overhead  = %11s (%5.2f%%)\n\
   normal      = %10u\t\t     unused    = %11s (%5.2f%%)\n\
   oversized   = %10u\t\t   entry sizes (min/mean/max)\n\
   large-obj   = %10u\t\t     key+val   = %u/%u/%u\n\
\t\t\t\t\t     key       = %u/%u/%u\n\
\t\t\t\t\t     val       = %u/%u/%u\n\
\t\t\t\t\t     lob       = %u/%u/%u\n\
\t\t\t\t\t   entries/page (min/mean/max)\n\
\t\t\t\t\t               = %u/%u/%u\n",
               kmg(page_size_buf,dbinfo.db_page_size),
               dbinfo.db_num_pages,
               dbinfo.db_hash_func,dbinfo.db_hash_funcname,
               dbinfo.db_num_pages - dbstats.num_free_pages,
               (double)(dbinfo.db_num_pages - dbstats.num_free_pages)*100/dbinfo.db_num_pages,
               dbinfo.db_max_dir_shift ? 1 << dbinfo.db_max_dir_shift : 0,
               dbstats.num_free_pages,
               dbinfo.db_max_pages,kmg(max_pages_buf,
                                       (uint64_t)dbinfo.db_max_pages * dbinfo.db_page_size),
               kmg(db_size_buf,dbsize),
               dbinfo.db_spill_size,
               dbinfo.db_cache_mode, mdbm_get_cachemode_name(dbinfo.db_cache_mode),

               dbinfo.db_dir_width,
               (unsigned long long)dbstats.num_active_entries,
               dbinfo.db_num_dir_pages,
               (double)dbinfo.db_num_dir_pages*100/dbinfo.db_num_pages,
               (unsigned long long)(dbstats.num_active_entries - dbstats.num_active_lob_entries),
               dbinfo.db_dir_min_level,
               (unsigned long long)dbstats.num_active_lob_entries,
               dbinfo.db_dir_max_level,
               kmg(bytes_buf,tot),
               (double)tot*100/dbsize,
               dbinfo.db_dir_num_nodes,
               kmg(normal_bytes_buf,normal),
               (double)normal*100/dbsize,
               kmg(lob_bytes_buf,lob),
               (double)lob*100/dbsize,
               dbstats.num_active_pages,
               kmg(overhead_bytes_buf,dbstats.sum_overhead_bytes),
               (double)dbstats.sum_overhead_bytes*100/dbsize,
               dbstats.num_normal_pages,
               kmg(unused_bytes_buf,unused),
               (double)unused*100/dbsize,
               dbstats.num_oversized_pages,
               dbstats.num_lob_pages,
               dbstats.min_entry_bytes,
               (uint32_t)(dbstats.num_active_entries
                          ? tot/dbstats.num_active_entries : 0),
               dbstats.max_entry_bytes,
               dbstats.min_key_bytes,
               (uint32_t)(dbstats.num_active_entries
                          ? dbstats.sum_key_bytes/dbstats.num_active_entries : 0),
               dbstats.max_key_bytes,
               dbstats.min_val_bytes,
               (uint32_t)(dbstats.num_active_entries
                          ? dbstats.sum_normal_val_bytes/dbstats.num_active_entries : 0),
               dbstats.max_val_bytes,
               dbstats.min_lob_bytes,
               (uint32_t)(dbstats.num_active_lob_entries
                          ? dbstats.sum_lob_val_bytes/dbstats.num_active_lob_entries : 0),
               dbstats.max_lob_bytes,
               dbstats.min_page_entries,
               (uint32_t)(dbstats.num_active_entries / dbinfo.db_dir_width),
               dbstats.max_page_entries
            );
    }

    {
        int incr = dbstats.max_page_used_space/MDBM_STAT_BUCKETS;
        int i;
        int print = 0;

        fprintf(stdout, "\n\
  Bytes used        --- Pages ---  Entries    ------ Bytes -------     ---- Free bytes ---\n\
                    count   %%tot      mean    min     mean     max     min    mean     max\n");
        /*3840 -   4096      4006 (96.6%)     197    3954       0    4060 */


        for (i = 0; i < MDBM_STAT_BUCKETS; i++) {
            mdbm_bucket_stat_t* b = dbstats.buckets+i;
            if (!print && !b->sum_entries) {
                continue;
            }
            print = 1;
            fprintf(stdout, "%6d - %6d  %8d (%5.1f%%) %7llu %7d %7llu %7d %7d %7llu %7d\n",
                   i*incr,(i+1)*incr,
                   b->num_pages,
                   dbstats.num_active_pages
                   ? (double)b->num_pages*100/dbstats.num_active_pages
                   :0.0,
                   (unsigned long long)(b->num_pages ? b->sum_entries/b->num_pages : 0),
                   b->min_bytes,
                   (unsigned long long)(b->num_pages ? b->sum_bytes/b->num_pages : 0),
                   b->max_bytes,
                   b->min_free_bytes,
                   (unsigned long long)(b->num_pages ? b->sum_free_bytes/b->num_pages : 0),
                   b->max_free_bytes);
        }
    }

    if (get_oversized_pages) {
        print_oversized_pages(db, dbinfo.db_page_size, dbstats.num_oversized_pages);
    }

    mdbm_close(db);
    return 0;
}
