/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <sys/fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>

#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_util.h"

#define MDBM_DUMP_DELETED       0x1
#define MDBM_DUMP_KEYS          0x2
#define MDBM_DUMP_VALUES        0x4
#define MDBM_DUMP_PAGES_ONLY    0x8
#define MDBM_DUMP_FREE          0x10
#define MDBM_DUMP_CACHEINFO     0x20

typedef struct mdbm_dump_info {
    MDBM* db;
    int flags;
    int free;
} mdbm_dump_info_t;


void
print_bytes (const char* prefix, datum d, int print_all)
{
    int i, len;
    int max;
    const char* s;

    len = d.dsize;
    if (!print_all) {
        s = getenv("MDBM_DUMP_BYTES");
        if (s) {
            max = atoi(s);
            putchar('\n');
        } else {
            max = 16;
        }
        if (len > max) {
            len = max;
        }
    }
    s = d.dptr;
    while (len > 0) {
        printf("%s%08x  ",prefix,(unsigned int)(intptr_t)s);
        for (i = 0; i < 16; i++) {
            if (i == 8) {
                putchar(' ');
            }
            if (i < len) {
                printf("%02x ",(unsigned char)(s[i]));
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


const char* PTYPE[] = {"free","data","dir ","lob "};


static int
chunk (void* user, const mdbm_chunk_info_t* info)
{
    mdbm_dump_info_t* di = (mdbm_dump_info_t*)user;

    if ((di->flags & MDBM_DUMP_FREE) && info->page_type != MDBM_PTYPE_FREE) {
        return 0;
    }

    printf("%06d %s %6dp (%9db) prev=%-6d prevnum=%06d",
           info->page_num,
           PTYPE[info->page_type],
           info->num_pages,
           info->num_pages * info->page_size,
           info->prev_num_pages,
           info->page_num-info->prev_num_pages);
    switch (info->page_type) {
    case MDBM_PTYPE_FREE:
        printf(" +--next_free   = %d\n",info->page_data);
        di->free = info->page_data;
        break;

    case MDBM_PTYPE_DATA:
        printf(" %c  num_entries = %-5d  page_num = %d\n",
               (di->free > 0) ? '|' : ' ',info->page_data,info->dir_page_num);
        break;

    case MDBM_PTYPE_DIR:
        putchar('\n');
        break;

    case MDBM_PTYPE_LOB:
        printf(" %c  vallen = %-10d  page_num = %d\n",
               (di->free > 0) ? '|' : ' ',info->page_data,info->dir_page_num);
        break;
    }
    return 0;
}


static int
dump (void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
    mdbm_dump_info_t* di = (mdbm_dump_info_t*)user;

    if (info->i_entry.entry_index == 0) {
        printf("P%06d/%06d # "
               "entries=%-5d (%6d bytes)  deleted=%-5d (%6d bytes)"
               "  free=%-6d (%6d total)"
               "  sizes=%d/%d/%d\n",
               info->i_page.page_num,
               info->i_page.mapped_page_num,
               info->i_page.page_active_entries,
               info->i_page.page_active_space,
               info->i_page.page_deleted_entries,
               info->i_page.page_deleted_space,
               info->i_page.page_free_space,
               info->i_page.page_free_space + info->i_page.page_deleted_space,
               info->i_page.page_min_entry_size,
               info->i_page.page_max_entry_size,
               info->i_page.page_active_entries
               ? (info->i_page.page_active_space / info->i_page.page_active_entries)
               : 0);
    }
    if (di->flags & MDBM_DUMP_PAGES_ONLY) {
        return 0;
    }
    if (!(info->i_entry.entry_flags & MDBM_ENTRY_DELETED) || di->flags & MDBM_DUMP_DELETED) {
        printf("  P%06d/%06d [E%04d] %s klen=%-5d vlen=%-5d koff=%-5d voff=%-5d",
               info->i_page.page_num,
               info->i_page.mapped_page_num,
               info->i_entry.entry_index,
               (info->i_entry.entry_flags & MDBM_ENTRY_DELETED)
               ? "D"
               : ((info->i_entry.entry_flags & MDBM_ENTRY_LARGE_OBJECT)
                  ? "L"
                  : " "),
               kv->key.dsize,
               kv->val.dsize,
               info->i_entry.key_offset,
               info->i_entry.val_offset);
        if (info->i_entry.entry_flags & MDBM_ENTRY_LARGE_OBJECT) {
            printf(" lob-page=%d/%p lob-pages=%d",
                   info->i_entry.lob_page_num,
                   (void*)info->i_entry.lob_page_addr,
                   info->i_entry.lob_num_pages);
        }
        if (di->flags & MDBM_DUMP_CACHEINFO) {
            printf("    num_accesses=%u access_time=%u (t-%lu) priority=%f",
                   info->i_entry.cache_num_accesses,
                   info->i_entry.cache_access_time,
                   info->i_entry.cache_access_time
                   ? (long)(get_time_sec() - info->i_entry.cache_access_time)
                   : 0,
                   info->i_entry.cache_priority);
        }
        putchar('\n');
        if (di->flags & MDBM_DUMP_KEYS) {
#if 0
            int p;
            MDBM_ITER iter;
            datum key, val;

            if ((p = mdbm_get_page(di->db,&kv->key)) != info->i_page.page_num) {
                printf("### ENTRY ON WRONG PAGE (should be %d)\n",p);
            }
            key = kv->key;
            if (mdbm_fetch_r(di->db,&key,&val,&iter) < 0) {
                printf("### UNABLE TO FETCH\n");
            }
#endif
            print_bytes("  K ",kv->key,1);
        }
        if (di->flags & MDBM_DUMP_VALUES) {
            print_bytes("  V ",kv->val,1);
        }
    }
    return 0;
}


static void
usage (int err)
{
    printf("\
Usage: mdbm_dump [options] <db filename>\n\
Options:\n\
        -C      Show cache info\n\
        -c      Show chunks\n\
        -d      Include deleted entries\n\
        -f      Show free chunks only\n\
        -k      Include key data\n\
        -l      use legacy mode (V3 only)\n\
        -L      Do not lock db\n\
        -P      Dump page headers only\n\
        -p <num> Dump specified page\n\
        -v      Include value data\n\
        -w <size>  Use windowed mode with specified window size\n\
");
    exit(err);
}


int
main (int argc, char** argv)
{
    MDBM* db;
    mdbm_dump_info_t di;
    int opt;
    int chunks = 0;
    int flags = 0;
    int oflags = 0;
    int pno = -1;
    uint64_t winsize = 0;
    int legacy = 0;

    di.db = NULL;
    di.flags = 0;
    di.free = 0;

    while ((opt = getopt(argc,argv,"CcdhfklLp:Pvw:")) != -1) {
        switch (opt) {
        case 'C':
            di.flags |= MDBM_DUMP_CACHEINFO;
            break;

        case 'c':
            chunks = 1;
            break;

        case 'd':
            di.flags |= MDBM_DUMP_DELETED;
            break;

        case 'f':
            di.flags |= MDBM_DUMP_FREE;
            break;

        case 'h':
            usage(0);

        case 'k':
            di.flags |= MDBM_DUMP_KEYS;
            break;

        case 'l':
            legacy = 1;
            break;

        case 'L':
            flags |= MDBM_ITERATE_NOLOCK;
            oflags |= MDBM_OPEN_NOLOCK;
            break;

        case 'P':
            di.flags |= MDBM_DUMP_PAGES_ONLY;
            break;

        case 'p':
            pno = atoi(optarg);
            break;

        case 'v':
            di.flags |= MDBM_DUMP_VALUES;
            break;

        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
            if (winsize == 0) {
                printf("Window size must be positive, size=%lu\n\n", (unsigned long)winsize);
                usage(1);
            }
            oflags |= (MDBM_OPEN_WINDOWED | MDBM_O_RDWR);  /* Windowed mode fails with read-only access  */
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
        perror(argv[optind]);
        return 1;
    }

    if (legacy || mdbm_get_version(db)==2) {
        if (pno < 0) {
            mdbm_dump_all_page(db);
        } else {
            mdbm_dump_page(db,pno);
        }
        mdbm_close(db);
        return 0;
    }

    if (winsize) {
        errno = 0;
        if (mdbm_set_window_size(db, winsize) != 0) {
            printf("Unable to set window size to %llu (%s), exiting\n",
                (unsigned long long) winsize, strerror(errno));
            mdbm_close(db);
            exit(1);
        }
    }

    if (chunks) {
        di.free = -1;
        mdbm_chunk_iterate(db,chunk,flags,&di);
    } else {
        di.db = db;
        if (!(di.flags & MDBM_DUMP_PAGES_ONLY)) {
            flags |= MDBM_ITERATE_ENTRIES;
        }
        mdbm_iterate(db,pno,dump,flags,&di);
    }
    mdbm_close(db);
    return 0;
}
