/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Truncate an MDBM to a single empty page. */
/* This removes all data and some settings from the DB. */

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_util.h"

#define PROG "mdbm_trunc "

static void
usage(int err)
{
    /* Add the -l option after figuring out how to make it work.
     * Currently, truncation loses all mdbm attributes.  Most attributes can
     * be restored through per-attribute call, but large objects doesn't have
     * that API. Related code has been commented out via: #if 0 #endif.
     * After creating the necessary interface, add to the following fprintf:
        -l              Enable large-object mode\n\"
     */
    fprintf(stderr, "\
Usage: " PROG "[options] <mdbm>\n\
\n\
Options:\n\
        -D <mbytes>     Specify initial/maximum size of db (default: no size limit).\n\
                        Suffix k/m/g may be used to override the default of m.\n\
        -h              This message\n\
        -f              Force - do not prompt\n\
        -L              Open with no locking\n\
        -p <bytes>      Presize database to <bytes>\n\
        -S <mbytes>     Specify size of main db (remainder is large-object/overflow space).\n\
                        Suffix k/m/g may be used to override the default of m.\n\
                        Requires the -l flag.\n\
        -s              Sync database after trunc.\n\
        -w <size>       Use windowed mode with specified window size\n\
                        Suffix k/m/g may be used to override the default of bytes.\n\
\n\
Truncating an mdbm does not preserve any mdbm configuration attributes.\n\
You must use the available options to explicitly set the attributes that you need.\n\
\n\
Currently, there is no way to preserve large objects.\n\
\n");
    exit(err);
}

int
main (int argc, char** argv)
{
    MDBM* db;
    int opt;
    int opt_force = 0;
#if 0
    int opt_large = 0;
#endif
    int opt_limit = 0;
    int opt_presize = 0;
    int opt_sync = 0;
    int oflags = 0;
    int pagesize = 0;
    uint64_t dbsize = 0;
    uint64_t space = 0;
    uint64_t winsize = 0;

#if 0
    while ((opt = getopt(argc, argv, "D:hfLlp:S:s")) != -1) {
#else
    while ((opt = getopt(argc, argv, "D:hfLp:S:sw:")) != -1) {
#endif
        switch (opt) {
        case 'D':
            opt_limit = 1;
            dbsize = mdbm_util_get_size(optarg, 1024 * 1024);
            break;

        case 'h':
            usage(0);

        case 'f':
            opt_force = 1;
            break;

        case 'L':
            oflags |= MDBM_OPEN_NOLOCK;
            break;

#if 0
        case 'l':
            opt_large = 1;
            oflags |= MDBM_LARGE_OBJECTS;
            break;
#endif

        case 'p':
            opt_presize = atoi(optarg);
            break;

        case 'S':
#ifdef MDBM_CREATE_V3
            space = mdbm_util_get_size(optarg, 1024 * 1024);
#else
            fputs(PROG ": -S not supported in this version\n", stderr);
            exit(1);
#endif
            break;

        case 's':
            opt_sync = 1;
            break;

        case 'w':
            winsize = mdbm_util_get_size(optarg, 1);
            if (winsize == 0) {
                printf("Window size must be positive, size=%lu\n\n", (unsigned long)winsize);
                usage(1);
            }
            oflags |= MDBM_OPEN_WINDOWED;
            break;

        default:
            usage(1);
        }
    }

    if ((argc - optind) != 1) {
        usage(1);
    }

    if (!(oflags&MDBM_OPEN_NOLOCK)) {
      oflags |= MDBM_ANY_LOCKS;
    }

    if ((db = mdbm_open(argv[optind], MDBM_O_RDWR|oflags, 0, 0, 0)) == NULL) {
        fprintf(stderr, PROG ": mdbm_open(%s): %s\n",
                argv[optind], strerror(errno));
        if (errno == EINVAL) {
            fprintf(stderr, PROG ": Use mdbm_create to create db with required options.\n");
        }
        exit(1);
    }

    pagesize = mdbm_get_page_size(db);

#if 0
#ifdef MDBM_CREATE_V3
    if (space) {
        if (! opt_large) {
            fputs(PROG "The main db size (-S) flag must be used with"
                  " the large object flag (-l)\n", stderr);
            usage(1);
        }

        int pgs = space / pagesize;
        if ((space % pagesize) || (pgs & (pgs-1))) {
            fputs(PROG ": Specified main db size (-S) must result in power-of-2 database pages\n",
                  stderr);
            usage(1);
        }
        if (space > dbsize) {
            fputs(PROG ": Must specify total db size (-D) >=  main db size (-S)\n",
                  stderr);
            usage(1);
        }
    }
#endif
#endif

    if (!opt_force) {
        int c;
        printf("WARNING:  This will remove configuration and ALL DATA from the specified MDBM:\n");
        printf("  %s\n", argv[optind]);
        printf("Are you sure you want to do this (y/n): ");fflush(stdout);

        c = getchar();
        if (! ((c == 'y') || (c == 'Y'))) {
            printf("Skipping.\n");
            mdbm_close(db);
            exit(0);
        }
    }

    printf("Truncating.\n");
    mdbm_truncate(db);

    if (! opt_limit) {
        printf("Setting limit size to 0 pages.  This removes any size limit from this mdbm\n");
    }
    else {
        if (dbsize) {
            mdbm_ubig_t pages = dbsize / pagesize;
            printf("Setting limit size to %llu (%u pages).\n", (unsigned long long)dbsize, pages);
            mdbm_limit_size_v3(db, pages, NULL, NULL);
        }
    }

#ifdef MDBM_CREATE_V3
    if (winsize) {
        errno = 0;
        if (mdbm_set_window_size(db, winsize) != 0) {
            printf("Unable to set window size to %llu (%s) exiting\n",
                (unsigned long long) winsize, strerror(errno));
            mdbm_close(db);
            exit(1);
        }
    }
    if (space) {
        mdbm_ubig_t pages = space / pagesize;
        mdbm_limit_dir_size(db, pages);
        mdbm_pre_split(db, pages);
    } else
#endif
        {
            if (opt_presize) {
                mdbm_ubig_t pages = opt_presize / pagesize;
                int ret;
                ret = mdbm_pre_split(db, pages);
                if (ret < 0) {
                    fprintf(stderr, PROG ": mdbm_pre_split(%s): %s\n",
                            argv[optind], strerror(errno));
                }
            }
        }

    if (opt_sync)
        mdbm_sync(db);

    mdbm_close(db);
    return 0;
}
