/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_util.h"

static void
usage(int exit_code)
{
    printf(
"Usage: mdbm_check [options] <db filename>\n"
"\n"
"Options:\n"
"        -d <level>      Set database check level (0-4, default: 3)\n"
"                        Each check level includes checks at lower levels.\n"
"                          0 - no header checks (invokes page level checking only)\n"
"                          1 - header, chunks (list of free pages and allocated pages)\n"
"                          2 - directory pages\n"
"                          3 - data pages and large objects\n"
"        -h              This help message\n"
"        -L              Do not lock the DB.\n"
"        -l mode         Lock mode \n"
lockstr_to_flags_usage("                          ")
"        -p <n>          Check specified page\n"
"        -V              Display mdbm file version.\n"
"                        This option may only be used with the -v option.\n"
"                        Use `-v 0' to return just the unadorned version number.\n"
"        -v <level>      Set verbosity level\n"
"        -w <size>       Use windowed mode with specified window size.\n"
"        -X <n>          Verify that the mdbm file version matches this number\n");
    exit(exit_code);
}

static void
checkVersionUsage(int opt_version)
{
    if (opt_version) {
        printf ("The -V option may only be used with the -v option.\n\n");
        usage(1);
    }
}

int
main(int argc, char* argv[])
{
    MDBM* db;
    int ret = 0;
    int opt;
    int opt_version = 0;
    int opt_nonversion = 0;
    int opt_verify_version = 0;
    int pno = -1;
    const char* fn;
    int oflags = MDBM_ANY_LOCKS;
    int verbose = 1;
    int dbcheck = 3;
    int lock = 1;
    uint64_t winsize = 0;

    while ((opt = getopt(argc,argv,"d:hLl:p:Vv:w:X:")) != -1) {
        switch (opt) {
        case '2':
            break;

        case 'd':
            opt_nonversion = 1;
            checkVersionUsage(opt_version);
            dbcheck = atoi(optarg);
            break;

        case 'h':
            opt_nonversion = 1;
            usage(0);

        case 'L':
            opt_nonversion = 1;
            checkVersionUsage(opt_version);
            oflags |= MDBM_OPEN_NOLOCK;
            lock = 0;
            break;

        case 'l':
            opt_nonversion = 1;
            checkVersionUsage(opt_version);
            {
              int lock_flags = 0;
              if (mdbm_util_lockstr_to_flags(optarg, &lock_flags)) {
                fprintf(stderr, "Invalid locking argument, argument=%s, ignoring\n", optarg);
                usage(1);
              }
              oflags |= lock_flags;
            }
            if (oflags & MDBM_OPEN_NOLOCK) {            
              lock = 0;
            }
            break;

        case 'p':
            opt_nonversion = 1;
            checkVersionUsage(opt_version);
            pno = atoi(optarg);
            break;

        case 'V':
            opt_version = 1;
            checkVersionUsage(opt_nonversion);
            break;

        case 'v':
            verbose = atoi(optarg);
            break;

        case 'w':
            opt_nonversion = 1;
            checkVersionUsage(opt_version);
            winsize = mdbm_util_get_size(optarg,1);
            if (winsize == 0) {
                printf("Window size must be positive, size=%lu\n\n", (unsigned long)winsize);
                usage(1);
            }
            oflags |= MDBM_OPEN_WINDOWED | MDBM_O_RDWR;  /* Windowed mode fails with read-only access */
            break;

        case 'X':
            opt_nonversion = 1;
            opt_verify_version = atoi(optarg);
            break;

        default:
            opt_nonversion = 1;
            checkVersionUsage(opt_version);
            usage(1);
        }
    }

    if (optind+1 != argc) {
        usage(1);
    }

    fn = argv[optind];

    mdbm_log_minlevel(LOG_INFO);

    if (!winsize) {
        oflags |= MDBM_O_RDONLY;
    }

    db = mdbm_open(fn,oflags,0,0,0);
    if (!db) {
        mdbm_logerror(LOG_ERR,0,"%s: mdbm_open failure",fn);
        return 2;
    }

    if (opt_verify_version) {
        uint32_t version = mdbm_get_version(db);
        if (version != (uint32_t)opt_verify_version) {
            printf("MDBM versions do not match"
                   ", expected version=%d"
                   ", actual version=%d\n",
                   opt_verify_version, version);
            exit(1);
        }
    }

    if (!opt_version) {

#ifdef MDBM_CREATE_V3
        if (winsize) {
            errno = 0;
            if (mdbm_set_window_size(db, winsize) != 0) {
                printf("Unable to set window size to %llu (%s), exiting\n",
                    (unsigned long long) winsize, strerror(errno));
                mdbm_close(db);
                exit(1);
            }
        }
        if (dbcheck) {
            if (lock) {
                if (mdbm_lock(db) != 1) {
                    mdbm_logerror(LOG_ERR,0,"%s: mdbm_lock failure",fn);
                }
            }
            ret = mdbm_check(db,dbcheck,verbose);
            if (lock) {
                if (mdbm_unlock(db) != 1) {
                    mdbm_logerror(LOG_ERR,0,"%s: mdbm_unlock failure",fn);
                }
            }
            if (ret && verbose) {
                mdbm_log(LOG_ERR,"%s: failed integrity check",fn);
            }
        } else {
#endif
            /* This returns -1 upon error; 0 otherwise */
            if (pno < 0) {
                ret = mdbm_chk_all_page(db);
            } else {
                ret = mdbm_chk_page(db,pno);
            }
#ifdef MDBM_CREATE_V3
        }
#endif
    }
    else {
        /* Display version. */
        uint32_t version = mdbm_get_version(db);
        printf("%s%d\n",
               (verbose ? "MDBM version: " : ""), version);
    }

    mdbm_close(db);
    return ret ? 1 : 0;
}
