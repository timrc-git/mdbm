/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_util.h"

#define PROG "mdbm_create"

static void
usage (void)
{
    printf(
"Usage: mdbm_create [options] <file name> ...\n"
"Options:\n"
"    -a <bytes>  Set record alignment (8-, 16-, 32-, 64-bits; default: 8)\n"
"    -c <mode>   Set cache mode (1=lfu 2=lru 3=gdsf; default: disabled)\n"
"    -D <mbytes> Specify initial/maximum size of db (default: no size limit)\n"
"    -d <mbytes> Specify initial size of db (default: minimum).\n"
"                Suffix k/m/g may be used to override default of m.\n"
"    -h <hash>   Hash function (default: 5/FNV32)\n"
"                   0  CRC-32\n"
"                   1  EJB\n"
"                   2  PHONG\n"
"                   3  OZ\n"
"                   4  TOREK\n"
"                   5  FNV32\n"
"                   6  STL\n"
"                   7  MD5\n"
"                   8  SHA-1\n"
"                   9  Jenkins\n"
"                  10  Hsieh SuperFast\n"
"    -K mode     locking mode \n"
lockstr_to_flags_usage("                  ")
"    -L          Enable large-object mode\n"
"    -l <bytes>  Set the spill size for data to be put on large-object heap. Dependent on -L\n"
"    -m <int>    File permissions (default: 0666)\n"
"    -N          Do not lock the DB.\n"
"    -p <bytes>  Page size (default: 4096).\n"
"                Suffix k/m/g may be used to override the default of bytes.\n"
"    -s <mbytes> Specify size of main db (remainder is large-object/overflow space).\n"
"                Suffix k/m/g may be used to override default of m.\n"
"                mbytes must evaluate to be a power-of-2 number of pages.\n"
"    -z          Truncate db if it already exists\n"
"\n"
"Do not use both options -d and -D on a command line.  If you use both, the last value\n"
"will be used for the mbytes value.  If -D is present, it will set the maximum db size.\n"
    );
    exit(1);
}


int
main (int argc, char** argv)
{
    int opt;
    int verflag = 0;
    int align = 0;
    uint64_t dbsize = 0;
    int hash = -1; /* No defaault.  Let mdbm_open use internal default. */
    int largeflag = 0;
    int spillsize = 0;
    int pagesize = 4096;
    int truncflag = 0;
    int lockflag = 0;
    int mode = 0666;
    int limit_size = 0;
#ifdef MDBM_CREATE_V3
    int cachemode = 0;
    uint64_t space = 0;
    uint64_t winsize = 0;
    int oflags = MDBM_O_FSYNC;
#endif
    int i;

#ifdef MDBM_CONFIG_DEFAULT_DBVER_V3
    verflag = MDBM_CREATE_V3;
#endif

    while ((opt = getopt(argc,argv,"23a:c:D:d:h:K:Ll:m:Np:s:w:z")) != -1) {
        switch (opt) {
        case '3':
#ifdef MDBM_CREATE_V3
            verflag = MDBM_CREATE_V3;
#else
            fputs(PROG ": -3 not supported in this version\n",stderr);
            exit(1);
#endif
            break;

        case 'a':
            switch (atoi(optarg)) {
            case 8:
                align = 0;
                break;

            case 16:
                align = MDBM_ALIGN_16_BITS;
                break;

            case 32:
                align = MDBM_ALIGN_32_BITS;
                break;

            case 64:
                align = MDBM_ALIGN_64_BITS;
                break;

            default:
                fprintf(stderr,PROG ": Invalid alignment (must be 8, 16, 32, or 64): %s\n",optarg);
                exit(1);
            }
            break;

        case 'c':
#ifdef MDBM_CREATE_V3
            cachemode = atoi(optarg);
            if (cachemode < 0 || cachemode > MDBM_CACHEMODE_MAX) {
                fprintf(stderr,PROG ": Invalid cachemode: %s\n",optarg);
                exit(1);
            }
#else
            fputs(PROG ": -C not supported in this version\n",stderr);
            exit(1);
#endif
            break;

        case 'D':
            limit_size = 1;
        case 'd':
            dbsize = mdbm_util_get_size(optarg,1024*1024);
            break;

        case 'h':
            hash = atoi(optarg);
            if (hash < 0 || hash > MDBM_MAX_HASH) {
                fprintf(stderr,PROG ": Invalid hash: %s\n",optarg);
                exit(1);
            }
            break;

        case 'L':
            largeflag |= MDBM_LARGE_OBJECTS;
            break;

        case 'l':
            spillsize = mdbm_util_get_size(optarg,1);
            break;

        case 'm': {
            char* s;
            errno = 0;
            mode = strtol(optarg,&s,0);
            if (errno || s == optarg || *s) {
                fprintf(stderr,PROG ": Invalid mode: %s\n",optarg);
                exit(1);
            }
            break;
        }

        case 'K': {
            {
              int lflags=0;
              if (mdbm_util_lockstr_to_flags(optarg, &lflags)) {
                fprintf(stderr, "Invalid locking argument, argument=%s\n", optarg);
                usage();
              }
              lockflag |= lflags;
            }
            break;
        }

        case 'N': {
            lockflag |= MDBM_OPEN_NOLOCK;
            break;
        }

        case 'p':
            pagesize = mdbm_util_get_size(optarg,1);
            break;

        case 's':
#ifdef MDBM_CREATE_V3
            space = mdbm_util_get_size(optarg,1024*1024);
#else
            fputs(PROG ": -s not supported in this version\n",stderr);
            exit(1);
#endif
            break;

#ifdef MDBM_CREATE_V3
        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
            oflags |= MDBM_OPEN_WINDOWED;
            break;
#endif

        case 'z':
            truncflag |= MDBM_O_TRUNC;
            break;

        default:
            usage();
        }
    }

    if (optind == argc) {
        fputs(PROG ": Missing db filename\n",stderr);
        usage();
    }

#ifdef MDBM_CREATE_V3
    if (space) {
        int pgs = space / pagesize;
        if ((space % pagesize) || (pgs & (pgs-1))) {
            fputs(PROG ": Specified main db size (-s) must result in power-of-2 database pages\n",
                  stderr);
            usage();
        }
        if (space > dbsize) {
            fputs(PROG ": Must specify total db size (-d or -D) >=  main db size (-s)\n",stderr);
            usage();
        }
    }
#endif

    for (i = optind; i < argc; i++) {
        MDBM *db;
        int flags;

        /*
        // unlink fixes issue where overwriting existing DB leaves config
        // info behind and interferes with the new DB created.
        // Ex: if unlink() not performed
        //  ./mdbm_create -3 -l 2000  /tmp/t3.mdbm
        //  new_mdbm_stat /tmp/t3.mdbm # Reports no spill size...ok
        //  ./mdbm_create -3 -l 2000 -L /tmp/t3.mdbm
        //  new_mdbm_stat /tmp/t3.mdbm # Still reports no spill size...bad
        */
        unlink(argv[i]);

        flags = MDBM_O_RDWR|MDBM_O_CREAT|truncflag|largeflag|verflag|lockflag|oflags;
        if ((db = mdbm_open(argv[i],flags,mode,pagesize,0)) == NULL) {
            fprintf(stderr,PROG ": %s: %s\n",argv[i],strerror(errno));
            exit(2);
        }
        if (limit_size) {
            mdbm_limit_size_v3(db,dbsize / pagesize,NULL,NULL);
        }
#ifdef MDBM_CREATE_V3
        if (winsize) {
            mdbm_set_window_size(db,winsize);
        }
        if (space) {
            mdbm_limit_dir_size(db,space / pagesize);
            mdbm_pre_split(db,space / pagesize);
        } else
#endif
            if (dbsize) {
                mdbm_pre_split(db,dbsize / pagesize);
            }
        if (align) {
            mdbm_set_alignment(db,align);
        }
#ifdef MDBM_CREATE_V3
        if (cachemode) {
            mdbm_set_cachemode(db,cachemode);
        }
#endif
        if (hash >= 0) {
            mdbm_sethash(db,hash);
        }
        if (spillsize > 0) {
            if (largeflag == 0)
                printf("WARNING: Verify you enabled Large object support(-L) else spill size may have no effect\n");
            if (mdbm_setspillsize(db, spillsize) < 0)
                printf("ERROR: failed to set spill size=%d errno=%d. Verify you enabled Large object support(-L)\n", spillsize, errno);
        }

        mdbm_fsync(db); /* hugetlbfs needs an explicit fsync to flush to disk. */
        mdbm_close(db);
    }

    return 0;
}
