/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Fetch a specific key from an MDBM. */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>

#include "mdbm.h"
#include "mdbm_util.h"

#define PREFIX "mdbm_fetch"

static void
usage( int err )
{
    fprintf(stderr, "\
Usage: mdbm_fetch [options] <mdbm> key\n\
\n\
Options are:\n\
  -h            This message\n\
  -L            Open with no locking\n\
  -p            Print page number\n\
  -P            Use partitioned locking\n\
  -v            Show verbose info\n\
  -w <size>     Use windowed mode with specified window size\n\
\n");
    exit( err );
}


int
main (int argc, char** argv)
{
    MDBM* db;
    int opt;
    int oflags = 0;
    uint64_t winsize = 0;
    int verbose = 0, print_page_num = 0;

    while ((opt = getopt(argc,argv,"hLpPvw:")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);

        case 'L':
            oflags |= MDBM_OPEN_NOLOCK;
            break;

        case 'p':
            print_page_num = 1;
            break;

        case 'P':
            oflags |= MDBM_PARTITIONED_LOCKS;
            break;

        case 'v':
            ++verbose;
            break;

        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
            if (winsize == 0) {
                printf("Window size must be positive, size=%lu\n\n", (unsigned long)winsize);
                usage(1);
            }
            oflags |= (MDBM_OPEN_WINDOWED | MDBM_O_RDWR);  /* Windowed mode fails with read-only access */
            break;

        default:
            usage(1);
        }
    }
    if (!(oflags&MDBM_OPEN_NOLOCK)) {
      oflags |= MDBM_ANY_LOCKS;
    }

    if ((argc - optind) != 2) {
        usage(1);
    }

    if (!winsize) {
        oflags |= MDBM_O_RDONLY;
    }

    if (( db = mdbm_open( argv[optind], oflags, 0, 0, 0 )) == NULL ) {
        fprintf(stderr, PREFIX ": mdbm_open(%s): %s\n",
                argv[optind], strerror(errno) );
        exit(1);
    }
    if (winsize) {
        errno = 0;
        if (mdbm_set_window_size(db, winsize) != 0) {
            printf("Unable to set window size to %llu (%s) exiting\n",
                (unsigned long long) winsize, strerror(errno));
            mdbm_close(db);
            exit(1);
        }
    }

    /* ASSERT: db is open.  Fetch key. */
    {
        datum key, val;
        MDBM_ITER iter;
        
        key.dptr = argv[optind+1];
        key.dsize = strlen( key.dptr );
        
        if (oflags & MDBM_PARTITIONED_LOCKS) {
            mdbm_plock(db,&key,0);
        } else {
            mdbm_lock(db);
        }

        MDBM_ITER_INIT( &iter );
        while (mdbm_fetch_dup_r( db, &key, &val, &iter ) == 0) {
            if (verbose) {
                printf("key (%4d bytes): %.*s\n",key.dsize,key.dsize,key.dptr);
                printf("val (%4d bytes): %.*s\n",val.dsize,val.dsize,val.dptr);
                if (print_page_num) {
                    printf("Page-number is: %u\n", mdbm_get_page(db, &key));
                }
            } else {
                fwrite(key.dptr,key.dsize,1,stdout);
                fputs(" :: ",stdout);
                fwrite(val.dptr,val.dsize,1,stdout);
                if (print_page_num) {
                    fputs(" :: ",stdout);
                    fprintf(stdout, "page-number= %u", mdbm_get_page(db, &key));
                }
                fputc('\n',stdout);
            }
        }

        mdbm_unlock(db);
    }

    mdbm_close(db);
    return 0;
}
