/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_util.h"

#define PREFIX "mdbm_purge "

static void
usage(int err)
{
    fprintf(stderr, "\
Usage: " PREFIX "[options] <mdbm>\n\
Options:\n\
        -h              Show this help message\n\
        -L              Open with no locking\n\
        -w <size>       Use windowed mode with specified window size.\n\
                        Suffix k/m/g may be used to override default of b.\n\
");
    exit(err);
}


int
main(int argc, char** argv)
{
    MDBM* db;
    int opt;
    int oflags = 0;
    uint64_t winsize = 0;

    while ((opt = getopt(argc,argv,"hLw:")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);
            break;

        case 'L':
            oflags |= MDBM_OPEN_NOLOCK;
            break;

        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
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

    if (( db = mdbm_open( argv[optind], MDBM_O_RDWR|oflags, 0, 0, 0 )) == NULL ) {
        fprintf(stderr, PREFIX "mdbm_open(%s): %s\n",
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

    mdbm_purge(db);
    mdbm_close(db);
    return 0;
}
