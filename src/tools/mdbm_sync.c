/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdbm.h"

#define PREFIX "mdbm_sync "

static void
usage(int exit_code)
{
    fprintf(stderr, "\
Usage: mdbm_sync [options] <mdbm>\n\
\n\
Options are:\n\
  -h     This message\n\
  -L     Open with no locking\n\
\n");
    exit(exit_code);
}

int
main(int argc, char** argv)
{
    MDBM* db;
    int opt;
    int oflags = 0;

    while ((opt = getopt(argc,argv,"hL")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);

        case 'L':
            oflags |= MDBM_OPEN_NOLOCK;
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

    if ((db = mdbm_open( argv[optind], MDBM_O_RDONLY|oflags, 0, 0, 0)) == NULL) {
        fprintf(stderr, PREFIX "mdbm_open(%s): %s\n", argv[optind], strerror(errno));
        exit(1);
    }
    if (mdbm_sync(db)) {
        fprintf(stderr, PREFIX "mdbm_sync(%s): %s\n", argv[optind], strerror(errno));
        exit(1);
    }

    mdbm_close(db);
    return 0;
}
