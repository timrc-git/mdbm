/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "mdbm.h"

#define PROG "mdbm_compress "

static void
usage(int err)
{
    fprintf(stderr, "\
Usage: " PROG "[options] <mdbm>\n\
\n\
Options:\n\
        -h              This message\n\
\n");
    exit(err);
}

int
main (int argc, char** argv)
{
    MDBM* db;
    int opt;
    int exit_code;

    while ((opt = getopt(argc, argv, "D:hfLp:S:s")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);

        default:
            usage(1);
        }
    }

    if ((argc - optind) != 1) {
        usage(1);
    }

    db = mdbm_open(argv[1], MDBM_O_RDWR|MDBM_ANY_LOCKS, 0, 0, 0);
    if (!db) {
        fprintf(stderr, "%s: %s\n", argv[1], strerror(errno));
        return 2;
    }

    if (mdbm_get_version(db) == 3) {
        exit_code = 3;
        fprintf(stderr, "%s: mdbm_compress is not support yet for mdbmv3.\n", argv[1]);
    }
    else {
        exit_code = 0;
        mdbm_compress_tree(db);
    }

    mdbm_close(db);
    return exit_code;
}
