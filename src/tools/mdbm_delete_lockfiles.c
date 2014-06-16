/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* 
 * Given pathname of an MDBM, remove all its lock files.
 * Use this program with extreme caution: you should only use it when all processes that
 * access the MDBM whose locks are being deleted are not running.  Otherwise, the MDBM
 * will likely be corrupted.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_util.h"

#define PREFIX "mdbm_delete_lockfiles "

static void
Usage(int err)
{
    fprintf(stderr, "\
Usage: " PREFIX "[options] <Fully_Qualified_Path_Of_MDBM>\n\n\
  Remove all the lock files belonging to an MDBM.\n\
  Use this program with extreme caution: you should only use it when all processes that\n\
  access the MDBM whose locks are being deleted are not running.  Otherwise, the MDBM\n\
  will likely be corrupted.\n\
Options:\n\
        -h              Show this help message\n\
");
    exit(err);
}


int
main(int argc, char** argv)
{
    int opt;

    while ((opt = getopt(argc,argv,"h")) != -1) {
        switch (opt) {
        case 'h':
            Usage(0);
            break;
        default:
            Usage(1);
        }
    }

    if ((argc - optind) != 1) {
        Usage(1);
    }

    if (mdbm_delete_lockfiles( argv[optind] )) {
        fprintf(stderr, PREFIX "Failed when deleting lockfiles of %s: %s\n",
                argv[optind], strerror(errno) );
        return (1);
    }

    return 0;
}

