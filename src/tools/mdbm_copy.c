/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_internal.h"

#define PROG "mdbm_copy"

static void
usage(int ec)
{
    printf("\
Usage: mdbm_copy [options] SOURCE DEST\n\
Options:\n\
        -h      This help message\n\
        -l      Lock the entire database during copy\n\
        -L      Do not lock the database at all\n");
    exit(ec);
}


int
main(int argc, char** argv)
{
    int opt;
    int lock = 0;
    MDBM *db;
    int f;
    int oflags = MDBM_ANY_LOCKS;
    int nolock = 0;
    const int MAX_TRIES = 3;

    while ((opt = getopt(argc,argv,"hLl")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);
            break;
            
        case 'L':
            oflags = MDBM_OPEN_NOLOCK;
            lock = 0;
            nolock = 1;
            break;

        case 'l':
            lock = 1;
            break;

        default:
            usage(1);
        }
    }

    if (optind+2 > argc) {
        fputs(PROG ": Must specify source and destination files\n",stderr);
        usage(1);
    }

    if (lock && nolock) {
        fputs(PROG ": Options '-L' and '-l' conflict and cannot be used together\n",stderr);
        usage(1);
    }

    if ((db = mdbm_open(argv[optind],MDBM_O_RDONLY|oflags,0,0,0)) == NULL) {
        perror(argv[optind]);
        exit(2);
    }

    if ((f = open(argv[optind+1],O_RDWR|O_CREAT|O_TRUNC,0666)) < 0) {
        perror(argv[optind+1]);
        exit(2);
    }

    if (mdbm_internal_fcopy(db,f,lock ? MDBM_COPY_LOCK_ALL : 0, MAX_TRIES) < 0) {
        perror("fcopy");
        exit(2);
    }

    close(f);
    mdbm_close(db);

    return 0;
}
