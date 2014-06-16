/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Recreates an MDBM database from a transportable format. */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <mdbm.h>


#define PREFIX "mdbm_restore: "


static void
usage (int err)
{
    fprintf(stderr,"\
Usage: mdbm_restore [options] <dbfilename> <filename>\n\
        -h              This message\n\
        -L              Open with no locking\n");
    exit(err);
}


int
main (int argc, char** argv)
{
    MDBM *db;
    int opt;
    int oflags = 0;

    while ((opt = getopt(argc,argv,"h")) != -1) {
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

    if ((argc - optind) != 2) {
        usage(1);
    }

    if (!(oflags&MDBM_OPEN_NOLOCK)) {
      oflags |= MDBM_ANY_LOCKS;
    }

    /* Make sure <dbfilename> doesn't exist; and <filename> does.
     * This will help stop invocations to generate a new mdbm when the
     * args are backwards.
     *
     * Ideally we'd confirm the backup header in the savefile before
     * opening the mdbm; but mdbm_restore() doesn't work that way.
     * This is a quick hack that should help.
     *
     * This does stop mdbm_restore from being able to write over an
     * existing mdbm, but that's probably a good thing.  If that
     * functionality is required, the user should probably use
     * mdbm_restore to create a new mdbm; and mdbm_replace to put it
     * in place.
     */
    {
        struct stat st;
        /* Make sure savefile exists */
        if (stat( argv[optind+1], &st ) != 0) {
            /* Stat failed.  Savefile does *not* exist. */
            fprintf(stderr,PREFIX "save file [%s] does not exist: %s\n",
                    argv[optind+1], strerror(errno));
            exit(2);
        }

        /* Make sure destination MDBM does *not* exist */
        if (stat( argv[optind], &st ) == 0) {
            /* Stat succeded - something is there. */
            fprintf(stderr,PREFIX "mdbm [%s] already exists!\n",
                    argv[optind+1]);
            exit(2);
        }
    }

    if ((db = mdbm_open(argv[optind],MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|oflags,0666,0,0)) == NULL) {
        fprintf(stderr,PREFIX "mdbm_open(%s): %s\n",argv[optind],strerror(errno));
        exit(1);
    }

    if (mdbm_restore(db,argv[optind+1]) < 0) {
        fprintf(stderr,PREFIX "restore failed: %s\n",strerror(errno));  
        return 2;
    }

    mdbm_close(db);
    return 0;
}


