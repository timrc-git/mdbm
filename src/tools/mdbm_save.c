/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Saves an MDBM database to a transportable format. */

#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <mdbm.h>


#define PREFIX "mdbm_save: "


static void
usage (int err)
{
    fprintf(stderr,"\
Usage: mdbm_save [options] <dbfilename> <filename>\n\
        -h              This message\n\
        -c              Compress db tree before saving\n\
        -C <level>      Use specified compression level (default: none)\n\
        -L              Open with no locking\n");
    exit(err);
}


int
main (int argc, char** argv)
{
    MDBM *db;
    int opt;
    int flags = MDBM_O_CREAT|MDBM_O_TRUNC;
    int compression = 0;
    int oflags = 0;

    while ((opt = getopt(argc,argv,"hcC:L")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);

        case 'c':
            flags |= MDBM_SAVE_COMPRESS_TREE;
            break;

        case 'C':
            compression = atoi(optarg);
            break;

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

    if ((db = mdbm_open(argv[optind],
                        ((flags & MDBM_SAVE_COMPRESS_TREE) ? MDBM_O_RDWR : MDBM_O_RDONLY)|oflags,0,0,0))
        == NULL)
    {
        fprintf(stderr,PREFIX "mdbm_open(%s): %s\n",argv[optind],strerror(errno));
        exit(1);
    }

    if (mdbm_save(db,argv[optind+1],flags,0666,compression) < 0) {
        fprintf(stderr,PREFIX "save failed: %s\n",strerror(errno));     
        return 2;
    }

    mdbm_close(db);
    return 0;
}


