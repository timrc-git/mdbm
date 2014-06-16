/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_util.h"

#define PROG "mdbm_digest"

static void
usage(int ec)
{
    printf("\
Usage: mdbm_digest [options] DBFILE\n\
Options:\n\
        -h            This help message\n\
        -m            Generate md5\n\
        -s            Generate sha-1\n\
        -v            Show verbose stats\n\
        -w <size>     Open database in windowed mode with specified window size.\n\
                      Suffix k/m/g may be used to override the default of bytes.\n\
");
    exit(ec);
}


int
main (int argc, char** argv)
{
    int opt;
    MDBM *db;
    int md5 = 0;
    int sha1 = 0;
    int verbose = 0;
    uint64_t winsize = 0;
    int oflags = MDBM_ANY_LOCKS;

    while ((opt = getopt(argc,argv,"hmsvw:")) != -1) {
        switch (opt) {
        case 'h':
            usage(0);
            break;

        case 'm':
            md5 = 1;
            break;

        case 's':
            sha1 = 1;
            break;

        case 'v':
            verbose++;
            break;

        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
            if (winsize == 0) {
                printf("Window size must be positive, size=%lu\n\n", (unsigned long)winsize);
                usage(1);
            }
            oflags |= (MDBM_OPEN_WINDOWED | MDBM_O_RDWR);
            break;

        default:
            usage(1);
        }
    }

    if (optind+1 > argc) {
        fputs(PROG ": Must specify db filename\n",stderr);
        usage(1);
    }

    if (!md5 && !sha1) {
        sha1 = 1;
    }

    if (!winsize) {
        oflags |= MDBM_O_RDONLY;
    }
    if ((db = mdbm_open(argv[optind],oflags,0,0,0)) == NULL) {
        perror(argv[optind]);
        exit(2);
    }
    if (winsize) {
        errno = 0;
        if (mdbm_set_window_size(db, winsize) != 0) {
            printf("Unable to set window size to %llu (%s), exiting\n",
                (unsigned long long) winsize, strerror(errno));
            mdbm_close(db);
            exit(1);
        }
    }

    if (mdbm_lock(db) != 1) {
        perror("mdbm_lock");
    } else {
        kvpair kv;
        MD5_CTX md5_ctx;
        SHA_CTX sha_ctx;
        uint32_t nitems = 0;
        uint64_t kbytes = 0;
        uint64_t vbytes = 0;

        MD5_Init(&md5_ctx);
        SHA1_Init(&sha_ctx);

        kv = mdbm_first(db);
        while (kv.val.dptr) {
            nitems++;
            kbytes += kv.key.dsize;
            vbytes += kv.val.dsize;
            if (md5) {
                MD5_Update(&md5_ctx,kv.key.dptr,kv.key.dsize);
                MD5_Update(&md5_ctx,kv.val.dptr,kv.val.dsize);
            }
            if (sha1) {
                SHA1_Update(&sha_ctx,kv.key.dptr,kv.key.dsize);
                SHA1_Update(&sha_ctx,kv.val.dptr,kv.val.dsize);
            }
            kv = mdbm_next(db);
        }

        mdbm_unlock(db);

        if (md5) {
            unsigned char d[MD5_DIGEST_LENGTH];
            int i;

            MD5_Final(d,&md5_ctx);
            printf("MD5(%s)= ",argv[optind]);
            for (i = 0; i < sizeof(d); i++) {
                printf("%02x",d[i]);
            }
            putchar('\n');
        }
        if (sha1) {
            unsigned char d[SHA_DIGEST_LENGTH];
            int i;

            SHA1_Final(d,&sha_ctx);
            printf("SHA1(%s)= ",argv[optind]);
            for (i = 0; i < sizeof(d); i++) {
                printf("%02x",d[i]);
            }
            putchar('\n');
        }
        if (verbose) {
            printf("Total items= %u\n",nitems);
            printf("Total key bytes= %llu\n",(unsigned long long)kbytes);
            printf("Total value bytes= %llu\n",(unsigned long long)vbytes);
        }
    }

    mdbm_close(db);

    return 0;
}
