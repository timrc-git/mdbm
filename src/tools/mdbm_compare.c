/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <sys/fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_util.h"

#define MDBM_DUMP_DELETED       0x1
#define MDBM_DUMP_KEYS          0x2
#define MDBM_DUMP_VALUES        0x4
#define MDBM_DUMP_CDB           0x8

static int flags;
static FILE* outfile = NULL;

void print_bytes(const char* prefix, datum d, int print_all) {
    int i, len;
    int max;
    const char* s;

    len = d.dsize;
    if (!print_all) {
        s = getenv("MDBM_DUMP_BYTES"); /* TODO WTF? */
        if (s) {
            max = atoi(s);
            putchar('\n');
        } else { max = 16; }
        if (len > max) { len = max; }
    }
    s = d.dptr;
    while (len > 0) {
        printf("%s%08x  ",prefix,(unsigned int)(intptr_t)s);
        for (i = 0; i < 16; i++) {
            if (i == 8) { putchar(' '); }
            if (i < len) { printf("%02x ",(unsigned char)(s[i])); }
            else { fputs("   ",outfile); }
        }
        fputs("  ",outfile);
        for (i = 0; i < 16; i++) {
            if (i < len) {
                if (s[i] > ' ' && s[i] < 0x7f) { putchar(s[i]); }
                else { putchar('.'); }
            }
        }
        s += 16;
        len -= 16;
        putchar('\n');
    }
}

static void cdb_dump(FILE* fp, datum key, datum val) {
    fprintf(fp, "+%d,%d:", key.dsize, val.dsize);
    fwrite(key.dptr, key.dsize, 1, fp);
    fwrite("->", 2, 1, fp);
    fwrite(val.dptr, val.dsize, 1, fp);
    fputc('\n', fp);
}


static void dump_kv(datum key, datum val) {
    if (flags & MDBM_DUMP_CDB) { 
      cdb_dump(outfile, key, val); 
    } else {
      if (flags & MDBM_DUMP_KEYS) { 
        print_bytes("  K ",key,1); 
      }
      if (flags & MDBM_DUMP_VALUES) { 
        print_bytes("  V ",val,1); 
      }
    }
}

static uint64_t dump_different(MDBM* db1, MDBM* db2, int missing_only, int no_missing) {
    uint64_t retval = 0;
    kvpair kv;
    datum d;
    MDBM_ITER iter;

    MDBM_ITER_INIT(&iter);
    kv = mdbm_first_r(db1, &iter);
    while (kv.key.dptr != NULL) {
        d = mdbm_fetch(db2, kv.key);

        if (NULL == d.dptr || 0 == d.dsize) {
          if (!no_missing) {
            dump_kv(kv.key, kv.val);
            ++retval;
          }
        } else if ((kv.val.dsize != d.dsize) || memcmp(kv.val.dptr, d.dptr, kv.val.dsize)) {
          if (!missing_only) {
            dump_kv(kv.key, kv.val);
            ++retval;
          }
        }
        kv = mdbm_next_r(db1, &iter);
    }
    return retval;
}


static void usage (void) {
    printf("\
Usage: mdbm_compare [options] <db_filename1> <db_filename2>\n\
Compares two MDBMs, dumping out differing entries.\n\
Options:\n\
        -k          Print key data\n\
        -L          Do not lock db\n\
        -v          Print value data\n\
        -f <file>   Specify an output file (defaults to stdout)\n\
        -F <format> Specify alternate output format (currently only \"cdb\")\n\
        -m          Missing. Only show entries not in the second DB.\n\
        -M          Not missing. Only show entries that exist (but differ) in both DBs.\n\
                    NOTE: -m and -M are mutually exclusive.\n\
        -w <size>   Use windowed mode with specified window size\n\
");
}

int main(int argc, char** argv) {
    uint64_t diffcount = 0;
    int retval = 0;
    int opt;
    int oflags = 0;
    uint64_t winsize = 0;
    MDBM *db1 = NULL;
    MDBM *db2 = NULL;
    int missing = 0;
    int common = 0;
    const char* outname = NULL;

    /* initialize to be unit-test friendly */
    flags = 0;
    outfile = stdout;

    while ((opt = getopt(argc,argv,"hf:F:kLmMvw:")) != -1) {
        switch (opt) {
        case 'f':
            outname=optarg;
            if (!(outname && outname[0])) {
              fprintf(stderr, "ERROR: must specify an output filename!\n");
              usage();
              return -1;
            }
            break;
        case 'F':
            if (0==strcmp(optarg, "cdb")) {
              flags |= MDBM_DUMP_CDB;
            } else {
              fprintf(stderr, "ERROR: Unknown output format option!\n");
              usage();
              return -1;
            }
            break;
        case 'h':
            usage(); 
            return -1;
        case 'k':
            flags |= MDBM_DUMP_KEYS;
            break;
        case 'L':
            oflags |= MDBM_OPEN_NOLOCK;
            break;
        case 'm':
            missing = 1;
            break;
        case 'M':
            common = 1;
            break;
        case 'v':
            flags |= MDBM_DUMP_VALUES;
            break;
        case 'w':
            winsize = mdbm_util_get_size(optarg,1);
            oflags |= MDBM_OPEN_WINDOWED;
            break;
        }
    }

    if (missing && common) {
      fprintf(stderr, "ERROR: only specify one of -m or -M !\n");
      usage();
      return -1;
    }
    if (optind+2 != argc) { 
      fprintf(stderr, "ERROR: must provide two mdbm files to compare !\n");
      usage(); 
      return 1; 
    }

    if (!(oflags&MDBM_OPEN_NOLOCK)) {
      oflags |= MDBM_ANY_LOCKS;
    }

    db1 = mdbm_open(argv[optind],MDBM_O_RDONLY|oflags,0,0,0);
    if (!db1) {
        perror(argv[optind]);
        retval = -1;
        goto cleanup;
    }
    if (winsize) {
        mdbm_set_window_size(db1,winsize);
    }

    db2 = mdbm_open(argv[optind+1],MDBM_O_RDONLY|oflags,0,0,0);
    if (!db2) {
        perror(argv[optind+1]);
        retval = -1;
        goto cleanup;
    }
    if (winsize) {
        mdbm_set_window_size(db2,winsize);
    }
    
    if (outname) {
      outfile = fopen(outname, "wb");
      if (!outfile) {
        retval = -1;
        goto cleanup;
      }
    }

    if (missing) {
      diffcount = dump_different(db1, db2, 1, 0);
    } else if (common) {
      diffcount = dump_different(db1, db2, 0, 1);
    } else {
      /*fprintf(stderr,"********** A vs B ************\n"); */
      /* Things that are different or in db1 only */
      diffcount = dump_different(db1, db2, 0, 0);
      /*fprintf(stderr,"********** B vs A ************\n"); */
      /* Things that are in db2 only */
      diffcount += dump_different(db2, db1, 1, 0);
    }

    if (flags & MDBM_DUMP_CDB) { 
        /* cdbdump requires extra newline at the end */
        fputc('\n', outfile);
    }

    retval = (diffcount!=0) ? 1:0;

cleanup:
    if (db1) { 
      mdbm_close(db1); 
      db1=NULL; 
    }
    if (db2) { 
      mdbm_close(db2); 
      db2=NULL; 
    }
    if (outfile && outfile!=stdout) { 
      fclose(outfile); 
      outfile=NULL; 
    }
    return retval;
}

