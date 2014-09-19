/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Reads a key/val format and populates an MDBM database file. */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include <mdbm.h>
#include <mdbm_util.h>

static void
usage()
{
    fprintf(stderr, 
"usage: mdbm_import [options] outfile.mdbm\n"
"  -3           Create V3 DB\n"
"  -c           Input is in cdbdump format (default db_dump)\n"
"  -D           Delete keys with zero-length values.\n"
"  -d dbsize    Create DB with initial <dbsize> DB size.\n"
"               Suffix k/m/g may be used to override default of m.\n"
"  -f           Fast mode (don't lock DB while reading)\n"
"  -h           Help\n"
"  -i infile    Read from <infile> instead of stdin\n"
"  -l           Create DB with Large Object support\n"
"  -L locking   Specify the type of locking to use:\n"
lockstr_to_flags_usage("                 ")
"  -p pgsize    Create DB with page size.\n"
"               Suffix k/m/g may be used to override default of bytes.\n"
"  -s hash      Create DB with <hash> hash function\n"
"  -S flag      Store flag value:\n"
"                 0 - MDBM_INSERT -- Store fails if there is an existing key\n"
"                 1 - MDBM_REPLACE -- Store replaces an existing entry, \n"
"                     or creates a new one (default)\n"
"                 2 - MDBM_INSERT_DUP -- Store adds a duplicate entry for an existing key\n"
"                 3 - MDBM_MODIFY -- Store fails if an existing key does not exist\n"
"  -T           Input has no db_dump header\n"
"  -y pgcnt     Set DB maximum size with <pgcnt> pages\n"
"               Suffix k/m/g may be used.\n"
"  -z size      Set large object spill size (requires Large Objects enabled)\n"
"               Suffix k/m/g may be used to override default of bytes.\n"
"  -Z           Truncate existing DB before importing\n"
"\n"
"Creation time options are used only when an mdbm is created, and not on\n"
"subsequent imports.\n"
"\n"
"Truncating a DB will remove the previous creation-time configuration.\n"
);
}

static char inputFile[PATH_MAX + 1];
static char *mdbmFile;
static int opt_delete_zero = 0;
static int lineno = 1;

static void
err_readformat(char badchar, int linenum)
{
    fprintf(stderr, "Unable to read bad input format, char=%c, mdbm=%s, inputFile=%s, line=%d\n",
            badchar, mdbmFile, inputFile, linenum);
}

static int
dbload_header(FILE *fp, int *pgsize, int *pgcount, int *large)
{
    char key[100], value[100];

    for (lineno = 1;; lineno++) {
        if (fscanf(fp,"%99[^=]=%99s\n", key, value) != 2) {
            fprintf(stderr, "Bad input record format (expecting key=val), mdbm=%s, inputFile=%s, line=%d\n",
                    mdbmFile, inputFile, lineno);
            return -1;
        }

        if (strcmp(key, "HEADER") == 0)
            break;              // end of metadata

        if (strcmp(key, "format") == 0 && strcmp(value, "print") != 0) {
            fprintf(stderr, "Cannot parse input format, mdbm=%s, inputFile=%s, line=%d, value=%s\n",
                    mdbmFile, inputFile, lineno, value);
            return -1;
        }

        if (strcmp(key, "mdbm_pagesize") == 0)
            *pgsize = atoi(value);

        if (strcmp(key, "mdbm_pagecount") == 0)
            *pgcount = atoi(value);

        if (strcmp(key, "mdbm_large") == 0)
            *large = (atoi(value) == 1) ? 1 : 0;
    }

    return 0;
}

inline char
x2c(int c1, int c2)
{
    int digit =
        (((c1 >= 'A') ? ((c1 & 0xdf) - 'A') + 10 : (c1 - '0')) << 4)
        | ((c2 >= 'A' ? ((c2 & 0xdf) - 'A') + 10 : (c2 - '0')));
    return (char)digit;
}

inline int
read_str(FILE *fp, std::string &str)
{
    int ch, ch2, ch3;

    str.erase();

    for (;;) {
        ch = getc(fp);
        switch(ch) {
        case '\n':
            return 0;
        case EOF:
            return EOF;
        case '\\':
            ch2 = getc(fp);
            if (ch2 == '\\') {
                str.append((size_t)1, '\\');
            }
            else {
                if (ch2 != '\n') {
                    // expect two hex characters
                    ch3 = getc(fp);
                    if (isxdigit(ch2) && isxdigit(ch3)) {
                        str.append((size_t)1, x2c(ch2, ch3));
                    }
                    else {
                        fprintf(stderr, "Bad input escape format, mdbm=%s, inputFile=%s, line=%d\n",
                                mdbmFile, inputFile, lineno);
                        return -2;
                    }
                }
            }
            break;
        default:
            str.append((size_t)1, ch);
        }
    }
    /*NOTREACHED*/
}

static int
store(MDBM *db, datum key, datum val, int store_flag)
{
    int rc = 0, lock_ret = 1;

    if ((lock_ret = mdbm_lock_smart(db, &key, MDBM_O_RDWR)) == -1) {
        fprintf(stderr, "Cannot acquire smart lock, mdbm=%s, inputFile=%s, line=%d, errno=%s\n",
                mdbmFile, inputFile, lineno, strerror(errno));
        return -1;
    }

    if (opt_delete_zero && 0==val.dsize) {
        rc = mdbm_delete(db, key);
        //if (rc) { fprintf(stderr, "Failed delete, mdbm=%s, errno=%s\n", mdbmFile, strerror(errno)); }
        // ignore error, it may have been already deleted or never added
        rc = 0;
    } else {
        rc = mdbm_store(db, key, val, store_flag);
    }

    if (lock_ret == 1) {
        if (mdbm_unlock_smart(db, &key, 0) != 1) {
            fprintf(stderr, "Cannot release smart lock, mdbm=%s, inputFile=%s, line=%d, errno=%s\n",
                    mdbmFile, inputFile, lineno, strerror(errno));
        }
    }

    switch (rc) {
    case -1:
        fprintf(stderr, "Cannot store record, store_flag=%d, mdbm=%s, inputFile=%s, line=%d, errno=%s\n",
                store_flag, mdbmFile, inputFile, lineno, strerror(errno));
        break;

    case 1:
        fprintf(stderr, "Cannot overwrite existing key, store_flag=%d, mdbm=%s, inputFile=%s, line=%d\n",
                store_flag, mdbmFile, inputFile, lineno);
        break;
    }

    return rc;
}

static int
do_dbload(MDBM *db, FILE *fp, int store_flag)
{
    std::string keybuf(""), valbuf("");
    datum key, val;
    int status;

    for (;; lineno++) {
        if ((status = read_str(fp, keybuf)) != 0) {
            if (status == EOF)
                break;
            else
                return -2;
        }

        if (keybuf.size() == 0) {
            fprintf(stderr, "Bad (zero) key length, mdbm=%s, inputFile=%s, line=%d\n",
                    mdbmFile, inputFile, lineno);
            return -2;
        }

        if ((status = read_str(fp, valbuf)) != 0) {
            if (status == EOF)
                fprintf(stderr, "Unexpected EOF, mdbm=%s, inputFile=%s, line=%d\n",
                        mdbmFile, inputFile, lineno);
            return -2;
        }

        key.dptr = (char*) keybuf.data();
        key.dsize = keybuf.size();

        val.dptr = (char*) valbuf.data();
        val.dsize = valbuf.size();

        if (store(db, key, val, store_flag) == -1) {
            return -3;
        }
    }

    return 0;
}

static int
do_cdbmake(MDBM *db, FILE *fp, int store_flag)
{
    unsigned int klen;
    unsigned int klenMax = 429496720;   /* aproximately, 2^32 / 10 */
    unsigned int dlen;
    unsigned int dlenMax = klenMax;
    unsigned int i;
    int ch;
    std::string keybuf, valbuf;
    datum key, val;

    lineno = 1;
    for (;;) {
        /* Read key length. */
        ch = getc(fp);
        if (ch == '\n')
        {
            lineno++;
            break;
        }
        if (ch != '+') {
            err_readformat(ch, lineno);
            return -1;
        }

        klen = 0;
        for (;;) {
            /* Read value length. */
            ch = getc(fp);
            if (ch == ',')
                break;
            if ((ch < '0') || (ch > '9')) {
                err_readformat(ch, lineno);
                return -1;
            }
            if (klen > klenMax) {
                errno = ENOMEM;
                fprintf(stderr, "Key length too large, klen=%u, max=%d, mdbm=%s, inputFile=%s, line=%d, errno=%s\n",
                        klen, klenMax, mdbmFile, inputFile, lineno, strerror(errno));
                return -2;
            }
            klen = klen * 10 + (ch - '0');
        }
        if (klen == 0) {
            fprintf(stderr, "Bad input key length (zero), mdbm=%s, inputFile=%s, line=%d\n",
                    mdbmFile, inputFile, lineno);
            return -1;
        }

        dlen = 0;
        for (;;) {
            ch = getc(fp);
            if (ch == ':')
                break;
            if ((ch < '0') || (ch > '9')) {
                err_readformat(ch, lineno);
                return -1;
            }
            if (dlen > dlenMax) {
                errno = ENOMEM;
                fprintf(stderr,
                        "Data length too large, dlen=%u, dlenMax=%u, mdbm=%s, inputFile=%s, line=%d, errno=%s\n",
                        dlen, dlenMax, mdbmFile, inputFile, lineno, strerror(errno));
                return -2;
            }
            dlen = dlen * 10 + (ch - '0');
        }

        /* Read key. */
        keybuf.erase();
        if (klen > keybuf.capacity()) {
            keybuf.reserve(klen);
        }
        for (i = 0;i < klen;++i) {
            ch = getc(fp);
            if (ch == EOF) {
                fprintf(stderr, "Unexpected EOF, mdbm=%s, inputFile=%s, line=%d\n",
                        mdbmFile, inputFile, lineno);
                return -3;
            }
            keybuf.append((size_t)1, (char)ch);
        }
        ch = getc(fp);
        if (ch != '-') {
            err_readformat(ch, lineno);
            return -1;
        }
        ch = getc(fp);
        if (ch != '>') {
            err_readformat(ch, lineno);
            return -1;
        }

        /* Read Value. */
        valbuf.erase();
        if (dlen > valbuf.capacity()) {
            valbuf.reserve(dlen);
        }
        for (i = 0;i < dlen;++i) {
            ch = getc(fp);
            if (ch == EOF) {
                fprintf(stderr, "Unexpected EOF, mdbm=%s, inputFile=%s, line=%d\n",
                        mdbmFile, inputFile, lineno);
                return -3;
            }
            valbuf.append((size_t)1, (char)ch);
        }

        ch = getc(fp);
        if (ch != '\n') {
            err_readformat(ch, lineno);
            return -1;
        }
        lineno++;

        key.dptr = (char*) keybuf.data();
        key.dsize = klen;

        val.dptr = (char*) valbuf.data();
        val.dsize = dlen;

        if (store(db, key, val, store_flag) == -1) {
            return -3;
        }
    }

    return 0;
}

int
main(int argc, char** argv)
{
    int c;
    int err_flag = 0;
    int opt_help = 0;
    int opt_fast = 0;
    int opt_cdbdump = 0;
    char *opt_infile = NULL;
    int flags = MDBM_O_RDWR|MDBM_O_CREAT;
    int opt_header = 1;
    int opt_pagesize = -1;
    uint64_t opt_dbsize = 0;
    int opt_hashfnid = -1;
    int opt_setspillsize = -1;
    int opt_store_flag = MDBM_REPLACE;
    int opt_limit_size = -1;
    int opt_version = 0;
    int opt_trunc = 0;
    opt_delete_zero = 0; // reset here to be unit-test friendly
    bool locking_requested = false;

    while ((c = getopt(argc, argv, "3hi:lL:p:d:DfcTs:S:y:z:Z")) != EOF) {
        switch (c) {
        case '3':
            opt_version = MDBM_CREATE_V3;
            break;
        case 'h':
            opt_help = 1;
            break;
        case 'f':
            fprintf(stderr, "Option \"-f\" is deprecated, use \"-L none\" \n");
            flags |= MDBM_OPEN_NOLOCK;
            opt_fast = 1;
            break;
        case 'c':
            opt_cdbdump = 1;
            break;
        case 'T':
            opt_header = 0;
            break;
        case 'i':
            opt_infile = optarg;
            break;
        case 'p':
            opt_pagesize = mdbm_util_get_size(optarg, 1);
            if ((opt_pagesize < MDBM_MINPAGE) ||
                (opt_pagesize > MDBM_MAXPAGE)) {
                fprintf(stderr, "Invalid page size, size=%s, min=%d, max=%d\n",
                        optarg, MDBM_MINPAGE, MDBM_MAXPAGE);
                err_flag = 1;
            }
            break;
        case 'd':
            opt_dbsize = mdbm_util_get_size(optarg, 1024*1024);
            break;
        case 'D':
            opt_delete_zero = 1;
            break;
        case 'l':
            flags |= MDBM_LARGE_OBJECTS;
            break;
        case 'L':
            if (optarg) {
              int lock_flags = 0;
              if (mdbm_util_lockstr_to_flags(optarg, &lock_flags)) {
                fprintf(stderr, "Invalid locking argument, argument=%s, ignoring\n", optarg);
              }
              flags |= lock_flags;
            }
            break;
        case 's':
            opt_hashfnid = atoi(optarg);
            break;
        case 'S':
            opt_store_flag = atoi(optarg);
            break;
        case 'y':
            opt_limit_size = mdbm_util_get_size(optarg, 1);
            break;
        case 'z':
            opt_setspillsize = mdbm_util_get_size(optarg, 1);
            break;
        case 'Z':
            opt_trunc = MDBM_O_TRUNC;
            break;
        default:
            err_flag = 1;
        }
    }

    if (opt_help) {
        usage();
        return 0;
    }

    if (err_flag || ((argc - optind) != 1)) {
        usage();
        return 1;
    }

    if (flags & MDBM_OPEN_NOLOCK) {
      locking_requested = true;
      opt_fast = 0;
    } else {
      locking_requested = false;
      opt_fast = 1;
    }

    if (flags & MDBM_PARTITIONED_LOCKS) {
        if ((opt_limit_size == -1) && (opt_dbsize == 0)) {
            fprintf(stderr, "Partition locking requires a fixed size DB.\n"
                            "Consider using the -d or -y options to specify the size\n");
        }
    }

    if (locking_requested && opt_fast) {
        fprintf(stderr, "Fast mode (-f) and locking mode (-L mode) cannot be used together.\n");
        return 1;
    }

    FILE *fp = stdin;
    if (opt_infile) {
        if ((fp = fopen(opt_infile, "r")) == NULL) {
            perror(opt_infile);
            return(1);
        }
        strncpy(inputFile, opt_infile, sizeof(inputFile) - 1);
    } else {
        strncpy(inputFile, "stdin", sizeof(inputFile) - 1);
    }


    mdbmFile = argv[optind];

    int pagesize = 0;
    uint64_t dbsize = 0;
    if (!opt_cdbdump && opt_header) {
        int pgsize = -1, pgcount = -1, large = 0;

        if (dbload_header(fp, &pgsize, &pgcount, &large) != 0) {
            fclose(fp);
            return(1);
        }

        if (pgcount > 0 && pgsize > 0) {
            pagesize = pgsize;
            dbsize = pgcount * pgsize;
        }

        if (large)
            flags |= MDBM_LARGE_OBJECTS;
    }

    flags |= opt_version;
    flags |= opt_trunc;

    // command-line flags can override metadata
    if (opt_pagesize != -1)
        pagesize = opt_pagesize;

    if (opt_dbsize)
        dbsize = opt_dbsize;

    if (dbsize && (dbsize < (uint64_t)pagesize)) {
        fprintf(stderr, "DB size smaller than page size, dbSize=%llu, pageSize=%d\n",
                (long long unsigned int)dbsize, pagesize);
        usage();
        return 1;
    }

#if __x86_64__ // 64-bit
    if (dbsize > (uint64_t)9223372036854775807LL) {
        fprintf(stderr, "DB size too large, size=%llu\n", (unsigned long long) dbsize);
        usage();
        return 1;
    }
#else // 32-bit
    if (dbsize > (uint64_t)INT_MAX) {
        fprintf(stderr, "DB size too large, size=%llu\n", (unsigned long long) dbsize);
        usage();
        return 1;
    }
#endif

    if (opt_version == MDBM_CREATE_V3) {
        dbsize /= (1024*1024);
        flags |= MDBM_DBSIZE_MB;
    }


    MDBM *db;
    if ((db = mdbm_open(mdbmFile, flags, 0666, pagesize, (int)dbsize)) == NULL) {
        fprintf(stderr, "Open failed, mdbm=%s, errno=%s\n", mdbmFile, strerror(errno));
        return(1);
    }

    if (opt_setspillsize != -1) {
        if (mdbm_setspillsize(db, opt_setspillsize)) {
            fprintf(stderr, "mdbm_setspillsize failed, mdbm=%s, errno=%s\n", mdbmFile, strerror(errno));
            return(1);
        }
    }

    if (opt_limit_size != -1) {
        if (mdbm_limit_size_v3(db, opt_limit_size, NULL, NULL)) {
            fprintf(stderr, "mdbm_limit_size_v3 failed, mdbm=%s, errno=%s\n", mdbmFile, strerror(errno));
            return(1);
        }
    }

    if (opt_hashfnid != -1) {
        if (!mdbm_sethash(db, opt_hashfnid)) {
            fprintf(stderr, "Unknown hash function, hashId=%d, mdbm=%s\n", opt_hashfnid, mdbmFile);
            return(1);
        }
    }

    int errcode = 0;

    if (opt_cdbdump) {
        if (do_cdbmake(db, fp, opt_store_flag) != 0) {
            errcode = 1;
        }
    } else {
        if (do_dbload(db, fp, opt_store_flag) != 0) {
            errcode = 1;
        }
    }

    mdbm_sync(db);
    mdbm_close(db);
    fclose(fp);

    return(errcode);
}
