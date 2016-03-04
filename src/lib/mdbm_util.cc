/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <string>

#include "mdbm.h"
#include "mdbm_internal.h"
#include <mdbm_util.h>

void reset_getopt()
{
    optind=1; // reset getopt()
    // TODO some platforms may require additional steps
#ifndef __linux__
    // NOTE: on some platforms, getopt needs to be told to "reset"
    optreset = 1;
#endif

}

extern int
mdbm_util_get_size_ref (const char* arg, int default_multiplier, uint64_t *sval)
{
    char* s;
    uint64_t val;

    val = strtoull(arg,&s,0);
    if (s[0]) {
        switch (tolower(s[0])) {
        case 'g':
            if (val >= (1ULL<<54)/(1024*1024)) {
                fprintf(stderr,"Argument value too large: %s\n",optarg);
                return(1);
            }
            val *= 1024 * 1024 * 1024;
            if (tolower(*(++s)) == 'b') {
                s++;
            }
            break;

        case 'm':
            if (val >= (1ULL<<54)/1024) {
                fprintf(stderr,"Argument value too large: %s\n",optarg);
                return(1);
            }
            val *= 1024 * 1024;
            if (tolower(*(++s)) == 'b') {
                s++;
            }
            break;

        case 'k':
            if (val >= (1ULL<<54)) {
                fprintf(stderr,"Argument value too large: %s\n",optarg);
                return(1);
            }
            val *= 1024;
            if (tolower(*(++s)) == 'b') {
                s++;
            }
            break;
        }
        if (*s) {
            fprintf(stderr,"Invalid option value: %s\n",optarg);
            return(1);
        }
    } else {
        val *= default_multiplier;
    }

    *sval = val;
    return 0;
}

uint64_t
mdbm_util_get_size (const char* arg, int default_multiplier) {
  uint64_t val;
  int ret = mdbm_util_get_size_ref(arg, default_multiplier, &val);
  if (!ret) {
    return val;
  }
  exit(ret);
}


int 
mdbm_util_lockstr_to_flags(const char* lock_string, int *lock_flags) 
{
    if (lock_string) {
        if (strcasecmp(lock_string, "exclusive") == 0) {
            *lock_flags = 0;
            return 0;
        } else if (strcasecmp(lock_string, "partition") == 0) {
            *lock_flags = MDBM_PARTITIONED_LOCKS;
            return 0;
        } else if (strcasecmp(lock_string, "shared") == 0) {
            *lock_flags = MDBM_RW_LOCKS;
            return 0;
        } else if (strcasecmp(lock_string, "none") == 0) {
            *lock_flags = MDBM_OPEN_NOLOCK;
            return 0;
        } else if (strcasecmp(lock_string, "any") == 0) {
            *lock_flags = MDBM_ANY_LOCKS;
            return 0;
        }
    }
    return -1;
}


inline static int
dbdump_str(datum d, FILE *fp)
{
    int i, ret;

    for (i = 0; i < d.dsize; i++) {
        ret = 1;
        switch(d.dptr[i]) {
        case '\\':
            ret = fwrite("\\\\", 2, 1, fp);
            break;
        case '\0':
            ret = fwrite("\\00", 3, 1, fp);
            break;
        case '\012':
            ret = fwrite("\\0a", 3, 1, fp);
            break;
        case '\015':
            ret = fwrite("\\0d", 3, 1, fp);
            break;
        default:
            if (fputc(d.dptr[i], fp) == EOF) {
                ret = 0;
            }
        }
        if (ret <= 0) {
            errno = EBADF;
            return -1;
        }
    }
    return ((fputc('\n', fp) != EOF) ? 0 : -1);
}

int
mdbm_dbdump_to_file(kvpair kv, FILE *fp)
{
    if (fp == NULL) {
        errno = EINVAL;
        return -1;
    }

    if (dbdump_str(kv.key, fp) < 0) {
        return -1;
    }
    return dbdump_str(kv.val, fp);
}

/* No trailer: just closes the file */
int
mdbm_dbdump_trailer_and_close(FILE *fp)
{
    return fclose(fp);
}

inline static uint32_t
get_dbdump_size(datum dtum)
{
    uint32_t i = 0, len, totlen = 0;
    uint32_t siz = dtum.dsize;
    char *ptr = dtum.dptr;

    for (; i < siz; ++i, ++ptr) {
        switch(*ptr) {
        case '\\':
            len = 2;
            break;
        case '\0':
        case '\012':
        case '\015':
            len = 3;
            break;
        default:
            len = 1;
            break;
        }
        totlen += len;
    }

    return totlen + 1;  /* Total+1 for the newline */
}

inline static void
add_dbdump_record(datum d, char *buffer)
{
    int i;

    for (i = 0; i < d.dsize; i++) {
        switch(d.dptr[i]) {
        case '\\':
            *buffer++ = '\\';
            *buffer++ = '\\';
            break;
        case '\0':
            *buffer++ = '\\';
            *buffer++ = '0';
            *buffer++ = '0';
            break;
        case '\012':
            *buffer++ = '\\';
            *buffer++ = '0';
            *buffer++ = 'a';
            break;
        case '\015':
            *buffer++ = '\\';
            *buffer++ = '0';
            *buffer++ = 'd';
            break;
        default:
            *buffer++ = d.dptr[i];
        }
    }
    *buffer = '\n';
}

static int
realloc_if_needed(char **bufptr, uint32_t *bufsize, uint32_t buf_offset, uint32_t siz)
{
    if (*bufsize < (buf_offset + siz)) {
        size_t new_size = buf_offset + siz + sysconf(_SC_PAGESIZE);  /* add 4k padding */
        char *tmp = (char *) realloc(*bufptr, buf_offset + new_size);
        if (tmp == NULL) {
            errno = ENOMEM;
            return -1;
        }
        *bufptr = tmp;
        *bufsize = new_size;
    }
    return 0;
}

/*
// Determine total data size and store in datasize, then see if buffer needs to be realloc()-ed.
// If realloc()-ed, then update bufsize.  buf_offset points to the offset where to start writing
// the object.  Sizes are 32 bit to make sure users write the data to external storage before
// we reach 4GB, due to the performance costs of calling realloc so many times.
*/ 

int
mdbm_dbdump_add_record(kvpair kv, uint32_t *datasize, 
                       char **bufptr, uint32_t *bufsize, uint32_t buf_offset)
{
    uint32_t ksize;
    uint32_t totsiz = ksize = get_dbdump_size(kv.key);  /* total object size is key+value size */
    totsiz += get_dbdump_size(kv.val);

    if (realloc_if_needed(bufptr, bufsize, buf_offset, totsiz) != 0) {
        return -1;
    }

    *datasize += totsiz;
    add_dbdump_record(kv.key, (*bufptr) + buf_offset);
    add_dbdump_record(kv.val, (*bufptr) + buf_offset + ksize);
    return 0;
}

int
mdbm_cdbdump_to_file(kvpair kv, FILE *fp)
{
    int ret = 0;
    if (fp == NULL) {
        errno = EINVAL;
        return -1;
    }

    if ((ret = fprintf(fp, "+%d,%d:", kv.key.dsize, kv.val.dsize)) < 0) {
        return ret;
    }
        
    if (fwrite(kv.key.dptr, kv.key.dsize, 1, fp) <= 0) {
        errno = EBADF;
        return -1;
    }
    if (fwrite("->", 2, 1, fp) == 0) {
        errno = EBADF;
        return -1;
    }
    fwrite(kv.val.dptr, kv.val.dsize, 1, fp);
    if (fputc('\n', fp) == EOF) {
        errno = EBADF;
        return -1;
    }
    return 0;
}

 /* Closes and also adds newline trailer */
int
mdbm_cdbdump_trailer_and_close(FILE *fp)
{
    if (fputc('\n', fp) == EOF) {
        errno = EBADF;
        return -1;
    }
    return fclose(fp);
}

int
mdbm_cdbdump_add_record(kvpair kv, uint32_t *datasize, 
                        char **bufptr, uint32_t *bufsize, uint32_t buf_offset)
{
    char *cur;
    char tmpbuf[32];
    int len = snprintf(tmpbuf, sizeof(tmpbuf), "+%d,%d:", kv.key.dsize, kv.val.dsize);
    uint32_t totsize = len + kv.key.dsize + kv.val.dsize + 3;  /* +1: newline, +2 for "->" */

    if (realloc_if_needed(bufptr, bufsize, buf_offset, totsize) != 0) {
        return -1;
    }

    *datasize += totsize;
    memcpy((*bufptr) + buf_offset, tmpbuf, len);
    memcpy((*bufptr) + buf_offset + len, kv.key.dptr, kv.key.dsize);
    cur = (*bufptr) + buf_offset + len + kv.key.dsize;
    *cur++ = '-';
    *cur++ = '>';
    memcpy(cur, kv.val.dptr, kv.val.dsize);
    cur += kv.val.dsize;
    *cur = '\n';
    return 0;
}

// Take an open MDBM and write/export a DBdump header
//     MDBM *db - The open MDBM handle
//     FILE *fp - The file into which the DBdump header should be written to
int
mdbm_dbdump_export_header(MDBM *db, FILE *fp) 
{
    if (fp == NULL) {
        return -1;
    }

    fputs("format=print\n", fp);
    fputs("type=hash\n", fp);
    fprintf(fp, "mdbm_pagesize=%d\n", mdbm_get_page_size(db));
    fprintf(fp, "mdbm_pagecount=%d\n", (int)(mdbm_get_size(db) / mdbm_get_page_size(db)));
    fputs("HEADER=END\n", fp);
    return 0;
}

// An print msg if an error occurred while writing
int 
print_write_err(const char *filename)
{
    fprintf(stderr, "%s: unable to create: %s\n", filename, strerror(errno));
    return -2;
}

// An print msg if an data being read doesn't follow expected format
int
print_read_format_error(char badchar, int linenum, const char *filename)
{
    fprintf(stderr, "%s: unable to read input: bad format char(%c) on line(%d)\n",
            filename, badchar, linenum);
    return -1;
}

// Read the DBdump header from FILE *fp
// Function will set, if available at the beginning of the file pointed to by "fp",
// the following parameter values:
//     pgsize = page-size read from "fp"
//     pgcount = page-count,
//     large = large-object-support
//     lineno = the line number following the header (for tracking progress)
int
mdbm_dbdump_import_header(FILE *fp, int *pgsize, int *pgcount, int *large, uint32_t *lineno)
{
    char key[100], value[100];

    *pgsize = *pgcount = *large = 0;    // Initialize

    for (*lineno = 1;; (*lineno)++) {
        if (fscanf(fp,"%99[^=]=%99s\n", key, value) != 2) {
            fprintf(stderr, "line %u: bad format (expecting key=val)\n",
                    *lineno);
            return -1;
        }

        if (strcmp(key, "HEADER") == 0)
            break;              /* end of metadata */

        if (strcmp(key, "format") == 0 && strcmp(value, "print") != 0) {
            fprintf(stderr, "line %u: can't parse format %s\n", *lineno, value);
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

// Convert an ASCII character in parameter "c" to an integer value.
// Return the 4-bit value of a hex character inside an 8 bit return value.
static inline char
parse_nybble(int c)
{
    return ((c >= 'A' ? ((c & 0xdf) - 'A') + 10 : (c - '0')));
}

// Convert 2 ASCII characters in parameters c1 and c2 into an integer value
// Return the 8-bit value of the 2 hex characters inside an 8 bit return value.
char
mdbm_internal_hex_to_byte(int c1, int c2)
{
    int digit =
        (parse_nybble(c1) << 4) | parse_nybble(c2);
    return (char)digit;
}

// Read key or value in DBdump format
//     FILE *fp    - [in]  the file containing the DBdump data
//     string &str - [out] the string parameter to read into
//     lineno      - [in]  the current line number for printing error messages
static inline int
read_str(FILE *fp, std::string &str, uint32_t lineno)
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
                        str.append((size_t)1, mdbm_internal_hex_to_byte(ch2, ch3));
                    }
                    else {
                        fprintf(stderr, "line %u: bad escape format\n", lineno);
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

// Function locks the MDBM, stores the data, then unlocks.  Also handles error conditions.
// store value in parameter "val" using "key" as the key, into MDBM *db
// store_flag - MDBM_INSERT | MDBM_REPLACE | MDBM_INSERT_DUP | MDBM_MODIFY
static int
lock_and_store(MDBM *db, datum key, datum val, int store_flag)
{
    int rc = 0, lock_ret = 1;

    if (-1 == (lock_ret = mdbm_lock_smart(db, &key, MDBM_O_RDWR))) {
        fprintf(stderr, "failed to lock, errno=%s\n", strerror(errno));
        return -1;
    }

    rc = mdbm_store(db, key, val, store_flag);

    if (lock_ret == 1) {
        if (mdbm_unlock_smart(db, &key, 0) != 1) {
            fprintf(stderr, "failed to unlock partition, errno=%s\n", strerror(errno));
        }
    }

    switch (rc) {
    case -1:
        fprintf(stderr, "failed writing to %s, store_flag=%d, errno=%s\n",
                mdbm_get_filename(db), store_flag, strerror(errno));
                
        break;

    case 1:
        fprintf(stderr, "cannot overwrite existing key to %s, store_flag=%d\n",
                mdbm_get_filename(db), store_flag);
        break;
    }

    return rc;
}


// Read data from FILE *fp into MDBM *db, using DBdump format.
// input_file - name of the file pointed to by "fp".
// store_flag - MDBM_INSERT | MDBM_REPLACE | MDBM_INSERT_DUP | MDBM_MODIFY
// lineno is a pointer to the  current line number, used to keep track of the current line number
// in order to generate meaningful error messages.  May not start from one if a header was
// previously read.  Also documented in mdbm.h
int
mdbm_dbdump_import(MDBM *db, FILE *fp, const char *input_file, int store_flag, uint32_t *lineno)
{
    std::string keybuf(""), valbuf("");
    datum key, val;
    int status;

    for (;; (*lineno)++) {
        if ((status = read_str(fp, keybuf, *lineno)) != 0) {
            if (status == EOF) {
                break;
            } else {
                return -2;
            }
        }

        if (keybuf.size() == 0) {
            fprintf(stderr, "%s: bad key length\n", input_file);
            return -2;
        }

        if (0 != (status = read_str(fp, valbuf, *lineno))) {
            if (status == EOF)
                fprintf(stderr, "%s: unexpected EOF\n", input_file);
            return -2;
        }

        key.dptr = (char *) keybuf.data();
        key.dsize = keybuf.size();

        val.dptr = (char *) valbuf.data();
        val.dsize = valbuf.size();

        if (lock_and_store(db, key, val, store_flag) == -1) {
            return -3;
        }
    }

    return 0;
}

// Read data from FILE *fp into MDBM *db, using Cdb format.
// input_file - name of the file pointed to by "fp".
// store_flag - MDBM_INSERT | MDBM_REPLACE | MDBM_INSERT_DUP | MDBM_MODIFY
int
mdbm_cdbdump_import(MDBM *db, FILE *fp, const char *input_file, int store_flag)
{
    unsigned int klen;
    unsigned int dlen;
    unsigned int i;
    int ch;
    std::string keybuf, valbuf;
    datum key, val;

    int linenum = 0;
    for (;;) {
        ch = getc(fp);
        if (ch == '\n')
        {
            linenum++;
            break;
        }
        if (ch != '+') {
            return print_read_format_error(ch, linenum, input_file);
        }
        klen = 0;
        for (;;) {
            ch = getc(fp);
            if (ch == ',')
                break;
            if ((ch < '0') || (ch > '9')) {
                return print_read_format_error(ch, linenum, input_file);
            }
            if (klen > 429496720) {
                errno = ENOMEM;
                return print_write_err(mdbm_get_filename(db));
            }
            klen = klen * 10 + (ch - '0');
        }
        if (klen == 0) {
            fprintf(stderr, "%s: bad key length\n", input_file);
            return -1;
        }
        dlen = 0;
        for (;;) {
            ch = getc(fp);
            if (ch == ':')
                break;
            if ((ch < '0') || (ch > '9')) {
                return print_read_format_error(ch, linenum, input_file);
            }
            if (dlen > 429496720) {
                errno = ENOMEM;
                return print_write_err(mdbm_get_filename(db));
            }
            dlen = dlen * 10 + (ch - '0');
        }

        /* read key */
        keybuf.erase();
        if (klen > keybuf.capacity()) {
            keybuf.reserve(klen);
        }
        for (i = 0;i < klen;++i) {
            ch = getc(fp);
            keybuf.append((size_t)1, (char)ch);
        }
        ch = getc(fp);
        if (ch != '-') {
            return print_read_format_error(ch, linenum, input_file);
        }
        ch = getc(fp);
        if (ch != '>') {
            return print_read_format_error(ch, linenum, input_file);
        }

        valbuf.erase();
        if (dlen > valbuf.capacity()) {
            valbuf.reserve(dlen);
        }
        for (i = 0;i < dlen;++i) {
            ch = getc(fp);
            valbuf.append((size_t)1, (char)ch);
        }

        ch = getc(fp);
        if (ch != '\n') {
            return print_read_format_error(ch, linenum, input_file);
        }
        linenum++;

        key.dptr = (char*) keybuf.data();
        key.dsize = klen;

        val.dptr = (char*) valbuf.data();
        val.dsize = dlen;

        if (lock_and_store(db, key, val, store_flag) == -1) {
            return -3;
        }
    }

    return 0;
}

