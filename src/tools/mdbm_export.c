/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/* Writes an MDBM database to a human-readable format. */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "mdbm.h"
#include "mdbm_internal.h" /* Needed for hash_value(). */
#include "mdbm_util.h"

static void usage()
{
  fprintf(stderr, "usage: mdbm_export [options] infile.mdbm\n"
          "  -h           Help\n"
          "  -f           Fast mode (don't lock db while reading)\n"
          "  -L mode      Specify lock-mode, one of: \n"
          lockstr_to_flags_usage("                 ")
          "  -c           Export cdbdump format (default db_dump format)\n"
          "  -o outfile   Write to <outfile> instead of stdout\n"
          "  -r           Only write record metadata information.\n"
          "               Output format: keySize,valueSize,pageNumber\n"
          "               This may be used for generating size frequency distributions.\n"
          "               Note: pageNumber for a key may change across exports if that mdbm splits.\n"
          );
}

static void do_recordInfoDump(MDBM *db, FILE *fp)
{
  kvpair kv;

  for (kv = mdbm_first(db); kv.key.dptr != NULL; kv = mdbm_next(db)) {
    mdbm_hashval_t hashval = hash_value(db, &kv.key);
    mdbm_pagenum_t pagenum = hashval_to_pagenum(db, hashval);
    fprintf(fp, "%d,%d,%u\n", kv.key.dsize, kv.val.dsize, pagenum);
  }
}

/**
 * cdbmake/cdbdump format
 * http://cr.yp.to/cdb/cdbmake.html
 */
static void do_cdbdump(MDBM *db, FILE *fp)
{
  kvpair kv;

  for (kv = mdbm_first(db); kv.key.dptr != NULL; kv = mdbm_next(db))
    {
      fprintf(fp, "+%d,%d:", kv.key.dsize, kv.val.dsize);
      fwrite(kv.key.dptr, kv.key.dsize, 1, fp);
      fwrite("->", 2, 1, fp);
      fwrite(kv.val.dptr, kv.val.dsize, 1, fp);
      fputc('\n', fp);
    }

  /* cdbdump requires extra newline at the end */
  fputc('\n', fp);
}

static inline void dump_str(datum d, FILE *fp)
{
  int i;

  for (i = 0; i < d.dsize; i++)
    {
      switch(d.dptr[i])
        {
        case '\\':
          fwrite("\\\\", 2, 1, fp);
          break;
        case '\0':
          fwrite("\\00", 3, 1, fp);
          break;
        case '\012':
          fwrite("\\0a", 3, 1, fp);
          break;
        case '\015':
          fwrite("\\0d", 3, 1, fp);
          break;
        default:
          fputc(d.dptr[i], fp);
        }
    }

  fputc('\n', fp);
}

/**
 * Berkley DB's db_dump format
 * http://www.sleepycat.com/docs/utility/db_dump.html
 */
static void do_dbdump(MDBM *db, FILE *fp)
{
  kvpair kv;

  fputs("format=print\n", fp);
  fputs("type=hash\n", fp);
  fprintf(fp, "mdbm_pagesize=%d\n", mdbm_get_page_size(db));
  fprintf(fp, "mdbm_pagecount=%d\n", (int)(mdbm_get_size(db) / mdbm_get_page_size(db)));
  /*     fprintf(fp, "mdbm_largeobj=%d\n", */
  /*          (db->m_flags & _MDBM_LARGEOBJ) ? 1 : 0); */
  fputs("HEADER=END\n", fp);

  for (kv = mdbm_first(db); kv.key.dptr != NULL; kv = mdbm_next(db))
    {
      dump_str(kv.key, fp);
      dump_str(kv.val, fp);
    }
}

int main(int argc, char** argv)
{
  int c, err_flag = 0, opt_help = 0, opt_fast = 0, opt_cdbdump = 0;
  char *opt_outfile = NULL;
  char *opt_locktype = NULL;
  FILE *fp = stdout;
  MDBM *db;
  int   lock_flags = MDBM_ANY_LOCKS;
  int   opt_recordInfo = 0;

  while ((c = getopt(argc, argv, "hfL:o:cr")) != EOF) {
    switch (c) {
    case 'h':
      opt_help = 1;
      break;
    case 'f':
      fprintf(stderr, "Option \"-f\" is deprecated, use \"-L none\" \n");
      opt_fast = 1;
      break;
    case 'c':
      opt_cdbdump = 1;
      break;
    case 'o':
      opt_outfile = optarg;
      break;
    case 'L':
      opt_locktype = optarg;
      break;
    case 'r':
      opt_recordInfo = 1;
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

  if (opt_locktype && mdbm_util_lockstr_to_flags(opt_locktype, &lock_flags)) {
    fprintf(stderr, "Invalid locking argument %s\n", opt_locktype);
    usage();
    return 1;
  }
  if (lock_flags == (int)MDBM_OPEN_NOLOCK) {
    opt_fast = 1;
  }

  if (opt_recordInfo && opt_cdbdump) {
    fprintf(stderr, "Options \"-c\" and \"-r\" are not compatible.\n");
    return(1);
  }

  if ((db = mdbm_open(argv[optind], MDBM_O_RDONLY|lock_flags, 0, 0, 0)) == NULL) {
    fprintf(stderr, "%s: %s\n", argv[optind], strerror(errno));
    return(1);
  }

  if (opt_outfile) {
    if ((fp = fopen(opt_outfile, "w")) == NULL) {
      perror(opt_outfile);
      return(1);
    }
  }

  /* Buffer output a little more */
  setvbuf(fp, NULL, _IOFBF, BUFSIZ * 16);

  if (!opt_fast) {
    mdbm_lock(db);
  }

  if (opt_recordInfo) {
    do_recordInfoDump(db, fp);
  } else if (opt_cdbdump) {
    do_cdbdump(db, fp);
  } else {
    do_dbdump(db, fp);
  }

  fclose(fp);

  if (!opt_fast) {
    mdbm_unlock(db);
  }

  mdbm_close(db);
  return(0);
}
