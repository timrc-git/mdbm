/*  Copyright 2013 Yahoo! Inc.                                         */
/*  See LICENSE in the root of the distribution for licensing details. */

#ifdef __cplusplus
extern "C" {
#endif
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "stdarg.h"
#ifdef __cplusplus
}
#endif

#include "mdbm.h"
#include "mdbm_internal.h"

#include "shakedata.h"

typedef MDBM* MDBM_File;

typedef struct mdbm_shake_data_v3* ShakeData;


#define mdbm_TIEHASH(dbtype,filename,flags,mode,pagesize,dbsize) mdbm_open(filename,flags,mode,pagesize,dbsize)
#define mdbm_FETCH(db,key)			mdbm_fetch(db,key)
#define mdbm_STORE(db,key,value,flags)		mdbm_store(db,key,value,flags)
#define mdbm_DELETE(db,key)			mdbm_delete(db,key)
#define mdbm_FIRSTKEY(db)			mdbm_firstkey(db)
#define mdbm_NEXTKEY(db,key)			mdbm_nextkey(db)


/*
 *  Callback functions, these are wrappers to allow callbacks implemented
 *  in perl. You can have one callback structure per MDBM object. This
 *  might not be super optimal, but in most cases there shouldn't be more
 *  than a few callback structures "active" at any time.
 */
struct perl_callback {
  SV *self;
  MDBM_File db;
  SV *shake;
  SV *prune;
  struct perl_callback *next;
};

static struct perl_callback *callbacks = NULL;
/* static MDBM_ITER our_iter; */

static void
update_perl_callback(MDBM_File db, SV *self, SV *shake, SV *prune)
{
  struct perl_callback *cur, *prev;

  if (!db)
    return;

  /* Find our element, or the last element */
  for (cur=callbacks, prev=NULL; cur && (cur->db != db); cur=cur->next)
    prev = cur;

  /* Create a new callback element */
  if (!cur) {
    cur = (struct perl_callback *)malloc(sizeof(struct perl_callback));
    if (!cur)
      croak("Out of memory trying to allocate a callback element\n");
    else {
      /* Initialize */
      cur->db = db;
      cur->self = self;
      cur->shake = NULL;
      cur->prune = NULL;
      cur->next = NULL;
    }
  }

  /* Don't overwrite existing values with NULL. */
  if (shake)
    cur->shake = shake;
  if (prune)
    cur->prune = prune;

  if (prev)
    prev->next = cur;
  else
    callbacks = cur;
}

static struct perl_callback *
get_perl_callback(MDBM_File db)
{
  struct perl_callback *cur;

  if (!db || !callbacks)
    return NULL;

  for (cur=callbacks; cur && (cur->db != db); cur=cur->next)
    ;

  return cur;
}

void
remove_perl_callback(MDBM_File db)
{
  struct perl_callback *cur, *prev;

  if (!db || !callbacks)
    return;

  for (cur=callbacks, prev=NULL; cur && (cur->db != db); cur=cur->next)
    prev = cur;

  if (cur) {
    if (prev)
      prev->next = cur->next;
    else
      callbacks = cur->next; /* First element removed */
    free(cur);
  }
}


#if 0
struct mdbm_shake_data_v3 {
    uint32_t page_num;          /* index number of overflowing page */
    const char* page_begin;     /* beginning address of page */
    const char* page_end;       /* one byte past last byte of page */
    uint32_t page_free_space;   /* current free space on page */
    uint32_t space_needed;      /* space needed for current insert */
    uint32_t page_num_items;    /* number of items on page */
    uint32_t reserved;
    kvpair* page_items;         /* key/val pairs for all items on page */
    uint32_t* page_item_sizes;  /* total in-page size for each item on page */
    void* user_data;            /* user-data pointer passed to mdbm_limit_size_new */
};
#endif

int
shake_function_v3(MDBM_File db, const datum *k, const datum *xv, struct mdbm_shake_data_v3 *data_v3)
{
  int ret;

  dSP;
  ENTER;
  SAVETMPS;
  PUSHMARK(SP);

  SV* objsd = sv_newmortal();
  SV* blsobjsd = sv_setref_pv(objsd, "ShakeData", data_v3);
  XPUSHs(blsobjsd);

  XPUSHs(sv_2mortal(newSVpv(k->dptr, k->dsize)));
  XPUSHs(sv_2mortal(newSVpv(xv->dptr, xv->dsize)));

  if (data_v3 != NULL) {
    SV* objdb = sv_newmortal();
    SV* blsobjdb = sv_setref_pv(objdb, "MDBM_File", db);
    AddPtr(db);
    XPUSHs(blsobjdb);
  }

  PUTBACK;
  ret = call_pv("MDBM_File::pl_mdbm_shake_v3_callback", G_SCALAR | G_EVAL);
  SPAGAIN;

  if (SvTRUE(ERRSV)) {
    croak("Internal Perl Error in V3 shake: %s\n", SvPV_nolen(ERRSV));
    (void) POPs;
  } else if (ret != 1) {
    croak("Perl V3 shake function must return exactly one value.\n");
  }

  ret = POPi;

  PUTBACK;
  FREETMPS;
  LEAVE;
  return ret;
}



static int
prune_function(MDBM_File db, datum k, datum xv, void *param)
{
  struct perl_callback *callback = get_perl_callback(db);
  int ret;

  dSP;
  ENTER;
  SAVETMPS;
  PUSHMARK(sp);

  if (!callback || !callback->self || !callback->prune)
    croak("Critical error, no prune function is provided.\n");

  XPUSHs(callback->self);
  XPUSHs(sv_2mortal(newSVpv(k.dptr, k.dsize)));
  XPUSHs(sv_2mortal(newSVpv(xv.dptr, xv.dsize)));
  XPUSHs(sv_2mortal(newSVpv((char *)param, 0)));

  PUTBACK;
  ret = call_sv(callback->prune, G_SCALAR);
  SPAGAIN;

  if (ret != 1)
    croak("Perl prune function must return exactly one value.\n");
  ret = POPi;

  PUTBACK;
  FREETMPS;
  LEAVE; 

  return ret;
}

/* Constant handling functions, generated using
 * h2xs -n MDBM_File -M '^MDBM_' -p mdbm_ mdbm.h
 */
/* Start of auto-generated constant handling */

#define PERL_constant_NOTFOUND	1
#define PERL_constant_NOTDEF	2
#define PERL_constant_ISIV	3
#define PERL_constant_ISNO	4
#define PERL_constant_ISNV	5
#define PERL_constant_ISPV	6
#define PERL_constant_ISPVN	7
#define PERL_constant_ISSV	8
#define PERL_constant_ISUNDEF	9
#define PERL_constant_ISUV	10
#define PERL_constant_ISYES	11

#ifndef NVTYPE
typedef double NV; /* 5.6 and later define NVTYPE, and typedef NV to it.  */
#endif
#ifndef aTHX_
#define aTHX_ /* 5.6 or later define this for threading support.  */
#endif
#ifndef pTHX_
#define pTHX_ /* 5.6 or later define this for threading support.  */
#endif

static int
constant_11 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_INSERT MDBM_MODIFY MDBM_O_RDWR */
  /* Offset 8 gives the best switch position.  */
  switch (name[8]) {
  case 'D':
    if (memEQ(name, "MDBM_O_RDWR", 11)) {
    /*                       ^         */
#ifdef MDBM_O_RDWR
      *iv_return = MDBM_O_RDWR;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'E':
    if (memEQ(name, "MDBM_INSERT", 11)) {
    /*                       ^         */
#ifdef MDBM_INSERT
      *iv_return = MDBM_INSERT;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'I':
    if (memEQ(name, "MDBM_MODIFY", 11)) {
    /*                       ^         */
#ifdef MDBM_MODIFY
      *iv_return = MDBM_MODIFY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_12 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_HASH_OZ MDBM_O_ASYNC MDBM_O_CREAT MDBM_O_FSYNC MDBM_O_TRUNC
     MDBM_PAGESIZ MDBM_PROTECT MDBM_REPLACE MDBM_RESERVE */
  /* Offset 7 gives the best switch position.  */
  switch (name[7]) {
  case 'A':
    if (memEQ(name, "MDBM_O_ASYNC", 12)) {
    /*                      ^           */
#ifdef MDBM_O_ASYNC
      *iv_return = MDBM_O_ASYNC;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'C':
    if (memEQ(name, "MDBM_O_CREAT", 12)) {
    /*                      ^           */
#ifdef MDBM_O_CREAT
      *iv_return = MDBM_O_CREAT;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'F':
    if (memEQ(name, "MDBM_O_FSYNC", 12)) {
    /*                      ^           */
#ifdef MDBM_O_FSYNC
      *iv_return = MDBM_O_FSYNC;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'G':
    if (memEQ(name, "MDBM_PAGESIZ", 12)) {
    /*                      ^           */
#ifdef MDBM_PAGESIZ
      *iv_return = MDBM_PAGESIZ;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'O':
    if (memEQ(name, "MDBM_PROTECT", 12)) {
    /*                      ^           */
#ifdef MDBM_PROTECT
      *iv_return = MDBM_PROTECT;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'P':
    if (memEQ(name, "MDBM_REPLACE", 12)) {
    /*                      ^           */
#ifdef MDBM_REPLACE
      *iv_return = MDBM_REPLACE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'S':
    if (memEQ(name, "MDBM_HASH_OZ", 12)) {
    /*                      ^           */
#ifdef MDBM_HASH_OZ
      *iv_return = MDBM_HASH_OZ;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_RESERVE", 12)) {
    /*                      ^           */
#ifdef MDBM_RESERVE
      *iv_return = MDBM_RESERVE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'T':
    if (memEQ(name, "MDBM_O_TRUNC", 12)) {
    /*                      ^           */
#ifdef MDBM_O_TRUNC
      *iv_return = MDBM_O_TRUNC;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_13 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_HASH_EJB MDBM_HASH_FNV MDBM_HASH_MD5 MDBM_HASH_STL MDBM_NO_DIRTY
     MDBM_O_DIRECT MDBM_O_RDONLY MDBM_O_WRONLY MDBM_RW_LOCKS */
  /* Offset 10 gives the best switch position.  */
  switch (name[10]) {
  case 'C':
    if (memEQ(name, "MDBM_RW_LOCKS", 13)) {
    /*                         ^         */
#ifdef MDBM_RW_LOCKS
      *iv_return = MDBM_RW_LOCKS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'E':
    if (memEQ(name, "MDBM_HASH_EJB", 13)) {
    /*                         ^         */
#ifdef MDBM_HASH_EJB
      *iv_return = MDBM_HASH_EJB;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_O_DIRECT", 13)) {
    /*                         ^         */
#ifdef MDBM_O_DIRECT
      *iv_return = MDBM_O_DIRECT;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'F':
    if (memEQ(name, "MDBM_HASH_FNV", 13)) {
    /*                         ^         */
#ifdef MDBM_HASH_FNV
      *iv_return = MDBM_HASH_FNV;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'M':
    if (memEQ(name, "MDBM_HASH_MD5", 13)) {
    /*                         ^         */
#ifdef MDBM_HASH_MD5
      *iv_return = MDBM_HASH_MD5;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'N':
    if (memEQ(name, "MDBM_O_RDONLY", 13)) {
    /*                         ^         */
#ifdef MDBM_O_RDONLY
      *iv_return = MDBM_O_RDONLY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_O_WRONLY", 13)) {
    /*                         ^         */
#ifdef MDBM_O_WRONLY
      *iv_return = MDBM_O_WRONLY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'R':
    if (memEQ(name, "MDBM_NO_DIRTY", 13)) {
    /*                         ^         */
#ifdef MDBM_NO_DIRTY
      *iv_return = MDBM_NO_DIRTY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'S':
    if (memEQ(name, "MDBM_HASH_STL", 13)) {
    /*                         ^         */
#ifdef MDBM_HASH_STL
      *iv_return = MDBM_HASH_STL;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_14 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_ANY_LOCKS MDBM_CHECK_ALL MDBM_CLOCK_TSC MDBM_CREATE_V3 MDBM_DBSIZE_MB
     MDBM_O_ACCMODE MDBM_PROT_NONE MDBM_PROT_READ MDBM_PTYPE_DIR MDBM_PTYPE_LOB
     */
  /* Offset 12 gives the best switch position.  */
  switch (name[12]) {
  case 'A':
    if (memEQ(name, "MDBM_PROT_READ", 14)) {
    /*                           ^        */
#ifdef MDBM_PROT_READ
      *iv_return = MDBM_PROT_READ;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'D':
    if (memEQ(name, "MDBM_O_ACCMODE", 14)) {
    /*                           ^        */
#ifdef MDBM_O_ACCMODE
      *iv_return = MDBM_O_ACCMODE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'I':
    if (memEQ(name, "MDBM_PTYPE_DIR", 14)) {
    /*                           ^        */
      *iv_return = MDBM_PTYPE_DIR;
      return PERL_constant_ISIV;
    }
    break;
  case 'K':
    if (memEQ(name, "MDBM_ANY_LOCKS", 14)) {
    /*                           ^        */
#ifdef MDBM_ANY_LOCKS
      *iv_return = MDBM_ANY_LOCKS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'L':
    if (memEQ(name, "MDBM_CHECK_ALL", 14)) {
    /*                           ^        */
#ifdef MDBM_CHECK_ALL
      *iv_return = MDBM_CHECK_ALL;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'M':
    if (memEQ(name, "MDBM_DBSIZE_MB", 14)) {
    /*                           ^        */
#ifdef MDBM_DBSIZE_MB
      *iv_return = MDBM_DBSIZE_MB;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'N':
    if (memEQ(name, "MDBM_PROT_NONE", 14)) {
    /*                           ^        */
#ifdef MDBM_PROT_NONE
      *iv_return = MDBM_PROT_NONE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'O':
    if (memEQ(name, "MDBM_PTYPE_LOB", 14)) {
    /*                           ^        */
      *iv_return = MDBM_PTYPE_LOB;
      return PERL_constant_ISIV;
    }
    break;
  case 'S':
    if (memEQ(name, "MDBM_CLOCK_TSC", 14)) {
    /*                           ^        */
#ifdef MDBM_CLOCK_TSC
      *iv_return = MDBM_CLOCK_TSC;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'V':
    if (memEQ(name, "MDBM_CREATE_V3", 14)) {
    /*                           ^        */
#ifdef MDBM_CREATE_V3
      *iv_return = MDBM_CREATE_V3;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_15 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_BSOPS_FILE MDBM_BSOPS_MDBM MDBM_CACHE_ONLY MDBM_HASH_CRC32
     MDBM_HASH_HSIEH MDBM_HASH_PHONG MDBM_HASH_SHA_1 MDBM_HASH_TOREK
     MDBM_INSERT_DUP MDBM_KEYLEN_MAX MDBM_PROT_WRITE MDBM_PTYPE_DATA
     MDBM_PTYPE_FREE MDBM_VALLEN_MAX */
  /* Offset 14 gives the best switch position.  */
  switch (name[14]) {
  case '1':
    if (memEQ(name, "MDBM_HASH_SHA_", 14)) {
    /*                             1      */
#ifdef MDBM_HASH_SHA_1
      *iv_return = MDBM_HASH_SHA_1;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case '2':
    if (memEQ(name, "MDBM_HASH_CRC3", 14)) {
    /*                             2      */
#ifdef MDBM_HASH_CRC32
      *iv_return = MDBM_HASH_CRC32;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'A':
    if (memEQ(name, "MDBM_PTYPE_DAT", 14)) {
    /*                             A      */
      *iv_return = MDBM_PTYPE_DATA;
      return PERL_constant_ISIV;
    }
    break;
  case 'E':
    if (memEQ(name, "MDBM_BSOPS_FIL", 14)) {
    /*                             E      */
#ifdef MDBM_BSOPS_FILE
      *iv_return = (intptr_t)MDBM_BSOPS_FILE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_PROT_WRIT", 14)) {
    /*                             E      */
#ifdef MDBM_PROT_WRITE
      *iv_return = MDBM_PROT_WRITE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_PTYPE_FRE", 14)) {
    /*                             E      */
      *iv_return = MDBM_PTYPE_FREE;
      return PERL_constant_ISIV;
    }
    break;
  case 'G':
    if (memEQ(name, "MDBM_HASH_PHON", 14)) {
    /*                             G      */
#ifdef MDBM_HASH_PHONG
      *iv_return = MDBM_HASH_PHONG;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'H':
    if (memEQ(name, "MDBM_HASH_HSIE", 14)) {
    /*                             H      */
#ifdef MDBM_HASH_HSIEH
      *iv_return = MDBM_HASH_HSIEH;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'K':
    if (memEQ(name, "MDBM_HASH_TORE", 14)) {
    /*                             K      */
#ifdef MDBM_HASH_TOREK
      *iv_return = MDBM_HASH_TOREK;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'M':
    if (memEQ(name, "MDBM_BSOPS_MDB", 14)) {
    /*                             M      */
#ifdef MDBM_BSOPS_MDBM
      *iv_return = (intptr_t)MDBM_BSOPS_MDBM;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'P':
    if (memEQ(name, "MDBM_INSERT_DU", 14)) {
    /*                             P      */
#ifdef MDBM_INSERT_DUP
      *iv_return = MDBM_INSERT_DUP;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'X':
    if (memEQ(name, "MDBM_KEYLEN_MA", 14)) {
    /*                             X      */
#ifdef MDBM_KEYLEN_MAX
      *iv_return = MDBM_KEYLEN_MAX;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_VALLEN_MA", 14)) {
    /*                             X      */
#ifdef MDBM_VALLEN_MAX
      *iv_return = MDBM_VALLEN_MAX;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'Y':
    if (memEQ(name, "MDBM_CACHE_ONL", 14)) {
    /*                             Y      */
#ifdef MDBM_CACHE_ONLY
      *iv_return = MDBM_CACHE_ONLY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_16 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_API_VERSION MDBM_OPEN_NOLOCK MDBM_SINGLE_ARCH MDBM_STATS_BASIC
     MDBM_STATS_TIMED MDBM_STAT_CB_INC MDBM_STAT_CB_SET */
  /* Offset 15 gives the best switch position.  */
  switch (name[15]) {
  case 'C':
    if (memEQ(name, "MDBM_STATS_BASI", 15)) {
    /*                              C      */
#ifdef MDBM_STATS_BASIC
      *iv_return = MDBM_STATS_BASIC;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_STAT_CB_IN", 15)) {
    /*                              C      */
#ifdef MDBM_STAT_CB_INC
      *iv_return = MDBM_STAT_CB_INC;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'D':
    if (memEQ(name, "MDBM_STATS_TIME", 15)) {
    /*                              D      */
#ifdef MDBM_STATS_TIMED
      *iv_return = MDBM_STATS_TIMED;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'H':
    if (memEQ(name, "MDBM_SINGLE_ARC", 15)) {
    /*                              H      */
#ifdef MDBM_SINGLE_ARCH
      *iv_return = MDBM_SINGLE_ARCH;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'K':
    if (memEQ(name, "MDBM_OPEN_NOLOC", 15)) {
    /*                              K      */
#ifdef MDBM_OPEN_NOLOCK
      *iv_return = MDBM_OPEN_NOLOCK;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'N':
    if (memEQ(name, "MDBM_API_VERSIO", 15)) {
    /*                              N      */
#ifdef MDBM_API_VERSION
      *iv_return = MDBM_API_VERSION;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'T':
    if (memEQ(name, "MDBM_STAT_CB_SE", 15)) {
    /*                              T      */
#ifdef MDBM_STAT_CB_SET
      *iv_return = MDBM_STAT_CB_SET;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_17 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_ALIGN_8_BITS MDBM_CACHE_MODIFY MDBM_CHECK_CHUNKS MDBM_CHECK_HEADER
     MDBM_DEFAULT_HASH MDBM_HASH_JENKINS MDBM_STAT_CB_TIME */
  /* Offset 15 gives the best switch position.  */
  switch (name[15]) {
  case 'E':
    if (memEQ(name, "MDBM_CHECK_HEADER", 17)) {
    /*                              ^        */
#ifdef MDBM_CHECK_HEADER
      *iv_return = MDBM_CHECK_HEADER;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'F':
    if (memEQ(name, "MDBM_CACHE_MODIFY", 17)) {
    /*                              ^        */
#ifdef MDBM_CACHE_MODIFY
      *iv_return = MDBM_CACHE_MODIFY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'K':
    if (memEQ(name, "MDBM_CHECK_CHUNKS", 17)) {
    /*                              ^        */
#ifdef MDBM_CHECK_CHUNKS
      *iv_return = MDBM_CHECK_CHUNKS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'M':
    if (memEQ(name, "MDBM_STAT_CB_TIME", 17)) {
    /*                              ^        */
#ifdef MDBM_STAT_CB_TIME
      *iv_return = MDBM_STAT_CB_TIME;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'N':
    if (memEQ(name, "MDBM_HASH_JENKINS", 17)) {
    /*                              ^        */
#ifdef MDBM_HASH_JENKINS
      *iv_return = MDBM_HASH_JENKINS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'S':
    if (memEQ(name, "MDBM_DEFAULT_HASH", 17)) {
    /*                              ^        */
#ifdef MDBM_DEFAULT_HASH
      *iv_return = MDBM_DEFAULT_HASH;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'T':
    if (memEQ(name, "MDBM_ALIGN_8_BITS", 17)) {
    /*                              ^        */
#ifdef MDBM_ALIGN_8_BITS
      *iv_return = MDBM_ALIGN_8_BITS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_18 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_ALIGN_16_BITS MDBM_ALIGN_32_BITS MDBM_ALIGN_64_BITS
     MDBM_CACHEMODE_LFU MDBM_CACHEMODE_LRU MDBM_CACHE_REPLACE
     MDBM_COPY_LOCK_ALL MDBM_DEMAND_PAGING MDBM_LARGE_OBJECTS
     MDBM_OPEN_WINDOWED MDBM_STORE_SUCCESS */
  /* Offset 12 gives the best switch position.  */
  switch (name[12]) {
  case '2':
    if (memEQ(name, "MDBM_ALIGN_32_BITS", 18)) {
    /*                           ^            */
#ifdef MDBM_ALIGN_32_BITS
      *iv_return = MDBM_ALIGN_32_BITS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case '4':
    if (memEQ(name, "MDBM_ALIGN_64_BITS", 18)) {
    /*                           ^            */
#ifdef MDBM_ALIGN_64_BITS
      *iv_return = MDBM_ALIGN_64_BITS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case '6':
    if (memEQ(name, "MDBM_ALIGN_16_BITS", 18)) {
    /*                           ^            */
#ifdef MDBM_ALIGN_16_BITS
      *iv_return = MDBM_ALIGN_16_BITS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'B':
    if (memEQ(name, "MDBM_LARGE_OBJECTS", 18)) {
    /*                           ^            */
#ifdef MDBM_LARGE_OBJECTS
      *iv_return = MDBM_LARGE_OBJECTS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'C':
    if (memEQ(name, "MDBM_COPY_LOCK_ALL", 18)) {
    /*                           ^            */
#ifdef MDBM_COPY_LOCK_ALL
      *iv_return = MDBM_COPY_LOCK_ALL;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'D':
    if (memEQ(name, "MDBM_CACHEMODE_LFU", 18)) {
    /*                           ^            */
#ifdef MDBM_CACHEMODE_LFU
      *iv_return = MDBM_CACHEMODE_LFU;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_CACHEMODE_LRU", 18)) {
    /*                           ^            */
#ifdef MDBM_CACHEMODE_LRU
      *iv_return = MDBM_CACHEMODE_LRU;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'E':
    if (memEQ(name, "MDBM_CACHE_REPLACE", 18)) {
    /*                           ^            */
#ifdef MDBM_CACHE_REPLACE
      *iv_return = MDBM_CACHE_REPLACE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'N':
    if (memEQ(name, "MDBM_OPEN_WINDOWED", 18)) {
    /*                           ^            */
#ifdef MDBM_OPEN_WINDOWED
      *iv_return = MDBM_OPEN_WINDOWED;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'P':
    if (memEQ(name, "MDBM_DEMAND_PAGING", 18)) {
    /*                           ^            */
#ifdef MDBM_DEMAND_PAGING
      *iv_return = MDBM_DEMAND_PAGING;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'U':
    if (memEQ(name, "MDBM_STORE_SUCCESS", 18)) {
    /*                           ^            */
#ifdef MDBM_STORE_SUCCESS
      *iv_return = MDBM_STORE_SUCCESS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_19 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_CACHEMODE_GDSF MDBM_CACHEMODE_NONE MDBM_CLOCK_STANDARD
     MDBM_ITERATE_NOLOCK */
  /* Offset 18 gives the best switch position.  */
  switch (name[18]) {
  case 'D':
    if (memEQ(name, "MDBM_CLOCK_STANDAR", 18)) {
    /*                                 D      */
#ifdef MDBM_CLOCK_STANDARD
      *iv_return = MDBM_CLOCK_STANDARD;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'E':
    if (memEQ(name, "MDBM_CACHEMODE_NON", 18)) {
    /*                                 E      */
#ifdef MDBM_CACHEMODE_NONE
      *iv_return = MDBM_CACHEMODE_NONE;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'F':
    if (memEQ(name, "MDBM_CACHEMODE_GDS", 18)) {
    /*                                 F      */
#ifdef MDBM_CACHEMODE_GDSF
      *iv_return = MDBM_CACHEMODE_GDSF;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'K':
    if (memEQ(name, "MDBM_ITERATE_NOLOC", 18)) {
    /*                                 K      */
#ifdef MDBM_ITERATE_NOLOCK
      *iv_return = MDBM_ITERATE_NOLOCK;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_20 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_CHECK_DIRECTORY MDBM_ITERATE_ENTRIES MDBM_STAT_CB_ELAPSED
     MDBM_STAT_OPERATIONS MDBM_STAT_TYPE_FETCH MDBM_STAT_TYPE_STORE */
  /* Offset 16 gives the best switch position.  */
  switch (name[16]) {
  case 'E':
    if (memEQ(name, "MDBM_STAT_TYPE_FETCH", 20)) {
    /*                               ^          */
      *iv_return = MDBM_STAT_TYPE_FETCH;
      return PERL_constant_ISIV;
    }
    break;
  case 'I':
    if (memEQ(name, "MDBM_STAT_OPERATIONS", 20)) {
    /*                               ^          */
#ifdef MDBM_STAT_OPERATIONS
      *iv_return = MDBM_STAT_OPERATIONS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'P':
    if (memEQ(name, "MDBM_STAT_CB_ELAPSED", 20)) {
    /*                               ^          */
#ifdef MDBM_STAT_CB_ELAPSED
      *iv_return = MDBM_STAT_CB_ELAPSED;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'R':
    if (memEQ(name, "MDBM_ITERATE_ENTRIES", 20)) {
    /*                               ^          */
#ifdef MDBM_ITERATE_ENTRIES
      *iv_return = MDBM_ITERATE_ENTRIES;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'T':
    if (memEQ(name, "MDBM_CHECK_DIRECTORY", 20)) {
    /*                               ^          */
#ifdef MDBM_CHECK_DIRECTORY
      *iv_return = MDBM_CHECK_DIRECTORY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    if (memEQ(name, "MDBM_STAT_TYPE_STORE", 20)) {
    /*                               ^          */
      *iv_return = MDBM_STAT_TYPE_STORE;
      return PERL_constant_ISIV;
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant_21 (pTHX_ const char *name, IV *iv_return) {
  /* When generated this function returned values for the list of names given
     here.  However, subsequent manual editing may have added or removed some.
     MDBM_FETCH_FLAG_DIRTY MDBM_LOCKMODE_UNKNOWN MDBM_STAT_TYPE_DELETE */
  /* Offset 17 gives the best switch position.  */
  switch (name[17]) {
  case 'I':
    if (memEQ(name, "MDBM_FETCH_FLAG_DIRTY", 21)) {
    /*                                ^          */
#ifdef MDBM_FETCH_FLAG_DIRTY
      *iv_return = MDBM_FETCH_FLAG_DIRTY;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 'L':
    if (memEQ(name, "MDBM_STAT_TYPE_DELETE", 21)) {
    /*                                ^          */
      *iv_return = MDBM_STAT_TYPE_DELETE;
      return PERL_constant_ISIV;
    }
    break;
  case 'N':
    if (memEQ(name, "MDBM_LOCKMODE_UNKNOWN", 21)) {
    /*                                ^          */
#ifdef MDBM_LOCKMODE_UNKNOWN
      *iv_return = MDBM_LOCKMODE_UNKNOWN;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

static int
constant (pTHX_ const char *name, STRLEN len, IV *iv_return) {
  /* Initially switch on the length of the name.  */
  /* When generated this function returned values for the list of names given
     in this section of perl code.  Rather than manually editing these functions
     to add or remove constants, which would result in this comment and section
     of code becoming inaccurate, we recommend that you edit this section of
     code, and use it to regenerate a new set of constant functions which you
     then use to replace the originals.

     Regenerate these constant functions by feeding this entire source file to
     perl -x

#!/usr/bin/perl -w
use ExtUtils::Constant qw (constant_types C_constant XS_constant);

my $types = {map {($_, 1)} qw(IV)};
my @names = (qw(MDBM_ALIGN_16_BITS MDBM_ALIGN_32_BITS MDBM_ALIGN_64_BITS
	       MDBM_ALIGN_8_BITS MDBM_ANY_LOCKS MDBM_API_VERSION
	       MDBM_BSOPS_FILE MDBM_BSOPS_MDBM MDBM_CACHEMODE_GDSF
	       MDBM_CACHEMODE_LFU MDBM_CACHEMODE_LRU MDBM_CACHEMODE_NONE
	       MDBM_CACHE_MODIFY MDBM_CACHE_ONLY MDBM_CACHE_REPLACE
	       MDBM_CHECK_ALL MDBM_CHECK_CHUNKS MDBM_CHECK_DIRECTORY
	       MDBM_CHECK_HEADER MDBM_CLEAN MDBM_CLOCK_STANDARD MDBM_CLOCK_TSC
	       MDBM_COPY_LOCK_ALL MDBM_CREATE_V3 MDBM_DBSIZE_MB
	       MDBM_DEFAULT_HASH MDBM_DEMAND_PAGING MDBM_FETCH_FLAG_DIRTY
	       MDBM_HASH_CRC32 MDBM_HASH_EJB MDBM_HASH_FNV MDBM_HASH_HSIEH
	       MDBM_HASH_JENKINS MDBM_HASH_MD5 MDBM_HASH_OZ MDBM_HASH_PHONG
	       MDBM_HASH_SHA_1 MDBM_HASH_STL MDBM_HASH_TOREK MDBM_INSERT
	       MDBM_INSERT_DUP MDBM_ITERATE_ENTRIES MDBM_ITERATE_NOLOCK
	       MDBM_KEYLEN_MAX MDBM_LARGE_OBJECTS MDBM_LOCKMODE_UNKNOWN
	       MDBM_MAGIC MDBM_MODIFY MDBM_NO_DIRTY MDBM_OPEN_NOLOCK
	       MDBM_OPEN_WINDOWED MDBM_O_ACCMODE MDBM_O_ASYNC MDBM_O_CREAT
	       MDBM_O_DIRECT MDBM_O_FSYNC MDBM_O_RDONLY MDBM_O_RDWR
	       MDBM_O_TRUNC MDBM_O_WRONLY MDBM_PAGESIZ MDBM_PARTITIONED_LOCKS
	       MDBM_PROTECT MDBM_PROT_NONE MDBM_PROT_READ MDBM_PROT_WRITE
	       MDBM_REPLACE MDBM_RESERVE MDBM_RW_LOCKS MDBM_SINGLE_ARCH
	       MDBM_STATS_BASIC MDBM_STATS_TIMED MDBM_STAT_CB_ELAPSED
	       MDBM_STAT_CB_INC MDBM_STAT_CB_SET MDBM_STAT_CB_TIME
	       MDBM_STAT_OPERATIONS MDBM_STORE_ENTRY_EXISTS MDBM_STORE_SUCCESS
	       MDBM_VALLEN_MAX),
            {name=>"MDBM_PTYPE_DATA", type=>"IV", macro=>"1"},
            {name=>"MDBM_PTYPE_DIR", type=>"IV", macro=>"1"},
            {name=>"MDBM_PTYPE_FREE", type=>"IV", macro=>"1"},
            {name=>"MDBM_PTYPE_LOB", type=>"IV", macro=>"1"},
            {name=>"MDBM_STAT_TYPE_DELETE", type=>"IV", macro=>"1"},
            {name=>"MDBM_STAT_TYPE_FETCH", type=>"IV", macro=>"1"},
            {name=>"MDBM_STAT_TYPE_STORE", type=>"IV", macro=>"1"});

print constant_types(), "\n"; # macro defs
foreach (C_constant ("MDBM_File", 'constant', 'IV', $types, undef, 3, @names) ) {
    print $_, "\n"; # C constant subs
}
print "\n#### XS Section:\n";
print XS_constant ("MDBM_File", $types);
__END__
   */

  switch (len) {
  case 10:
    /* Names all of length 10.  */
    /* MDBM_CLEAN MDBM_MAGIC */
    /* Offset 7 gives the best switch position.  */
    switch (name[7]) {
    case 'E':
      if (memEQ(name, "MDBM_CLEAN", 10)) {
      /*                      ^         */
#ifdef MDBM_CLEAN
        *iv_return = MDBM_CLEAN;
        return PERL_constant_ISIV;
#else
        return PERL_constant_NOTDEF;
#endif
      }
      break;
    case 'G':
      if (memEQ(name, "MDBM_MAGIC", 10)) {
      /*                      ^         */
#ifdef MDBM_MAGIC
        *iv_return = MDBM_MAGIC;
        return PERL_constant_ISIV;
#else
        return PERL_constant_NOTDEF;
#endif
      }
      break;
    }
    break;
  case 11:
    return constant_11 (aTHX_ name, iv_return);
    break;
  case 12:
    return constant_12 (aTHX_ name, iv_return);
    break;
  case 13:
    return constant_13 (aTHX_ name, iv_return);
    break;
  case 14:
    return constant_14 (aTHX_ name, iv_return);
    break;
  case 15:
    return constant_15 (aTHX_ name, iv_return);
    break;
  case 16:
    return constant_16 (aTHX_ name, iv_return);
    break;
  case 17:
    return constant_17 (aTHX_ name, iv_return);
    break;
  case 18:
    return constant_18 (aTHX_ name, iv_return);
    break;
  case 19:
    return constant_19 (aTHX_ name, iv_return);
    break;
  case 20:
    return constant_20 (aTHX_ name, iv_return);
    break;
  case 21:
    return constant_21 (aTHX_ name, iv_return);
    break;
  case 22:
    if (memEQ(name, "MDBM_PARTITIONED_LOCKS", 22)) {
#ifdef MDBM_PARTITIONED_LOCKS
      *iv_return = MDBM_PARTITIONED_LOCKS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  case 23:
    if (memEQ(name, "MDBM_STORE_ENTRY_EXISTS", 23)) {
#ifdef MDBM_STORE_ENTRY_EXISTS
      *iv_return = MDBM_STORE_ENTRY_EXISTS;
      return PERL_constant_ISIV;
#else
      return PERL_constant_NOTDEF;
#endif
    }
    break;
  }
  return PERL_constant_NOTFOUND;
}

/* End of auto-generated constant handling */

/* Here starts the actual MDBM module implementation. */
MODULE = MDBM_File	PACKAGE = MDBM_File	PREFIX = mdbm_

PROTOTYPES: DISABLE

MDBM_File
mdbm_TIEHASH(char* /*dbtype*/, char* filename, int flags, int mode, int pagesize = 0, int dbsize = 0)

void
mdbm_DESTROY(MDBM_File db)
    CODE:
        if (db && (DeleteExistingPtr(db) == NULL)) {
          remove_perl_callback(db);
          mdbm_close(db);
        }

datum
mdbm_FETCH(MDBM_File db, datum key)

datum
mdbm_fetch_r(MDBM_File db, datum key, MDBM_ITER* iter)
    CODE:
        mdbm_fetch_r(db, &key, &RETVAL, iter);
    OUTPUT:
        RETVAL

datum
mdbm_fetch_dup_r(MDBM_File db, datum key, MDBM_ITER* iter)
    CODE:
        mdbm_fetch_dup_r(db, &key, &RETVAL, iter);
    OUTPUT:
        RETVAL

int
mdbm_EXISTS(MDBM_File db, datum key)
    PREINIT:
        static datum val;
    CODE:
        val = mdbm_fetch(db, key);
        if ((val.dsize == 0) && (val.dptr == NULL)) {
            RETVAL = 0;
        } else {
            RETVAL = 1;
        }
    OUTPUT:
        RETVAL

# int
# mdbm_fetch_str(MDBM_File db, const char *)

int
mdbm_DELETE(MDBM_File db, datum key)

int
mdbm_delete_r(MDBM_File db, MDBM_ITER* iter)

# int
# mdbm_delete_str(MDBM_File db, const char *)

int
mdbm_STORE(MDBM_File db, datum key, datum value, int flags = MDBM_REPLACE)
    CLEANUP:
	if (RETVAL) {
            if (RETVAL < 0 && errno == EPERM)  croak("No write permission to mdbm file");
            croak("mdbm store returned %d, errno %d, key \"%s\"",
                  RETVAL,errno,key.dptr);
	}

int
mdbm_store_r(MDBM_File db, datum key, datum value, int flags, MDBM_ITER* iter)
    CODE:
        RETVAL = mdbm_store_r(db, &key, &value, flags, iter);
    OUTPUT:
        RETVAL
    CLEANUP:
	if (RETVAL) {
	    if (RETVAL < 0 && errno == EPERM)
		croak("No write permission to mdbm file");
	    croak("mdbm store returned %d, errno %d, key \"%s\"",
			RETVAL,errno,key.dptr);
	}

# int
# mdbm_store_str(MDBM_File db, const char *, const char *, int)

# kvpair
# mdbm_first(MDBM_File db, MDBM_ITER *)

# kvpair
# mdbm_first_r(MDBM_File db, MDBM_ITER *)

datum
mdbm_FIRSTKEY(MDBM_File db)

datum
mdbm_firstkey_r(MDBM_File db, MDBM_ITER* iter)

# kvpair
# mdbm_next(MDBM_File db)

# kvpair
# mdbm_next_r(MDBM_File db, MDBM_ITER *)

datum
mdbm_NEXTKEY(MDBM_File db, datum /*key*/)

datum
mdbm_nextkey_r(MDBM_File db, MDBM_ITER* iter)

int     
mdbm_pre_split(MDBM_File db, mdbm_ubig_t n)

int     
mdbm_set_alignment(MDBM_File db, int amask)


int
mdbm_limit_size_v3_xs(MDBM_File db, mdbm_ubig_t max_page, SV *shake_not_null)
    CODE:
        RETVAL=0;
        if ((int) SvIV(shake_not_null) ) {
          RETVAL = mdbm_limit_size_v3(db, max_page, &shake_function_v3, db);
        } else {
          RETVAL = mdbm_limit_size_v3(db, max_page, NULL, NULL);
        }
    OUTPUT:
        RETVAL

void
mdbm_prune(MDBM_File db, SV *prune, char *param)
    CODE:
        if (SvTYPE(SvRV(prune)) == SVt_PVCV) {
          update_perl_callback(db, ST(0), NULL, newSVsv(prune));
          mdbm_prune(db, &prune_function, (void *)param); 
        }

void
mdbm_compress_tree(MDBM_File db)

void
mdbm_stat_header(MDBM_File db)

void    
mdbm_stat_all_page(MDBM_File db)

void    
mdbm_chk_all_page(MDBM_File db)

void    
mdbm_dump_all_page(MDBM_File db)

void    
mdbm_sync(MDBM_File db)

void    
mdbm_fsync(MDBM_File db)

void    
mdbm_close(MDBM_File db)

void    
mdbm_close_fd(MDBM_File db)

void    
mdbm_truncate(MDBM_File db)

int     
mdbm_lock(MDBM_File db)

int    
mdbm_trylock(MDBM_File db)

int     
mdbm_islocked(MDBM_File db)

int     
mdbm_isowned(MDBM_File db)

int     
mdbm_unlock(MDBM_File db)

int 
mdbm_lock_smart(MDBM_File db, datum key, int flags) 
    CODE:
        RETVAL = mdbm_lock_smart(db, &key, flags);
    OUTPUT:
        RETVAL

int 
mdbm_trylock_smart(MDBM_File db, datum key, int flags)
    CODE:
        RETVAL = mdbm_trylock_smart(db, &key, flags);
    OUTPUT:
        RETVAL

int 
mdbm_unlock_smart(MDBM_File db, datum key, int flags)
    CODE:
        RETVAL = mdbm_unlock_smart(db, &key, flags);
    OUTPUT:
        RETVAL

int
mdbm_lock_shared(MDBM_File db)

int
mdbm_trylock_shared(MDBM_File db)

int
mdbm_plock(MDBM_File db, datum key, int flags)
    CODE:
        RETVAL = mdbm_plock(db, &key, flags);
        /*NOTE: was: XSprePUSH; PUSHi((IV)RETVAL); 
          instead of OUPUT section */
    OUTPUT:
        RETVAL


int
mdbm_tryplock(MDBM_File db, datum key, int flags)
    CODE:
        RETVAL = mdbm_tryplock(db, &key, flags);
        /*NOTE: was: XSprePUSH; PUSHi((IV)RETVAL); 
          instead of OUPUT section */
    OUTPUT:
        RETVAL

int
mdbm_punlock(MDBM_File db, datum key, int flags)
    CODE:
        RETVAL = mdbm_punlock(db, &key, flags);
        /*NOTE: was: XSprePUSH; PUSHi((IV)RETVAL); 
          instead of OUPUT section */
    OUTPUT:
        RETVAL

int     
mdbm_sethash(MDBM_File db, int number)

int
mdbm_replace_db(MDBM_File db, const char *newfile)

int
mdbm_replace_file(const char* oldfile, const char* newfile)

int
mdbm_setspillsize(MDBM_File db, int size)

uint64_t
mdbm_get_size(MDBM_File db)

uint64_t
mdbm_get_limit_size(MDBM_File db)

int
mdbm_get_page_size(MDBM_File db)

int
mdbm_get_hash(MDBM_File db)

int
mdbm_get_alignment(MDBM_File db)

int
mdbm_save(MDBM_File db, const char *file, int flags, int mode, int compressionLevel)

int
mdbm_restore(MDBM_File db, const char *file)

char *
mdbm_getHashId(db)
    MDBM_File db
    CODE:
         static char ptrValue[32];
         snprintf(ptrValue, sizeof(ptrValue), "%p", db);
         RETVAL = ptrValue;
    OUTPUT:
        RETVAL
    

PROTOTYPES: ENABLE

void
constant(sv)
    PREINIT:
#ifdef dXSTARG
	dXSTARG; /* Faster if we have it.  */
#else
	dTARGET;
#endif
	STRLEN		len;
        int		type;
	IV		iv=0;
	/* NV		nv;	Uncomment this if you need to return NVs */
	/* const char	*pv;	Uncomment this if you need to return PVs */
    INPUT:
	SV *		sv;
        const char *	s = SvPV(sv, len);
    PPCODE:
        /* Change this to constant(aTHX_ s, len, &iv, &nv);
           if you need to return both NVs and IVs */
	type = constant(aTHX_ s, len, &iv);
      /* Return 1 or 2 items. First is error message, or undef if no error.
           Second, if present, is found value */
        switch (type) {
        case PERL_constant_NOTFOUND:
          sv = sv_2mortal(newSVpvf("%s is not a valid MDBM_File macro", s));
          PUSHs(sv);
          break;
        case PERL_constant_NOTDEF:
          sv = sv_2mortal(newSVpvf(
	    "Your vendor has not defined MDBM_File macro %s, used", s));
          PUSHs(sv);
          break;
        case PERL_constant_ISIV:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHi(iv);
          break;
	/* Uncomment this if you need to return NOs
        case PERL_constant_ISNO:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHs(&PL_sv_no);
          break; */
	/* Uncomment this if you need to return NVs
        case PERL_constant_ISNV:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHn(nv);
          break; */
	/* Uncomment this if you need to return PVs
        case PERL_constant_ISPV:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHp(pv, strlen(pv));
          break; */
	/* Uncomment this if you need to return PVNs
        case PERL_constant_ISPVN:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHp(pv, iv);
          break; */
	/* Uncomment this if you need to return SVs
        case PERL_constant_ISSV:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHs(sv);
          break; */
	/* Uncomment this if you need to return UNDEFs
        case PERL_constant_ISUNDEF:
          break; */
	/* Uncomment this if you need to return UVs
        case PERL_constant_ISUV:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHu((UV)iv);
          break; */
	/* Uncomment this if you need to return YESs
        case PERL_constant_ISYES:
          EXTEND(SP, 1);
          PUSHs(&PL_sv_undef);
          PUSHs(&PL_sv_yes);
          break; */
        default:
          sv = sv_2mortal(newSVpvf(
	    "Unexpected return type %d while processing MDBM_File macro %s, used",
               type, s));
          PUSHs(sv);
        }


# Implement the iterators for all the _r methods 
MODULE = MDBM_File	PACKAGE = MDBM_Iter	PREFIX = mdbm_iter_

MDBM_ITER*
mdbm_iter_new(char* class)
    PREINIT:
        MDBM_ITER* iter;
    CODE:
        (void)class;
        if (items > 1)
	   Perl_croak(aTHX_ "Usage: new MDBM_Iter");
	New(0, iter, 1, MDBM_ITER);
	MDBM_ITER_INIT(iter);
	RETVAL = iter;
    OUTPUT:
        RETVAL

MODULE = MDBM_File	PACKAGE = MDBM_ITERPtr	PREFIX = mdbm_iter_

void
mdbm_iter_DESTROY(MDBM_ITER* iter)
    CODE:
        if (iter) {
            Safefree(iter);
	    iter = NULL;
	}

void
mdbm_iter_reset(MDBM_ITER* iter)
    CODE:
        MDBM_ITER_INIT(iter);


MODULE = MDBM_File     PACKAGE = ShakeData

datum
getPageEntryKey(shakeinfo, index)
        ShakeData shakeinfo
        unsigned int index

datum
getPageEntryValue(shakeinfo, index)
       ShakeData shakeinfo
       unsigned int index

unsigned int
getCount(shakedata)
       ShakeData shakedata
        CODE:
        RETVAL = getCount(shakedata);
        OUTPUT:
        RETVAL

unsigned int
getPageNum(shakedata)
        ShakeData shakedata
        CODE:
        RETVAL = getPageNum(shakedata);
        OUTPUT:
        RETVAL

unsigned int
getFreeSpace(shakedata)
       ShakeData shakedata
        CODE:
        RETVAL = getFreeSpace(shakedata);
        OUTPUT:
        RETVAL

unsigned int
getSpaceNeeded(shakedata)
       ShakeData shakedata
        CODE:
        RETVAL = getSpaceNeeded(shakedata);
        OUTPUT:
        RETVAL

void
setEntryDeleted(shakedata, index)
        ShakeData shakedata
        unsigned int index


