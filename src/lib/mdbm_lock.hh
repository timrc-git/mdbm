/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef DONT_MULTI_INCLUDE_MDBM_LOCK_HH
#define DONT_MULTI_INCLUDE_MDBM_LOCK_HH

#include "atomic.h"
#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_stats.h"

#ifdef __cplusplus
class MdbmLockBase;
extern "C" {
#else // __cplusplus
  typedef void* MdbmLockBase;
#endif /* __cplusplus */
  const int MLOCK_ASYNC=0; /* Asynchronous (try) operation */
  const int MLOCK_BLOCK=1; /* Blocking (wait) operation */

  const int MLOCK_ANY=-2;  /* Pick an arbitrary lock index, for SHARED mode */
  enum MLockTypeEnum {
    MLOCK_UNCHECKED = -3, /* indicates the locking mode will be read from the file */
    MLOCK_INTERNAL  = -2, /* internal (special) lock */
    MLOCK_EXCLUSIVE = -1, /* single-mutex exclusive locking mode */
    /* MLOCK_SINGLE = 0,     single-mutex exclusive locking mode  */
    MLOCK_SINGLE = -1,     /* single-mutex exclusive locking mode  */
    MLOCK_SHARED    = 1,  /* shared read/write locking mode */
    MLOCK_INDEX     = 2,  /* partitioned (index) locking mode */
    MLOCK_UPGRADE   = 4   /* convert index/shared to exclusive TODO REPLACE w/ MLOCK_EXCLUSIVE? */
  };

  /* helper */
  typedef enum MLockTypeEnum MLockType;
  //typedef MdbmLockBase* mdbm_lock_t;

  typedef MdbmLockBase* (*mdbm_lock_creator)(void);

  
  int mdbm_add_lock_plugin(const char* name, mdbm_lock_creator creator) 
    __attribute__ ((visibility ("default")));

  int do_delete_lockfiles(const char* dbname);
  int get_lock_flags(const char* dbname, int defaults);
  MdbmLockBase* mdbm_create_lock(const char* name);
  void print_lock_state_inner(struct mdbm_locks* locks);
  void print_lock_state(MDBM* db);
  //void lock_error(MDBM* db, const char* what, ...);
  int db_is_locked(MDBM* db);
  uint32_t db_get_lockmode(MDBM *db);
  int db_get_part_count(MDBM* db);
  int db_internal_is_owned(MDBM* db);
  int db_is_owned(MDBM* db);
  int db_part_owned(MDBM* db);
  int db_is_multi_lock(MDBM* db);
  int db_multi_part_locked(MDBM* db);
  //int get_lockfile_name(const char* dbname, char* lockname, int maxlen);
  struct mdbm_locks* open_locks_inner(const char* dbname, int flags, int do_lock, int* need_check);
  void close_locks_inner(struct mdbm_locks* locks);
  int open_locks(MDBM* db, int flags, int do_lock, int* need_check);
  void close_locks(MDBM* db);
  int lock_internal(MDBM* db);
  int unlock_internal(MDBM* db);
  int do_lock_x(MDBM* db, int pageno, int nonblock, int check);
  int do_lock(MDBM* db, int write, int nonblock, const datum* key,
        mdbm_hashval_t* hashval, mdbm_pagenum_t* pagenum);
  int do_unlock_x(MDBM* db);
  int mdbm_internal_do_unlock(MDBM* db, const datum* key);
  int do_lock_reset(const char* dbfilename, int flags);
#ifdef __cplusplus
}
#endif /* __cplusplus */

#ifdef __cplusplus
// Abstract interface for locking plugins
class MdbmLockBase {
public:
  //MdbmLockBase();
  virtual ~MdbmLockBase();
  // NOTE: name is the name of the object being protected
  // the locking class will internally determine the actual path/name
  virtual int open(const char* name, int flags, MLockType type, int count, int do_lock, int* need_check) = 0;
  virtual void close() = 0;
  // TODO allow for multiple files-per-lock
  //   consider mdbm_delete_file() that wipes MDBM, and lockfiles
  //   (user still needs to deal with directory hierarchy)
  virtual int getFilename(const char* dbname, char* lockname, int maxlen) = 0;
  virtual void printState() = 0;
  // returns one of EXCL, SHARED, or PART 
  virtual MLockType getMode() = 0;

  // type is EXCL, SHARED, PART, or INTERNAL
  //   async is "BLOCK" or "ASYNC" (may return EWOULDBLOCK), 
  //   part is partition (optional)
  //   if previous owner died: sets need_check
  //   if EXCL owner died: may return ESTALE, take out an EXCL lock, and sets 'upgrade'
  //     TODO, consider if we should do this for mlock
  virtual int lock(int type, int async, int part, int &need_check, int &upgrade) = 0;
  virtual int unlock(int type=MLOCK_EXCLUSIVE) = 0;
  // returns the number of locks of 'type' owned by 'self' (this thread), or anyone
  //   'index', if >=0 indicates the index of the partitioned/shared lock to query
  virtual int getHeldCount(int type, bool self) = 0;
  virtual int getHeldCount(int type, bool self, int index) = 0;
  // returns the number of locks of that type
  virtual int getCount(int type) = 0;
  // hacky function... shouldn't exist, optional, may return -1, ENOTSUP
  virtual int reset(const char* dbfilename, int flags) = 0;

  // TODO int upgrade()/downgrade(int part?) ?
  //   actually, handle it with special 'type' arg to lock()
  // TODO owner_t getOwnerId() for debugging?

#ifdef DYNAMIC_LOCK_EXPANSION
  virtual int expand(int type, int count) = 0;
#endif
};
inline MdbmLockBase::~MdbmLockBase() {}
#endif //__cplusplus

//#define REGISTER_MDBM_LOCK_PLUGIN(name, creator) \ ;
// int mdbm_auto_register_dummy##name() {          \ ;
//   return mdbm_add_lock_plugin(#name, creator);  \ ;
// }                                               \ ;
// int mdbm_plugin_dummy_##name =                  \ ;
//    mdbm_auto_register_dummy##name();

#define REGISTER_MDBM_LOCK_PLUGIN(name, creator) \
 void __attribute__ ((constructor)) mdbm_lock_register##name(void) { \
   (void)mdbm_add_lock_plugin(#name, creator);                       \
 }



#endif /* DONT_MULTI_INCLUDE_MDBM_LOCK_HH */
