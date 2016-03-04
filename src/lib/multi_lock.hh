/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef DONT_MULTI_INCLUDE_MULTI_LOCK_H
#define DONT_MULTI_INCLUDE_MULTI_LOCK_H


#include <stdio.h>
/* #include <stddef.h> */
#include <stdint.h>
/*#include <linux/types.h>*/
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <pthread.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/mman.h>

/*#define HAVE_ROBUST_PTHREADS 1 */

#include "mdbm_lock.hh"

typedef uint32_t owner_t;
#define OWNER_FMT "%u"

#ifdef __cplusplus


struct PMutexRecord {
    pthread_mutex_t mutex;
    // TODO verify that pthread_self() is stable as an int
    //  and unique enough across processes to act as a lock owner-id
    volatile owner_t owner;
    volatile int32_t count;
    // Attempt to pad structure out to cache-line-size 64 bytes (vs original 48).
    // This seems not to have much effect (vs ramdom timing variations).
    //int32_t reserved0;
    //int32_t reserved1;
    //int32_t reserved2;
    //int32_t reserved3;
};

class PMutex {
  public:
    PMutex(PMutexRecord* inst=NULL, bool init=true, uint32_t idx=0);
    ~PMutex();
    // returns the (platform specific) size of a mutex record
    static int GetRecordSize();
    // initialize the lock structure
    bool Init();
    // free memory and lock resources
    void DeInit();
//    // take (shared) control of an existing mutex record
//    bool Assume(void* instance, bool init);
    // Lock the mutex, current thread-id is passed in for efficiency
    // NOTE: nested/recursive locking is allowed. Robust locks are used, if available.
    int Lock(bool blocking, owner_t tid);
    // Unlock the mutex, current thread-id is passed in for efficiency
    int Unlock(owner_t tid);
#ifdef ALLOW_MLOCK_RESET
    // Re-initialize the lock and mutex record. USE WITH EXTREME CARE!
    int ResetLock();
#endif
    // Returns the thread-id of the owning thread (or 0 if un-owned)
    owner_t GetOwnerId();
    // returns the sum of lock nesting counts for all processes
    int GetLockCount();
    // returns the sum of lock nesting counts for this process/thread
    int GetLocalCount(owner_t tid);
    // slower version of above
    int GetLocalCount();
    // limited check for lock validity, we can't validate the pthread data
    bool IsValid();

  //private:
  public:
    PMutexRecord* rec;       // pointer to mutex data (likely in mmap)
    uint32_t     index;      // index (in PLockFile) for debugging
    bool         allocated;  // dynamically allocated (by this class) or not
};

class PLockHdr {
public:
  // stored in shared memory, accessed by multiple threads... it's all volatile
  volatile uint32_t version;           // Version number
  volatile uint32_t mutexSize;         // PMutexRecord size (for platform safety)
  volatile uint32_t registerCount;     // register count
  volatile uint32_t mutexCount;        // PMutexRecord count
  volatile uint32_t mutexInitialized;  // How many of the mutexes have been initialized
};

class PLockFile {
  /// A memory mapped lock and register file
  /// It contains: 
  ///   The PLockHdr header structure
  ///   an array of (int32_t) registers (zero-initialized)
  ///   an array of PMutexRecords
public:
  PLockFile();
  ~PLockFile();
  void Close();
  int Open(const char* fname, int regCount, int lockCount, bool &created, int mode=-1);
#ifdef DYNAMIC_LOCK_EXPANSION
  // NOTE: it's unreasonable/unsafe to expand both registers and locks, as this
  // would force one to move, or for there to be one highly-contended lock for *all* operations
  int Expand(int newLockCount); 
#endif // DYNAMIC_LOCK_EXPANSION
  // Returns the number of mutex locks
  int GetNumLocks();
  // Returns the number of atomic integer registers
  int GetNumRegisters();
  // Performs a limited check of header validity,
  // if nonzero, the header has changed or is corrupted
  int CheckHeader();

#ifdef ALLOW_MLOCK_RESET
  int ResetAllLocks();
#endif

  // Persistent Atomic Integers
  // get the register value at position 'index'
  int32_t RegisterGet(int index);
  // unconditionally set the value, returns 0 on success
  int RegisterSet(int index, int32_t newVal);
  // atomic increment the value by 1, returns 0 on success
  int RegisterInc(int index);
  // atomic decrement the value by 1, returns 0 on success
  int RegisterDec(int index);
  // atomic increment the value by 'delta', returns 0 on success
  int RegisterAdd(int index, int32_t delta);
  // atomic, conditional replace 'oldVal' by 'newVal', returns 0 on success
  // fails and returns nonzero if the register is != 'oldVal'
  int RegisterCas(int index, int32_t oldVal, int32_t newVal);

  // This needs to be super-fast, so we let our internal user (MLock) access data directly,
  // instead of adding yet another layer of wrappers, especially the locks array.
  // Because of this, users are responsible for bounds checking access.
public:
  char*       filename;       // filename
  int         fd;             // open file descriptor
  size_t      lockFileSize;   // expected file-size in bytes
  uint8_t*    base;           // pointer to base of mmapped region
  PLockHdr   *hdr;            // lockfile header
  int         numRegs;        // number of registers
  int32_t    *registers;      // array of registers
  int         numLocks;       // number of locks
  PMutex*    *locks;          // array of mutexes
};
#endif /* __cplusplus */

//#include "multi_lock_wrap.h"


#ifdef __cplusplus

#define MREG_LOCK_TYPE  0
#define MREG_LOCK_COUNT 1
#define MREG_SHAR_INDEX 2

struct MLockTlsEntry;

// multi-lock
//   We divide locks up into some number of "base" single locks,
//   and one composite lock that can be single, shared (R/W), or partitioned (indexed). 
class MLock {
public:
  MLock();
  ~MLock();
  int Open(const char* fname, int baseCount, MLockType t, int partCount, int mode=-1);
#ifdef DYNAMIC_LOCK_EXPANSION
  int Expand(int basic_locks, MLockType type, int lock_count);
#endif // DYNAMIC_LOCK_EXPANSION
  void Close();
#ifdef ALLOW_MLOCK_RESET
  int ResetAllLocks();
#endif
  MLockType GetLockType();
  int GetNumLocks();

  int UnlockBase(int index=0);
  int LockBase(int index=0, bool blocking=true);
#ifdef ALLOW_MLOCK_RESET
  int ResetBaseLock(int index=0);
#endif
  owner_t GetBaseOwnerId(int index=0);
  int GetBaseLockCount(int index=0);
  int GetBaseLocalCount(int index=0);

  int Unlock(int index=-1);
  int Lock(int index, bool blocking=true);
  int Upgrade(bool blocking=true);
  int Downgrade(int index=-1, bool blocking=true);
#ifdef ALLOW_MLOCK_RESET
  int ResetLock(int index=0);
#endif
  owner_t GetOwnerId(int index=-1);
  int GetLockCount(int index=-1);
  int GetLocalCount(int index=-1);
  // over shared/partioned locks (ignores base locks)
  int GetLockCountTotal();
  int GetLocalCountTotal();
  int GetPartCount();
  int GetLockedPartCount();
  int GetLocalPartCount();
  void DumpLockState(FILE* file=NULL);

protected:
  int LockSingle(bool blocking, MLockTlsEntry* mte);

public:
  PLockFile locks;      //
  MLockType  lockType;
  int       base;
  int       parts;
};
#endif /* __cplusplus */

#endif /* DONT_MULTI_INCLUDE_MULTI_LOCK_H */
