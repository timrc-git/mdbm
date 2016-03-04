/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "multi_lock.hh"

#include "atomic.h"
#include "util.hh"

#include "mdbm_internal.h"
#include "mdbm_lock.hh"


// Per-thread per-mlock data
struct MLockTlsEntry {
  owner_t tid;          // thread-id
  uint32_t exclCount;   // exclusive lock-count
  uint32_t partCount;   // partition lock-count
  int32_t  lastLocked;  // last-locked partition index (lock iteration is slow)
};

typedef std::map<void*, MLockTlsEntry*> MLockTlsMap;

// Per-thread data
struct TlsEntry {
  owner_t tid;
  MLockTlsMap entries;
};

// Thread-local-storage cleanup function 
void tls_entry_destruct(void* entry) {
  if (!entry) { return; }
  TlsEntry* te = (TlsEntry*)entry;

  MLockTlsMap::iterator iter = te->entries.begin();
  while (iter != te->entries.end()) {
    MLockTlsEntry* mte = iter->second;
    if (mte) {
      delete mte;
      ++iter;
    }
  }

  delete te;
}

// Thread-local-storage management class
class TlsData {
private:
    pthread_key_t    key_;                 // The TLS lookup key
    static uint32_t  pid;                  // process id
    static TlsData    *atfork_installed;   // at_fork install guard
public:
    TlsData() { 
      // Create a special pthread-key to lookup TLS data.
      // This is done once for a process/thread family.
      int ret = pthread_key_create(&key_, tls_entry_destruct);
      if (ret) {
        mdbm_log(LOG_ERR, "Unable to create TlsData storage %d %s\n", ret, strerror(ret));
        exit(1);
      }
    }
    ~TlsData() { 
      // delete the tls lookup key on exit (not per-thread)
      pthread_key_delete(key_); 
    }
    // When forking, pthreads copies the TLS data, which will be incorrect (tid, etc)
    static void fork_reset() {
      pid = getpid();
      void* olddata = pthread_getspecific(atfork_installed->key_);
      pthread_setspecific(atfork_installed->key_, NULL);
      if (olddata) {
        // delete old pre-fork entry
        tls_entry_destruct(olddata);
      }
    }
    // install fork_reset if we haven't already.
    void install_atfork() {
      if (!atfork_installed) {
        atfork_installed = this;
        int ret = pthread_atfork(NULL, NULL, fork_reset);
        if (ret) {
          mdbm_log(LOG_ERR, "Unable to install TlsData fork handler %d %s\n", ret, strerror(ret));
          exit(1);
        }
      }
    }
    // Lookup the TLS data for this thread, creating a default if needed.
    TlsEntry* getEntry() { 
      //AUTO_TSC("TlsData::get()");
      TlsEntry *te = (TlsEntry*)pthread_getspecific(key_);
      if (!te) {
        te = new TlsEntry;
        te->tid = gettid();
        //fprintf(stderr, "TlsData tid:%llu \n", tid);
        install_atfork();
        pthread_setspecific(key_, (void*)te);
      }
      return te;
    }
    void deleteMLockEntry(void* mlockPtr) {
      TlsEntry *te = (TlsEntry*)pthread_getspecific(key_);
      if (!te) { // don't tls entry create if it doesn't exist
        return;
      }
      MLockTlsMap::iterator iter = te->entries.find(mlockPtr);
      if (iter != te->entries.end()) {
        MLockTlsEntry* mte = iter->second;
        te->entries.erase(iter);
        delete mte;
      }
    }
    // Lookup data for a particular MLock in our TLS data
    MLockTlsEntry* getMLockEntry(void* mlockPtr) {
      TlsEntry *te = getEntry();
      MLockTlsMap::iterator iter = te->entries.find(mlockPtr);
      if (iter != te->entries.end()) {
        return iter->second;
      }
      MLockTlsEntry* mte = new MLockTlsEntry;
//fprintf(stderr, "@@@ RESETTING ALL LOCK COUNTS\n");
      mte->tid = te->tid;
      mte->exclCount = 0;
      mte->partCount = 0;
      mte->lastLocked = 0;
      te->entries[mlockPtr] = mte;
      return mte;
    }
    // Lookup the tid in our TLS data. gettid() is *far* too slow.
    uint64_t get() { 
      //AUTO_TSC("TlsData::get()");
      TlsEntry *te = getEntry();
      return te->tid;
    }
};
TlsData *TlsData::atfork_installed = NULL;
uint32_t  TlsData::pid = 0;
 

// singleton Thread-local-storage for mlocks
TlsData internal_thread_id;

// get the tid from TLS
inline owner_t get_thread_id() {
  return internal_thread_id.get();
}

// get per-MLock TLS data
inline MLockTlsEntry* get_tls_entry(MLock* locks) {
  return internal_thread_id.getMLockEntry((void*)locks);
}

inline void reset_tls_entry(MLock* locks) {
  MLockTlsEntry* mte =  get_tls_entry(locks);
  if (mte) {
//fprintf(stderr, "@@@ RESETTING TLS-ENTRY LOCK COUNTS\n");
      mte->exclCount = 0;
      mte->partCount = 0;
      mte->lastLocked = 0;
  }
}

inline void delete_tls_entry(MLock* locks) {
  internal_thread_id.deleteMLockEntry((void*)locks);
}




PMutex::PMutex(PMutexRecord* inst, bool init, uint32_t idx) : rec(inst), index(idx), allocated(false) { 
  if (!inst) {
    allocated = true;
    rec = new PMutexRecord;
  }
  if (init) {
    if (!Init()) {
      rec = NULL;
    }
  }
}
PMutex::~PMutex() { 
  DeInit();
}
int PMutex::GetRecordSize() { 
  return sizeof(PMutexRecord);
}
#define statcase(s) case s : return #s;
const char* ErrToStr(int err) {
  switch(err) {
    statcase(EBUSY)
    statcase(EINPROGRESS)
    statcase(EINVAL)
    statcase(EAGAIN)
    //statcase(EWOULDBLOCK) // for many platforms, EWOULDBLOCK==EAGAIN
    statcase(ENOENT)
    statcase(ENOMEM)
    statcase(EPERM)
    statcase(EOPNOTSUPP)
    default: return "<E-UNKNOWN>";
  }
}
const char* MLockTypeToStr(int lt) {
  switch(lt) {
    statcase(MLOCK_UNCHECKED)
    statcase(MLOCK_SINGLE)
    statcase(MLOCK_INDEX)
    statcase(MLOCK_SHARED)
    default: return "<MLOCK-type-unknown>";
  }
}

bool PMutex::Init() {
  int ret=0;
  rec->owner=0;
  rec->count=0;
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr)) {
    perror("pthread_mutexattr_init ");
    return false;
  }

#ifdef HAVE_ROBUST_PTHREADS
//fprintf(stderr, "setting robust on mutex %p\n", &rec->mutex);
  //// An old bug (2010) with glibc-2.5 is fixed by setting priority
  //if (0 != (ret=pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT))) {
  //  perror("pthread_mutexattr_setprotocol ");
  //  pthread_mutexattr_destroy(&attr);
  //  return false;
  //}
  if (0 != (ret=pthread_mutexattr_setrobust_np(&attr, PTHREAD_MUTEX_ROBUST_NP))) {
    perror("pthread_mutexattr_setrobust_np ");
    pthread_mutexattr_destroy(&attr);
    return false;
  }
//#warning "USING ROBUST MUTEXES"
//#else
//#warning "ROBUST MUTEXES ARE NOT AVAILABLE"
#endif// HAVE_ROBUST_PTHREADS

  //if(0 != pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK)) {
  //  perror("pthread_mutexattr_settype(PTHREAD_MUTEX_ERRORCHECK) ");
  //  pthread_mutexattr_destroy(&attr);
  //  return false;
  //}
  //if(0 != pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE)) {
  //  perror("pthread_mutexattr_settype(PTHREAD_MUTEX_RECURSIVE) ");
  //  pthread_mutexattr_destroy(&attr);
  //  return false;
  //}
  if (0 != (ret=pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))) {
    perror("pthread_mutexattr_setpshared ");
    pthread_mutexattr_destroy(&attr);
    return false;
  }
  if (0 != (ret=pthread_mutex_init(&rec->mutex, &attr))) {
    mdbm_log(LOG_ERR, "ERROR initializing MutexPthread %p:%p ret:%d %s %d (%s)\n", (void*)this, (void*)&rec->mutex, ret, ErrToStr(ret), errno, strerror(errno));
    perror("pthread_mutex_init ");
    pthread_mutexattr_destroy(&attr);
    return false;
  }
  pthread_mutexattr_destroy(&attr);
  return true;
}
void PMutex::DeInit() {
  if (allocated && rec) { 
    pthread_mutex_destroy(&rec->mutex);
    delete rec; 
  }
  allocated = false;
  rec = NULL;
}
#ifdef ALLOW_MLOCK_RESET
int PMutex::ResetLock() {
  PMutexRecord *r = allocated ? NULL: rec;
  DeInit();
  if (r) {
    rec = r;
  } else {
    rec = new PMutexRecord;
  }
  return (true == Init());
}
#endif

//bool PMutex::Assume(void* instance, bool init) {
//  DeInit();
//  rec = (PMutexRecord*)instance;
//  if (init) {
//    if (!Init()) {
//      rec = NULL;
//    }
//  }
//  return true;
//}

inline int PMutex::Lock(bool blocking, owner_t tid) {
  AUTO_TSC("PMutex::Lock()");
//#ifdef TRACE_LOCKS
//  PRINT_LOCK_TRACE(blocking?"TRY Lock(BLOCK)":"TRY Lock(ASYNC)");
//#endif
//  CHECKPOINTV("  Lock(%s) any:%u owner:" OWNER_FMT " %p", blocking?"BLOCK":"ASYNC", rec->count, rec->owner, rec);
  //uint32_t tid = gettid();
  //owner_t tid = get_thread_id();
  int ret = -1;
  if (tid == rec->owner) {
    ret=0;
  } else {
    if (blocking) {
      //AUTO_TSC("pth_lock()");
//fprintf(stderr, "lock mutex %p\n", &rec->mutex);
      ret = pthread_mutex_lock(&rec->mutex);
    } else {
      //AUTO_TSC("pth_trylock()");
//fprintf(stderr, "trylock mutex %p\n", &rec->mutex);
      ret = pthread_mutex_trylock(&rec->mutex);
    }
//fprintf(stderr, "did (try)lock mutex %p\n", &rec->mutex);
    if (likely(!ret)) {
      rec->owner = tid;
#ifdef HAVE_ROBUST_PTHREADS
    } else if (ret == EOWNERDEAD) {
//fprintf(stderr, "OWNERDEAD lock mutex %p\n", &rec->mutex);
      mdbm_log(LOG_ERR, "EOWNERDEAD locking MutexPthread %p\n", (void*)this);
      pthread_mutex_consistent_np(&rec->mutex);
      // return special for owner-died
      errno = EINPROGRESS;
      // clean up our book-keeping
      rec->owner = tid;
      rec->count = 0;
#endif //HAVE_ROBUST_PTHREADS
    } else if (ret == EBUSY) { // async try failed, already owned
      CHECKPOINTV("  Lock(%s) NOTICE (%d)", blocking?"BLOCK":"ASYNC", ret);
      errno = EWOULDBLOCK; // old mutex system uses this code, despite the documentation
      return ret;
    } else if (ret == EDEADLK) { // failed, already owned by us
      mdbm_log(LOG_ERR, "Already Owned Error (%d:%s) locking MutexPthread %p\n", ret, strerror(ret), (void*)this);
      errno = EBUSY; // ysys mutex uses this code
    } else {
      mdbm_log(LOG_ERR, "Error (%d:%s) locking MutexPthread %p idx:%u\n", ret, strerror(ret), (void*)this, index);
      CHECKPOINTV("  Lock(%s) FAIL (%d)", blocking?"BLOCK":"ASYNC", ret);
      return -1;
    } 
  }

  //{AUTO_TSC("lock_update()");
  atomic_inc32s((int32_t*)&rec->count);
//  assert(rec->count>0);
//#ifdef TRACE_LOCKS
//  PRINT_LOCK_TRACE(blocking?"Lock(BLOCK)":"Lock(ASYNC)");
//#endif
//  CHECKPOINTV("  Lock(%s) SUCCESS (%d)", blocking?"BLOCK":"ASYNC", ret);
  //}
  return ret;
}

inline int PMutex::Unlock(owner_t tid) {
  AUTO_TSC("PMutex::Unlock()");
  //uint32_t tid = gettid();
  //owner_t tid = get_thread_id();
//#ifdef TRACE_LOCKS
//  PRINT_LOCK_TRACE("TRY Unlock()");
//#endif
//  CHECKPOINTV("  Unlock() %p", rec);
  if (unlikely(rec->count<0)) {
    mdbm_log(LOG_ERR, "Unlock() (count<0) locking MutexPthread %p idx:%u count:%d owner:" OWNER_FMT "\n", (void*)this, index, rec->count, rec->owner);
    errno = EPERM;
    return -1;
  }
  //assert(rec->count>=0);
  if (unlikely(tid!=rec->owner || !rec->count)) {
    mdbm_log(LOG_ERR, "Error unlocking UN-OWNED MutexPthread %p idx:%u rec:%p any:%d owner:" OWNER_FMT " self:" OWNER_FMT " pid:%d\n", (void*)this, index, (void*)rec, rec->count, rec->owner, get_thread_id(), getpid());
    print_trace();
    errno = EPERM;
    return -1;
  }
  int ret = 0, e=0;
  bool released = (1==rec->count);
  atomic_dec32s((int32_t*)&rec->count);
  if (released) { // no-hint, or unlikely
    rec->owner = 0;
    //AUTO_TSC("pth_unlock()");
    ret = pthread_mutex_unlock(&rec->mutex);
    e = errno;
  }
  if (likely(!ret)) {
//#ifdef TRACE_LOCKS
//  PRINT_LOCK_TRACE("Unlock()");
//#endif
    return 0;
  } else {
    mdbm_log(LOG_ERR, "Error (ret:%d:%s, err:%d:%s:%s) unlocking MutexPthread %p idx:%u thread:" OWNER_FMT "\n", ret, ErrToStr(ret), errno, ErrToStr(errno), strerror(errno), (void*)this, index, get_thread_id());
    errno = e; // reset after print
    if (ret == EPERM) {
      errno = EPERM;
    }
    return ret;
  }
  //CHECKPOINTV("  Unlock() RETURN (%d) any:%u pid:%u)", ret, rec->count, rec->owner); 
  //return ret;
}

owner_t PMutex::GetOwnerId() { return rec->owner; }
int PMutex::GetLockCount() { return rec->count; }
int PMutex::GetLocalCount(owner_t tid) {
  return (rec->owner==tid) ? rec->count : 0; 
}
int PMutex::GetLocalCount() { 
  return (rec->owner==get_thread_id()) ? rec->count : 0; 
}
bool PMutex::IsValid() { return (rec!=NULL); }


PLockFile::PLockFile() : filename(NULL), fd(-1), lockFileSize(0), base(NULL), hdr(NULL), numRegs(0), registers(NULL), numLocks(0), locks(NULL) {
}
PLockFile::~PLockFile() {
  Close();
}
void PLockFile::Close() {
  if (locks) {
    owner_t tid = get_thread_id();
    for (int i=0; i<numLocks; ++i) {
      // release any locks we own
      while (locks[i]->GetLocalCount()>0) {
        int ret;
        if (0 != (ret=locks[i]->Unlock(tid))) {
          mdbm_log(LOG_ERR, "PLockFile: Couldn't release lock %d in Close(): %d\n", i, ret);
          break;
        } else {
          //fprintf(stderr, "PLockFile: AUTO-RELEASE lock %d in Close()\n", i);
        }
      }
      delete locks[i];
    }
    numRegs = 0;
    registers = NULL;
    numLocks = 0;
    delete[] locks;
    locks = NULL;
  }
  if (base) {
    munmap(base, lockFileSize);
    //fprintf(stderr, "@@@   Close() MUNMAP %d bytes at %p  (end %p)\n", (int)lockFileSize, base, base+lockFileSize-1);
    base = NULL;
  }
  if (fd>=0) {
    close(fd);
    fd = -1;
  }
  hdr = NULL;
  //delete filename;
  if (filename) {
    free(filename);
    filename = NULL;
  }
}

const unsigned PLOCK_HDR_VERSION = 1;

/* we need an exact mode (not borked by umask) so that multiple users */
/* can all peacefully share the lock files */
int open_no_umask(const char* path, int flags, mode_t mode) {
  int err_save, ret = -1;
  mode_t oldMask = umask(0); /* save umask */
  ret = open(path, flags, mode); /* create with mode (vs mode|~umask) */
  /* umask doesn't return errors, but it might affect errno */
  err_save = errno; 
  umask(oldMask); /* restore umask */
  errno = err_save;
  return ret;
}

int PLockFile::Open(const char* fname, int regCount, int lockCount, bool &created, int mode) { // create or open&validate
  unsigned sHeader = sizeof(PLockHdr);
  unsigned sMutex = PMutex::GetRecordSize();
  unsigned sReg = sizeof(int32_t);
  int i, offset=0;
  bool init = false;
  bool openUnchecked = (lockCount < 0);
  bool tooSmall = false;
  size_t expectedSize = 0;

  created = false;
  if ((lockCount<0) != (regCount<0)) {
    mdbm_log(LOG_ERR, "PLockFile: for permissive Open, both regCount and lockCount must be < 0: %s\n", fname);
    errno = EINVAL;
    return -1;
  }

  if (!fname || !fname[0]) {
    errno = EINVAL;
    return -1;
  }

  // ensure FD isn't already open
  Close();

  filename = strdup(fname);

  numLocks = 0;
  // calculate size needed
  lockFileSize = sHeader + regCount*sReg + lockCount*sMutex;

  if (mode>=0) {
    fd = open_no_umask(fname, O_RDWR, mode);
  } else {
    fd = open(fname, O_RDWR);
  }

  if ((fd < 0) && !openUnchecked) {
    PLockHdr plockHeader = {PLOCK_HDR_VERSION, sMutex, (uint32_t)regCount, (uint32_t)lockCount, 0};
    // open failed, try create
    //fprintf(stderr, "*******************PLockFile: creating: %s\n", fname);
    //fd = open(fname, O_RDWR|O_CREAT|O_EXCL, S_IRWXU|S_IRWXG);
    fd = open_no_umask(fname, O_RDWR|O_CREAT|O_EXCL, mode);
    if (fd>=0) {
      created=true;
      init = true;
      // initialize header
      write(fd, &plockHeader, sizeof(plockHeader));
      lseek(fd, lockFileSize, SEEK_SET);
      char zero = 0;
      write(fd, &zero, 1);
    }
  }
  if (fd < 0) {
    // second chance, in case another process created it while we were trying
    //fprintf(stderr, "*******************PLockFile: Using Second Chance: %s\n", fname);
    fd = open(fname, O_RDWR);
  }
  if (fd < 0) {
    mdbm_log(LOG_ERR, "PLockFile: Couldn't open or create: %s\n", fname);
    return -1;
  }

  struct stat lockStat;
  int tries=10; // 1 second total wait
  int ret;
  // give another process a chance to fill out the file...
  while (tries>0) {
    --tries;
    ret = fstat(fd, &lockStat);
    if (ret) {
      mdbm_log(LOG_ERR, "PLockFile: Couldn't stat: %s fd:%d errno %d, %s\n", fname, fd, errno, strerror(errno));
      Close();
      return -1;
    }
    if (openUnchecked && lockStat.st_size>0) {
      break;
    } else if (((size_t)lockStat.st_size) >= lockFileSize) {
      break;
    }
    usleep(100000); // .1 second
  }
  if (openUnchecked) {
    lockFileSize = (size_t)lockStat.st_size;
  } else {
    // since Expand() can actually contract the lock count, ignore larger file size
    if (lockStat.st_size < sHeader) {
      mdbm_log(LOG_ERR, "PLockFile: [%s] smaller than header %d vs %d bytes \n", fname, (int)lockStat.st_size, sHeader);
      Close();
      errno=ENOLCK;
      return -1;
    } else if (((size_t)lockStat.st_size) < lockFileSize) {
      tooSmall = true;
      expectedSize = lockFileSize;
      lockFileSize = ((size_t)lockStat.st_size);
      // NOTE: must delay reporting this error in case it's a symptom of mixed 32/64-bit mutexes
      //mdbm_log(LOG_ERR, "PLockFile: [%s] smaller than expected, %d vs %d bytes \n", fname, (int)lockStat.st_size, (int)lockFileSize);
      // TODO could loop here for retries...
    }
  }
  
  base = (uint8_t*)mmap(NULL, lockFileSize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  //fprintf(stderr, "@@@   Open() MMAP %d bytes (%d per) at %p  (end %p)\n", (int)lockFileSize, sMutex, base, base+lockFileSize-1);
  CHECKPOINTV("   Open() MMAP %d bytes (%d per) at %p  (end %p)", (int)lockFileSize, sMutex, base, base+lockFileSize-1);
  if (!base) {
    mdbm_log(LOG_ERR, "PLockFile: Couldn't map: %s errno %d, %s\n", fname, errno, strerror(errno));
    Close();
    errno = ENOMEM;
    return -1;
  }
  hdr = (PLockHdr*)base;

  if (!init) {
    if (hdr->version != PLOCK_HDR_VERSION) {
      mdbm_log(LOG_ERR, "PLockFile: %s header version mis-match %d vs %d\n", fname, hdr->version, PLOCK_HDR_VERSION);
      Close();
      errno = EBADF;
      return -1;
    }

    if (hdr->mutexSize != sMutex) {
      mdbm_log(LOG_ERR, "PLockFile: %s header mutex size mis-match %d vs %d "
          "(perhaps mixed 32-bit and 64-bit access to the same lock file)\n",
          fname, hdr->mutexSize, sMutex);
      Close();
      //errno=ENOLCK;
      // we need a special error code for things like Java to tell users 
      // that they are mixing 32 and 64-bit access.
      errno=EXDEV;
      return -1;
    }

    if (tooSmall) {
      mdbm_log(LOG_ERR, "PLockFile: [%s] is too small %d vs %d bytes \n", fname, (int)lockStat.st_size, (int)expectedSize);
      Close();
      errno=ENOLCK;
      return -1;
    }

    if (openUnchecked) {
      regCount = hdr->registerCount;
      lockCount = hdr->mutexCount;
    }
    if (hdr->registerCount != (unsigned)regCount) {
      mdbm_log(LOG_ERR, "PLockFile: %s header register count mis-match %d vs %d\n", fname, hdr->registerCount, regCount);
      Close();
      errno=ENOLCK;
      return -1;
    }
    if (hdr->mutexCount != (unsigned)lockCount) {
      mdbm_log(LOG_ERR, "PLockFile: %s header mutex count mis-match %d vs %d\n", fname, hdr->mutexCount, lockCount);
      Close();
      errno=ENOLCK;
      return -1;
    }
    // 
    // TODO limited loop with delay for hdr->mutexInitialized < mutexCount
    //
    if (hdr->mutexCount != hdr->mutexInitialized) {
      mdbm_log(LOG_ERR, "PLockFile: %s mutexes not all initialized... %d of %d\n", fname, hdr->mutexInitialized, hdr->mutexCount);
      Close();
      errno = ENODEV;
      return -1;
    }
  }

  numRegs = regCount;
  registers = NULL;
  if (numRegs) {
    registers = (int32_t*)(base+sHeader);
    if (init) {
      // initialize the registers to zero
      memset(registers, 0, sReg*numRegs);
    }
  }

  locks = new PMutex*[lockCount];
  offset = sHeader+sReg*numRegs;
  for (i=0; i<lockCount; ++i) {
    locks[i] = new PMutex((PMutexRecord*)(base+offset), init, i); 
    //fprintf(stderr, "************************ INITIALIZE (%d/%d) ************************\n", i, lockCount);
    if (!locks[i]->IsValid()) {
      CHECKPOINTV("******** INVALID (%d/%d) *********\n", i, lockCount);
      mdbm_log(LOG_ERR, "PLockFile: Initialize failed for lock (%d/%d) (%s)\n", i, lockCount, fname);
      Close();
      errno = ENODEV;
      return -1;
    }
    offset += sMutex;
    ++numLocks;
    if (init) {
      CHECKPOINTV("Open Initialized mutex %d / %d", i, (int)hdr->mutexInitialized);
      atomic_inc32u((uint32_t*)&hdr->mutexInitialized);
      //fprintf(stderr, "************************ INCREMENT (%u/%u) ************************\n", hdr->mutexInitialized, hdr->mutexCount);
    }
  }
  return 0;
}


int PLockFile::GetNumLocks() {
  if (base && hdr) {
    //return hdr->mutexCount;
    return numLocks;
  }
  return -1;
}
int PLockFile::GetNumRegisters() {
  if (base && hdr) {
    //return hdr->registerCount;
    return numRegs;
  }
  return -1;
}
int PLockFile::CheckHeader() {
  if (numLocks != (int)hdr->mutexCount) {
    return -1;
  } else if (numRegs != (int)hdr->registerCount) {
    return -1;
  }
  return 0;
}


//extern "C" { extern void dump_lock_file(const char* fname, int all); };

#ifdef DYNAMIC_LOCK_EXPANSION

int PLockFile::Expand(int newLockCount) {
  int sHeader = sizeof(PLockHdr);
  int sMutex = PMutex::GetRecordSize();
  int sReg = sizeof(int32_t);
  size_t newFileSize = lockFileSize + (newLockCount-numLocks)*sMutex;
  int ret = 0;
  int err = 0;
  int offset;
  int oldLockCount = numLocks;
  PMutex **newLocks = NULL;
  uint8_t *newBase = NULL;
  bool init = false;
  bool ownerDied = false;
  int i;

  owner_t tid = get_thread_id();
  //if (!base || (newLockCount<numLocks && newLockCount>=0)) {
  if (!base) {
    errno = EINVAL;
    return -1;
  }

  //if (newLockCount > (int)hdr->mutexCount) {
  if (newLockCount < 0) {
    // header-change forced-an-expand case
    newLockCount = hdr->mutexCount;
  } else {
    // owner request-to-expand (or contract) case
    init = true;
  }

  for (int i=0; i<numLocks; ++i) {
    if (locks[i]->GetLocalCount()) {
      mdbm_log(LOG_ERR, "PLockFile: Expand requires all locks be initially free [file:%s], index:%d owned \n", filename, i);
      errno = EWOULDBLOCK;
      return -1;
    }
  }

  if (init) {
    // first, acquire all current locks...
    for (int i=0; i<numLocks; ++i) {
      //fprintf(stderr, "PLockFile::Expand() acquiring lock %d/%d old:%d new:%d\n", i, numLocks, oldLockCount, newLockCount);
      int ret = locks[i]->Lock(true, tid);
      if (EOWNERDEAD == ret) {
        ownerDied = true;
      } else if (ret) {
        err = errno;
        mdbm_log(LOG_ERR, "PLockFile: Expand Couldn't re-map: %s errno %d, %s\n", filename, errno, strerror(errno));
        // failed to acquire a lock.. release and punt
        for (int j=0; j<i; ++j) {
          locks[j]->Unlock(tid);
        }
        errno = err;
        return ret;
      }
    }

    if (newLockCount > (int)hdr->mutexCount) {
      // grow file
      lseek(fd, newFileSize, SEEK_SET);
      char zero = 0;
      write(fd, &zero, 1);
      // bump header lock count
      hdr->mutexCount = newLockCount;
    } else {
      // truncate? (be careful with map and other junk...
    }
  }

  // get new mmap
  newBase = (uint8_t*)mmap(NULL, newFileSize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  //fprintf(stderr, "@@@   Expand() MMAP %d bytes at %p  (end %p)\n", (int)newFileSize, newBase, newBase+newFileSize-1);
  if (!newBase) {
    err = errno;
    mdbm_log(LOG_ERR, "PLockFile: Expand Couldn't re-map: %s errno %d, %s\n", filename, errno, strerror(errno));
    ret = -1;
    goto expand_exit;
  }

#ifdef TRACE_LOCKS
  mdbm_log(LOG_ERR, "PLockFile::Expand remapping base from %p to %p pid:%d self:" OWNER_FMT "\n", base, newBase, getpid(), get_thread_id());
#endif
  // expand/contract locks array
  newLocks = new PMutex*[newLockCount];
  for (i=0; i<newLockCount; ++i) {
    newLocks[i] = NULL;
  }
  offset = sHeader+sReg*numRegs;
  for (i=0; i<newLockCount; ++i) {
    // initialize and increment header initialized count
    bool doInit = init && (unsigned)i>=hdr->mutexInitialized;
    //CHECKPOINTV("Expand Initializing mutex %d / %d at %p doInit:%d", i, (int)hdr->mutexInitialized, (newBase+offset), doInit);
    newLocks[i] = new PMutex((PMutexRecord*)(newBase+offset), doInit, i); 
    if (!newLocks[i]->IsValid()) {
      ret = -1;
      err = ENODEV;
      goto expand_exit;
    }
    if (doInit) {
      //CHECKPOINTV("Expand Initialized mutex %d / %d at %p (%p)", i, (int)hdr->mutexInitialized, newLocks[i], ((PMutex*)newLocks[i])->rec);
      atomic_inc32u((uint32_t*)&hdr->mutexInitialized);
    }
    offset += sMutex;
  }
  hdr->mutexInitialized = newLockCount;
  hdr->mutexCount = newLockCount;

expand_exit:
  if (init) {
    //int toUnlock = (oldLockCount<=newLockCount) ? oldLockCount : newLockCount;
    int toUnlock = oldLockCount;
    for (int i=0; i<toUnlock; ++i) {
      //CHECKPOINTV("Expand Unlocking old mutex %d / %d new:%d old:%d at %p (%p)\n", i, toUnlock, newLockCount, oldLockCount, locks[i], ((PMutex*)locks[i])->rec);
      locks[i]->Unlock(tid);
    }
  }

  if (ret) {
    // error cleanup...
    if (newLocks) {
      for (i=0; i<newLockCount; ++i) {
        delete newLocks[i];
      }
      delete[] newLocks;
    }
    if (newBase) {
      munmap(newBase, newFileSize);
      //fprintf(stderr, "@@@   Expand() MUNMAP %d bytes at %p  (end %p)\n", (int)newFileSize, newBase, newBase+newFileSize-1);
    }
    errno = err;
  } else {
    // success, update structures...
    PMutex **oldLocks = locks;
    locks = newLocks;
    numLocks = newLockCount;
    // destroy old lock array
    for (i=0; i<oldLockCount; ++i) {
      CHECKPOINTV("Expand Deleting old mutex %d / %d at %p (%p)\n", i, oldLockCount, oldLocks[i], ((PMutex*)oldLocks[i])->rec);
      delete oldLocks[i];
    }
    delete[] oldLocks;
    // release the old mmap
    munmap(base, lockFileSize);
    //fprintf(stderr, "@@@   Expand() MUNMAP %d bytes at %p  (end %p)\n", (int)lockFileSize, base, base+lockFileSize-1);
    // correct base, hdr, registers, locks, numLocks for new mmap
    base = newBase;
    hdr = (PLockHdr*)base;
    registers = (int32_t*)(base+sHeader);
    lockFileSize = newFileSize;
  }

  return ret;
}
#endif // DYNAMIC_LOCK_EXPANSION

#ifdef ALLOW_MLOCK_RESET
int PLockFile::ResetAllLocks() {
  CHECKPOINTV("  ResetAllLocks(%s)", "");
  for (int i=0; i<numLocks; ++i) {
    bool ret = locks[i]->ResetLock();
    if (!ret) {
      return -1;
    }
  }
  return 0;
}
#endif

#define CHECK_REGISTER_RANGE(func, idx) \
  if (idx<0 || idx>numRegs) { \
    mdbm_log(LOG_ERR, "PLockFile: Invalid %s index: %d / %d\n", #func, idx, numRegs); \
    errno = ENOENT; \
    return -1;\
  }

int32_t PLockFile::RegisterGet(int index) {
  CHECK_REGISTER_RANGE(RegisterGet, index);
  return registers[index];
}
int PLockFile::RegisterSet(int index, int32_t newVal) {
  CHECK_REGISTER_RANGE(RegisterSet, index);
  registers[index] = newVal;
  return 0;
}
int PLockFile::RegisterInc(int index) {
  CHECK_REGISTER_RANGE(RegisterInc, index);
  atomic_inc32s(&registers[index]);
  return 0;
}
int PLockFile::RegisterDec(int index) {
  CHECK_REGISTER_RANGE(RegisterDec, index);
  atomic_dec32s(&registers[index]);
  return 0;
}
int PLockFile::RegisterAdd(int index, int32_t delta) {
  CHECK_REGISTER_RANGE(RegisterAdd, index);
  atomic_add32s(&registers[index], delta);
  return 0;
}
int PLockFile::RegisterCas(int index, int32_t oldVal, int32_t newVal) {
  CHECK_REGISTER_RANGE(RegisterCas, index);
  // atomic_swap32s(&registers[index], oldVal, newVal);
  bool success = __sync_bool_compare_and_swap(&registers[index], oldVal, newVal);
  return success ? 0 : 1;
}



//MLock::MLock() : lockType(MLOCK_SINGLE), base(0), parts(0), lastLocked(0) {
MLock::MLock() : lockType(MLOCK_SINGLE), base(0), parts(0) {
}
MLock::~MLock() {
  locks.Close();
  delete_tls_entry(this);
}
int MLock::Open(const char* fname, int baseCount, MLockType t, int partCount, int mode) {
  bool created = false;
  //CHECKPOINTV("  Open(%s, %d, %d(%s), %d)", fname, baseCount, (int)t, MLockTypeToStr(t), partCount);
  if (t<MLOCK_UNCHECKED || t>MLOCK_INDEX) {
    mdbm_log(LOG_ERR, "MLock: Invalid locktype: %d / %d\n", t, MLOCK_INDEX);
    errno = EINVAL;
    return -1;
  }
  int ret = -1;
  if (MLOCK_UNCHECKED==t) {
    ret = locks.Open(fname, -1, -1, created, mode);
  } else {
    ret = locks.Open(fname, 3, baseCount+partCount+1, created, mode);
  }
  if (ret) {
    return ret;
  }

  //fprintf(stderr, "Opened MLOCK wanted %s, has %s\n", MLockTypeToStr(t), MLockTypeToStr(locks.RegisterGet(MREG_LOCK_TYPE)));
  base = baseCount;
  if (MLOCK_UNCHECKED==t) {
    lockType = (MLockType)locks.RegisterGet(MREG_LOCK_TYPE);
    parts = locks.GetNumLocks() - (baseCount+1);
    //fprintf(stderr, "MLock: Unchecked Open type:%s/%d, parts %d [%s]\n", MLockTypeToStr(lockType), lockType, parts, fname);
  } else {
    // verify the lock type;
    if (!created && locks.RegisterGet(MREG_LOCK_TYPE) != (int32_t)t) {
      mdbm_log(LOG_ERR, "MLock: Open failed type:%s (%d), vs %s(%d) parts %d [%s]\n", MLockTypeToStr(lockType), lockType, MLockTypeToStr(locks.RegisterGet(MREG_LOCK_TYPE)), locks.RegisterGet(MREG_LOCK_TYPE), parts, fname);
      errno = ENOLCK;
      locks.Close();
      return -1;
    }
    lockType = t;
    parts = partCount;
    //fprintf(stderr, "MLock: Normal Open type:%s/%d, parts %d [%s] \n", MLockTypeToStr(lockType), lockType, parts, fname);
  }
  if (created) {
    int e = errno; // preserve errno
    // TODO
    // TODO add a second register for baseCount
    // TODO
    // if we created the lockfile, initialize the lock-type
    locks.RegisterSet(MREG_LOCK_TYPE, (int32_t)t);
    locks.RegisterSet(MREG_SHAR_INDEX, (int32_t)0);
    errno = e;
  }
  return ret;
}
#ifdef DYNAMIC_LOCK_EXPANSION
int MLock::Expand(int basic_locks, MLockType type, int lock_count) {
  int ret = -1;
  // note: the number of registers is not allowed to change
  if (type == MLOCK_UNCHECKED) {
    // some other process should have already expanded the lock
    // re-open in permissive mode and get type and lock_count from the file...
    ret = locks.Expand(-1);
    if (!ret) {
      lockType = (MLockType)locks.RegisterGet(MREG_LOCK_TYPE);
      parts = locks.GetNumLocks() - (base+1);
    mdbm_log(LOG_ERR, "MLock: Unchecked expand type:%s/%d, parts %d [%s]\n", MLockTypeToStr(lockType), lockType, parts, locks.filename);
    }
  } else { 
    ret = locks.Expand(basic_locks+lock_count+1);
    if (!ret) {
      lockType = type;
      locks.RegisterSet(MREG_LOCK_TYPE, (int32_t)type);
      locks.RegisterSet(MREG_SHAR_INDEX, (int32_t)0);
      parts = lock_count;
      //fprintf(stderr, "MLock: Normal expand type:%s/%d, parts %d [%s]\n", MLockTypeToStr(lockType), lockType, parts, locks.filename);
    }
  }
  return ret;
}
#endif // DYNAMIC_LOCK_EXPANSION
void MLock::Close() {
  CHECKPOINTV("  MLock::Close(%s)", "");
  locks.Close();
  reset_tls_entry(this);
}
#ifdef ALLOW_MLOCK_RESET
int MLock::ResetAllLocks() {
  CHECKPOINTV("  MLock::ResetAllLocks(%s)", "");
  int ret = locks.ResetAllLocks();
  if (!ret) { reset_tls_entry(this); }
  return ret;
}
#endif

MLockType MLock::GetLockType() {
  return (MLockType)locks.RegisterGet(MREG_LOCK_TYPE);
}
int MLock::GetNumLocks() {
  return locks.GetNumLocks() - base;
}


int MLock::UnlockBase(int index) {
  AUTO_TSC("MLock::UnlockBase()");
  CHECKPOINTV("  UnlockBase(%d)", index);
  if (index<0 || index>=base) {
    mdbm_log(LOG_ERR, "MLock: Invalid UnlockBase index: %d / %d\n", index, base);
    errno = ENOENT;
    return -1;
  }
  owner_t tid = get_thread_id();
  return locks.locks[index]->Unlock(tid);
}
int MLock::LockBase(int index, bool blocking) {
  AUTO_TSC("MLock::LockBase()");
  CHECKPOINTV("  LockBase(%d, %s)", index, blocking?"BLOCK":"ASYNC");
  // check header for changes
  if (locks.CheckHeader()) {
    errno=ESTALE;
    return -1;
  }
  if (index<0 || index>=base) {
    mdbm_log(LOG_ERR, "MLock: Invalid LockBase index: %d / %d\n", index, base);
    errno = ENOENT;
    return -1;
  }
  owner_t tid = get_thread_id();
  int ret = locks.locks[index]->Lock(blocking, tid);
  CHECKPOINTV("  LockBase(%d, %s) DONE ", index, blocking?"BLOCK":"ASYNC");
  return ret;
}
#ifdef ALLOW_MLOCK_RESET
int MLock::ResetBaseLock(int index) {
  CHECKPOINTV("  ResetBaseLock(%d)", index);
  if (index<0 || index>=base) {
    mdbm_log(LOG_ERR, "MLock: Invalid ResetBaseLock index: %d / %d\n", index, base);
    errno = ENOENT;
    return -1;
  }
  bool ret = locks.locks[index]->ResetLock();
  CHECKPOINTV("  ResetBaseLock(%d) DONE ", index);
  return ret==true ? 0 : -1;
}
#endif
int MLock::Unlock(int index) {
  AUTO_TSC("MLock::Unlock()");
  CHECKPOINTV("  Unlock(%d) pid:%d", index, getpid());
  int ret=-1;
  PMutex **lockArray = locks.locks;
  //fprintf(stderr, "  Unlock(%d) pid:%d\n", index, getpid());
  // allow (index == -2) for "any held" index/partition
  MLockTlsEntry *mte = get_tls_entry(this);
  owner_t tid = mte->tid;
  int lastLocked = mte->lastLocked;
  if (index==MLOCK_ANY) {
    if (MLOCK_SINGLE  == lockType) {
      // single is always "EXCLUSIVE"
      ret =  lockArray[base]->Unlock(tid);
      if (unlikely(ret)) { DumpLockState(stderr); }
      else { 
//fprintf(stderr, "@@@ UNLOCK (ANY,SINGLE) DECREMENTING EXCL LOCK COUNTS\n");
        --mte->exclCount; 
      }
      return ret;
    } else { // ((MLOCK_INDEX  == lockType) || (MLOCK_SHARED == lockType))
      if (likely(tid == lockArray[base+1+lastLocked]->GetOwnerId())) {
//fprintf(stderr, "@@@ UNLOCK (ANY,MULTI-LAST-LOCKED) DECREMENTING EXCL LOCK COUNTS\n");
        AUTO_TSC("MLock::Unlock()-stored-index");
        ret =  lockArray[base+1+lastLocked]->Unlock(tid);
        if (unlikely(ret)) { DumpLockState(stderr); }
        else { --mte->partCount; } // TODO can this be an exclusive lock?
        return ret;
      } else {
//fprintf(stderr, "@@@ UNLOCK (ANY,MULTI-SEARCH) DECREMENTING EXCL LOCK COUNTS\n");
        AUTO_TSC("MLock::Unlock()-find-lock");
        for (int i=-1; i<parts; ++i) { 
           if (tid == lockArray[base+1+i]->GetOwnerId()) {
             //index = i;
             ret =  lockArray[base+1+i]->Unlock(tid);
             if (unlikely(ret)) { DumpLockState(stderr); }
             else { --mte->partCount; } // TODO can this be an exclusive lock?
             return ret;
             //break;
           }
        }
      }
      mdbm_log(LOG_ERR, "MLock: Unlock(MLOCK_ANY) not-found index: %d / %d self:" OWNER_FMT " pid:%d\n", index, parts, get_thread_id(), getpid());
      print_trace();
      DumpLockState(stderr);
      errno = EPERM; // no parts owned by this process
      return -1;
    }
  } else if (index==MLOCK_EXCLUSIVE) {
    if (MLOCK_SINGLE  == lockType) {
      AUTO_TSC("MLock::Unlock()-exclusive-single");
      ret =  lockArray[base]->Unlock(tid);
      if (unlikely(ret)) { DumpLockState(stderr); }
      else { 
//fprintf(stderr, "@@@ UNLOCK (EXCL,SINGLE) DECREMENTING EXCL LOCK COUNTS\n");
        --mte->exclCount; 
      }
    } else {
      // unlock all once, core lock last
      AUTO_TSC("MLock::Unlock()-exclusive");
      for (int i=0; i<parts; ++i) {
        ret =  lockArray[base+1+i]->Unlock(tid);
        if (unlikely(ret)) { 
          DumpLockState(stderr);
          return ret;
        }
      }
      ret =  lockArray[base]->Unlock(tid);
      if (unlikely(ret)) { DumpLockState(stderr); }
      else { 
//fprintf(stderr, "@@@ UNLOCK (EXCL,MULTI) DECREMENTING EXCL LOCK COUNTS\n");
        --mte->exclCount; 
      }
    }
  } else {
    if (index<-1 || index>=parts) {
      mdbm_log(LOG_ERR, "MLock: Invalid Unlock index: %d / %d\n", index, parts);
      errno = ENOENT;
      return -1;
    }
    ret =  lockArray[base+1+index]->Unlock(tid);
    if (unlikely(ret)) { DumpLockState(stderr); }
    else { --mte->partCount; } // TODO can this be an exclusive lock?
  }
  return ret;
}

int MLock::LockSingle(bool blocking, MLockTlsEntry* mte) {
  AUTO_TSC("MLock::Lock()-single");
  PMutex **lockArray = locks.locks;
  owner_t tid = mte->tid;

  int ret = lockArray[base]->Lock(blocking, tid);
  if (likely(!ret || ret==EOWNERDEAD)) {
    //++mte->partCount;
//fprintf(stderr, "@@@ LOCK (SINGLE) INCREMENTING EXCL LOCK COUNTS\n");
    ++mte->exclCount;
  }
  return ret;
}

int MLock::Lock(int index, bool blocking) {
  AUTO_TSC("MLock::Lock()");
  CHECKPOINTV("  Lock(%d, %s) pid:%d", index, blocking?"BLOCK":"ASYNC", getpid());
  PMutex **lockArray = locks.locks;
  MLockTlsEntry *mte = get_tls_entry(this);
  //owner_t tid = get_thread_id();
  owner_t tid = mte->tid;
  //fprintf(stderr, "  Lock(%d, %s) pid:%d\n", index, blocking?"BLOCK":"ASYNC", getpid());
  // check header for changes
  if (unlikely(locks.CheckHeader())) {
    mdbm_log(LOG_ERR, "MLock: Lock.. Header changed! : %d / %d\n", index, parts);
    errno=ESTALE;
    return -1;
  }
  if (MLOCK_SINGLE  == lockType) {
    // TODO assert sane 'index' (MLOCK_ANY, MLOCK_EXCLUSIVE, 0)
    return LockSingle(blocking, mte);
  }
  // (index == -2) for "any available" index/partition
  if (index==MLOCK_ANY) {
    //if (likely((MLOCK_INDEX  == lockType) || (MLOCK_SHARED == lockType))) {
      AUTO_TSC("MLock::Lock()-shared-try");
      bool basedied = false;
      // TODO should this be allowed for MLOCK_INDEX?
      if (unlikely(lockArray[base]->GetOwnerId())) {
        AUTO_TSC("MLock::Lock()-writer-fairness");
        // avoid writer starvation: try exclusive lock (temporarily) if it seems to be held
        int ret = lockArray[base]->Lock(blocking, tid);
        if (ret == EOWNERDEAD) {
          mdbm_log(LOG_ERR, "EOWNERDEAD locking base MLock index:%d,ret:%d\n", index, ret);
          basedied = true;
        } else if (ret) {
          CHECKPOINTV("  Lock(%d, %s) Shared lock Failed on core lock (%d) ", index, blocking?"BLOCK":"ASYNC", ret);
          // may return EBUSY/EWOULDBLOCK... that's ok
          return ret;
        }
        lockArray[base]->Unlock(tid);
      }
      // choose a deterministic pid-based offset first for speed
      index = tid % parts;  // based on the TID/PID
      int idx = index;
      for (int i=0; i<parts; ++i) { 
        AUTO_TSC("MLock::Lock()-try-single");
        if (++idx >= parts) { idx = 0; }
        int ret = lockArray[base+1+idx]->Lock(false, tid);
        if (likely(!ret)) {
          mte->lastLocked = idx;
          ++mte->partCount;
          //fprintf(stderr, "MLock: SHARED ANY got idx: %d / %d\n", idx, parts);
          if (unlikely(basedied)) {
            errno = EINPROGRESS;
            return EOWNERDEAD;
          }
          return 0;
        } else if (unlikely(ret == EOWNERDEAD)) {
          mte->lastLocked = idx;
          ++mte->partCount;
          // TODO return 0 for MLOCK_SHARED (i.e. died reader doesn't need validation)
          errno = EINPROGRESS;
          return EOWNERDEAD;
        }
      }
      // fall thru to allow blocking wait
      //fprintf(stderr, "MLock: SHARED ANY Falling thru idx: %d / %d\n", index, parts);
    //}
  }
  if (unlikely(index<-1 || index>=parts)) {
    mdbm_log(LOG_ERR, "MLock: Invalid Lock index: %d / %d\n", index, parts);
    errno = ENOENT;
    return -1;
  }
  if (index>MLOCK_EXCLUSIVE) {
    //if (  (MLOCK_INDEX  == lockType) 
    //    ||(MLOCK_SHARED == lockType)) {
      // TODO should shared automatically iterate over locks?
      // acquire base first...
      int bret = 0;
      AUTO_TSC("MLock::Lock()-single-specific");
      int ret = lockArray[base+1+index]->Lock(blocking, tid);
      // release base
//      locks.Unlock(base);
      if (likely(!ret)) {
        mte->lastLocked = index;
        ++mte->partCount;
      } else if (bret == EOWNERDEAD || ret == EOWNERDEAD) { 
        mdbm_log(LOG_ERR, "EOWNERDEAD locking specific MLock index:%d, bret:%d ret:%d\n", index, bret, ret);
        mte->lastLocked = index;
        ++mte->partCount;
        errno = EINPROGRESS;
        return EOWNERDEAD;
      }
      CHECKPOINTV("  Lock(%d, %s) DONE (%d) ", index, blocking?"BLOCK":"ASYNC", ret);
      //fprintf(stderr, "  Lock(%d, %s) DONE (%d) \n", index, blocking?"BLOCK":"ASYNC", ret);
      return ret;
    //}
  } else {
    AUTO_TSC("MLock::Lock()-exclusive");
    //if ((MLOCK_INDEX == lockType)
    //       || (MLOCK_SHARED == lockType)) {
      //fprintf(stderr, "EXCLUSIVE LOCK TRY\n");
      // Acquire base, wait for all, note OWNERDIED ...
      // get base, forces all requestors to serialize and
      //   avoids deadlock for multiple exclusive requestors
      bool basedied = false;
      bool partdied = false;
      // TODO, if we own any other locks and fail on base, then we have to 
      //   return a special code to let caller know we'll deadlock
      int ret = lockArray[base]->Lock(blocking, tid);
      if (unlikely(ret == EOWNERDEAD)) {
        mdbm_log(LOG_ERR, "EOWNERDEAD locking base MLock index:%d,ret:%d\n", index, ret);
        basedied = true;
      } else if (unlikely(ret)) {
        CHECKPOINTV("  Lock(%d, %s) Failed on core lock (%d) ", index, blocking?"BLOCK":"ASYNC", ret);
        return ret;
      }
      //fprintf(stderr, "Acquired Lock: Base\n");
      // EXCLUSIVE lock, lock them all once
      for (int i=0; i<parts; ++i) {
        ret = lockArray[base+1+i]->Lock(blocking, tid);
        //fprintf(stderr, "Acquired Lock: %d\n", i);
        if (unlikely(ret == EOWNERDEAD)) {
          mdbm_log(LOG_ERR, "EOWNERDEAD locking part MLock index:%d,ret:%d\n", i, ret);
          partdied = true;
        } else if (unlikely(ret)) {
          // another owner, release base lock and punt
          if (basedied || (MLOCK_INDEX==lockType && partdied)) {
            // TODO deal with death notifications...
            // TODO add a checked variable here and to Upgrade
          }
          for (int j=0; j<i; ++j) {
            lockArray[base+1+j]->Unlock(tid);
          }
          lockArray[base]->Unlock(tid);
          //fprintf(stderr, "EXCLUSIVE LOCK FAIL\n");
          errno = EAGAIN;
          return -1;
        }
        // ysys seems to handle upgrade() wrt pre-owned locks by keeping them held
//          locks.Unlock(base+1+i);
      }
      if (unlikely(basedied)) {
        // write or exclusive owner died
        ret = -1;
        errno = EINPROGRESS;
      } else if (unlikely(MLOCK_INDEX==lockType && partdied)) {
        // paritioned-lock died
        ret = -1;
        errno = EINPROGRESS;
      }
      //fprintf(stderr, "EXCLUSIVE LOCK END (ret:%d)\n", ret);
      CHECKPOINTV("  Lock(%d, %s) DONE (%d) ", index, blocking?"BLOCK":"ASYNC", ret);
//fprintf(stderr, "@@@ LOCK (MULTI, EXCL) INCREMENTING EXCL LOCK COUNTS\n");
      ++mte->exclCount;
      return ret;
    //}
  }
  mdbm_log(LOG_ERR, "MLock: Shouldn't get here... LockType:%d Lock index: %d / %d\n", lockType, index, parts);
  return -1;
}
int MLock::Upgrade(bool blocking) {
  CHECKPOINTV("  Upgrade(%s)", blocking?"BLOCK":"ASYNC");
  if (MLOCK_SINGLE == lockType) {
    mdbm_log(LOG_ERR, "ERROR: Upgrade() attempt on MLOCK_SINGLE\n");
    // Error
    return -1;
  } else if ((MLOCK_INDEX == lockType)
         || (MLOCK_SHARED == lockType)) {
    int ret = Lock(MLOCK_EXCLUSIVE);
    CHECKPOINTV("  Upgrade(%s) RETURNING:%d errno:%d (%s)", blocking?"BLOCK":"ASYNC", ret, errno, ret?strerror(errno):"");
    return ret;
  }
  mdbm_log(LOG_ERR, "ERROR: Upgrade() attempt on MLOCK_<UNKNOWN>\n");
  return -1;
}
int MLock::Downgrade(int index, bool blocking) {
  CHECKPOINTV("  Downgrade(%d,%s)", index, blocking?"BLOCK":"ASYNC");
  if (index<-1 || index>=parts) {
    mdbm_log(LOG_ERR, "MLock: Invalid Downgrade index: %d / %d\n", index, parts);
    errno = ENOENT;
    return -1;
  }
  owner_t tid = get_thread_id();

  if (MLOCK_SINGLE == lockType) {
    // Error
    return -1;
  } 
  if (index<0) {
    if (MLOCK_INDEX == lockType) {
      return -1;
    }
   // choose random index... any value would do
   // could optimize by doing getpid()%parts here and for shared lock
   index = parts-1;
  }

  // verify ownership 
  if (locks.locks[base]->GetLocalCount(tid)<=0) {
    errno = EPERM;
    return -1;
  }
  int ret = locks.locks[index+base+1]->Lock(blocking, tid);
  if (ret) {
    // should never happen, should already have exclusive lock
    return ret;
  }
  // TODO FIXME... this looks horribly wrong, should it be this->Unlock(MLOCK_EXCLUSIVE,tid); ?
  //return locks.locks[base]->Unlock(tid);
  //return Unlock(MLOCK_EXCLUSIVE, tid);
  return Unlock(MLOCK_EXCLUSIVE);
}

#ifdef ALLOW_MLOCK_RESET
int MLock::ResetLock(int index) {
  CHECKPOINTV("  ResetLock(%d)", index);
  if (index==MLOCK_ANY) {
    mdbm_log(LOG_ERR, "MLock::ResetLock(MLOCK_ANY): invalid index\n");
    errno = EINVAL;
    return -1;
  }
  if (index<-1 || index>=parts 
      || (MLOCK_SINGLE  == lockType && index<0)){
    mdbm_log(LOG_ERR, "MLock: Invalid ResetLock index: %d / %d\n", index, parts);
    errno = ENOENT;
    return -1;
  }
  bool ret = locks.locks[base+1+index]->ResetLock();
  CHECKPOINTV("  ResetLock(%d) DONE ", index);
  return ret==true ? 0 : -1;
}
#endif

owner_t MLock::GetBaseOwnerId(int index) {
  if (index<0 || index>=base) {
    mdbm_log(LOG_ERR, "MLock: Invalid GetBaseOwnerId index: %d / %d\n", index, base);
    errno = ENOENT;
    return -1;
  }
  return locks.locks[index]->GetOwnerId();
}
int MLock::GetBaseLockCount(int index) {
  if (index<0 || index>=base) {
    mdbm_log(LOG_ERR, "MLock: Invalid GetBaseLockCount index: %d / %d\n", index, base);
    errno = ENOENT;
    return -1;
  }
  return locks.locks[index]->GetLockCount();
}
int MLock::GetBaseLocalCount(int index) {
  if (index<0 || index>=base) {
    mdbm_log(LOG_ERR, "MLock: Invalid GetBaseLocalCount index: %d / %d\n", index, base);
    errno = ENOENT;
    return -1;
  }
  return locks.locks[index]->GetLocalCount();
}
owner_t MLock::GetOwnerId(int index) {
  if (index<-1 || index>=parts) {
    mdbm_log(LOG_ERR, "MLock: Invalid GetOwnerId index: %d / %d\n", index, parts);
    errno = ENOENT;
    return -1;
  }
  return locks.locks[base+1+index]->GetOwnerId();
}
int MLock::GetLockCount(int index) {
  if (index<-1 || index>=parts) {
    mdbm_log(LOG_ERR, "MLock: Invalid GetLockCount index: %d / %d\n", index, parts);
    errno = ENOENT;
    return -1;
  }
  int ret = locks.locks[base+1+index]->GetLockCount();
  return ret;
}
int MLock::GetLocalCount(int index) {
  if (index<-1 || index>=parts) {
    mdbm_log(LOG_ERR, "MLock: Invalid GetLocalCount index: %d / %d\n", index, parts);
    errno = ENOENT;
    return -1;
  }
  return locks.locks[base+1+index]->GetLocalCount();
}

int MLock::GetLockCountTotal() {
  int total = 0;
  if (MLOCK_SINGLE==lockType) {
      total += locks.locks[base]->GetLockCount();
  } else {
    for (int i=0; i<=parts; ++i) {
      total += locks.locks[base+i]->GetLockCount();
    }
  }
  return total;
}

int MLock::GetLocalCountTotal() {
  MLockTlsEntry *mte = get_tls_entry(this);
  //owner_t tid = get_thread_id();
  owner_t tid = mte->tid;
  int total = 0;
  if (MLOCK_SINGLE==lockType) {
      total += locks.locks[base]->GetLocalCount(tid);
    //fprintf(stderr, "GetLocalCountTotal() single:%d \n", mte->exclCount, total);
  } else {
    //fprintf(stderr, "GetLocalCountTotal() excl:%d part:%d\n", mte->exclCount, mte->partCount);
    return mte->exclCount + mte->partCount;
    //for (int i=0; i<=parts; ++i) {
    //  total += locks.GetLocalCount(base+i, tid);
    //}
  }
  //fprintf(stderr, "******************************\n");
  //fprintf(stderr, "GetLocalCountTotal() => %d\n", total);
  //DumpLockState(stderr);
  //fprintf(stderr, "******************************\n");
  return total;
}
int MLock::GetPartCount() {
  return parts;
}

int MLock::GetLockedPartCount() {
  int total = 0;
  // NOTE: does not include exclusive lock member, only shared/partitions
  if (MLOCK_SINGLE!=lockType) {
    for (int i=1; i<=parts; ++i) {
      if (locks.locks[base+i]->GetLockCount()) {
        ++total;
      }
    }
  }
  return total;
}

int MLock::GetLocalPartCount() {
  owner_t tid = get_thread_id();
  int total = 0;
  // NOTE: does not include exclusive lock member, only shared/partitions
  if (MLOCK_SINGLE!=lockType) {
    for (int i=1; i<=parts; ++i) {
      if (locks.locks[base+i]->GetLocalCount(tid)) {
        ++total;
      }
    }
  }
  return total;
}

void MLock::DumpLockState(FILE* file) {
  int local, lock;
  owner_t owner;

  if (!file) {
    file = stderr;
  }
  fprintf(file, "Begin Lock state (for non-zero locks) (pid:%d, tid:%d self:%llx uuid:" OWNER_FMT " ):\n", getpid(), gettid(), (long long unsigned)pthread_self(), get_thread_id());
  fprintf(file, "  BaseLocks:%d Core:%d Shared/Partitioned:%d mode:%s (%d)\n", base, 1, parts, MLockTypeToStr(GetLockType()), GetLockType());
  for (int i=0; i<base; ++i) {
    local = locks.locks[i]->GetLocalCount();
    lock = locks.locks[i]->GetLockCount();
    owner = locks.locks[i]->GetOwnerId();
    if (local || lock || owner) {
      fprintf(file, "  base[%d] local:%d any:%d, owner:" OWNER_FMT " \n", i, local, lock, owner);
    }
  }
  {
    local = locks.locks[base]->GetLocalCount();
    lock = locks.locks[base]->GetLockCount();
    owner = locks.locks[base]->GetOwnerId();
    if (local || lock || owner) {
      fprintf(file, "  core[] local:%d any:%d, owner:" OWNER_FMT " \n", local, lock, owner);
    }
  }
  int j;
  for (int i=1; i<parts; ++i) {
    local = locks.locks[base+1+i]->GetLocalCount();
    lock = locks.locks[base+1+i]->GetLockCount();
    owner = locks.locks[base+1+i]->GetOwnerId();
    if (local || lock || owner) {
      for (j=i+1; j<parts; ++j) {
        if (local != locks.locks[base+1+j]->GetLocalCount()) { break; }
        if (lock != locks.locks[base+1+j]->GetLockCount()) { break; }
        if (owner != locks.locks[base+1+j]->GetOwnerId()) { break; }
      }
      --j;
      if (i!=j) {
        fprintf(file, "  part[%d-%d] local:%d any:%d, owner:" OWNER_FMT " \n", i, j, local, lock, owner);
        i=j+1;
      } else {
        fprintf(file, "  part[%d] local:%d any:%d, owner:" OWNER_FMT " \n", i, local, lock, owner);
      }
    }
  }
  fprintf(file, "End Lock state\n");
}




// struct mlock_struct {
//   MLock inner;
// };
// 
// mlock_t* mlock_open(const char* filename, int basic_locks, MLockType type, int lock_count) {
// 
//   mlock_t *m = new mlock_t;
//   int ret = m->inner.Open(filename, basic_locks, type, lock_count);
//   if (0==ret) {
//     //fprintf(stderr, "mlock_open SUCCESS %s %p : %d\n", filename, &m->inner, m->inner.parts);
//     return m;
//   }
//   //fprintf(stderr, "mlock_open FAIL %s %p : %d \n", filename, &m->inner, m->inner.parts);
//   delete m;
//   return NULL;
// }
// mlock_t* mlock_open_mod(const char* filename, int basic_locks, MLockType type, int lock_count, int mode) {
// 
//   mlock_t *m = new mlock_t;
//   int ret = m->inner.Open(filename, basic_locks, type, lock_count, mode);
//   if (0==ret) {
//     //fprintf(stderr, "mlock_open SUCCESS %s %p : %d\n", filename, &m->inner, m->inner.parts);
//     return m;
//   }
//   //fprintf(stderr, "mlock_open FAIL %s %p : %d \n", filename, &m->inner, m->inner.parts);
//   delete m;
//   return NULL;
// }
// 
// #ifdef DYNAMIC_LOCK_EXPANSION
// int mlock_expand(mlock_t* m, int basic_locks, MLockType type, int lock_count) {
//   if (!m) {
//     errno = ENOENT;
//     return -1;
//   }
//   return m->inner.Expand(basic_locks, type, lock_count);
// }
// #endif //DYNAMIC_LOCK_EXPANSION
// 
// int mlock_close(mlock_t* m) {
//   if (!m) {
//     errno = ENOENT;
//     return -1;
//   }
//   m->inner.Close();
//   delete m;
//   return 0;
// }
// MLockType mlock_get_lock_type(mlock_t* m) {
//   return m->inner.GetLockType();
// }
// int mlock_get_num_locks(mlock_t* m) {
//   return m->inner.GetNumLocks();
// }
// 
// void mlock_dump_state(mlock_t* m, FILE* file) {
//   m->inner.DumpLockState();
// }
// #ifdef ALLOW_MLOCK_RESET
// int mlock_reset_all_locks(mlock_t* m) {
//   return m->inner.ResetAllLocks();
// }
// #endif
// 
// 
// int mlock_base_unlock(mlock_t* m, int index) {
//   return m->inner.UnlockBase(index);
// }
// int mlock_base_lock(mlock_t* m, int index, int blocking) {
//   return m->inner.LockBase(index, blocking);
// }
// #ifdef ALLOW_MLOCK_RESET
// int mlock_base_reset_lock(mlock_t* m, int index) {
//   return m->inner.ResetBaseLock(index);
// }
// #endif
// owner_t mlock_base_get_owner_id(mlock_t* m, int index) {
//   return m->inner.GetBaseOwnerId(index);
// }
// int mlock_base_get_lock_count(mlock_t* m, int index) {
//   return m->inner.GetBaseLockCount(index);
// }
// int mlock_base_get_local_count(mlock_t* m, int index) {
//   return m->inner.GetBaseLocalCount(index);
// }
// 
// int mlock_unlock(mlock_t* m, int index) {
//   return m->inner.Unlock(index);
// }
// int mlock_lock(mlock_t* m, int index, int blocking) {
//   return m->inner.Lock(index, blocking);
// }
// int mlock_upgrade(mlock_t* m, int blocking) {
//   return m->inner.Upgrade(blocking);
// }
// int mlock_downgrade(mlock_t* m, int index, int blocking) {
//   return m->inner.Downgrade(index, blocking);
// }
// #ifdef ALLOW_MLOCK_RESET
// int mlock_reset_lock(mlock_t* m, int index) {
//   return m->inner.ResetLock(index);
// }
// #endif
// owner_t mlock_get_owner_id(mlock_t* m, int index) {
//   return m->inner.GetOwnerId(index);
// }
// int mlock_get_lock_count(mlock_t* m, int index) {
//   return m->inner.GetLockCount(index);
// }
// int mlock_get_local_count(mlock_t* m, int index) {
//   AUTO_TSC("mlock_get_local_count");
//   return m->inner.GetLocalCount(index);
// }
// int mlock_get_lock_count_total(mlock_t* m) {
//   return m->inner.GetLockCountTotal();
// }
// int mlock_get_local_count_total(mlock_t* m) {
//   AUTO_TSC("mlock_get_local_count_total");
//   return m->inner.GetLocalCountTotal();
// }
// int mlock_get_part_count(mlock_t* m) {
//   AUTO_TSC("mlock_get_part_count");
// //fprintf(stderr, "mlock_get_part_count:%d / %d base:%d typ:%d %p\n", m->inner.GetPartCount(), m->inner.locks.GetNumLocks(), m->inner.base, m->inner.lockType, &m->inner);
//   return m->inner.GetPartCount();
// }
// int mlock_get_locked_part_count(mlock_t* m) {
//   return m->inner.GetLockedPartCount();
// }
// int mlock_get_local_part_count(mlock_t* m) {
//   return m->inner.GetLocalPartCount();
// }


//#include "mdbm.h"


int mkdir_no_umask(char* path, mode_t mode);
int ensure_lock_path(char* path, int skiplast);


class PthrLock : public MdbmLockBase {
public:
  PthrLock();
  ~PthrLock();
  int open(const char* name, int flags, MLockType type, int count, int do_lock, int* need_check);
  void close();
  int getFilename(const char* dbname, char* lockname, int maxlen);
  void printState();
  MLockType getMode();
  int lock(int type, int async, int part, int &need_check, int &upgrade);
  int unlock(int type=MLOCK_EXCLUSIVE);
  int getHeldCount(int type, bool self);
  int getHeldCount(int type, bool self, int index);
  int getCount(int type);
  int reset(const char* dbname, int flags);
#ifdef DYNAMIC_LOCK_EXPANSION
  int expand(int type, int count);
#endif
private:
  MLock locks;
};


MdbmLockBase* pthread_lock_creator(void) {
  //fprintf(stderr, "***PTHREAD***\n");
  return new PthrLock();
}
// not pre-main threadsafe... moved to mdbm_lock_and_load()
//REGISTER_MDBM_LOCK_PLUGIN(plock, &pthread_lock_creator)


PthrLock::PthrLock() {
}
PthrLock::~PthrLock() {
}
void PthrLock::close() {
  // TODO NOTE: this should implicitly release any locks we're holding
  //   both 'regular' and 'internal'
  locks.Close();
}
/* Populates 'lockname' with the name of the lockfile for db 'dbname'.
 * 'dbname' is expected to be an absolute path.
 * On success, buffer contains the lock file name associated with dbname
 * and returns the lock filename size in bytes.
 * If maxlen is too small, lockname is unchanged and the (negative) required 
 * length is returned.
 * Returns 0 if dbname is invalid.
 */
int PthrLock::getFilename(const char* dbname, char* lockname, int maxlen) {
  const char* prefix = "/tmp/.mlock-named";
  const char* suffix = "._int_";
  int plen, slen, dblen, llen; 
  if (!dbname || !dbname[0] || dbname[0]!='/') {
    mdbm_logerror(LOG_ERR, 0, "%s: invalid db name in get_lockfile_name()", dbname);
    return 0;
  }
  plen = strlen(prefix);
  slen = strlen(suffix);
  dblen = strlen(dbname);
  llen = plen + dblen + slen + 1; /* prefix, name, suffix, trailing NULL */
  if (maxlen < llen) {
    return -llen;
  }
  strncpy(lockname, prefix, plen);
  strncpy(lockname+plen, dbname, dblen);
  strncpy(lockname+plen+dblen, suffix, slen);
  lockname[llen-1] = 0; /* trailing null */
  return llen;
}
void PthrLock::printState() {
  locks.DumpLockState();
}
MLockType PthrLock::getMode() {
  return locks.GetLockType();
}
int PthrLock::lock(int type, int async, int part, int &need_check, int &upgrade) {
  int ret = -1;
  if (type == MLOCK_INTERNAL) {
    /*NOTE("Lock(%s) INTERNAL", (async==MLOCK_ASYNC)?"ASYNC":"BLOCK"); */
    //fprintf(stderr, "++++ PthrLock::lock Internal\n");
    ret = locks.LockBase(0, async);
  } else if (type == MLOCK_EXCLUSIVE) {
    //fprintf(stderr, "++++ PthrLock::lock Exclusive\n");
    ret =  locks.Lock(type, async);
  } else {
    MLockType hasType = locks.GetLockType();
    if (type == MLOCK_UPGRADE) {
    //fprintf(stderr, "++++ PthrLock::lock Upgrade->Exclusive\n");
      ret =  locks.Lock(MLOCK_EXCLUSIVE, async);
    } else if (type != hasType) {
      //fprintf(stderr, "++++ PthrLock::lock type mismatch( %d vs %d)\n", type, hasType);
      mdbm_logerror(LOG_ERR, 0, "%s PthrLock::lock type mismatch( %d vs %d)\n", locks.locks.filename, type, hasType);
      print_trace();
      //upgrade = 1;
      //ret = locks.Lock(MLOCK_EXCLUSIVE, async);
      errno=EINVAL;
      return -1;
    } else if (type == MLOCK_INDEX) {
    //fprintf(stderr, "++++ PthrLock::lock Part(%d)\n", part);
      ret = locks.Lock(part, async); 
    } else if (type == MLOCK_SHARED) {
    //fprintf(stderr, "++++ PthrLock::lock Shared\n");
      ret = locks.Lock(MLOCK_ANY, async); 
    }
  }
  if (ret == EOWNERDEAD) {
    // MLock doesn't auto-upgrade...
    need_check = true;
  }
  return ret;
}
int PthrLock::unlock(int type) {
  int ret = -1;
  MLockType hasType = locks.GetLockType();
  if (type == MLOCK_INTERNAL) {
    //fprintf(stderr, "---- PthrLock::unlock Internal\n");
    /*NOTE("Unlock(%s) INTERNAL", (async==MLOCK_ASYNC)?"ASYNC":"BLOCK"); */
    ret =locks.UnlockBase(0);
  } else if (type == MLOCK_EXCLUSIVE) {
    //fprintf(stderr, "---- PthrLock::unlock Exclusive\n");
    ret =  locks.Unlock(type);
  } else {
    //MLockType hasType = locks.GetLockType();
    if (type != hasType) {
    //fprintf(stderr, "---- PthrLock::unlock type mismatch %d vs %d \n", type, hasType);
      errno=EINVAL;
      return -1;
    } else if (type == MLOCK_INDEX) {
      // TODO FIXME, is this right?
      //ret = locks.Unlock(type); 
    //fprintf(stderr, "---- PthrLock::unlock Part(ANY)\n");
      ret = locks.Unlock(MLOCK_ANY); 
    } else if (type == MLOCK_SHARED) {
    //fprintf(stderr, "---- PthrLock::unlock Share(ANY)\n");
      ret = locks.Unlock(MLOCK_ANY); 
    } else { // shouldn't happen
    //fprintf(stderr, "---- PthrLock::unlock BORKED %d vs %d \n", type, hasType);
    }
  }
  //if (ret) {
  //  fprintf(stderr, "UNLOCK---error: type:%d, hasType:%d\n", type, hasType);
  //}
  return ret;
}
int PthrLock::getHeldCount(int type, bool self) {
  MLockType hasType = locks.GetLockType();
  if (self) { // held by this process
    switch (type) {
      case MLOCK_INTERNAL  : return locks.GetBaseLocalCount(0);
      case MLOCK_EXCLUSIVE : return locks.GetLocalCount(-1);
      case MLOCK_SHARED    : 
          return (type==hasType) ? locks.GetLocalCountTotal()-locks.GetLocalCount(-1) : 0;
      case MLOCK_INDEX     : 
          return (type==hasType) ? locks.GetLocalCountTotal()-locks.GetLocalCount(-1) : 0;
      default: break;
    };
  } else { // held by anyone
    switch (type) {
      case MLOCK_INTERNAL  : return locks.GetBaseLockCount(0);
      case MLOCK_EXCLUSIVE : return locks.GetLockCount(-1);
      case MLOCK_SHARED    : 
          return (type==hasType) ? locks.GetLockCountTotal()-locks.GetLockCount(-1) : 0;
      case MLOCK_INDEX     : 
          return (type==hasType) ? locks.GetLockCountTotal()-locks.GetLockCount(-1) : 0;
      default: break;
    };
  }
  return 0;
}
int PthrLock::getHeldCount(int type, bool self, int index) {
  MLockType hasType = locks.GetLockType();
  if (self) { // held by this process
    switch (type) {
      case MLOCK_INTERNAL  : return locks.GetBaseLocalCount(0);
      case MLOCK_EXCLUSIVE : return locks.GetLocalCount(-1);
      case MLOCK_SHARED    : return (type==hasType) ? locks.GetLocalCount(index) : 0;
      case MLOCK_INDEX     : return (type==hasType) ? locks.GetLocalCount(index) : 0;
      default: break;
    };
  } else { // held by anyone
    switch (type) {
      case MLOCK_INTERNAL  : return locks.GetBaseLockCount(0);
      case MLOCK_EXCLUSIVE : return locks.GetLocalCount(-1);
      case MLOCK_SHARED    : return (type==hasType) ? locks.GetLocalCount(index) : 0;
      case MLOCK_INDEX     : return (type==hasType) ? locks.GetLocalCount(index) : 0;
      default: break;
    };
  }
  return 0;
}
int PthrLock::getCount(int type) {
  MLockType hasType = locks.GetLockType();
  switch (type) {
    case MLOCK_INTERNAL  : return 1;
    case MLOCK_EXCLUSIVE : return 1;
    case MLOCK_SHARED    : return hasType==type ? locks.GetPartCount() : 0;
    case MLOCK_INDEX     : return hasType==type ? locks.GetPartCount() : 0;
    default: break;
  };
  return -1;
}
int PthrLock::reset(const char* dbname, int flags) {
  //errno = ENOTSUP;
  //return -1;
  return locks.ResetAllLocks();
}
#ifdef DYNAMIC_LOCK_EXPANSION
int PthrLock::expand(int type, int count) {
}
#endif


/* we need an exact mode (not borked by umask) so that multiple users */
/* can all peacefully share the lock directories under /tmp/.mlock-named/ */
int mkdir_no_umask(char* path, mode_t mode) {
  int err_save, ret = -1;
  mode_t oldMask = umask(0); /* save umask */
  ret = mkdir(path, mode); /* create with mode (vs mode|~umask) */
  /* umask doesn't return errors, but it might affect errno */
  err_save = errno; 
  umask(oldMask); /* restore umask */
  errno = err_save;
  return ret;
}


/* Recursively create directories as needed.
 * Assumes last path component should be a directory.
 * returns 0 on success. 
 */
int ensure_lock_path(char* path, int skiplast) {
  int ret = -1;
  struct stat sb;
  char *slash=NULL;
  int mode = S_IRWXU|S_IRWXG|S_IRWXO|S_ISVTX; /* BSD: S_ISTXT vs S_ISVTX */

  /* fprintf(stderr, "ensure_lock_path(%s, %d); \n", path, skiplast); */
  if (!path || !path[0] || path[0]!='/') {
    mdbm_logerror(LOG_ERR, 0, "%s: invalid path in ensure_lock_path()", path);
    return -1;
  }
  slash = strrchr(path, '/');
  if (skiplast) {
    /* fprintf(stderr, "==== About to create lockfile: [[ %s ]] ====\n", path); */
    slash[0] = 0; /* temporarily truncate string to avoid copy */
    ret = ensure_lock_path(path, 0); /* ensure parent */
    slash[0] = '/';
    return ret;
  }
stat:
  if (stat(path,&sb) == -1) {
    if (errno==ENOENT) {
      if (slash == path) { /* shouldn't be NULL, checked for leading '/' above */
        /* fall thru, root of the filesystem should always exist ;-) */
        ret = 0;
      } else {
        slash[0] = 0; /* temporarily truncate string to avoid copy */
        ret = ensure_lock_path(path, 0); /* ensure parent */
        slash[0] = '/';
      }
      if (ret) { /* fall thru with ret == -1 */
      } else if (0 == mkdir_no_umask(path, mode)) {
        ret = 0;
      } else if (errno == EEXIST) { /* another process created it */
        goto stat;
      } else {
        ret = -1;
      }
    } /* else fall thru with ret == -1 */
  } else if ((sb.st_mode & S_IFMT) != S_IFDIR) {
      errno = EEXIST;
  } else {
    ret = 0;
  }

  if (ret) {
    mdbm_logerror(LOG_ERR, errno,"mdbm-lock: Unable to create lock path %s", path);
  }
  return ret;
}


int PthrLock::open(const char* dbname, int flags, MLockType type, int lock_count, int do_lock, int* need_check) {
    //int db_part_num;       /* total partition locks (sans internal mutex) */
    int e = 0;

    //fprintf(stderr, "$$$$$ Opening PTHREAD LOCKFILE for db filename:%s $$$$$\n", dbname);
    char fn[MAXPATHLEN+1];
    struct stat st;
    mode_t mode = 0664;
    int rc, rcs;
    int locks_exist = 0;
    gid_t db_group;
    uid_t db_owner;
    //MLockType typ = MLOCK_EXCLUSIVE;

    if (0 == getFilename(dbname, fn, sizeof(fn))) {
      return -1;
    }

    rcs = stat(fn, &st);
    if (rcs<0) {
      if (0 != ensure_lock_path(fn, 1)) {
        return -1;
      }
      if (flags & MDBM_DBFLAG_MEMONLYCACHE) {
        mode = ACCESSPERMS;
        db_group = getgid();
        db_owner = getuid();
      } else {
        // [BUG 4637883] Named mutexes must be opened using same "mode" as underlying mdbm.
        // Get the permission of the mdbm, and use that for creating directories/lock files.
        rc = stat(dbname, &st);
        if (rc == -1) {
            mdbm_logerror(LOG_ERR, 0, "%s: open_locks unable to stat existing db file",
                          dbname);
            return rc;
        }
        mode = ACCESSPERMS & st.st_mode;
        db_group = st.st_gid;
        db_owner = st.st_uid;
      }
      // [BUG 5354707] Lockfiles inherently require modification to acquire a lock, 
      //   even to acquire a read-lock. So the lockfile must have write permission
      //   for each entity that has read-permission on the mdbm file.
      if (mode & S_IRUSR) {
          mode |= S_IWUSR;
      }
      if (mode & S_IRGRP) {
          mode |= S_IWGRP;
      }
      if (mode & S_IROTH) {
          mode |= S_IWOTH;
      }

      mdbm_log(LOG_DEBUG, "%s: open_locks, mode=0%o", dbname, mode);
    } else {
      locks_exist = 1;
    }

    //db_part_num = 1;
    //if (flags & MDBM_RW_LOCKS) {
    //  typ = MLOCK_SHARED;
    //  /* ysys_mmutex does 2*CPUs */
    //  db_part_num = 2*get_cpu_count();
    //} else if (flags & MDBM_PARTITIONED_LOCKS) {
    //  typ = MLOCK_INDEX;
    //  db_part_num = MDBM_NUM_PLOCKS;
    //}
    int ret = -1;
    if (locks_exist) {
      if (flags & MDBM_ANY_LOCKS) {
        ret = locks.Open(fn, 1, MLOCK_UNCHECKED, lock_count);
      } else {
        ret = locks.Open(fn, 1, type, lock_count);
      }
    } else {
      ret = locks.Open(fn, 1, type, lock_count, mode);
      // try matching owner and group-id to the db for a ykeykey issue Allen has (root->ykk)
      fchown(locks.locks.fd, db_owner, db_group);
    }
    e = errno; /* save errno */
    if (ret && ENOLCK==e) {
      if (flags & MDBM_ANY_LOCKS) {
        /* open-permissive */
        if (locks_exist) {
          locks.Open(fn, 1, MLOCK_UNCHECKED, lock_count);
        }
      } else {
#ifdef DYNAMIC_LOCK_EXPANSION
        /*NOTE("LOCK(BLOCK) SECOND-CHANCE OPEN UNCHECKED"); */
        /* try re-opening with a more permissive mode */
        if (locks_exist) {
          ret = locks.Open(fn, 1, MLOCK_UNCHECKED, lock_count);
        } else {
          ret = locks.Open(fn, 1, MLOCK_UNCHECKED, lock_count, mode);
        }
        if (ret) {
          NOTE("LOCK(BLOCK) SECOND-CHANCE OPEN UNCHECKED --FAILED--");
          return -1;
        }
        MLockType hasTyp = locks.GetLockType();
        if (hasTyp == MLOCK_EXCLUSIVE) {
          /* try to expand to a finer grained lock... */
          /* NOTE: we don't try to "expand" down to a single-lock */
          int ret = locks.Expand(1, type, lock_count);
          if (ret) {
            NOTE("LOCK(BLOCK) SECOND-CHANCE OPEN EXPAND --FAILED--");
            locks.Close();
            errno = e;
            return -1;
          }
        }
#endif
      }
    }
    if (ret) {
      errno=e;
      return -1;
    }
    if (do_lock) {
      /*NOTE("LOCK(BLOCK) OPEN-EXCLUSIVE"); */
      /* exclusive locking: */
      if (locks.Lock(MLOCK_EXCLUSIVE, MLOCK_BLOCK)) {
        if (errno == EINPROGRESS) {
            mdbm_log(LOG_NOTICE,"%s: mdbm integrity check due to previous lock owner death",
                     dbname);
            *need_check = 1;
        } else {
          ERROR();
          locks.Close();
          return -1;
        }
      }
    }
    return 0;
}


// NOTE We don't support this at all right now
// int
// do_lock_reset(const char* dbname, int flags)
// {
//     if (flags) {
//         errno = EINVAL;
//         return -1;
//     }
// 
// #  ifndef DYNAMIC_LOCK_EXPANSION
//     /* resetting the locking mode isn't allowed... */
//     mdbm_logerror(LOG_ERR,0,"%s:  mdbm_lock_reset() not supported",dbname);
//     errno = ENOTSUP;
//     return -1;
// 
// #  else /*DYNAMIC_LOCK_EXPANSION */
//     int ret = 0, need_check = 0;
//     struct mdbm_locks *db_locks = NULL;
//     /*NOTE("RESETTING LOCKFILE!"); */
//     /*fprintf(stderr, "RESETTING LOCKFILE! [%s]\n", dbname); */
//  
//     /*unlink(dbname); */
//     /*char pathname[MAXPATHLEN+1]; */
//     /* FIXME It's unclear if this should reset the db internal lock, or all... */
//     /*   Old implementation appears to reset all, and turn locking mode back into SINGLE */
//     if (open_locks_inner(NULL, dbname, &db_locks, flags, 0, &need_check)) {
//       return -1;
//     }
//     ret = mlock_expand(db_locks->db_mlocks, 1, MLOCK_EXCLUSIVE, 1);
//     if (!ret) {
//       ret = mlock_reset_all_locks(db_locks->db_mlocks);
//     }
//     mlock_close(db_locks->db_mlocks);
//     free(db_locks);
//     return ret;
// #  endif /*DYNAMIC_LOCK_EXPANSION */
// }





/*////////////////////////////////////////////////////////////////////////////*/
/* hack debugging function, for dumping pthreads mutex state */
/*////////////////////////////////////////////////////////////////////////////*/

/*

typedef struct pil
{
  struct pil *prev;
  struct pil *next;
} pil_t;

typedef struct mutex_s {
  int          lock;   //byte[0]  //
  unsigned int count;  //byte[4]  //
  int          owner;  //byte[8]  //
  unsigned int nusers; //byte[12] //
  int          kind;   //byte[16] //
  int          spins;  //byte[20] //
  pil_t        list;   //byte[24] //
} mutex_t;

typedef union {
  mutex_t data;
  //char __size[__SIZEOF_PTHREAD_MUTEX_T]; //
  long int __align;
} mutex_union_t;

typedef struct p_mutex_record_s {
    mutex_union_t mutex;
    volatile uint32_t owner;
    volatile int32_t count;
} p_mutex_record;

typedef struct p_lock_hdr_s {
  // stored in shared memory, accessed by multiple threads... it's all volatile //
  volatile uint32_t version;           // Version number //
  volatile uint32_t mutexSize;         // PMutexRecord size (for platform safety) //
  volatile uint32_t registerCount;     // register count //
  volatile uint32_t mutexCount;        // PMutexRecord count //
  volatile uint32_t mutexInitialized;  // How many of the mutexes have been initialized //
} p_lock_hdr;

void dump_header(p_lock_hdr* hdr, const char* fname) {
  fprintf(stdout, "[%s] ver:%u, mtxSz:%u, regs:%u, mtxs:%u, mtxInit:%u\n", fname,
    hdr->version, 
    hdr->mutexSize, 
    hdr->registerCount, 
    hdr->mutexCount,
    hdr->mutexInitialized
  );
}

void dump_registers(uint32_t* regs, int count) {
  int i;
  for (i=0; i<count; ++i) {
    fprintf(stdout, "reg[%d]=%u\n", i, regs[i]);
  }
}
void dump_mutexes(p_mutex_record* mutex, int count, int all) {
  int i;
  for (i=0; i<count; ++i) {
    p_mutex_record* cur = &mutex[i];
    mutex_t* m = &cur->mutex.data;
    if (all || cur->owner || cur->count || m->lock || m->count>1 || m->owner || m->nusers || m->kind!=144 || m->spins || m->list.prev || m->list.next) {
      fprintf(stdout, "mutex[%d] own:%u count:%u"
                      "    :: lock:%d, count:%u, owner:%d,"
                      " nusr:%u, kind:%i, spins:%d p:%p n:%p\n",
          i, cur->owner, cur->count,
          m->lock, m->count, m->owner, 
          m->nusers, m->kind, m->spins, (void*)m->list.prev, (void*)m->list.next
      );
    }
  }
}

void dump_lock_file(const char* fname, int all) {
  int fd;
  uint8_t* base;
  uint8_t* cur;
  p_lock_hdr* hdr = NULL;
  size_t fsize = 0;
  struct stat lockStat;

  fd = open(fname, O_RDWR);
  if (fd<0) {
    fprintf(stderr, "Error opening file [%s]\n", fname);
    return;
  } 

  if (fstat(fd, &lockStat)) {
    fprintf(stderr, "Error stat-ing file [%s]\n", fname);
    close(fd);
    return;
  }
  fsize = lockStat.st_size;
  base = (uint8_t*)mmap(NULL, fsize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  if (!base) {
    fprintf(stderr, "Error mmapping file [%s]\n", fname);
    close(fd);
    return;
  } 
  hdr = (p_lock_hdr*)base;
  dump_header(hdr, fname);
  cur = base + sizeof(p_lock_hdr);
  if (all) {
    dump_registers((uint32_t*)cur, hdr->registerCount);
  }
  cur = cur + sizeof(uint32_t)*(hdr->registerCount);
  dump_mutexes((p_mutex_record*)cur, hdr->mutexCount, all);

  munmap(base, fsize);
  close(fd);
}
*/


/*////////////////////////////////////////////////////////////////////////////*/
/*////////////////////////////////////////////////////////////////////////////*/
