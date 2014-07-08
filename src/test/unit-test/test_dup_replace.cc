/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_hash  mdbm_sethash
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/wait.h>

#include <iostream>
#include <vector>
#include <algorithm>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "TestBase.hh"

class DupReplaceTestBase;

struct thread_info {
  pthread_t threadId;
  int threadNum;
  DupReplaceTestBase* drtb;
  MDBM* db;
  int lockMode;
  int rdonly;
  bool* done;
};


class DupReplaceTestBase : public CppUnit::TestFixture, public TestBase
{

public:
    DupReplaceTestBase(int vFlag) : TestBase(vFlag, "DupReplace Test Suite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    void DupReplace();
    void DupReplaceReadOnly();
    void DupReplaceShared();
    void DupReplacePart();
    //void V2ToV3Replace();
    //void V2ToV3ReplaceND();

protected:
  string file1, file2;
  void MakeTestMdbm(const string& fname, int flags, int limit = 0);
  void* ReplaceThread(thread_info& info);
  void* UseThread(thread_info& info);
  static void* ReplaceThreadStatic(void* obj);
  static void* UseThreadStatic(void* obj);
  void DupReplaceInner(int lockMode, int rdonly=0);
};


void DupReplaceTestBase::setUp()
{
    file1 = GetTmpName();
    file2 = GetTmpName();
    fprintf(stderr, "Using filenames \n\t%s\n\t%s\n", file1.c_str(), file2.c_str());
    //MakeTestMdbm(file1, versionFlag);
}

void DupReplaceTestBase::MakeTestMdbm(const string& fname, int flags, int limit) {
    int i;
    char key[4096];
    char val[4096];
    datum kdat, vdat;
    int oflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | flags;
    //fprintf(stderr, "MakeTestMdbm (flags:0x%x, oflags:0x%x)\n", flags, oflags);
    MdbmHolder db = mdbm_open(fname.c_str(), oflags, 0644, 1024, 4096);
    if (limit) {
      mdbm_limit_size_v3(db, limit, NULL, NULL);
    }

    //mdbm_lock_dump(db);

    kdat.dptr = key;
    vdat.dptr = val;
    // store some values
    for (i=10; i<100; ++i) {
      kdat.dsize = sprintf(key, "key %04d ==val", i);
      vdat.dsize = sprintf(val, "whereas this is value %07d", i);
      mdbm_store(db, kdat, vdat, MDBM_REPLACE);
    }
}

void* DupReplaceTestBase::ReplaceThreadStatic(void* obj) {
  CPPUNIT_ASSERT(obj);
  thread_info *inf = ((thread_info*)obj);
  return inf->drtb->ReplaceThread(*inf);
}
void* DupReplaceTestBase::UseThreadStatic(void* obj) {
  CPPUNIT_ASSERT(obj);
  thread_info *inf = ((thread_info*)obj);
  return inf->drtb->UseThread(*inf);
}

void* DupReplaceTestBase::ReplaceThread(thread_info& info) {
  //fprintf(stderr, "ReplaceThread starting\n");
  for (int i=0; i<50; ++i) {
  //for (int i=0; i<1000; ++i) {
    MakeTestMdbm(file2, versionFlag | info.lockMode, 10);
    int flags = MDBM_O_RDWR | versionFlag | info.lockMode;
    MdbmHolder db = mdbm_open(file1.c_str(), flags, 0644, 1024, 10*1024);
    //{
    //  MdbmHolder db2 = mdbm_open(file2.c_str(), flags, 0644, 1024, 10*1024);
    //  // NOTE: calling mdbm_pre_split() once there is any data is very bad...
    //  mdbm_limit_size_v3(db2, 10, NULL, NULL);
    //  mdbm_pre_split(db2, 10);
    //}
    if (!(i%10)) { fprintf(stderr, "ReplaceThread replace %d\n", i); }
    mdbm_replace_db(db, file2.c_str());
    //sleep(1);
    usleep(125000); // replace 8x per second
  }
  usleep(125000); // small delay for UseThreads to notice last replace
  fprintf(stderr, "ReplaceThread done\n");
  return NULL;
}

void* DupReplaceTestBase::UseThread(thread_info& info) {
  //fprintf(stderr, "UseThread starting\n");
  int flags = MDBM_O_RDWR | versionFlag | info.lockMode;
  MdbmHolder db2 = mdbm_open(file1.c_str(), flags, 0644, 0, 0);
  mdbm_limit_size_v3(db2, 10, NULL, NULL);
  //mdbm_pre_split(db2, 10);
  MDBM* db = info.db;
  for (int i=0; i<7500 && !*info.done; ++i) {
    int j, ret;
    char key[4096];
    char val[4096];
    datum kdat, vdat;

    kdat.dptr = key;
    vdat.dptr = val;
    if (!info.rdonly) {
      // store some values
      for (j=10; j<100 && !*info.done; ++j) {
        kdat.dsize = sprintf(key, "key %04d ==val", j);
        vdat.dsize = sprintf(val, "whereas this is value %07d tid:%d", j, info.threadNum);
        mdbm_store(db, kdat, vdat, MDBM_REPLACE);
      }
    }
    for (j=10; j<100 && !*info.done; ++j) {
      kdat.dsize = sprintf(key, "key %04d ==val", i);
      ret = mdbm_lock_smart(db, &kdat, MDBM_O_RDONLY);
      if (ret < 0) { fprintf(stderr, "LOCK ERROR!!!!\n"); }
      vdat = mdbm_fetch(db, kdat);
      ret = mdbm_unlock_smart(db, &kdat, MDBM_O_RDONLY);
      if (ret < 0) { fprintf(stderr, "UNLOCK ERROR!!!!\n"); }

      ret = mdbm_lock_smart(db2, &kdat, MDBM_O_RDONLY);
      if (ret < 0) { fprintf(stderr, "LOCK ERROR!!!!\n"); }
      vdat = mdbm_fetch(db2, kdat);
      ret = mdbm_unlock_smart(db2, &kdat, MDBM_O_RDONLY);
      if (ret < 0) { fprintf(stderr, "UNLOCK ERROR!!!!\n"); }
    }
  }
  //fprintf(stderr, "UseThread done\n");
  return NULL;
}

void DupReplaceTestBase::DupReplaceInner(int lockMode, int rdonly)
{
    //int numThreads = 100;
    //int numThreads = 50;
    //int numThreads = 10;
    int numThreads = 3;
    struct thread_info *thrInfo = NULL;
    pthread_attr_t attr;
    int i, ret;
    void* res;
    std::string cmd;
    bool done = false;

    // // delete the lockfiles, in case dynamic-lock-expansion isn't allowed
    // cmd = "rm -rf /tmp/.mlock-named" + file1 + "._int_";
    // fprintf(stderr, "%s executing [[ %s ]]\n", __func__, cmd.c_str());
    // system(cmd.c_str());
    // cmd = "rm -rf /tmp/.mlock-named" + file2 + "._int_";
    // fprintf(stderr, "%s executing [[ %s ]]\n", __func__, cmd.c_str());
    // system(cmd.c_str());

    MakeTestMdbm(file1, versionFlag|lockMode, 10);

    ret = pthread_attr_init(&attr);
    CPPUNIT_ASSERT(ret == 0);
    thrInfo = (thread_info*)calloc(numThreads, sizeof(struct thread_info));
    CPPUNIT_ASSERT(thrInfo);

    int flags = MDBM_O_RDWR | versionFlag | lockMode;
    MdbmHolder db = mdbm_open(file1.c_str(), flags, 0644, 1024, 10*1024);
    MdbmHolder dbro = mdbm_open(file1.c_str(), (flags & ~MDBM_O_RDWR), 0644, 1024, 10*1024);
    mdbm_limit_size_v3(db, 10, NULL, NULL);
    //mdbm_pre_split(db, 10);

    for (i=numThreads-1; i>=0; --i) {
        thrInfo[i].drtb = this;
        thrInfo[i].threadNum = i;
        if (!rdonly || i<2) {
          thrInfo[i].db = mdbm_dup_handle(db, 0);
          thrInfo[i].rdonly = 0;
        } else {
          thrInfo[i].db = mdbm_dup_handle(dbro, 0);
          thrInfo[i].rdonly = 1;
        }
        thrInfo[i].lockMode = lockMode;
        thrInfo[i].done = &done;
        // pthread_create() stores the thread ID into corresponding element of thrInfo[]
        if (i == 0) {
          // create the replace thread last
          ret = pthread_create(&thrInfo[i].threadId, &attr, &ReplaceThreadStatic, &thrInfo[i]);
        } else {
          ret = pthread_create(&thrInfo[i].threadId, &attr, &UseThreadStatic, &thrInfo[i]);
        }
        CPPUNIT_ASSERT(ret == 0);
    }

    // Now join with each thread, and display its returned value
    for (i = 0; i < numThreads; i++) {
        ret = pthread_join(thrInfo[i].threadId, &res);
        done = true;
        CPPUNIT_ASSERT(ret == 0);
        printf("Joined thread %d\n", thrInfo[i].threadNum);
        mdbm_close(thrInfo[i].db);
        //free(res);      // Free memory allocated by thread
    }
    free(thrInfo);
    pthread_attr_destroy(&attr);

}

void DupReplaceTestBase::DupReplace() {
  TRACE_TEST_CASE("DupReplace");
  SKIP_IF_FAST_VALGRIND()
  DupReplaceInner(0); // exclusive
}

void DupReplaceTestBase::DupReplaceReadOnly() {
  TRACE_TEST_CASE("DupReplaceReadOnly");
  SKIP_IF_FAST_VALGRIND()
  DupReplaceInner(MDBM_RW_LOCKS,1);
}

void DupReplaceTestBase::DupReplaceShared() {
  TRACE_TEST_CASE("DupReplaceShared");
  SKIP_IF_FAST_VALGRIND()
  DupReplaceInner(MDBM_RW_LOCKS);
}

void DupReplaceTestBase::DupReplacePart() {
  TRACE_TEST_CASE("DupReplacePartitioned");
  // we should run at least 1, and this is the fastest (~70sec)
  // SKIP_IF_FAST_VALGRIND() 
  DupReplaceInner(MDBM_PARTITIONED_LOCKS);
}



struct Synchro {
public:
  int didQuit;
  int shouldQuit;
  int flag;
  pthread_mutex_t mutex;

public:
  void Init() {
    // NOTE: we expect a prior memset(this, sizeof(Synchro), 0);
    //
    // create a process-shared pthreads mutex
    pthread_mutexattr_t attr;
    CPPUNIT_ASSERT(0 == pthread_mutexattr_init(&attr));
    CPPUNIT_ASSERT(0 == pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED));
    CPPUNIT_ASSERT(0 == pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK));
    CPPUNIT_ASSERT(0 == pthread_mutex_init(&mutex, &attr));
    pthread_mutexattr_destroy(&attr);
  }
  int Lock() {
    int ret = pthread_mutex_lock(&mutex);
    if (ret) { fprintf(stderr, "ERROR: %d locking Syncronization mutex\n", ret); }
    return ret;
  }
  int UnLock() {
    int ret = pthread_mutex_unlock(&mutex);
    if (ret) { fprintf(stderr, "ERROR: %d unlocking Syncronization mutex\n", ret); }
    return ret;
  }
};


class DupReplaceTestV3PLock : public DupReplaceTestBase
{
    CPPUNIT_TEST_SUITE(DupReplaceTestV3PLock);
    CPPUNIT_TEST(DupReplace);
    CPPUNIT_TEST(DupReplaceShared);
    CPPUNIT_TEST(DupReplaceReadOnly);
    CPPUNIT_TEST(DupReplacePart);
    CPPUNIT_TEST_SUITE_END();

public:
    DupReplaceTestV3PLock() : DupReplaceTestBase(MDBM_CREATE_V3|MDBM_SINGLE_ARCH) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(DupReplaceTestV3PLock);

