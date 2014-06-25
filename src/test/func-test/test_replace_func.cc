/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-replace-func.cc
//    Purpose: Functional tests for replace function

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>

#include <iostream>
#include <ostream>
#include <string.h>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <mdbm.h>
//#include <mdbm_internal.h>
#include "TestBase.hh"

#define NUM_CHILDREN 500
#define NUM_THREADS 2

using namespace std;

class TestReplaceFuncSuite : public CppUnit::TestFixture, public TestBase {
  public:
    TestReplaceFuncSuite(int vFlag) : TestBase(vFlag, "TestReplaceFuncSuite") {}

    void test_replace_func_01();
    void test_replace_func_02();
    void test_replace_func_03();
    void test_replace_func_04();
    void test_replace_func_05();

  private:
    static int _pageSize;
    static int _initialDbSize;
    static mdbm_ubig_t _limitSize;
    static int _largeObjSize;
    static int _spillSize;
    static string _mdbmDir;
    static string _testDir;
    static int _flagsDefaultLock;
    static int _flagsNoLock;
    static int _flagsShared;
    static int _flagsPartition;
    static int _flagsPartitionLO;
    static int _iterations;

    //int doReplaces(string, string, int);
    void waitForAllChildren();
    void setEnv();
};

class TestReplaceFunc : public TestReplaceFuncSuite {
    CPPUNIT_TEST_SUITE(TestReplaceFunc);

    CPPUNIT_TEST(test_replace_func_01);
    CPPUNIT_TEST(test_replace_func_02);
    CPPUNIT_TEST(test_replace_func_03);
    CPPUNIT_TEST(test_replace_func_04);
    CPPUNIT_TEST(test_replace_func_05);

    CPPUNIT_TEST_SUITE_END();

    public:
    TestReplaceFunc() : TestReplaceFuncSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestReplaceFunc);

int TestReplaceFuncSuite::_pageSize = 4096;
// MDBM_DBSIZE_MB enabling this flag for specifying Db size in Megabytes
int TestReplaceFuncSuite::_initialDbSize = 4096*2;
mdbm_ubig_t TestReplaceFuncSuite::_limitSize = 10; // The limit size is in terms of number of pages
int TestReplaceFuncSuite::_largeObjSize = 3073;
int TestReplaceFuncSuite::_spillSize = 3072;
int TestReplaceFuncSuite::_iterations = 10;
string TestReplaceFuncSuite::_mdbmDir = "";
string TestReplaceFuncSuite::_testDir = "";
const int BASE_FLAGS = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_CREATE_V3;
// If we use no flag for locking then defaul is exclusive locking.
int TestReplaceFuncSuite::_flagsDefaultLock = BASE_FLAGS;
// Flags for shared locking
int TestReplaceFuncSuite::_flagsShared = BASE_FLAGS|MDBM_RW_LOCKS;
// Flags for partitioned locking
int TestReplaceFuncSuite::_flagsPartition = BASE_FLAGS|MDBM_PARTITIONED_LOCKS;
// Flags for partitioned locking with LO support
int TestReplaceFuncSuite::_flagsPartitionLO = BASE_FLAGS|MDBM_PARTITIONED_LOCKS|MDBM_LARGE_OBJECTS;
int TestReplaceFuncSuite::_flagsNoLock = BASE_FLAGS|MDBM_OPEN_NOLOCK;

// Some global vars
struct threadData {
  MDBM* oldMdbm;
  string old_name;
  string new_name;
  string testDir;
  //MDBM* oldMdbm;
};

void TestReplaceFuncSuite::setEnv() {
  GetTmpDir(_testDir);
}

int doReplaces(string old_name, string new_name, int duration, string testDir) {
  string replace_cmd = getMdbmToolPath("mdbm_replace");
  string copy_cmd    = getMdbmToolPath("mdbm_copy");
  string check_cmd   = getMdbmToolPath("mdbm_check");

  int i = 1;
  clock_t start;
  //clock_t end;

  start = clock();

  while(((clock()-start)/(double)CLOCKS_PER_SEC) < duration) {
    ostringstream num;
    num<<i;
    const string clone_name  = testDir + "/copy" + num.str() + ".mdbm";
    string copy = copy_cmd + " " +  old_name + " " + clone_name;
    if (system(copy.c_str()) != 0) {
      cout<<"Failed to create the copy"<<endl;
      exit(1);
    }

    // Copy clone mdbm to new mdbm
    CPPUNIT_ASSERT_EQUAL(0, system(("cp " + clone_name + " " + new_name).c_str()));
    CPPUNIT_ASSERT_EQUAL(0, system((check_cmd + " " + new_name).c_str()));

    // Replace new mdbm by copy mdbm
    if (system((check_cmd + " " + clone_name).c_str()) == 0) {
      if (system((replace_cmd + " " +  old_name + " " +  new_name).c_str()) == 0) {
        //cout<<i<<".Replace was successful"<<endl;
      }else{
        cout<<i<<".Replace was not successful"<<endl;
        exit(1);
      }
    }else{
      cout<<i<<".mdbm_check is failed for copy mdbm"<<endl;
      exit(1);
    }

    // Sleep nanosleep(1 - 100ms)
    sleepForRandomTime();
    i++;
    //end = clock();
  }
  return 0;
}

void TestReplaceFuncSuite::waitForAllChildren() {
  int status;
  // Parent waits here for all children to exit
  while (true) {
    pid_t done = wait(&status);
    if (done == -1) {
      if (errno == ECHILD) break; // no more child processes
    } else {
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        cerr << "pid " << done << " failed" << endl;
        exit(1);
      }
    }
  }
}

void TestReplaceFuncSuite::test_replace_func_01() {
  SKIP_IF_FAST();
  setEnv();
  const string old_name  = _testDir + "/old01.mdbm";
  const string new_name  = _testDir + "/new01.mdbm";
  datum k, v;
  int pid = -1;

  MDBM *old_mdbm = createMdbmNoLO(old_name.c_str(), _flagsDefaultLock, _pageSize, _initialDbSize);
  CPPUNIT_ASSERT(old_mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(old_mdbm, _limitSize, NULL, NULL));

  // Create key-value pair
  k.dptr = (char*)"key";
  k.dsize = strlen(k.dptr);
  v.dptr = (char*)"value_p";
  v.dsize = strlen(v.dptr);

  // Store a record using parent process
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(old_mdbm, k, v, MDBM_REPLACE));
  CPPUNIT_ASSERT_EQUAL(0, checkRecord(old_mdbm, k, v));

  // Fork main process to create a process
  pid = fork();
  CPPUNIT_ASSERT(pid >= 0);
  if (pid > 0) { // Parent process
    for (int i = 0; i < 10; i++) {
      // Sleep
      sleepForRandomTime();
      CPPUNIT_ASSERT_EQUAL(0, checkRecord(old_mdbm, k, v));
    }

    // Parent waits here for all children to exit
    waitForAllChildren();
  } else { // (pid = 0)child process
    // Don't ASSERT in child! CppUnit catches it an continues to run in both procs.
    if (0 != doReplaces(old_name, new_name, 1, _testDir)) {
      exit(-1);
    }
    mdbm_close(old_mdbm);
    exit(EXIT_SUCCESS);
  }

  mdbm_close(old_mdbm);
}

void TestReplaceFuncSuite::test_replace_func_02() {
  SKIP_IF_FAST();
  setEnv();
  const string old_name  = _testDir + "/old02.mdbm";
  const string new_name  = _testDir + "/new02.mdbm";
  datum k, v;
  int pid = -1;

  MDBM *old_mdbm = createMdbmNoLO(old_name.c_str(), _flagsNoLock, _pageSize, _initialDbSize);
  CPPUNIT_ASSERT(old_mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(old_mdbm, _limitSize, NULL, NULL));

  // Create key-value pair
  k.dptr = (char*)("key");
  k.dsize = strlen(k.dptr);
  v.dptr = (char*)("value_p");
  v.dsize = strlen(v.dptr);

  // Store a record using parent process
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(old_mdbm, k, v, MDBM_REPLACE));
  CPPUNIT_ASSERT_EQUAL(0, checkRecord(old_mdbm, k, v));

  // Fork main process to create a process
  pid = fork();
  CPPUNIT_ASSERT(pid >= 0);

  if (pid > 0) { // Parent process
    for (int i = 0; i < 10; i++) {
      sleepForRandomTime();
      CPPUNIT_ASSERT_EQUAL(0, checkRecord(old_mdbm, k, v));
    }

    // Parent waits here for all children to exit
    waitForAllChildren();
  } else { // (pid = 0)child process
    CPPUNIT_ASSERT_EQUAL(0, doReplaces(old_name, new_name, 1, _testDir));
    mdbm_close(old_mdbm);
    exit(EXIT_SUCCESS);
  }

  mdbm_close(old_mdbm);
}


void TestReplaceFuncSuite::test_replace_func_03() {
  SKIP_IF_FAST();
  setEnv();
  const string old_name  = _testDir + "/old03.mdbm";
  const string new_name  = _testDir + "/new03.mdbm";
  datum k, v;
  int pid = -1;

  // Create a Mdbm
  MDBM *old_mdbm = createMdbmNoLO(old_name.c_str(), _flagsNoLock, _pageSize, _initialDbSize);
  CPPUNIT_ASSERT(old_mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(old_mdbm, _limitSize, NULL, NULL));

  // Create key-value pair
  k.dptr = (char*)("key");
  k.dsize = strlen(k.dptr);
  v.dptr = (char*)("value_p");
  v.dsize = strlen(v.dptr);

  // Store a record using parent process
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(old_mdbm, k, v, MDBM_REPLACE));
  CPPUNIT_ASSERT_EQUAL(0, checkRecord(old_mdbm, k, v));

  // Fork first child that will be doing replaces
  pid = fork();
  CPPUNIT_ASSERT(pid >= 0);

  if (pid > 0) { // Parent process
    // Fork multiple reading children from parent
    for (int num = 0; num < NUM_CHILDREN; num++) {
      pid_t readerPid = fork();
      if (readerPid > 0) {//Parent
        // Do nothing
      } else if (readerPid < 0) {//Error
        exit(2);
      } else { //child
        // Do reads
        //for (int i = 0; i < 100; i++) {
          int rc = checkRecord(old_mdbm, k, v);
          exit(rc == 0 ? 0 : 1);
        //}
      }
    }
  } else { // (pid = 0)child process
    CPPUNIT_ASSERT_EQUAL(0, doReplaces(old_name, new_name, 1, _testDir));
    mdbm_close(old_mdbm);
    exit(EXIT_SUCCESS);
  }

  // Wait for all children to finish
  waitForAllChildren();

  // Close Mdbm
  mdbm_close(old_mdbm);
}

void* threadsDoReads(void* threadArg) {
  datum k, v, value;
  struct threadData *myData;
  myData = (struct threadData *) threadArg;
  MDBM* oldMdbm = myData->oldMdbm;
  string old_name = myData->old_name;
  string new_name = myData->new_name;

  k.dptr = (char*)"key";
  k.dsize = strlen(k.dptr);
  v.dptr = (char*)"value";
  v.dsize = strlen(v.dptr);

  // Store a record using parent thread
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(oldMdbm, k, v, MDBM_REPLACE));

  sleepForRandomTime();
  // Fetch the value for stored key
  value = mdbm_fetch(oldMdbm, k);

  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));

  mdbm_close(oldMdbm);
  return NULL;
}

void* threadsDoReplaces(void* threadArg) {
  struct threadData *myData;
  myData = (struct threadData *) threadArg;
  string old_name = myData->old_name;
  string new_name = myData->new_name;
  string testDir = myData->testDir;
  MDBM* oldMdbm = myData->oldMdbm;

  CPPUNIT_ASSERT_EQUAL(0, doReplaces(old_name, new_name, 1, testDir));
  mdbm_close(oldMdbm);
  return NULL;
}

void TestReplaceFuncSuite::test_replace_func_04() {
  SKIP_IF_FAST();
  setEnv();
  const string old_name  = _testDir + "/old04.mdbm";
  const string new_name  = _testDir + "/new04.mdbm";
  pthread_t tId[NUM_THREADS];
  struct threadData threadDataArray[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    // Create Mdbm per thread
    MDBM* mdbm = createMdbmNoLO(old_name.c_str(), _flagsNoLock, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    threadDataArray[i].old_name = old_name;
    threadDataArray[i].new_name = new_name;
    threadDataArray[i].oldMdbm = mdbm;
    threadDataArray[i].testDir = _testDir;
    if (i == 0) {
      pthread_create(&tId[i], NULL, threadsDoReplaces, (void*)&threadDataArray[i]);
    }else{
      pthread_create(&tId[i], NULL, threadsDoReads, (void*)&threadDataArray[i]);
    }
  }

  for (int j = 0; j < NUM_THREADS; j++) {
    pthread_join(tId[j], NULL);
  }
}

void TestReplaceFuncSuite::test_replace_func_05() {
  SKIP_IF_FAST();
  setEnv();
  const string old_name  = _testDir + "/old05.mdbm";
  const string new_name  = _testDir + "/new05.mdbm";
  pthread_t tId[NUM_THREADS];
  struct threadData threadDataArray[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    MDBM* mdbm = createMdbmNoLO(old_name.c_str(), _flagsDefaultLock, _pageSize, _initialDbSize);
    // Create a Mdbm
    CPPUNIT_ASSERT(mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    threadDataArray[i].old_name = old_name;
    threadDataArray[i].new_name = new_name;
    threadDataArray[i].oldMdbm = mdbm;
    threadDataArray[i].testDir = _testDir;
    if (i == 0) {
      pthread_create(&tId[i], NULL, threadsDoReplaces, (void*)&threadDataArray[i]);
    } else {
      pthread_create(&tId[i], NULL, threadsDoReads, (void*)&threadDataArray[i]);
    }
  }

  for (int j = 0; j < NUM_THREADS; j++) {
    pthread_join(tId[j], NULL);
  }
}


