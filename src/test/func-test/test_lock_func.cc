/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-lock-func.cc
//    Purpose: Functional tests for locking modes

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
#include <mdbm_internal.h>
#include "TestBase.hh"

using namespace std;

class TestLockFuncSuite : public CppUnit::TestFixture, public TestBase {
  public:
    TestLockFuncSuite(int vFlag) : TestBase(vFlag, "TestLockFuncSuite") {}

    void setEnv();
    void setUp();
    void tearDown();

    void test_lock_func_01();
    void test_lock_func_02();
    void test_lock_func_03();
    //void test_lock_func_04();
    void test_lock_func_05();
    void test_lock_func_06();
    void test_lock_func_07();
    void test_lock_func_08();
    void test_lock_func_09();
    //Commented these to prevent build failure. Need to investigate more here
    //void test_lock_func_10();
    void test_lock_func_11();
    void test_lock_func_12();
    void test_lock_func_13();
    void test_lock_func_14();
    void test_lock_func_15();

  private:
    static int _pageSize;
    static int _initialDbSize;
    static mdbm_ubig_t _limitSize;
    static int _largeObjSize;
    static int _spillSize;
    static string _mdbmDir;
    static string _testDir;
    static int _flagsExLock;
    static int _flagsShared;
    static int _flagsPartition;
    static int _flagsPartitionLO;
    static int _iterations;

};

class TestLockFunc : public TestLockFuncSuite {
    CPPUNIT_TEST_SUITE(TestLockFunc);

    CPPUNIT_TEST(setEnv);
    CPPUNIT_TEST(setUp);
    CPPUNIT_TEST(tearDown);

    CPPUNIT_TEST(test_lock_func_01);
    CPPUNIT_TEST(test_lock_func_02);
    CPPUNIT_TEST(test_lock_func_03);

    // This test seems to be invalid. It has a fundamental race/deadlock issue.
    // A parent and child both loop:
    //   taking a shared lock,
    //   then performing ops that implicitly take a write/exclusive lock,
    //   then unlock
    // If both manage to grab the shared lock before the write operations,
    // then they will mutually deadlock waiting for the write lock.
    //CPPUNIT_TEST(test_lock_func_04);

    CPPUNIT_TEST(test_lock_func_05);
    CPPUNIT_TEST(test_lock_func_06);
    CPPUNIT_TEST(test_lock_func_07);

    CPPUNIT_TEST(test_lock_func_08);
    CPPUNIT_TEST(test_lock_func_09);
    //CPPUNIT_TEST(test_lock_func_10);
    CPPUNIT_TEST(test_lock_func_11);
    CPPUNIT_TEST(test_lock_func_12);
    CPPUNIT_TEST(test_lock_func_13);
    CPPUNIT_TEST(test_lock_func_14);
    CPPUNIT_TEST(test_lock_func_15);

    CPPUNIT_TEST_SUITE_END();

    public:
    TestLockFunc() : TestLockFuncSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestLockFunc);

int TestLockFuncSuite::_pageSize = 4096;
// MDBM_DBSIZE_MB enabling this flag for specifying Db size in Megabytes
int TestLockFuncSuite::_initialDbSize = 4096*2;
mdbm_ubig_t TestLockFuncSuite::_limitSize = 10; // The limit size is in terms of number of pages
int TestLockFuncSuite::_largeObjSize = 3073;
int TestLockFuncSuite::_spillSize = 3072;
int TestLockFuncSuite::_iterations = 10;
string TestLockFuncSuite::_mdbmDir = "";
string TestLockFuncSuite::_testDir = "";
// If we use no flag for locking then defaul is exclusive locking.
int TestLockFuncSuite::_flagsExLock = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_CREATE_V3;
// Flags for shared locking
int TestLockFuncSuite::_flagsShared = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_RW_LOCKS|
  MDBM_CREATE_V3;
// Flags for partitioned locking
int TestLockFuncSuite::_flagsPartition = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3;
// Flags for partitioned locking with LO support
int TestLockFuncSuite::_flagsPartitionLO = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_LARGE_OBJECTS|MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3;


// Some global vars
static int sleepTime = 0;
MDBM *mdbm1 = NULL;
int lockMode = 0;
struct threadData{
  MDBM* mdbmHandle;
};


void TestLockFuncSuite::setUp() {
}

void TestLockFuncSuite::tearDown() {
}

void TestLockFuncSuite::setEnv() {
  GetTmpDir(_testDir);
}


// Test case - Mdbm is opened with an exclusive lock. Write using one process and try to write
// using other process. Second process should wait until process 1 finishes
// Test cases for write, update and fetch are covered in this scenario
void TestLockFuncSuite::test_lock_func_01() {
    const string mdbm_name  = _testDir + "/test01.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Mdbm
    //CPPUNIT_ASSERT((mdbm = createMdbmNoLO(db_name, _flagsExLock, _pageSize, _initialDbSize))
    MdbmHolder mdbm = createMdbmNoLO(db_name, _flagsShared, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Fork main process to create a process
    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (pid > 0) { // Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

            v.dptr = (char*)("value_p");
            v.dsize = sizeof(v.dptr);

            // Store a record using parent process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

            sleep(sleepTime);

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));

            sleep(sleepTime);
        }

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
    } else { // (pid = 0) child process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

            v.dptr = (char*)("value_c");
            v.dsize = sizeof(v.dptr);

            // Store a record using child process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

            sleep(sleepTime);

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));

            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }
}


// Test case - Mdbm is opened with an exclusive lock. Write using one process and try to
// delete using other process. Second process should wait until process 1 finishes
void TestLockFuncSuite::test_lock_func_02() {
    const string mdbm_name  = _testDir + "/test02.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v, value;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, _flagsExLock, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (pid > 0) { // Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

            v.dptr = (char*)("value_p");
            v.dsize = sizeof(v.dptr);

            // Store a record using parent process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

            // Fetch the value for stored key
            value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            // Delete the record
            CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

            sleep(sleepTime);

            // Try to fetch the record for deleted key. This time it should return
            // datum with NULL value. Fetch the value for same key
            value = mdbm_fetch(mdbm, k);

            // Verify that the returned value is NULL
            CPPUNIT_ASSERT_EQUAL(0, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }
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
    } else { // (pid = 0)child process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

            v.dptr = (char*)("value_c");
            v.dsize = sizeof(v.dptr);

            // Store a record using child process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            // Delete the record
            CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

            sleep(sleepTime);

            // Try to fetch the record for deleted key. This time it should datum
            // with NULL value. Fetch the value for same key
            value = mdbm_fetch(mdbm, k);

            // Verify that the returned value is NULL
            CPPUNIT_ASSERT_EQUAL(0, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));

            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }
}




//
// Shared lock
//

// Test case - Mdbm is opened with an exclusive lock. Write using one process and try to write
// using other process. Second process should wait until process 1 finishes
// Test cases for write, update and fetch are covered in this scenario
void TestLockFuncSuite::test_lock_func_03() {
    const string mdbm_name  = _testDir + "/test03.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, _flagsShared, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (pid > 0) { // Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

            v.dptr = (char*)("value_p");
            v.dsize = sizeof(v.dptr);

            // Store a record using parent process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));
            sleep(sleepTime);

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }
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
    } else { // (pid = 0)child process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

            v.dptr = (char*)("value_c");
            v.dsize = sizeof(v.dptr);

            // Store a record using child process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

            sleep(sleepTime);
            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }
}



// Test case - Mdbm is opened with an shared lock. Write using one process and try to
// delete using other process. Second process should wait until process 1 finishes

/*
void TestLockFuncSuite::test_lock_func_04() {
    const string mdbm_name  = _testDir + "/test04.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v, v_c, v_p;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = createMdbmNoLO(db_name, _flagsShared, _pageSize, _initialDbSize))
     != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store a record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Fork main process to create a process
    pid = fork();
    CPPUNIT_ASSERT(pid >= 0); // ensure fork succeeded

    if (pid > 0) {// Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbm));

            v_p.dptr = (char*)("value_p");
            v_p.dsize = sizeof(v_p.dptr);

            if (mdbm_store(mdbm, k, v_p, MDBM_REPLACE) == 0) {
                // Fetch the value for stored key
                datum value = mdbm_fetch(mdbm, k);

                // Verify that fetched value is same as stored value
                CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v_p.dptr, value.dsize));
                CPPUNIT_ASSERT_EQUAL(v_p.dsize, value.dsize);

                // Delete the record
                if (mdbm_delete(mdbm, k) == 0) {
                    sleep(sleepTime);

                    // Try to fetch the record for deleted key. This time it should return
                    // datum with NULL value. Fetch the value for same key
                    value = mdbm_fetch(mdbm, k);
                    // Verify that the returned value is NULL
                    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
                }
            }

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }

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
    } else { // (pid = 0)child process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbm));

            v_p.dptr = (char*)("value_c");
            v_c.dsize = sizeof(v_c.dptr);

            if (mdbm_store(mdbm, k, v_c, MDBM_REPLACE) == 0) {
                // Fetch the value for stored key
                datum value = mdbm_fetch(mdbm, k);

                // Verify that fetched value is same as stored value
                CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v_c.dptr, value.dsize));
                CPPUNIT_ASSERT_EQUAL(v_c.dsize, value.dsize);

                // Delete the record
                if (mdbm_delete(mdbm, k) == 0) {
                    sleep(sleepTime);
                    // Try to fetch the record for deleted key. This time it should return
                    // datum with NULL value. Fetch the value for same key
                    value = mdbm_fetch(mdbm, k);

                    // Verify that the returned value is NULL
                    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
                }
            }

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }

    // Close Mdbm
    mdbm_close(mdbm);
}
*/

// Test case - Mdbm is opened with an shared lock. Two processes are trying to read
// same record simultaneously.
void TestLockFuncSuite::test_lock_func_05() {
    const string mdbm_name  = _testDir + "/test05.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, _flagsShared, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store a record using parent process
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Fork main process to create a process
    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);

    if (pid > 0) { // Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbm));

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);
            sleep(sleepTime);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }
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
    } else { // (pid = 0)child process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbm));

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);
            sleep(sleepTime);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }
}


//
// Partition lock
//

// Test case - Mdbm is opened with an partition lock. Write using one process and try to
// write/update using other process. Second process should wait until process 1 finishes
void TestLockFuncSuite::test_lock_func_06() {
    const string mdbm_name  = _testDir + "/test06.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, _flagsPartition, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Fork main process to create a process
    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (pid > 0) { // Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbm, &k, 0));

            v.dptr = (char*)("value_p");
            v.dsize = strlen(v.dptr);

            // Store a record using parent process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));
            sleep(sleepTime);

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm, &k, 0));
            sleep(sleepTime);
        }
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
    } else { // (pid = 0)child process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbm, &k, 0));

            v.dptr = (char*)("value_c");
            v.dsize = strlen(v.dptr);

            // Store a record using parent process
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));
            sleep(sleepTime);

            // Fetch the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

            CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm, &k, 0));
            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }
}

// Test case - Mdbm is opened with an shared lock. Write using one process and try to
// delete using other process. Second process should wait until process 1 finishes

void TestLockFuncSuite::test_lock_func_07() {
    const string mdbm_name  = _testDir + "/test07.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int pid = -1, status = -1;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, _flagsPartition, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Fork main process to create a process
    pid = fork();
    CPPUNIT_ASSERT(pid >= 0); // ensure fork succeeded

    if (pid > 0) { // Parent process
        for (int i = 0; i < 1000; i++) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbm, &k, 0));

            v.dptr = (char*)("value_p");
            v.dsize = strlen(v.dptr);

            if (mdbm_store(mdbm, k, v, MDBM_REPLACE) == 0) {
                // Fetch the value for stored key
                datum value = mdbm_fetch(mdbm, k);

                // Verify that fetched value is same as stored value
                CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
                CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

                // Delete the record
                if (mdbm_delete(mdbm, k) == 0) {
                    sleep(sleepTime);

                    // Try to fetch the record for deleted key. This time it should return
                    // datum with NULL value. Fetch the value for same key
                    value = mdbm_fetch(mdbm, k);

                    // Verify that the returned value is NULL
                    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
                }
            }

            CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm, &k, 0));
            sleep(sleepTime);
        }

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
    } else { // (pid = 0)child process
        for (int i = 0; i < 1000; i++) {
            if (1 != mdbm_plock(mdbm, &k, 0)) {
                fprintf(stderr, "%s:%d %s() ASSERT FAILED!\n", __FILE__, __LINE__, __func__);
                exit(1);
            }

            v.dptr = (char*)("value_c");
            v.dsize = strlen(v.dptr);

            if (mdbm_store(mdbm, k, v, MDBM_REPLACE) == 0) {
                // Fetch the value for stored key
                datum value = mdbm_fetch(mdbm, k);

                // Verify that fetched value is same as stored value
                if (0 != memcmp(value.dptr, v.dptr, value.dsize)) {
                    fprintf(stderr, "%s:%d %s() ASSERT FAILED!\n", __FILE__, __LINE__, __func__);
                    exit(1);
                }
                if (v.dsize != value.dsize) {
                    fprintf(stderr, "%s:%d %s() ASSERT FAILED!\n", __FILE__, __LINE__, __func__);
                    exit(1);
                }

                // Delete the record
                if (mdbm_delete(mdbm, k) == 0) {
                    sleep(sleepTime);

                    // Try to fetch the record for deleted key. This time it should return
                    // datum with NULL value. Fetch the value for same key
                    value = mdbm_fetch(mdbm, k);

                    // Verify that the returned value is NULL
                    if (0 != value.dsize) {
                        fprintf(stderr, "%s:%d %s() ASSERT FAILED!\n", __FILE__, __LINE__, __func__);
                        exit(1);
                    }
                }
            }

            if (1 != mdbm_punlock(mdbm, &k, 0)) {
                fprintf(stderr, "%s:%d %s() ASSERT FAILED!\n", __FILE__, __LINE__, __func__);
                exit(1);
            }
            sleep(sleepTime);
        }
        exit(EXIT_SUCCESS);
    }
}

///////////////////////////////////////////////////////
// Using pthreads
///////////////////////////////////////////////////////

//
// Exclusive lock
//

// Function to run the threads
//*runThread1(void *dummyPtr)
void* runThread1(void* threadArg) {
    // Get ID of calling thread
    pthread_t tid = pthread_self();
    datum k, v, value;
    struct threadData *myData;
    myData = (struct threadData *) threadArg;
    MDBM* mdbmHandle = myData->mdbmHandle;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = reinterpret_cast<char*>(&tid);
    v.dsize = sizeof(tid);

    switch (lockMode) {
        case 1: // Exclusive lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbmHandle)); break;
        case 2: // Shared lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbmHandle)); break;
        case 3: // Partitioned lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbmHandle, &k, 0)); break;
        default: // error, unexpected value
            CPPUNIT_ASSERT(1 == 0);
    }

    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbmHandle, k, v, MDBM_REPLACE));
    sleep(sleepTime);

    // Fetch the value for stored key
    value = mdbm_fetch(mdbmHandle, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));

    switch (lockMode) {
        case 1: // Exclusive lock, fall thru
        case 2: // Shared lock, same as exclusive
            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbmHandle)); break;
        case 3: // Partitioned lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbmHandle, &k, 0)); break;
    }

    mdbm_close(mdbmHandle);
    sleep(sleepTime);

    return NULL;
}


// Test case - Mdbm is opened with an exclusive lock. Write using one thread and
// read using other thread
void TestLockFuncSuite::test_lock_func_08() {
    const string mdbm_name  = _testDir + "/test08.mdbm";
    const char* db_name = mdbm_name.c_str();
    int numThreads = 3;
    std::vector<pthread_t> tId;
    std::vector<struct threadData> threadDataArray;
    tId.resize(numThreads);
    threadDataArray.resize(numThreads);
    lockMode = 1;


    for (int i = 0; i < numThreads; i++) {
        // Create Mdbm per thread
        MDBM* mdbm = mdbm_open(db_name, _flagsExLock, 0600, _pageSize, _initialDbSize);
        CPPUNIT_ASSERT(NULL != mdbm);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

        threadDataArray[i].mdbmHandle = mdbm;
        pthread_create(&tId[i], NULL, runThread1, (void*)&threadDataArray[i]);
    }

    for (int j = 0; j < numThreads; j++) {
        pthread_join(tId[j], NULL);
    }

    lockMode = 0;
}

// Test case - Mdbm is opened with an shared lock. Write using one thread and
// read using other thread
void TestLockFuncSuite::test_lock_func_09() {
/*
    const string mdbm_name  = _testDir + "/test09.mdbm";
    const char* db_name = mdbm_name.c_str();
    int numThreads = 3;
    pthread_t tId[numThreads];
    lockMode = 2;
    struct threadData threadDataArray[numThreads];

    for (int i = 0; i < numThreads; i++) {
        // Create Mdbm per thread
        MDBM* mdbm = mdbm_open(db_name, _flagsShared, 0600, _pageSize, _initialDbSize);
        CPPUNIT_ASSERT(NULL != mdbm);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

        threadDataArray[i].mdbmHandle = mdbm;
        pthread_create(&tId[i], NULL, runThread1, (void*)&threadDataArray[i]);
    }

    for (int j = 0; j < numThreads; j++) {
        pthread_join(tId[j], NULL);
    }

    lockMode = 0;
*/
}

/*
// Test case - Mdbm is opened with partitioned lock. Write using one thread and
// read using other thread (Keys in same partition)
void TestLockFuncSuite::test_lock_func_10() {
    const string mdbm_name  = _testDir + "/test10.mdbm";
    const char* db_name = mdbm_name.c_str();
    int numThreads = 3;
    pthread_t tId[numThreads];
    lockMode = 3;
    struct threadData threadDataArray[numThreads];

    for (int i = 0; i < numThreads; i++) {
        // Create Mdbm per thread
        MDBM* mdbm = mdbm_open(db_name, _flagsPartition, 0600, _pageSize, _initialDbSize);
        CPPUNIT_ASSERT(NULL != mdbm);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

        threadDataArray[i].mdbmHandle = mdbm;
        pthread_create(&tId[i], NULL, runThread1, (void*)&threadDataArray[i]);
    }

    for (int j = 0; j < numThreads; j++) {
        pthread_join(tId[j], NULL);
    }

    lockMode = 0;
}
*/

// Function to run the threads
void* runThread2(void* threadArg) {
    // Get ID of calling thread
    pthread_t tid = pthread_self();
    datum k, v, value;
    struct threadData *myData;
    myData = (struct threadData *) threadArg;
    MDBM* mdbmHandle = myData->mdbmHandle;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = reinterpret_cast<char*>(&tid);
    v.dsize = sizeof(tid);

    switch (lockMode) {
        case 1: // Exclusive lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbmHandle)); break;
        case 2: // Shared lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbmHandle)); break;
        case 3: // Partitioned lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbmHandle, &k, 0)); break;
        default: // error, unexpected value
            CPPUNIT_ASSERT(1 == 0);
    }

    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbmHandle, k, v, MDBM_REPLACE));

    // Fetch the value for stored key
    value = mdbm_fetch(mdbmHandle, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));

    // Fetch the value for stored key
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbmHandle, k));
    sleep(sleepTime);

    // Fetch the value for stored key
    value = mdbm_fetch(mdbmHandle, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    switch (lockMode) {
        case 1: // Exclusive lock, fall thru
        case 2: // Shared lock, same as exclusive
            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbmHandle)); break;
        case 3: // Partitioned lock
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbmHandle, &k, 0)); break;
    }

    mdbm_close(mdbmHandle);
    sleep(sleepTime);

    return NULL;
}



// Test case - Mdbm is opened with an exclusive lock. Write using one thread and
// read using other thread
void TestLockFuncSuite::test_lock_func_11() {
    const string mdbm_name  = _testDir + "/test11.mdbm";
    const char* db_name = mdbm_name.c_str();
    int numThreads = 3;
    std::vector<pthread_t> tId;
    std::vector<struct threadData> threadDataArray;
    tId.resize(numThreads);
    threadDataArray.resize(numThreads);
    lockMode = 1;

    for (int i = 0; i < numThreads; i++) {
        // Create Mdbm per thread
        MDBM* mdbm = mdbm_open(db_name, _flagsExLock, 0600, _pageSize, _initialDbSize);
        CPPUNIT_ASSERT(NULL != mdbm);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

        threadDataArray[i].mdbmHandle = mdbm;
        pthread_create(&tId[i], NULL, runThread2, (void*)&threadDataArray[i]);
    }

    for (int j = 0; j < numThreads; j++) {
        pthread_join(tId[j], NULL);
    }

    lockMode = 0;
}

// Test case - Mdbm is locked using shared lock. Use mdbm_trylock_smart()
// to lock Mdbm and call mdbm_get_lockmode() to verify the mode
void TestLockFuncSuite::test_lock_func_12() {
    const string mdbm_name  = _testDir + "/test12.mdbm";
    const char* db_name = mdbm_name.c_str();

    int flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_CREATE_V3|MDBM_RW_LOCKS;
    // Create Mdbm
    MDBM* mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != mdbm);

    datum key;
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
    uint32_t mode = mdbm_get_lockmode(mdbm);
    CPPUNIT_ASSERT(mode == MDBM_RW_LOCKS);

    mdbm_close(mdbm);
}

// Test case - Mdbm is locked using partitioned lock. Use mdbm_trylock_smart()
// to lock Mdbm and call mdbm_get_lockmode() to verify the mode
// Call mdbm_lock_reset() to reset locks and call mdbm_islocked() to verify that
// mdbm_lock_reset restes all locks
void TestLockFuncSuite::test_lock_func_13() {
    const string mdbm_name  = _testDir + "/test13.mdbm";
    const char* db_name = mdbm_name.c_str();

    int flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDONLY|MDBM_CREATE_V3|MDBM_PARTITIONED_LOCKS;
    // Create Mdbm
    MDBM* mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != mdbm);

    datum key;
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
    uint32_t mode = mdbm_get_lockmode(mdbm);
    CPPUNIT_ASSERT(mode == MDBM_PARTITIONED_LOCKS);
    //CPPUNIT_ASSERT_EQUAL(0, mdbm_lock_reset(db_name, 0));
    CPPUNIT_ASSERT(0 == mdbm_islocked(mdbm));

    mdbm_close(mdbm);
}

// Test case - Mdbm is opened using exclusive lock. Use mdbm_trylock_smart()
// to lock Mdbm and call mdbm_get_lockmode() to verify the mode
void TestLockFuncSuite::test_lock_func_14() {
    const string mdbm_name  = _testDir + "/test14.mdbm";
    const char* db_name = mdbm_name.c_str();

    int flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDONLY|MDBM_CREATE_V3;
    // Create Mdbm
    MDBM* mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != mdbm);

    datum key;
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
    uint32_t mode = mdbm_get_lockmode(mdbm);
    CPPUNIT_ASSERT(mode == 0);

    mdbm_close(mdbm);
}

// Test case - Mdbm is opened w/o any lock. Use mdbm_trylock_smart()
// to lock Mdbm and call mdbm_get_lockmode() to verify the mode
void TestLockFuncSuite::test_lock_func_15() {
    const string mdbm_name  = _testDir + "/test15.mdbm";
    const char* db_name = mdbm_name.c_str();

    int flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDONLY|MDBM_CREATE_V3|MDBM_OPEN_NOLOCK;
    // Create Mdbm
    MDBM* mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != mdbm);

    datum key;
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
    uint32_t mode = mdbm_get_lockmode(mdbm);
    CPPUNIT_ASSERT(mode == MDBM_OPEN_NOLOCK);

    mdbm_close(mdbm);
}



/*
// Test case - Mdbm is opened with an shared lock. Write using one thread and
// read using other thread
void TestLockFuncSuite::test_lock_func_12() {
    const string mdbm_name  = _testDir + "/test12.mdbm";
    const char* db_name = mdbm_name.c_str();
    int numThreads = 3;
    pthread_t tId[numThreads];
    lockMode = 2;
    struct threadData threadDataArray[numThreads];

    for (int i = 0; i < numThreads; i++) {
        MDBM* mdbm;
        // Create Mdbm
        CPPUNIT_ASSERT((mdbm = mdbm_open(db_name, _flagsShared, 0600, _pageSize,
        _initialDbSize)) != NULL);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

        threadDataArray[i].mdbmHandle = mdbm;
        pthread_create(&tId[i], NULL, runThread2, (void*)&threadDataArray[i]);
    }

    for (int j = 0; j < numThreads; j++) {
        pthread_join(tId[j], NULL);
    }

    lockMode = 0;
}

// Test case - Mdbm is opened with an partitioned lock. Delete using one thread and
// read using other thread (keys in same partition)
void TestLockFuncSuite::test_lock_func_13() {
    const string mdbm_name  = _testDir + "/test13.mdbm";
    const char* db_name = mdbm_name.c_str();
    int numThreads = 3;
    pthread_t tId[numThreads];
    lockMode = 3;
    struct threadData threadDataArray[numThreads];

    for (int i = 0; i < numThreads; i++) {
        MDBM* mdbm;
        // Create Mdbm
        CPPUNIT_ASSERT((mdbm = mdbm_open(db_name, _flagsPartition, 0600, _pageSize,
        _initialDbSize)) != NULL);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

        threadDataArray[i].mdbmHandle = mdbm;
        pthread_create(&tId[i], NULL, runThread2, (void*)&threadDataArray[i]);
    }

    for (int j = 0; j < numThreads; j++) {
        pthread_join(tId[j], NULL);
    }

    lockMode = 0;
}
*/
