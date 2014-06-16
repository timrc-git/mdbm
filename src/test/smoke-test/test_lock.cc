/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-lock.cc
// Purpose: Smoke tests for mdbm locking functionality

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>


#include <iostream>
#include <ostream>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"
#include "TestBase.hh"

#define EXCLUSIVE_LOCK 0
#define NTHREADS 3

using namespace std;

class SmokeTestLockSuite : public CppUnit::TestFixture, public TestBase {
public:
    SmokeTestLockSuite(int vFlag) : TestBase(vFlag, "SmokeTestLockSuite") {}
    void setUp();
    void tearDown();
    void initialSetup();
    // Smoke tests in this suite
    void smoke_test_lock_01();  //        TC 01
    void smoke_test_lock_02();  //        TC 02
    void smoke_test_lock_03();  //        TC 03
    void smoke_test_lock_04();  //        TC 04
    void smoke_test_lock_05();  //        TC 05
    void smoke_test_lock_06();  //        TC 06
    void smoke_test_lock_07();  //        TC 07
    void smoke_test_lock_08();  //        TC 08
    void smoke_test_lock_09();  //        TC 09
    void smoke_test_lock_10();  //        TC 10
    void smoke_test_lock_11();  //        TC 11
    void smoke_test_lock_12();  //        TC 12
    void smoke_test_lock_13();  //        TC 13
    void smoke_test_lock_14();  //        TC 14


    MDBM* create_mdbm(const string& mdbm_name,  int mode);
    void waitParent();
    void StoreFetchDelete(MDBM* mdbm_db, bool isParent, bool isDelete, bool isUpdate, int lockType);
    static void* ThreadStoreFetchDlete(void *arg);
    void lock_thread_tests(const string& mdbm_name, int mode);
    void lock_fork_tests(const string& mdbm_name, bool isDelete, bool isUpdate, int mode);

private:
    static string _basePath;
    static int _pageSize;
    static int _limitSize;
    static int _initialDbSize;
    static int _largeObjSize;
};

string SmokeTestLockSuite::_basePath = "";
int SmokeTestLockSuite::_pageSize = 4096;
int SmokeTestLockSuite::_initialDbSize = 4096*10;
int SmokeTestLockSuite::_limitSize = 120;

static int sleepTime = 0;
static MDBM* db_mdbm = NULL;
static int dblockType = EXCLUSIVE_LOCK;
static bool deleteEnabled = false;

void SmokeTestLockSuite::setUp() {
}

void SmokeTestLockSuite::tearDown() {
}

// Initial setup for all mdbm smoke tests
void SmokeTestLockSuite::initialSetup() {
    GetTmpDir(_basePath);
}

void SmokeTestLockSuite::smoke_test_lock_01() {
    string trprefix = "TC 01: Smoke Test DB Locking : exlusive lock, store and fetch: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_01.mdbm";

    lock_fork_tests(mdbm_name, false, false, EXCLUSIVE_LOCK);
}

void SmokeTestLockSuite::smoke_test_lock_02() {
    string trprefix = "TC 02: Smoke Test DB Locking : exlusive lock, store, fetch and delele: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_02.mdbm";

    lock_fork_tests(mdbm_name, true, false, EXCLUSIVE_LOCK);
}

void SmokeTestLockSuite::smoke_test_lock_03() {
    string trprefix = "TC 03: Smoke Test DB Locking : exlusive lock, store, fetch and update: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_03.mdbm";

    lock_fork_tests(mdbm_name, false, true, EXCLUSIVE_LOCK);
}

void SmokeTestLockSuite::smoke_test_lock_04() {
    string trprefix = "TC 04: Smoke Test DB Locking : Shared lock, store and fetch: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_04.mdbm";

    lock_fork_tests(mdbm_name, false, false, MDBM_RW_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_05() {
    string trprefix = "TC 05: Smoke Test DB Locking : Shared lock, store, fetch and delele: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_05.mdbm";

    lock_fork_tests(mdbm_name, true, false, MDBM_RW_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_06() {
    string trprefix = "TC 06: Smoke Test DB Locking : Shared lock, store, fetch and update: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_06.mdbm";

    lock_fork_tests(mdbm_name, false, true, MDBM_RW_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_07() {
    string trprefix = "TC 07: Smoke Test DB Locking : Partition lock, store and fetch: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_07.mdbm";

    lock_fork_tests(mdbm_name, false, false, MDBM_PARTITIONED_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_08() {
    string trprefix = "TC 08: Smoke Test DB Locking : partition lock, store, fetch and delele: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_08.mdbm";

    lock_fork_tests(mdbm_name, true, false, MDBM_PARTITIONED_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_09() {
    string trprefix = "TC 09: Smoke Test DB Locking : Partition lock, store, fetch and update: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_09.mdbm";

    lock_fork_tests(mdbm_name, false, true, MDBM_PARTITIONED_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_10() {
    string trprefix = "TC 10: Smoke Test DB Locking : Thread exlusive lock, store and fetch: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_10.mdbm";

    dblockType = EXCLUSIVE_LOCK;
    deleteEnabled = false;
    db_mdbm = NULL;

    lock_thread_tests(mdbm_name, EXCLUSIVE_LOCK);
}

void SmokeTestLockSuite::smoke_test_lock_11() {
    string trprefix = "TC 11: Smoke Test DB Locking : Thread Shared lock, store and fetch: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_11.mdbm";

    dblockType = MDBM_RW_LOCKS;
    deleteEnabled = false;
    db_mdbm = NULL;

    lock_thread_tests(mdbm_name, MDBM_RW_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_12() {
    string trprefix = "TC 12: Smoke Test DB Locking : Thread Partition lock, store and fetch: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_12.mdbm";

    dblockType = MDBM_PARTITIONED_LOCKS;
    deleteEnabled = false;

    lock_thread_tests(mdbm_name, MDBM_PARTITIONED_LOCKS);
}

void SmokeTestLockSuite::smoke_test_lock_13() {
    string trprefix = "TC 13: Smoke Test DB Locking : Thread exlusive lock, store, fetch and delete: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_13.mdbm";

    dblockType = EXCLUSIVE_LOCK;
    deleteEnabled = true;
    db_mdbm = NULL;

    lock_thread_tests(mdbm_name, EXCLUSIVE_LOCK);
}

void SmokeTestLockSuite::smoke_test_lock_14() {
    string trprefix = "TC 14: Smoke Test DB Locking : Thread Shared lock, store, fetch and delete: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_lock_14.mdbm";

    dblockType = MDBM_RW_LOCKS;
    deleteEnabled = true;
    db_mdbm = NULL;

    lock_thread_tests(mdbm_name, MDBM_RW_LOCKS);
}

void SmokeTestLockSuite::lock_thread_tests(const string& mdbm_name, int mode) {
    pthread_t thread_id[NTHREADS];
    db_mdbm = NULL;
    //Create mdbm
    db_mdbm = create_mdbm(mdbm_name, mode);

    for(int i = 0; i < NTHREADS; i++) {
        pthread_create(&thread_id[i], NULL, ThreadStoreFetchDlete, NULL);
    }

    for(int j = 0; j < NTHREADS; j++) {
        pthread_join(thread_id[j], NULL);
    }
    mdbm_close(db_mdbm);
}

void SmokeTestLockSuite::lock_fork_tests(const string& mdbm_name, bool isDelete, bool isUpdate, int mode) {
    int pid = -1;

    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, mode);

    //Fork main process to create a child process
    pid = fork();

    CPPUNIT_ASSERT(pid >= 0);

    isUpdate = isDelete ? false : isUpdate;
    if (pid) {
        StoreFetchDelete(mdbm_db, true, isDelete, isUpdate, mode);
        waitParent();
    } else {
        StoreFetchDelete(mdbm_db, false, isDelete, isUpdate, mode);
        exit(EXIT_SUCCESS);
    }
}

MDBM* SmokeTestLockSuite::create_mdbm(const string& mdbm_name,  int mode) {
    MDBM *mdbm_db = NULL;
    int openflags, ret = -1;

    openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_FSYNC | versionFlag | mode;

    CPPUNIT_ASSERT((mdbm_db = mdbm_open(mdbm_name.c_str(), openflags, 0644, _pageSize, _initialDbSize)) != NULL);

    if (!mdbm_db) {
        stringstream msg;
        msg << mdbm_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(mdbm_db);
        return NULL;
    }

    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm_db, static_cast<mdbm_ubig_t>(_limitSize), NULL, NULL)) == 0);

    return mdbm_db;
}

//function for store, fetch and delete operations
void SmokeTestLockSuite::StoreFetchDelete(MDBM* mdbm_db, bool isParent, bool isDelete, bool isUpdate, int lockType) {
    datum dk, dv, dv1, value;
    int ret = -1;
    int MAX = 2000;

    if (lockType == MDBM_PARTITIONED_LOCKS)
       MAX = 2;

    //Create key value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);

    if (isParent) {
        dv.dptr = (char*)"parent";
        dv.dsize = sizeof(dv.dptr);

        dv1.dptr = (char*)"parent_new";
        dv1.dsize = sizeof(dv.dptr);
    } else {
        dv.dptr = (char*)"child";
        dv.dsize = sizeof(dv.dptr);

        dv1.dptr = (char*)"child_new";
        dv1.dsize = sizeof(dv.dptr);
    }
    for(int i = 0; i < MAX; i++) {
        //Lock Mdbm
        if (lockType == EXCLUSIVE_LOCK) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm_db));
        } else if (lockType == MDBM_RW_LOCKS) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbm_db));
        } else if (lockType == MDBM_PARTITIONED_LOCKS) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbm_db, &dk, 0));
        }

        //Store a record using parent process
        if(mdbm_store(mdbm_db, dk, dv, MDBM_REPLACE) == 0) {
            //Sleep
            sleep(sleepTime);

            //Fetch the value for stored key
            value = mdbm_fetch(mdbm_db, dk);

            sleep(sleepTime);
            //Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
            CPPUNIT_ASSERT(value.dptr != NULL);
            CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

            if (isDelete) {
                //delete the record
                if(mdbm_delete(mdbm_db, dk) == 0) {
                    //Fetch the value for stored key
                    value = mdbm_fetch(mdbm_db, dk);

                    //Verify that fetched value is same as zero after deletion
                    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
                    CPPUNIT_ASSERT(value.dptr == NULL);

                    //Verify that after delete, data base is empty
                    kvpair kv;
                    kv = mdbm_first(mdbm_db);
                    CPPUNIT_ASSERT(kv.key.dsize == 0);
                    CPPUNIT_ASSERT(kv.key.dptr == NULL);
                    CPPUNIT_ASSERT(kv.val.dsize == 0);
                    CPPUNIT_ASSERT(kv.val.dptr == NULL);
               }
            }

            if (isUpdate) {
                if (mdbm_store(mdbm_db, dk, dv1, MDBM_REPLACE) == 0) {
                   //Fetch the value for stored key
                   value = mdbm_fetch(mdbm_db, dk);

                   //Verify that fetched value is same as stored value
                   CPPUNIT_ASSERT_EQUAL(value.dsize, dv1.dsize);
                   CPPUNIT_ASSERT(value.dptr != NULL);
                   CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv1.dptr, value.dsize)) == 0);
                }
            }
        }

        //Unlock Mdbm
        if ((lockType == EXCLUSIVE_LOCK) || (lockType == MDBM_RW_LOCKS)) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm_db));
        } else if (lockType == MDBM_PARTITIONED_LOCKS) {
            CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm_db, &dk, 0));
        }

        // Sleep
        sleep(sleepTime);
    }
    return;
}


// Function to run the threads
void* SmokeTestLockSuite::ThreadStoreFetchDlete(void *arg) {
    //Get ID of calling thread
    pthread_t tid = pthread_self();
    datum dk, dv, value;

    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = reinterpret_cast<char*>(&tid);
    dv.dsize = sizeof(tid);

    //lock db depending on the mode
    switch (dblockType) {
       case EXCLUSIVE_LOCK:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(db_mdbm));
            break;
       case MDBM_RW_LOCKS:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(db_mdbm));
            break;
       case MDBM_PARTITIONED_LOCKS:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(db_mdbm, &dk, 0));
            break;
       default:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(db_mdbm));
     }


    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db_mdbm, dk, dv, MDBM_REPLACE));

    // Sleep
    sleep(sleepTime);

    // Fetch the value for stored key
    value = mdbm_fetch(db_mdbm, dk);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(dv.dsize, value.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, dv.dptr, value.dsize));

    if (deleteEnabled) {
        // delete the value for stored key
        if (mdbm_delete(db_mdbm, dk) == 0) {
            //Fetch the value for stored key
            value = mdbm_fetch(db_mdbm, dk);

            //Verify that fetched value is same as zero after deletion
            CPPUNIT_ASSERT_EQUAL(0, value.dsize);
            CPPUNIT_ASSERT(value.dptr == NULL);

            //Verify that after delete, data base is empty
            kvpair kv;
            kv = mdbm_first(db_mdbm);
            CPPUNIT_ASSERT(kv.key.dsize == 0);
            CPPUNIT_ASSERT(kv.key.dptr == NULL);
            CPPUNIT_ASSERT(kv.val.dsize == 0);
            CPPUNIT_ASSERT(kv.val.dptr == NULL);
        }
    }

    //unlock db
    switch (dblockType) {
       case EXCLUSIVE_LOCK:
       case MDBM_RW_LOCKS:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(db_mdbm));
            break;
       case MDBM_PARTITIONED_LOCKS:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(db_mdbm, &dk, 0));
            break;
       default:
            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(db_mdbm));
    }

    // Sleep
    sleep(sleepTime);

    return NULL;
}


//Function for waiting parent to exit all children
void SmokeTestLockSuite::waitParent() {
   pid_t pid_done;
   int status = -1;
   while (true) {
       pid_done = wait(&status);
       if (pid_done == -1) {
          if (errno == ECHILD)
             break; // no more child processes
       } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          cerr << "pid " << pid_done << " failed" << endl << flush;
          exit(1);
       }
   }
   return;
}


class MdbmSmokeTest : public SmokeTestLockSuite {
    CPPUNIT_TEST_SUITE(MdbmSmokeTest);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(smoke_test_lock_01);   // TC 01
      CPPUNIT_TEST(smoke_test_lock_02);   // TC 02
      CPPUNIT_TEST(smoke_test_lock_03);   // TC 03
//      CPPUNIT_TEST(smoke_test_lock_04);   // TC 04
//      CPPUNIT_TEST(smoke_test_lock_05);   // TC 05
//      CPPUNIT_TEST(smoke_test_lock_06);   // TC 06
      CPPUNIT_TEST(smoke_test_lock_07);   // TC 07
//    CPPUNIT_TEST(smoke_test_lock_08);   // TC 08
      CPPUNIT_TEST(smoke_test_lock_09);   // TC 09
      CPPUNIT_TEST(smoke_test_lock_10);   // TC 10
//    CPPUNIT_TEST(smoke_test_lock_11);   // TC 11
//    CPPUNIT_TEST(smoke_test_lock_12);   // TC 12
      CPPUNIT_TEST(smoke_test_lock_13);   // TC 13
//    CPPUNIT_TEST(smoke_test_lock_14);   // TC 14
CPPUNIT_TEST_SUITE_END();

public:
    MdbmSmokeTest() : SmokeTestLockSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmSmokeTest);


