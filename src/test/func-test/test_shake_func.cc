/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-shake-func.cc
//    Purpose: Functional tests for space reclamation (shake function) feature

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

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

#include <mdbm.h>
#include "TestBase.hh"

using namespace std;

class TestShakeFuncSuite : public CppUnit::TestFixture, public TestBase {
public:
  TestShakeFuncSuite(int vFlag) : TestBase(vFlag, "TestShakeFuncSuite") {}

  void setUp();
  void tearDown();
  void setEnv();

  void test_shake_func_01();
  void test_shake_func_02();
  void test_shake_func_03();
  void test_shake_func_04();
  void test_shake_func_05();
  void test_shake_func_06();
  void test_shake_func_07();
  void test_shake_func_08();
  void test_shake_func_09();
  void test_shake_func_10();
  void test_shake_func_11();

private:
  static int _pageSize;
  static int _initialDbSize;
  static mdbm_ubig_t _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static string _mdbmDir;
  static string _testDir;
  static int flags;
  static int _largeObjectFlags;
};

class TestShakeFunc : public TestShakeFuncSuite {
  CPPUNIT_TEST_SUITE(TestShakeFunc);

  CPPUNIT_TEST(setEnv);

  CPPUNIT_TEST(test_shake_func_01);
  CPPUNIT_TEST(test_shake_func_02);
  CPPUNIT_TEST(test_shake_func_03);
  CPPUNIT_TEST(test_shake_func_04);
  CPPUNIT_TEST(test_shake_func_05);
  CPPUNIT_TEST(test_shake_func_06);
  CPPUNIT_TEST(test_shake_func_07);
  CPPUNIT_TEST(test_shake_func_08);
  CPPUNIT_TEST(test_shake_func_09);
  CPPUNIT_TEST(test_shake_func_10);
  CPPUNIT_TEST(test_shake_func_11);

  CPPUNIT_TEST_SUITE_END();

public:
  TestShakeFunc() :TestShakeFuncSuite(MDBM_CREATE_V3) {}
};



CPPUNIT_TEST_SUITE_REGISTRATION(TestShakeFunc);

int TestShakeFuncSuite::_pageSize = 4096;
// MDBM_DBSIZE_MB enabling this flag for specifying Db size in Megabytes
int TestShakeFuncSuite::_initialDbSize = 4096*2;
// The limit size is in terms of number of pages
mdbm_ubig_t TestShakeFuncSuite::_limitSize = 5;
int TestShakeFuncSuite::_largeObjSize = 3075;
int TestShakeFuncSuite::_spillSize = 3072;
string TestShakeFuncSuite::_mdbmDir = "";
string TestShakeFuncSuite::_testDir = "";
int TestShakeFuncSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3;
int TestShakeFuncSuite::_largeObjectFlags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR
  |MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3|MDBM_LARGE_OBJECTS;


void TestShakeFuncSuite::setUp() {
}

void TestShakeFuncSuite::tearDown() {
}

void TestShakeFuncSuite::setEnv() {
  GetTmpDir(_testDir);
}

// Helper functions

// Shake function where nothing is marked for deletion
static int shake1(MDBM *mdbm, const datum* k, const datum* v, struct mdbm_shake_data_v3* d) {
  return 1;
}

// Shake function where few items are  marked for deletion
static int shake2 (MDBM *mdbm, const datum* k, const datum* v, struct mdbm_shake_data_v3* d) {
  for (int index = 0; index < 2; index++) {
      d->page_items[index].key.dsize = 0;
  }
  return 1;
}

// Shake function where one item is marked for deletion
static int shake3 (MDBM *mdbm, const datum* k, const datum* v, struct mdbm_shake_data_v3* d) {
  int index = 0;
  d->page_items[index].key.dsize = 0;
  return 1;
}

// Test case - Store entries > size and store should fail
void TestShakeFuncSuite::test_shake_func_01() {
  const string mdbm_name  = _testDir + "/test01.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create Mdbm
  MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
  CPPUNIT_ASSERT(NULL != mdbm);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, shake1, NULL));

  // Store these records until Mdbm is full. This number 254 is derived
  // by trial.
  for (int i = 0; i < 510; i++) {
      // Add data (key-value pair) to Mdbm
      CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  // Now Mdbm is full. This store will call shake function. But in shake1
  // we are not deleting any records so this call should fail
  CPPUNIT_ASSERT_EQUAL(-1, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  // Close Mdbm
  mdbm_close(mdbm);
}

// Test case - Store entries till Mdbm is full. Delete some of them.
// Try to store some more records with size < deleted records size.
// It should succeed
void TestShakeFuncSuite::test_shake_func_02() {
    const string mdbm_name  = _testDir + "/test02.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create Mdbm
    MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, shake2, NULL));

    // Store these records until Mdbm is full. This number 254 is derived
    // by trial.
    for (int i = 0; i < 254; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    }

    datum k;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Verify that the Mdbm has exactly 254 records
    CPPUNIT_ASSERT_EQUAL(254, getNumberOfRecords(mdbm, k));

    // For this call shake2 fn will be called since Mdbm is full. Here only
    // one record  deleted (in shake2 fn) and key value pair of same size less
    // size is added
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

    // Verify that the Mdbm has exactly 254 records. After page is full shake2
    // is called and in that 2 records are marked for deletion. We are adding
    // 1 record and deleting 2 records, so number of records is 254
    CPPUNIT_ASSERT_EQUAL(253, getNumberOfRecords(mdbm, k));

    // Close Mdbm
    mdbm_close(mdbm);
}

// Test case - Store entries till Mdbm is full. Delete some of them. Try to
// store some  more records with size = deleted records size. It should succeed
void TestShakeFuncSuite::test_shake_func_03() {
    const string mdbm_name  = _testDir + "/test03.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 254 is derived
    // by trial.
    for (int i = 0; i < 254; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 254 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 254);

    // For this call shake3 fn will be called since Mdbm is full
    // Here only one key-value pair deleted (in shake3 fn) and key value
    //pair of  same size is added
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);

    // Verify that the Mdbm has exactly 254 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 254);

    int c = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(c, 0);

    // Close Mdbm
    mdbm_close(mdbm);
}

// Test case - Store entries till Mdbm is full. Delete some of them. Try
// to store some more records with size > deleted records size. It should
// fail
void TestShakeFuncSuite::test_shake_func_04() {
    const string mdbm_name  = _testDir + "/test04.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    // Hard coding values of initial Db size and max Db size. Because it will
    // be easy for this test cases if we don't have any space to create oversized  pages.
    // By having  page size = initial Db size = max. Db size we can acheive this
    MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, 4096);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, 1, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 254 is derived by trial.
    for (int i = 0; i < 254; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 254 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 254);

    // For this call shake3 fn will be called since Mdbm is full. Here only
    // one key-value pair deleted (in shake3 fn) and key value pair of
    // large size is added store will fail in this case
    v.dptr = (char*)("new largevalue");
    v.dsize = strlen(v.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 253 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 253);

    // Verify that new value is added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, -1);

    // Close Mdbm
    mdbm_close(mdbm);
}

// Test case - Store entries till Mdbm is full. Delete some of them. Try to
// store some more records with size > deleted records size. It should fail
void
TestShakeFuncSuite::test_shake_func_05() {
    const string mdbm_name  = _testDir + "/test05.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v, k1;
    int ret = -1, numOfRecords = 0;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Hard coding values of initial Db size and max Db size. Because it will
    // be easy for this test cases if we don't have any space to create
    // oversized pages. By having  page size = iniial Db size = max. Db size
    // we can acheive this
    MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, 4096);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, 1, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 254 is derived
    // by trial.
    for (int i = 0; i < 254; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 254 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 254);

    // For this call shake3 fn will be called since Mdbm is full
    // Here only one key-value pair deleted (in shake3 fn) and key value pair
    // of large size is added store will fail in this case. When 1 key-value
    // pair is deleted, 16 bytes are freed 8 bytes - "key" + "value" and 8
    // bytes for header information
    k1.dptr = (char*)("This key 17 bytes");
    k1.dsize = strlen(k1.dptr);
    v.dptr = (char*)("");
    v.dsize = strlen(v.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k1, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 253 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 253);

    // Verify that new value is not added in Mdbm
    ret = findRecord(mdbm, k1, v);
    CPPUNIT_ASSERT_EQUAL(ret, -1);

    // Close Mdbm
    mdbm_close(mdbm);
}

/*

LARGE OBJECTS

*/

// Test case - Store entries > size and store should fail
void TestShakeFuncSuite::test_shake_func_06() {
    const string mdbm_name  = _testDir + "/test06.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;
    char* large_obj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Large object
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }

    large_obj[_largeObjSize-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake1, NULL)) == 0);

    // Store these records until Mdbm is full. This number 3 is derived
    // by trial
    for (int i = 0; i < 3; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has3 exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Now Mdbm is full. This store will call shake function. But in
    // shake1 we are not deleting any records so this call should fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has3 exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}

// Test case - Store entries till Mdbm is full. Delete some of them.
// Try to store some more records with size < deleted records size. It
// should succeed
void TestShakeFuncSuite::test_shake_func_07() {
    const string mdbm_name  = _testDir + "/test07.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;
     char* large_obj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Large object
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 3 is derived by trial.
    for (int i = 0; i < 3; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    v.dptr = (char*)("small value");
    v.dsize = strlen(v.dptr);

    // For this call shake2 fn will be called since Mdbm is full. Here only
    // one key-value pair  deleted (in shake2 fn) and key value pair of same
    // size less size is added
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);

    // Verify that the Mdbm has exactly 4 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 4);

    // Verify that new value is not added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, 0);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}

// Test case - Store entries till Mdbm is full. Delete some of them. Try
// to store some  more records with size < spill size. It should succeed
void TestShakeFuncSuite::test_shake_func_08() {
    const string mdbm_name  = _testDir + "/test08.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;
    char* large_obj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Large object
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 3 is derived
    // by trial.
    for (int i = 0; i < 3; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Create a value of the size < spill size
    int size = 3071;
    char* small_obj = new char[size];
    for (int i = 0; i < size; i++) {
        small_obj[i] = 'z';
    }
    small_obj[size-1]=0;

    v.dptr = small_obj;
    v.dsize = strlen(v.dptr);

    // For this call shake3 fn will be called since Mdbm is full Here
    // only one key-value pair deleted (in shake3 fn) and key value pair
    // of same size is added
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);

    // Verify that the Mdbm has exactly 4 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 4);

    // Verify that new value is not added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, 0);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
    delete[] small_obj;
}

// Test case - Store entries till Mdbm is full. Delete some of them. Try
// to store some  more records with size = spill size. It should succeed
void TestShakeFuncSuite::test_shake_func_09() {
    const string mdbm_name  = _testDir + "/test09.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;
    char* large_obj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Large object
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 3 is derived
    // by trial.
    for (int i = 0; i < 3; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Create a value of the size = spill size (plus null)
    int size = 3072+1;
    char* small_obj = new char[size];
    for (int i = 0; i < size; i++) {
        small_obj[i] = 'z';
    }
    small_obj[size-1]=0;

    v.dptr = small_obj;
    v.dsize = strlen(v.dptr);

    // For this call shake3 fn will be called since Mdbm is full
    // Here only one key-value pair deleted (in shake3 fn) and key
    // value pair of same size is added
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Verify that new value is not added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, -1);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
    delete[] small_obj;
}

// Test case - Store entries till Mdbm is full. Delete some of them. Try
// to store some  more records with size > spill size. It should succeed
void TestShakeFuncSuite::test_shake_func_10() {
    const string mdbm_name  = _testDir + "/test10.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;
    char* large_obj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Large object
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 3 is derived
    // by trial
    for (int i = 0; i < 3; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Create a value of the size > spill size (i.e., Large object)
    int size = 3073;
    char* small_obj = new char[size];
    for (int i = 0; i < size; i++) {
        small_obj[i] = 'z';
    }
    small_obj[size-1]=0;

    v.dptr = small_obj;
    v.dsize = strlen(v.dptr);

    // For this call shake3 fn will be called since Mdbm is full. Here
    // only one key-value pair deleted (in shake3 fn) and key value pair
    // of same size is added
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Verify that new value is not added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, -1);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
    delete[] small_obj;
}


// Test case - Store entries till Mdbm is full. Delete some of them. Try to
// store some more  records with size > deleted records size. It should fail
void TestShakeFuncSuite::test_shake_func_11() {
    const string mdbm_name  = _testDir + "/test11.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, numOfRecords = 0;
    char* large_obj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create Large object
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create Mdbm
    MDBM *mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm, _limitSize, shake3, NULL)) == 0);

    // Store these records until Mdbm is full. This number 3 is derived
    // by trial.
    for (int i = 0; i < 3; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) == 0);
    }

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Create a value of the size > spill size (i.e., Large object)
    int size = 3075;
    char* new_obj = new char[size];
    for (int i = 0; i < size; i++) {
        new_obj[i] = 'z';
    }
    new_obj[size-1]=0;

    v.dptr = new_obj;
    v.dsize = strlen(v.dptr);

    // For this call shake3 fn will be called since Mdbm is full. Here only
    // one key-value pair deleted (in shake3 fn) and key value pair of
    // large size is added store will fail in this case
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 3 records
    numOfRecords = getNumberOfRecords(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 3);

    // Verify that new value is not added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, -1);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
    delete[] new_obj;
}


