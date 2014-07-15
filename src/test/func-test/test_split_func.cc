/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-split-func.cc
//    Purpose: Functional tests for dynamic growth (split function)

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

class TestSplitFuncSuite : public CppUnit::TestFixture, public TestBase {
public:
  TestSplitFuncSuite(int vFlag) : TestBase(vFlag, "TestSplitFuncSuite") {}

  void setUp();
  void tearDown();
  void setEnv();


  void test_split_func_01();
  void test_split_func_02();
  void test_split_func_03();
  void test_split_func_04();
  void test_split_func_05();
  void test_split_func_06();
  void test_split_func_07();
  void test_split_func_08();
  void test_split_func_09();
  void test_split_func_09b();
  void test_split_func_10();
  void test_split_func_11();
  void test_split_func_12();

private:
  static int _pageSize;
  static int _initialDbSize;
  static mdbm_ubig_t _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static string _testDir;
  static int flags;
  static int _largeObjectFlags;

};

class TestSplitFunc : public TestSplitFuncSuite {
  CPPUNIT_TEST_SUITE(TestSplitFunc);

  CPPUNIT_TEST(setEnv);
  CPPUNIT_TEST(test_split_func_01);
  CPPUNIT_TEST(test_split_func_02);
  CPPUNIT_TEST(test_split_func_03);
  CPPUNIT_TEST(test_split_func_04);
  CPPUNIT_TEST(test_split_func_05);
  CPPUNIT_TEST(test_split_func_06);
  CPPUNIT_TEST(test_split_func_07);
  CPPUNIT_TEST(test_split_func_08);
  CPPUNIT_TEST(test_split_func_09);
  CPPUNIT_TEST(test_split_func_09b);
  CPPUNIT_TEST(test_split_func_10);
  CPPUNIT_TEST(test_split_func_11);
  CPPUNIT_TEST(test_split_func_12);

  CPPUNIT_TEST_SUITE_END();

public:
  TestSplitFunc() : TestSplitFuncSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestSplitFunc);

int TestSplitFuncSuite::_pageSize = 4096;
// MDBM_DBSIZE_MB enabling this flag for specifying Db size in Megabytes
int TestSplitFuncSuite::_initialDbSize = 4096*10;
// The limit size is in terms of number of pages
mdbm_ubig_t TestSplitFuncSuite::_limitSize = 120;
int TestSplitFuncSuite::_largeObjSize = 3075;
int TestSplitFuncSuite::_spillSize = 3072;
string TestSplitFuncSuite::_testDir = "";
int TestSplitFuncSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR
  |MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3;
int TestSplitFuncSuite::_largeObjectFlags = MDBM_O_CREAT|MDBM_O_FSYNC|
  MDBM_O_RDWR|MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3|MDBM_LARGE_OBJECTS;


// Helper functions
void TestSplitFuncSuite::setUp() { }

void  TestSplitFuncSuite::tearDown() { }

// This function is called to set the environment
void TestSplitFuncSuite::setEnv() {
  GetTmpDir(_testDir);
}

// Test case - Mdbm has created with max Db size. Store some records until
// page is full. If we try to store some more records on the same page then
// it calls split function and  size of Mdbm grows dynamically
void TestSplitFuncSuite::test_split_func_01() {
    const string mdbm_name  = _testDir + "/test01.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, oldMdbmSize = 0, newMdbmSize = 0;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm );
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store these records until Mdbm page is full. This number 254 is derived by trial.
    int max_records = 254;
    for (int i = 0; i < max_records; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }

    // Verify that the Mdbm has exactly X records
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm page is full. This store will call split function.
    // Existing Mdbm grows in size.
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) > oldMdbmSize);

    // Verify that the Mdbm has exactly X+1 records
    CPPUNIT_ASSERT_EQUAL(max_records+1, getNumberOfRecords(mdbm, k));

    // Verify that new value is added in Mdbm
    CPPUNIT_ASSERT_EQUAL(0, findRecord(mdbm, k, v));
}


// Test case 02 - Mdbm has created with max Db size. Store some records
// until page is full. If we try to store some more records on the same
// page then it calls split function and size of Mdbm grows dynamically.
// It will grow until max Db size has reached. After that store call will
// fail
void TestSplitFuncSuite::test_split_func_02() {
    const string mdbm_name  = _testDir + "/test02.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, oldMdbmSize = 0, newMdbmSize = 0;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store these records until Mdbm is full. This number of records is derived by trial.
    int max_records = 9982;
    for (int i = 0; i < max_records; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Verify that the Mdbm has exactly X records
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Now Mdbm is grown to it's max size and cannot grow further. So
    // attempt to store more data sould fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Verify that Mdbm size is not increased after last store
    CPPUNIT_ASSERT_EQUAL(oldMdbmSize, newMdbmSize);

    // Verify that the Mdbm has exactly X records.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));
}

// Test case 03 :  Store some records with some key (e.g., key1) into Mdbm.
// Verify that the size of Mdbm increases as expected. After the split
// function is called first time, add some more records with some
// different key (e.g., key2). Verify that this created 2 pages in Mdbm
// rather than just creating 1 very  big oversized page (as it creates in
// previous test cases) (last step is manual)
void TestSplitFuncSuite::test_split_func_03() {
    const string mdbm_name  = _testDir + "/test03.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1;

    // Create key-value pair
    k.dptr = (char*)("key1");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value1");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store these records until Mdbm page is full. This number 254 is derived by trial.
    int max_records = 254;
    for (int i = 0; i < max_records; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }

    // Verify that the Mdbm has exactly X records.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Verify that new value is added in Mdbm
    CPPUNIT_ASSERT_EQUAL(0, findRecord(mdbm, k, v));

    k.dptr = (char*)("key2");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value2");
    v.dsize = strlen(v.dptr);

    // Store these records until Mdbm is full.
    max_records = 4096;
    for (int j = 0; j < max_records; j++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }

    // Verify that the Mdbm has exactly 4096 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Verify that new value is added in Mdbm
    CPPUNIT_ASSERT_EQUAL(0, findRecord(mdbm, k, v));
}

// Test case 04 - Mdbm has created with max Db size. Store some
// records until page is full. If we try to store some more records
// on the same page then it calls split function and  size of Mdbm
// grows dynamically. It will grow until max Db size has reached.
// Delete a record and add another record with size < deleted record
// size. This should succeed.
void TestSplitFuncSuite::test_split_func_04() {
    const string mdbm_name  = _testDir + "/test04.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1;

    // Create key-value pair
    k.dptr =(char*)("key1");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value1");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store these records until Mdbm is full. This number 8872 is derived by trial.
    int max_records = 8872;
    for (int i = 0; i < max_records; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    v.dptr = (char*)("value1value1");
    v.dsize = strlen(v.dptr);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Creat a new record with size < deleted record
    v.dptr = (char*)("value1");
    v.dsize = strlen(v.dptr);

    // Add new record with size < deleted record size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));
}

// Test case 05 - Mdbm has created with max Db size. Store some records
// until page is full. If we try to store some more records on the same page
// then it calls split function and size of Mdbm grows dynamically. It will
// grow until max Db size has reached. Delete a record and add another record
// with size = deleted record size. This should succeed.
void TestSplitFuncSuite::test_split_func_05() {
    const string mdbm_name  = _testDir + "/test05.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1;

    // Create key-value pair
    k.dptr = (char*)("key1");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value1");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store these records until Mdbm is full. This number 8872 is derived by trial.
    int max_records = 8872;
    for (int i = 0; i < max_records; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }

    // Verify that the Mdbm has exactly 8871 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    v.dptr = (char*)("value1value1");
    v.dsize = strlen(v.dptr);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Creat a new record with size < deleted record
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Add new record with size < deleted record size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
}

// Test case 06 - Mdbm has created with max Db size. Store some records
// until page is full. If we try to store some more records on the same
// page then it calls split function and size of Mdbm grows dynamically.
// It will grow until max Db size has reached. Delete a record and add
// another record with size > deleted record size. This should succeed.
void TestSplitFuncSuite::test_split_func_06() {
    const string mdbm_name  = _testDir + "/test06.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1;

    // Create key-value pair
    k.dptr = (char*)("key1");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value1");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store these records until Mdbm is full. This number 8872 is derived by trial.
    int max_records = 8872;
    for (int i = 0; i < max_records; i++) {
        // Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm)) != 0);
    }


    // Verify that the Mdbm has exactly 8872 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    v.dptr = (char*)("value1value1");
    v.dsize = strlen(v.dptr);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Creat a new record with size < deleted record
    v.dptr = (char*)("value1value12");
    v.dsize = strlen(v.dptr);

    // Add new record with size < deleted record size
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 8872 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));
}

// Test case07 - Mdbm has created with max Db size. Store only one
// record as a large object. If we try to store some more records on
// the same page then it calls split function and  size of Mdbm grows
// dynamically
void TestSplitFuncSuite::test_split_func_07() {

    const string mdbm_name  = _testDir + "/test07.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int oldMdbmSize = 0, newMdbmSize = 0, size = 8176;
    char* largeObj = new char[size];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    for (int i = 0; i < size; i++) {
        largeObj[i] = 'a';
    }
    largeObj[size-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    //MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store a record having very large size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Verify that the Mdbm has exactly 1 record with this key.
    CPPUNIT_ASSERT_EQUAL(1, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm page is full. This store will call split function.
    // Existing Mdbm grows in size.
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) > oldMdbmSize);

    // Verify that the Mdbm has exactly 2 records with this key.
    CPPUNIT_ASSERT_EQUAL(2, getNumberOfRecords(mdbm, k));

    delete[] largeObj;
}


// Test case 08 - Mdbm has created with max Db size. Store some records
// with large objects until Mdbm has grown to max. size. Any attempt to store
// record after that should fail
void TestSplitFuncSuite::test_split_func_08() {
    const string mdbm_name  = _testDir + "/test08.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, oldMdbmSize = 0, newMdbmSize = 0;
    char* largeObj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create a value having very large size
    for (int i = 0; i < _largeObjSize; i++) {
        largeObj[i] = 'a';
    }
    largeObj[_largeObjSize-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store few values having large size until Mdbm is full
    int max_records = 112;
    for (int i = 0; i < max_records; i++) {
        // Store large object value into Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm has grown to max. size. Any store calls after this should fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    delete[] largeObj;
}


// Test case 09 - Mdbm has created with max Db size. Store some records
// with large objects until Mdbm has grown to max. size. Any attempt to
// store record after that should fail Delete a record from Mdbm and try to
// store a record of the same size. It should succeed
void TestSplitFuncSuite::test_split_func_09() {
    const string mdbm_name  = _testDir + "/test09.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int oldMdbmSize = 0;
    char* largeObj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create a value with large object
    for (int i = 0; i < _largeObjSize; i++) {
        largeObj[i] = 'a';
    }
    largeObj[_largeObjSize-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    int max_records = 112;
    // Store few records until Mdbm is full
    for (int i = 0; i < max_records; i++) {
        // Store large object value into Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Verify that the Mdbm has exactly X-1 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm has grown to max. size. Any store calls after this should fail
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    delete[] largeObj;
}

// Test case 09 - Mdbm has created with max Db size. Store some records
// with psuedo-large objects until Mdbm has grown to max. size. Any attempt to
// store record after that should fail Delete a record from Mdbm and try to
// store a record of the same size. It should succeed
void TestSplitFuncSuite::test_split_func_09b() {
    const string mdbm_name  = _testDir + "/test09b.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int oldMdbmSize = 0;
    char* largeObj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create a value with large object
    for (int i = 0; i < _largeObjSize; i++) {
        largeObj[i] = 'a';
    }
    largeObj[_largeObjSize-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Store few records until Mdbm is full
    int max_records = 51;
    for (int i = 0; i < max_records; i++) {
        // Store large object value into Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Verify that the Mdbm has exactly X-1 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm has grown to max. size. Any store calls after this should fail
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    delete[] largeObj;
}

// Test case 10 - Mdbm has created with max Db size. Store some records
// with large objects until Mdbm has grown to max. size. Any attempt to
// store record after that should fail. Delete a record from Mdbm and try
// to store a record of small size. It should succeed
void TestSplitFuncSuite::test_split_func_10() {
    const string mdbm_name  = _testDir + "/test10.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, oldMdbmSize = 0, newMdbmSize = 0;
    char* largeObj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create a large object value
    for (int i = 0; i < _largeObjSize; i++) {
        largeObj[i] = 'a';
    }
    largeObj[_largeObjSize-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    int max_records = 112;
    // Store few records until Mdbm is full
    for (int i = 0; i < max_records; i++) {
        // Store large object value into Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Verify that the Mdbm has exactly 49 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Create a value with size < large object size(deleted value size)
    for (int j = 0; j < 3072; j++) {
        largeObj[j] = 'a';
    }
    largeObj[3072] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Now Mdbm has grown to max. size. Any store calls after this should fail
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) != 0);
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Verify that the Mdbm has exactly 49 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Verify that new value is added in Mdbm
    ret = findRecord(mdbm, k, v);
    CPPUNIT_ASSERT_EQUAL(ret, 0);

    delete[] largeObj;
}

// Test case 11 - Mdbm has created with max Db size. Store some records with
// large objects until Mdbm has grown to max. size. Any attempt to store record
// after that should fail. Delete a record from Mdbm and try to store a record
// of large size. It should fail
void TestSplitFuncSuite::test_split_func_11() {
    const string mdbm_name  = _testDir + "/test11.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int ret = -1, oldMdbmSize = 0, newMdbmSize = 0;
    char* largeObj = new char[_pageSize+1];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large object
    for (int i = 0; i < _largeObjSize; i++) {
        largeObj[i] = 'a';
    }
    largeObj[_largeObjSize-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    int max_records = 112;
    // Store few records until Mdbm is full (reached to max. size).
    for (int i = 0; i < max_records; i++) {
        // Store large object value into Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Verify that the Mdbm has exactly 49 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Create a record with size > deleted record size
    for (int j = 0; j < _pageSize; j++) {
        largeObj[j] = 'a';
    }
    largeObj[_pageSize] = 0;
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Now Mdbm has grown to max. size. Any store calls after this should fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT_DUP)) != 0);

    // Verify that the Mdbm has exactly 49 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1, getNumberOfRecords(mdbm, k));

    delete[] largeObj;
}

// Test case 12 - Mdbm has created with max Db size. Store some records
// with large objects until Mdbm has grown to max. size. Any attempt to store
// record after that should fail. Delete a record from Mdbm and try to store
// few records of very small sizes. It should succeed
void TestSplitFuncSuite::test_split_func_12() {
    const string mdbm_name  = _testDir + "/test12.mdbm";
    const char* db_name = mdbm_name.c_str();
    datum k, v;
    int oldMdbmSize = 0, newMdbmSize = 0;
    char* largeObj = new char[_largeObjSize];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large object
    for (int i = 0; i < _largeObjSize; i++) {
        largeObj[i] = 'a';
    }
    largeObj[_largeObjSize-1] = '\0';
    v.dptr = largeObj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MdbmHolder mdbm = createMdbm(db_name, _largeObjectFlags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    int max_records = 112;
    // Store few records until Mdbm is full (reached to max. size).
    for (int i = 0; i < max_records; i++) {
        // Store large object value into Mdbm
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }

    // Verify that the Mdbm has exactly X records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records, getNumberOfRecords(mdbm, k));

    // Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Now Mdbm is full. Delete a key value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Verify that the Mdbm has exactly 49 records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1, getNumberOfRecords(mdbm, k));

    // Create value having very small size
    v.dptr = (char*)("abc");
    v.dsize = strlen(v.dptr);

    // Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm)) != 0);

    // Add few records of small size
    int max_small = 30;
    for (int j = 0; j < max_small; j++) {
        // Now Mdbm has grown to max. size. Any store calls after this should fail
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }

    // Verify that the Mdbm has exactly max-1+small records with this key.
    CPPUNIT_ASSERT_EQUAL(max_records-1+max_small, getNumberOfRecords(mdbm, k));

    delete[] largeObj;
}

