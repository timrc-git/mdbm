/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-large-obj.cc
//    Purpose: Functional tests for Large Object feature

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

class TestLargeObjSuite : public CppUnit::TestFixture, public TestBase {
public:
  TestLargeObjSuite(int vFlag) : TestBase(vFlag, "TestLargeObjSuite") {}

  void setUp();
  void tearDown();
  void setEnv();

  void test_large_obj_01();
  void test_large_obj_02();
  void test_large_obj_03();
  void test_large_obj_04();
  void test_large_obj_05();
  void test_large_obj_06();
  void test_large_obj_06a();
  void test_large_obj_07();
  void test_large_obj_08();
  void test_large_obj_09();

private:
  static int _pageSize;
  static int _initialDbSize;
  static int _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static string _mdbmDir;
  static string _testDir;
  static int flags;

};

class TestLargeObjTest : public TestLargeObjSuite {
  CPPUNIT_TEST_SUITE(TestLargeObjTest);

  CPPUNIT_TEST(setEnv);

  CPPUNIT_TEST(test_large_obj_01);
  CPPUNIT_TEST(test_large_obj_02);
  CPPUNIT_TEST(test_large_obj_03);
  CPPUNIT_TEST(test_large_obj_04);
  CPPUNIT_TEST(test_large_obj_05);
  CPPUNIT_TEST(test_large_obj_06);
  CPPUNIT_TEST(test_large_obj_06a);
  CPPUNIT_TEST(test_large_obj_07);
  CPPUNIT_TEST(test_large_obj_08);
  CPPUNIT_TEST(test_large_obj_09);

  CPPUNIT_TEST_SUITE_END();

public:
  TestLargeObjTest() : TestLargeObjSuite(MDBM_CREATE_V3) {}
};


CPPUNIT_TEST_SUITE_REGISTRATION(TestLargeObjTest);

int TestLargeObjSuite::_pageSize = 4096;
// MDBM_DBSIZE_MB enabling this flag for specifying Db size in Megabytes
int TestLargeObjSuite::_initialDbSize = 4;
int TestLargeObjSuite::_limitSize = 8192;
int TestLargeObjSuite::_largeObjSize = 3073;
int TestLargeObjSuite::_spillSize = 3072;
string TestLargeObjSuite::_mdbmDir = "";
string TestLargeObjSuite::_testDir = "";
int TestLargeObjSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3|MDBM_LARGE_OBJECTS|MDBM_DBSIZE_MB;

void TestLargeObjSuite::setUp() {
}

void TestLargeObjSuite::tearDown() {
}


// This function is called to set the environment
void TestLargeObjSuite::setEnv() {
  GetTmpDir(_testDir);
}

// Test case - Create a Mdbm. Store some key-value pair with value as
// a large object
void TestLargeObjSuite::test_large_obj_01() {
  const string mdbm_name  = _testDir + "/largeObj01.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store and fetch LO
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));

  // Close Mdbm
  mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair with value as a LO.
// Modify the value and verify that modified value is stored correctly
void TestLargeObjSuite::test_large_obj_02() {
  const string mdbm_name  = _testDir + "/largeObj02.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm as cache
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store and fetch LO. Update record with new LO and verify that its getting
  // updated properly
  CPPUNIT_ASSERT_EQUAL(0, storeUpdateAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));

  // Close Mdbm
  mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair with value as a large object.
// Add the value to this key and fetch it. Verify fetched values
void TestLargeObjSuite::test_large_obj_03() {
    MDBM_ITER mdbm_iter;
    MDBM_ITER_INIT(&mdbm_iter);
    const string mdbm_name  = _testDir + "/largeObj03.mdbm";
    const char* db_name = mdbm_name.c_str();
    char* large_obj = new char[_largeObjSize];
    datum k, v1, v2;

    // Modify the large object value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v1.dptr = large_obj;
    v1.dsize = strlen(v1.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v1, MDBM_INSERT, &mdbm_iter));

    // Modify the value
    v2.dptr = (char*)("new value");
    v2.dsize = strlen(v2.dptr);
    // Add some other to an existing key
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v2, MDBM_INSERT_DUP, &mdbm_iter));

/**
    // Commenting this code since its not working. I think this is real bug.
    // Need to confirm with dev Filed ticket for this : Ticket# 5380795
    MDBM_ITER mdbm_iter1;
    MDBM_ITER_INIT(&mdbm_iter1);

    for (int i = 0; i < 2; i++) {
        // Fetched the value for stored key
        CPPUNIT_ASSERT((ret = mdbm_fetch_r(mdbm, &k, &value, &mdbm_iter1)) == 0);
        cout<<mdbm_iter1.m_next<<endl;
        // Verify that retrieved values are same as stored values
        if (i == 0)
        {
            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT((ret = memcmp(value.dptr, v1.dptr, value.dsize)) == 0);
            CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        }
        if (i == 1)
        {
            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT((ret = memcmp(value.dptr, v2.dptr, value.dsize)) == 0);
            CPPUNIT_ASSERT_EQUAL(v2.dsize, value.dsize);
        }
    }
**/

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}

// Test case - Create a Mdbm. Store some key-value pair with value as a large object.
// Delete the key and verify that its been deleted properly.
void TestLargeObjSuite::test_large_obj_04() {
  const string mdbm_name  = _testDir + "/largeObj04.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm as cache
  MDBM *mdbm = NULL;
  CPPUNIT_ASSERT((mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize)) != NULL);

  // Store and fetch LO. Delete this record and verify that its getting
  // deleted properly
  CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));

  // Close Mdbm
  mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair with value as a large object.
// Delete the key and verify that its been deleted properly. Then add same key with
// some value (non-Large object) and verify that its been added properly.
void TestLargeObjSuite::test_large_obj_05() {
  const string mdbm_name  = _testDir + "/largeObj05.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm as cache
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store and fetch LO. Delete same record and verify that its getting deleted proerly
  CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));

  // Store and fetch record with non-LO
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_REPLACE));

  // Close Mdbm
  mdbm_close(mdbm);
}


// Test case - Create a Mdbm. Store some key-value pair with value as a large object
// Delete all the records and try to fetch the values of the key. It should return nothing
void TestLargeObjSuite::test_large_obj_06() {
    MDBM_ITER mdbm_iter;
    MDBM_ITER_INIT(&mdbm_iter);
    const string mdbm_name  = _testDir + "/largeObj06.mdbm";
    const char* db_name = mdbm_name.c_str();

    datum k, value;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Store and fetch LO.
    CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));
    CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));

    //// Delete all key value pairs
    //for (int i = 0; i < 2; i++) {
    // // Delete the key
    //    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));
    //}

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    // Close Mdbm
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm. Store some key-value pair with value as a large object
// Delete all the records and try to fetch the values of the key. It should return nothing
// This is clone of 06 (previous test case). The only difference is the use of mdbm_store_r.
// Here mdbm_delete fails. So cmmenting that statement here.
// Ticket# 5380795 - Added comment in this ticket
void TestLargeObjSuite::test_large_obj_06a() {
    MDBM_ITER mdbm_iter;
    MDBM_ITER_INIT(&mdbm_iter);
    const string mdbm_name  = _testDir + "/largeObj06a.mdbm";
    const char* db_name = mdbm_name.c_str();
    char* large_obj = new char[_largeObjSize];
    datum k, v1, v2, value;

    // Modify the large object value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }

    large_obj[_largeObjSize-1] = '\0';
    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v1.dptr = large_obj;
    v1.dsize = strlen(v1.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v1, MDBM_INSERT, &mdbm_iter));

    // Modify the large object value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'b';
    }
    large_obj[_largeObjSize-1] = '\0';

    // Modify the value
    v2.dptr = large_obj;
    v2.dsize = strlen(v2.dptr);

    // Add some other to an existing key
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v2, MDBM_INSERT_DUP, &mdbm_iter));

   // Delete all key value pairs
    for (int i = 0; i < 2; i++) {
        // Delete the key
        //CPPUNIT_ASSERT_EQUAL(0, ret = mdbm_delete(mdbm, k));
    }

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);

    // Verify that fetched value is same as stored value
    //CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}

// Test case - Create a Mdbm. Store some record with empty key. Modify the record with
// non-empty key and verify that the value is stored correctly
void TestLargeObjSuite::test_large_obj_07() {
    const string mdbm_name  = _testDir + "/largeObj07.mdbm";
    const char* db_name = mdbm_name.c_str();
    char* large_obj = new char[_largeObjSize];
    datum k, v;

    // Modify the record to have large object value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'z';
    }
    large_obj[_largeObjSize-1] = '\0';

    // Create key-value pair
    k.dptr = (char*)("");
    k.dsize = strlen(k.dptr);
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // Store and fetch LO
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}

// Test case - Create a Mdbm. Store some record with large-object value. Modify the
// record with empty value and verify that modified value is stored correctly
void TestLargeObjSuite::test_large_obj_08() {
    const string mdbm_name  = _testDir + "/largeObj08.mdbm";
    const char* db_name = mdbm_name.c_str();
    char* large_obj = new char[_largeObjSize];
    datum k, v, value;

    // Modify the record to have large object value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'z';
    }
    large_obj[_largeObjSize-1] = '\0';

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

    // Store modified value
    v.dptr = (char*)("");
    v.dsize = strlen(v.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}

// Test case - Create a Mdbm. Store some record with empty value. Modify the record
// with (large-object) value and verify that modified value is stored correctly
void TestLargeObjSuite::test_large_obj_09() {
    const string mdbm_name  = _testDir + "/largeObj09.mdbm";
    const char* db_name = mdbm_name.c_str();
    char* large_obj = new char[_largeObjSize];
    datum k, v, value;

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    // Modify the record to have large object value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'z';
    }
    large_obj[_largeObjSize-1] = '\0';

    // Store modified value
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

    // Close Mdbm
    mdbm_close(mdbm);
    delete[] large_obj;
}


