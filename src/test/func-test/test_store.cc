/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-store.cc
//    Purpose: Functional tests for store functionality. This file contains
//    test cases for store, modify, delete, fetch utility

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

class TestStoreSuite : public CppUnit::TestFixture, public TestBase {
    public:
    TestStoreSuite(int vFlag) : TestBase(vFlag, "TestStoreSuite") {}

    void setUp();
    void tearDown();
    void setEnv();

    void test_store_01();
    void test_store_02();
    void test_store_03();
    void test_store_04();
    void test_store_05();
    void test_store_06();
    void test_store_07();
    void test_store_08();
    void test_store_09();
    void test_store_10();
    void test_store_11();
    void test_store_12();
    void test_store_13();
    void test_store_14();
    void test_store_15();
    void test_store_16();
    void test_store_17();
    void test_store_18();
    void test_store_19();
    void test_store_20();
    void test_store_21();
    void test_store_22();
    void test_store_23();
    void test_store_24();

  private:
    static int _pageSize;
    static int _initialDbSize;
    static mdbm_ubig_t _limitSize;
    static int _largeObjSize;
    static int _spillSize;
    static string _testDir;
    static int flags;
};

class TestStore
    : public TestStoreSuite
{
    CPPUNIT_TEST_SUITE(TestStore);

    CPPUNIT_TEST(setEnv);

    CPPUNIT_TEST(test_store_01);
    CPPUNIT_TEST(test_store_02);
    CPPUNIT_TEST(test_store_03);
    CPPUNIT_TEST(test_store_04);
    CPPUNIT_TEST(test_store_05);
    CPPUNIT_TEST(test_store_06);
    CPPUNIT_TEST(test_store_07);
    CPPUNIT_TEST(test_store_08);
    CPPUNIT_TEST(test_store_09);
    CPPUNIT_TEST(test_store_10);
    CPPUNIT_TEST(test_store_11);
    CPPUNIT_TEST(test_store_12);
    CPPUNIT_TEST(test_store_13);
    CPPUNIT_TEST(test_store_14);
    CPPUNIT_TEST(test_store_15);
    CPPUNIT_TEST(test_store_16);
    CPPUNIT_TEST(test_store_17);
    CPPUNIT_TEST(test_store_18);
    CPPUNIT_TEST(test_store_19);
    CPPUNIT_TEST(test_store_20);
    CPPUNIT_TEST(test_store_21);
    CPPUNIT_TEST(test_store_22);
    CPPUNIT_TEST(test_store_23);
    CPPUNIT_TEST(test_store_24);
    CPPUNIT_TEST_SUITE_END();

    public:
    TestStore() : TestStoreSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestStore);

int TestStoreSuite::_pageSize = 4096;
int TestStoreSuite::_initialDbSize = 4096*64;
//int TestStoreSuite::_limitSize = 256;
mdbm_ubig_t TestStoreSuite::_limitSize = 128;
int TestStoreSuite::_largeObjSize = 4097;
int TestStoreSuite::_spillSize = 3072;
string TestStoreSuite::_testDir = "";
int TestStoreSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR| MDBM_CREATE_V3;

void TestStoreSuite::setUp() {
}

void TestStoreSuite::tearDown() {
}

void TestStoreSuite::setEnv() {
  GetTmpDir(_testDir);
}

// Test case - Create a Mdbm. Store some key-value pair and fetch it.
// Verify that the fetched value is same as the stored value
void TestStoreSuite::test_store_01() {
    const string mdbm_name  = _testDir + "/store01.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Sore and fetch the record
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT));

    // Close Mdbm
    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair. Modify the value
// of an existing key and fetch it. Verify that the fetched value is same
// as the newly stored value
void TestStoreSuite::test_store_02() {
    const string mdbm_name  = _testDir + "/store02.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Sore and fetch the record. Update same record and verify that its
    // getting updated properly
    CPPUNIT_ASSERT_EQUAL(0, storeUpdateAndFetch(mdbm, MDBM_INSERT));

    // Close Mdbm
    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair. Add the value to
// an existing key and fetch it. Verify fetched values
void TestStoreSuite::test_store_03() {
    MDBM_ITER mdbm_iter;
    MDBM_ITER_INIT(&mdbm_iter);
    const string mdbm_name  = _testDir + "/store03.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v1, v2;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v1.dptr = (char*)("value");
    v1.dsize = strlen(v1.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize,  NULL, NULL));
    // Add data(key-value pair) to Mdbm
    //CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v1, MDBM_INSERT, &mdbm_iter));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT));

    // Modify the value
    v2.dptr = (char*)("new value");
    v2.dsize = strlen(v2.dptr);

    // Add some other to an existing key
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v2, MDBM_INSERT_DUP, &mdbm_iter));

    datum  value;
    // Commenting this code since its not working. I think this is real bug.
    // Need to confirm with Carney
    // Filed ticket for this : Ticket# 5380795
    MDBM_ITER mdbm_iter1;
    MDBM_ITER_INIT(&mdbm_iter1);

    // Get first key-value pair
    kvpair firstKv = mdbm_first_r(mdbm, &mdbm_iter1);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKv.key.dptr, k.dptr, firstKv.key.dsize));
    CPPUNIT_ASSERT_EQUAL(k.dsize, firstKv.key.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKv.val.dptr, v1.dptr, firstKv.val.dsize));
    CPPUNIT_ASSERT_EQUAL(v1.dsize, firstKv.val.dsize);

    // Get first key
    datum firstKey = mdbm_firstkey_r(mdbm, &mdbm_iter1);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKey.dptr, k.dptr, firstKey.dsize));
    CPPUNIT_ASSERT_EQUAL(k.dsize, firstKey.dsize);

    // Get second key-value pair
    kvpair nextKv = mdbm_next_r(mdbm, &mdbm_iter1);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(nextKv.key.dptr, k.dptr, nextKv.key.dsize));
    CPPUNIT_ASSERT_EQUAL(k.dsize, nextKv.key.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(nextKv.val.dptr, v2.dptr, nextKv.val.dsize));
    CPPUNIT_ASSERT_EQUAL(v2.dsize, nextKv.val.dsize);

    for (int i = 0; i < 2; i++) {
        // Fetched the value for stored key
        CPPUNIT_ASSERT_EQUAL(0, mdbm_fetch_r(mdbm, &k, &value, &mdbm_iter1));

        // Verify that retrieved values are same as stored values
        if (i == 0) {
            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
            CPPUNIT_ASSERT_EQUAL(0, mdbm_delete_r(mdbm, &mdbm_iter1));
        }
        if (i == 1) {
            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v2.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v2.dsize, value.dsize);
        }

    }

    // Close Mdbm
    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair. Delete a key and
// try to fetch the value for same key
void TestStoreSuite::test_store_04() {
    const string mdbm_name  = _testDir + "/store04.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Sore and fetch the record. Delete same record and verify that its
    // getting deleted properly
    CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetch(mdbm, MDBM_INSERT));

    mdbm_close(mdbm);
}


// Test case - Create a Mdbm. Store some key-value pair. Delete value of a
// key and add value of the same key and fetch it. Verify fetched values.
void TestStoreSuite::test_store_05() {
    const string mdbm_name  = _testDir + "/store05.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Sore and fetch the record. Delete same record and verify that its getting
    // deleted properly
    CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetch(mdbm, MDBM_INSERT));

    // Sore and fetch the record.
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store multiple key-value pairs. Delete
// key-value pairs and try to fetch the value for same key
void TestStoreSuite::test_store_06() {
    const string mdbm_name  = _testDir + "/store06.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v1, v2, value;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v1.dptr = (char*)("value1");
    v1.dsize = strlen(v1.dptr);
    v2.dptr = (char*)("value2");
    v2.dsize = strlen(v2.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT));

    // Add data(2nd value to same key) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v2, MDBM_INSERT_DUP));

    // delete the key-value pair two times
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

    // Try to fetch the record for deleted key. This time it should
    // datum with NULL value
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store key-value pair with null key.
// Fetch the value for same key
void TestStoreSuite::test_store_07() {
    const string mdbm_name  = _testDir + "/store07.mdbm";
    const char* db_name = mdbm_name.c_str();
    int ret = -1;
    // Create key-value pair
    datum k, v, value;
    k.dptr = (char*)("");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT)) != 0);

    // try to fetch the record for deleted key. This time it should
    // datum with NULL value
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store key-value pair with null value.
// Fetch the value for same key
void TestStoreSuite::test_store_08() {
    const string mdbm_name  = _testDir + "/store08.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v, value;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // try to fetch the record for deleted key. This time it should
    // datum with NULL value
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store key-value pair with null key.
// Modify the key and store it. Fetch the value for same key
void TestStoreSuite::test_store_09() {
    const string mdbm_name  = _testDir + "/store09.mdbm";
    const char* db_name = mdbm_name.c_str();
    int ret = -1;
    // Create key-value pair
    datum k, v, value;
    k.dptr = (char*)("");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, k, v, MDBM_INSERT)) != 0);

    // Modify the key to store non-NULL key
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // try to fetch the record for same key.
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store key-value pair with some value.
// Replace the value with NULL. Fetch the value for same key
void TestStoreSuite::test_store_10() {
    const string mdbm_name  = _testDir + "/store10.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v, value;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // try to fetch the record for same key.
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

    // Replace the value with NULL
    v.dptr = (char*)("");
    v.dsize = strlen(v.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Try to fetch the record for deleted key. This time it should
    // datum with NULL value
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store key-value pair with some value.
// Replace the value with NULL. Fetch the value for same key
void TestStoreSuite::test_store_11() {
    const string mdbm_name  = _testDir + "/store11.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v, value;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("");
    v.dsize = strlen(v.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // Try to fetch the record for deleted key. This time it should
    // datum with NULL value
    value = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    // Replace the value with NULL
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // try to fetch the record for same key.
    value = mdbm_fetch(mdbm, k);

    // Verify the value
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

    mdbm_close(mdbm);
}

// To test mdbm_fetch_buf
void TestStoreSuite::test_store_12() {
    const string mdbm_name  = _testDir + "/store12.mdbm";
    const char* db_name = mdbm_name.c_str();

    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Create key-value pair
    datum k, v, buff, v1;
    char *buf = (char*)malloc(5);
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);
    buff.dptr = buf;
    buff.dsize = 5;

    // Create a Mdbm
    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_fetch_buf(mdbm, &k, &v1, &buff, 0));

    // Verify that the returned value is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, v1.dptr, v1.dsize));
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, buff.dptr, buff.dsize));
    CPPUNIT_ASSERT_EQUAL(buff.dsize, v.dsize);
    CPPUNIT_ASSERT_EQUAL(v1.dsize, v.dsize);

    mdbm_close(mdbm);
    free(buff.dptr);
}

// To test mdbm_fetch_info
void TestStoreSuite::test_store_13() {
    const string mdbm_name  = _testDir + "/store13.mdbm";
    const char* db_name = mdbm_name.c_str();

    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Create key-value pair
    MDBM_ITER iter;
    struct mdbm_fetch_info mdbm_info;
    datum k, v, buff, v1;
    char *buf = (char*)malloc(5);
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);
    buff.dptr = buf;
    buff.dsize = 5;

    MDBM_ITER_INIT(&iter);

    // Create a Mdbm
    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // Check the return value with dev
    CPPUNIT_ASSERT_EQUAL(0, mdbm_fetch_info(mdbm, &k, &v1, &buff, &mdbm_info, &iter));

    mdbm_close(mdbm);
    free(buff.dptr);
}

// To test mdbm_fetch_str
void TestStoreSuite::test_store_14() {
    const string mdbm_name  = _testDir + "/store14.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Create key-value pair
    datum k, v, v1;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr) + 1;
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr) + 1;

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

    // Fetch value of stored key and verify that it is same as stored value
    v1.dptr = mdbm_fetch_str(mdbm, k.dptr);
    v1.dsize = strlen(v1.dptr) + 1;
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, v1.dptr, v1.dsize));

    mdbm_close(mdbm);
}

// To test mdbm_store_str and mdbm_delete_str
void TestStoreSuite::test_store_15() {
    const string mdbm_name  = _testDir + "/store15.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Create key-value pair
    datum k, v, v1;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr) + 1;
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr) + 1;

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_str(mdbm, k.dptr, v.dptr, MDBM_INSERT));

    // Fetch value of stored key and verify that it is same as stored value
    v1.dptr = mdbm_fetch_str(mdbm, k.dptr);
    v1.dsize = strlen(v1.dptr) + 1;
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, v1.dptr, v1.dsize));

    // Fetch value of stored key and verify that it is same as stored value
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete_str(mdbm, k.dptr));

    // Try to fetch the record for deleted key. This time it should
    // datum with NULL value
    v1 = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, v1.dsize);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_chk_all_page(mdbm));

    mdbm_close(mdbm);
}


// To test mdbm_store_str and mdbm_delete_str
void TestStoreSuite::test_store_16() {
    const string mdbm_name  = _testDir + "/store16.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Create key-value pair
    datum k, v, v1;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr) + 1;
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr) + 1;

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_str(mdbm, k.dptr, v.dptr, MDBM_INSERT));

    // Fetch value of stored key and verify that it is same as stored value
    v1.dptr = mdbm_fetch_str(mdbm, k.dptr);
    v1.dsize = strlen(v1.dptr) + 1;
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, v1.dptr, v1.dsize));

    // Truncate Db
    mdbm_truncate(mdbm);

    // Try to fetch the record. This time it should datum with NULL value
    v1 = mdbm_fetch(mdbm, k);

    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, v1.dsize);

    mdbm_close(mdbm);
}

// To test mdbm_sethash
void TestStoreSuite::test_store_17() {
    const string mdbm_name  = _testDir + "/store17.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize)) != NULL);

    // Hash value < 0. It should fail
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_sethash(mdbm, -1));

    // Hash value > 10. It should fail
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_sethash(mdbm, 11));

    // Create key-value pair
    datum k, v;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr) + 1;
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr) + 1;

    for (int i = 0; i <= 10; i++) {
        // Hash value > 0 and < 10. It should succeed
        CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, i));

        // Verify the hash function
        CPPUNIT_ASSERT_EQUAL(i, mdbm_get_hash(mdbm));

        // Store records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

        // Fetched the value for stored key
        datum value = mdbm_fetch(mdbm, k);

        // Verify that fetched value is same as stored value
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
        CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

        // Truncate Db
        mdbm_truncate(mdbm);
    }

    mdbm_close(mdbm);
}

// To test mdbm_setspillsize
void TestStoreSuite::test_store_18() {
    const string mdbm_name  = _testDir + "/store18.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize)) != NULL);

    // Hash value > 0 and < 10. It should succeed
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, 5));

    // Spill size > page size. It should fail
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_setspillsize(mdbm, _pageSize + 1));

    // Spill size = page size. It should fail
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_setspillsize(mdbm, _pageSize));

    mdbm_close(mdbm);
}

// To test mdbm_get_limit_size and mdbm_get_page_size
void TestStoreSuite::test_store_19() {
    const string mdbm_name  = _testDir + "/store19.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize)) != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Hash value > 0 and < 10. It should succeed
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, 5));

    // Get limit size of Mdbm. Need to confirm this with dev
    int size =  mdbm_get_limit_size(mdbm);
    cout<<"Size of Mdbm : "<<size<<endl;

    // Filed ticket for this - Ticket 5654136
    //CPPUNIT_ASSERT_EQUAL(_limitSize*_pageSize, size);

    // Get limit size of Mdbm
    CPPUNIT_ASSERT_EQUAL(4096, mdbm_get_page_size(mdbm));

    mdbm_close(mdbm);
}

// To test mdbm_get_version and mdbm_check
void TestStoreSuite::test_store_20() {
    const string mdbm_name  = _testDir + "/store20.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db_name, flags, 0600, _pageSize, _initialDbSize)) != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Set Hash value
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, 5));

    // Verify mdbm_check
    CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 0, 0));

    // Check Mdbm version
    CPPUNIT_ASSERT_EQUAL(static_cast<uint32_t>(3), mdbm_get_version(mdbm));

    mdbm_close(mdbm);
}


// Test case - Create a Mdbm. Store multiple key-value pairs.
// Use mdbm_firstkey() and verify that it returns correct (1st) key.
// Delete first key-value pairs and use mdbm_firstkey() to check
// if return correct (2nd) key
void TestStoreSuite::test_store_21() {
/**
 * This code is not working with Mdbm V4. Need to revisit this
    const string mdbm_name  = _testDir + "/store21.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k1, k2, v1, v2;
    k1.dptr = (char*)("key1");
    k1.dsize = strlen(k1.dptr);
    k2.dptr = (char*)("key2");
    k2.dsize = strlen(k2.dptr);
    v1.dptr = (char*)("value1");
    v1.dsize = strlen(v1.dptr);
    v2.dptr = (char*)("value2");
    v2.dsize = strlen(v2.dptr);

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    // Since Db is empty mdbm_firstkey() should return NULL
    datum firstKey = mdbm_firstkey(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKey.dptr, NULL, firstKey.dsize));
    CPPUNIT_ASSERT_EQUAL(0, firstKey.dsize);

    // Verify that mdbm_first returns correct key-value pair
    kvpair kvpair1 = mdbm_first(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair1.key.dptr, NULL, kvpair1.key.dsize));
    CPPUNIT_ASSERT_EQUAL(0, kvpair1.key.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair1.val.dptr, NULL, kvpair1.val.dsize));
    CPPUNIT_ASSERT_EQUAL(0, kvpair1.val.dsize);

    // Verify that mdbm_first returns correct key-value pair
    kvpair kvpair2 = mdbm_next(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair2.key.dptr, NULL, kvpair2.key.dsize));
    CPPUNIT_ASSERT_EQUAL(0, kvpair2.key.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair2.val.dptr, NULL, kvpair2.val.dsize));
    CPPUNIT_ASSERT_EQUAL(0, kvpair2.val.dsize);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k1, v1, MDBM_INSERT));

    // Add data(2nd key value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k2, v2, MDBM_INSERT_DUP));

    // Verify that mdbm_first returns correct key-value pair
    kvpair1 = mdbm_first(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair1.key.dptr, k1.dptr, kvpair1.key.dsize));
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair1.val.dptr, v1.dptr, kvpair1.val.dsize));

    // Verify that mdbm_first returns correct key-value pair
    kvpair2 = mdbm_next(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair2.key.dptr, k2.dptr, kvpair2.key.dsize));
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair2.val.dptr, v2.dptr, kvpair2.val.dsize));

    // Verify that mdbm_firstkey() returns correct key (k1)
    firstKey = mdbm_firstkey(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKey.dptr, k1.dptr, firstKey.dsize));
    CPPUNIT_ASSERT_EQUAL(k1.dsize, firstKey.dsize);

    // Verify that mdbm_firstkey() returns correct key (k1)
    datum nextKey = mdbm_nextkey(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(nextKey.dptr, k2.dptr, nextKey.dsize));
    CPPUNIT_ASSERT_EQUAL(k2.dsize, nextKey.dsize);

    // delete first key-value pair
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k1));

    // Verify that mdbm_firstkey() returns correct key (k2)
    firstKey = mdbm_firstkey(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKey.dptr, k2.dptr, firstKey.dsize));
    CPPUNIT_ASSERT_EQUAL(k2.dsize, firstKey.dsize);

    // Verify that mdbm_first returns correct key-value pair
    kvpair1 = mdbm_first(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair1.key.dptr, k2.dptr, kvpair1.key.dsize));
    CPPUNIT_ASSERT_EQUAL(0, memcmp(kvpair1.val.dptr, v2.dptr, kvpair1.val.dsize));

    mdbm_close(mdbm);
**/
}

// Test case to test mdbm_get_page and mdbm_dump_page
void TestStoreSuite::test_store_22() {
    const string mdbm_name  = _testDir + "/store22.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v1;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v1.dptr = (char*)("value");
    v1.dsize = strlen(v1.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize,  NULL, NULL));
    MDBM *mdbm1 = NULL;
    CPPUNIT_ASSERT_EQUAL(-1, int(mdbm_get_page(mdbm1, &k)));

    datum* k1 = NULL;
    CPPUNIT_ASSERT_EQUAL(-1, int(mdbm_get_page(mdbm1, k1)));


    //int p;
    //datum k2;
    //k2.dptr = (char*)("");
    //k2.dsize = 0;
    // It should return -1. Ticket : 5669602
    //  This code is not working with Mdbm V4. Need to revisit this
    //CPPUNIT_ASSERT_EQUAL(-1, int(mdbm_get_page(mdbm1, &k2)));

    //datum k3;
    //k3.dptr = NULL;
    //k3.dsize = 0;
    // It should return -1. Ticket : 5669602
    // This code is not working with Mdbm V4. Need to revisit this
    //CPPUNIT_ASSERT_EQUAL(-1, int(mdbm_get_page(mdbm1, &k3)));

    for (int i = 0 ; i < 1000; i++) {
        // Store records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT_DUP));

        // Fetched the value for stored key
        datum value = mdbm_fetch(mdbm, k);

        // Verify that fetched value is same as stored value
        CPPUNIT_ASSERT_EQUAL(0, strncmp(value.dptr, v1.dptr, value.dsize));
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);

        //mdbm_ubig_t pgNo;
        //int db = mdbm_get_size(mdbm);
        //cout<<endl<<"Db size = "<<db<<"  and i = "<<i<<endl;

        // Get page number and make sure that it does not return -1
        CPPUNIT_ASSERT(mdbm_get_page(mdbm, &k) != (mdbm_ubig_t)-1);
    }


    {
      StdoutDiverter diverter("/dev/null");
      int pg = (int)mdbm_get_page(mdbm, &k);
      mdbm_dump_page(mdbm, pg);
    }

    mdbm_close(mdbm);
}

// Set initial Db size = max Db size (limit size) and try to store more
// records. This fails. Ticket :
void TestStoreSuite::test_store_23() {
    const string mdbm_name  = _testDir + "/store23.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v1;
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v1.dptr = (char*)("value");
    v1.dsize = strlen(v1.dptr);

    // Create a Mdbm as cache
    int dbSize = 4096*64;
    int maxDbSize = 64;
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, dbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, maxDbSize,  NULL, NULL));

    // This fails after 254 records
    for (int i = 0 ; i < 1000; i++) {
        if (i <= 253) {
            // Store records
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT_DUP));

            // Fetched the value for stored key
            datum value = mdbm_fetch(mdbm, k);

            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        } else {
            // Store records
            CPPUNIT_ASSERT_EQUAL(-1, mdbm_store(mdbm, k, v1, MDBM_INSERT_DUP));
        }
    }

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pair. Add the value to
// an existing key and fetch it. Verify fetched values
void TestStoreSuite::test_store_24() {
    MDBM_ITER mdbm_iter;
    MDBM_ITER_INIT(&mdbm_iter);
    const string mdbm_name  = _testDir + "/store24.mdbm";
    const char* db_name = mdbm_name.c_str();
    // Create key-value pair
    datum k, v1;
    k.dptr = (char*)("firstkey");
    k.dsize = strlen(k.dptr);
    v1.dptr = (char*)("value");
    v1.dsize = strlen(v1.dptr);

    // Create a Mdbm as cache
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize,  NULL, NULL));
    // Add data (key-value pair) to Mdbm
    //CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v1, MDBM_INSERT, &mdbm_iter));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT));

    // Modify the value
    datum v2;
    v2.dptr = (char*)("new value");
    v2.dsize = strlen(v2.dptr);

    // Add some other to an existing key
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store_r(mdbm, &k, &v2, MDBM_INSERT_DUP, &mdbm_iter));

    datum firstKey1 = mdbm_firstkey(mdbm);
    cout<<"First key -> "<<firstKey1.dptr<<endl;

    datum  value;
    // Commenting this code since its not working. I think this is real bug.
    // Need to confirm with Carney
    // Filed ticket for this : Ticket# 5380795
    MDBM_ITER mdbm_iter1;
    MDBM_ITER_INIT(&mdbm_iter1);

    // Get first key-value pair
    kvpair firstKv = mdbm_first_r(mdbm, &mdbm_iter1);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKv.key.dptr, k.dptr, firstKv.key.dsize));
    CPPUNIT_ASSERT_EQUAL(k.dsize, firstKv.key.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKv.val.dptr, v1.dptr, firstKv.val.dsize));
    CPPUNIT_ASSERT_EQUAL(v1.dsize, firstKv.val.dsize);

    // Get first key
    datum firstKey = mdbm_firstkey_r(mdbm, &mdbm_iter1);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(firstKey.dptr, k.dptr, firstKey.dsize));
    CPPUNIT_ASSERT_EQUAL(k.dsize, firstKey.dsize);

    // Get second key-value pair
    kvpair nextKv = mdbm_next_r(mdbm, &mdbm_iter1);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(nextKv.key.dptr, k.dptr, nextKv.key.dsize));
    CPPUNIT_ASSERT_EQUAL(k.dsize, nextKv.key.dsize);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(nextKv.val.dptr, v2.dptr, nextKv.val.dsize));
    CPPUNIT_ASSERT_EQUAL(v2.dsize, nextKv.val.dsize);

    for (int i = 0; i < 2; i++) {
        // Fetched the value for stored key
        CPPUNIT_ASSERT_EQUAL(0, mdbm_fetch_r(mdbm, &k, &value, &mdbm_iter1));

        // Verify that retrieved values are same as stored values
        if (i == 0) {
            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
            CPPUNIT_ASSERT_EQUAL(0, mdbm_delete_r(mdbm, &mdbm_iter1));
        }
        if (i == 1) {
            // Verify that fetched value is same as stored value
            CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v2.dptr, value.dsize));
            CPPUNIT_ASSERT_EQUAL(v2.dsize, value.dsize);
        }
    }

    mdbm_close(mdbm);
}
