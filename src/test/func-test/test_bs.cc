/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-bs.cc
//
// Purpose: Functional tests for backing store utility

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

class BackingStoreFuncTestSuite : public CppUnit::TestFixture, public TestBase {
public:
  BackingStoreFuncTestSuite(int vFlag) : TestBase(vFlag, "BackingStoreFuncTestSuite") {}

  void setUp();
  void tearDown();
  void setEnv();

  void test_mdbm_bs_01();
  void test_mdbm_bs_02();
  void test_mdbm_bs_03();
  void test_mdbm_bs_04();
  void test_mdbm_bs_05();
  void test_mdbm_bs_06();
  void test_mdbm_bs_07();
  void test_mdbm_bs_08();
  void test_mdbm_bs_09();
  void test_mdbm_bs_10();
  void test_mdbm_bs_11();
  void test_mdbm_bs_12();
  void test_mdbm_bs_13();

private:
  static int _pageSize;
  static int _initialDbSize;
  static int _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static int _windowSize;
  static string _testDir;
  static int flags;
  static int flagsLO;
};


class BackingStoreFuncTest : public BackingStoreFuncTestSuite {
  CPPUNIT_TEST_SUITE(BackingStoreFuncTest);

  CPPUNIT_TEST(setEnv);

  CPPUNIT_TEST(test_mdbm_bs_01);
  CPPUNIT_TEST(test_mdbm_bs_02);
  CPPUNIT_TEST(test_mdbm_bs_03);
  CPPUNIT_TEST(test_mdbm_bs_04);
  CPPUNIT_TEST(test_mdbm_bs_05);
  CPPUNIT_TEST(test_mdbm_bs_06);
  CPPUNIT_TEST(test_mdbm_bs_07);
  CPPUNIT_TEST(test_mdbm_bs_08);
  CPPUNIT_TEST(test_mdbm_bs_09);
  CPPUNIT_TEST(test_mdbm_bs_10);
  CPPUNIT_TEST(test_mdbm_bs_11);
  CPPUNIT_TEST(test_mdbm_bs_12);
  CPPUNIT_TEST(test_mdbm_bs_13);

  CPPUNIT_TEST_SUITE_END();

public:
  BackingStoreFuncTest() : BackingStoreFuncTestSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(BackingStoreFuncTest);

int BackingStoreFuncTestSuite::_pageSize = 4096;
int BackingStoreFuncTestSuite::_initialDbSize = 4*4096;
int BackingStoreFuncTestSuite::_limitSize = 4;
int BackingStoreFuncTestSuite::_largeObjSize = 3073;
int BackingStoreFuncTestSuite::_spillSize = 3072;
int BackingStoreFuncTestSuite::_windowSize = 4096*4;
string BackingStoreFuncTestSuite::_testDir = "";
int BackingStoreFuncTestSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_CREATE_V3;
int BackingStoreFuncTestSuite::flagsLO = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_CREATE_V3|MDBM_LARGE_OBJECTS|MDBM_O_TRUNC;

void BackingStoreFuncTestSuite::setUp() {
}

void BackingStoreFuncTestSuite::tearDown() {
}

void BackingStoreFuncTestSuite::setEnv() {
    GetTmpDir(_testDir);
}

// Test case - Create and configure backing store. It should be set up without
// any error.(bs_01)
void BackingStoreFuncTestSuite::test_mdbm_bs_01() {
    const string bs_name  = _testDir + "/bs01.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache01.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT));

    mdbm_close(cache_db);
}

// Test case - Add some records to Mdbm and verify they are being added
// properly.(bs_02)
void BackingStoreFuncTestSuite::test_mdbm_bs_02() {
    const string bs_name  = _testDir + "/bs02.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache02.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT));

    mdbm_close(cache_db);
}

// Test case - Update/Modify existing record and verify that they are being
// modified peroperly.(bs_03)
void BackingStoreFuncTestSuite::test_mdbm_bs_03() {
    const string bs_name  = _testDir + "/bs03.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache03.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize, _initialDbSize,
        _limitSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Sore and fetch the record. Update same record and verify that its getting
    // updated correctly
    CPPUNIT_ASSERT_EQUAL(0, storeUpdateAndFetch(cache_db, MDBM_INSERT));

    mdbm_close(cache_db);
}

// Remove/Delete some records from existing Mdbm and verify that this happens
// as expected.(bs_04)
void BackingStoreFuncTestSuite::test_mdbm_bs_04() {
    const string bs_name  = _testDir + "/bs04.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache04.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize, _initialDbSize,
        _limitSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record. Delete same record and verify that its getting
    // deleted correctly
    CPPUNIT_ASSERT_EQUAL(0, storeUpdateAndFetch(cache_db, MDBM_INSERT));

    mdbm_close(cache_db);
}

// Store some record, delete it and store new record
void BackingStoreFuncTestSuite::test_mdbm_bs_05() {
    const string bs_name  = _testDir + "/bs05.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache05.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize, _initialDbSize,
        _limitSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record. Delete same record and verify that its getting
    // deleted correctly
    CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetch(cache_db, MDBM_INSERT));

    mdbm_close(cache_db);
}

// Store some record (LO) and fetch same record
void BackingStoreFuncTestSuite::test_mdbm_bs_06() {
    const string bs_name  = _testDir + "/bs06.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache06.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record. Delete same record and verify that its getting
    // deleted correctly
    //CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(cache_db, MDBM_INSERT, _largeObjSize));
    //CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT));

    mdbm_close(cache_db);
}

// Store some record (LO), udate same record
void BackingStoreFuncTestSuite::test_mdbm_bs_07() {
    const string bs_name  = _testDir + "/bs07.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache07.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record. Delete same record and verify that its getting
    // deleted correctly
    CPPUNIT_ASSERT_EQUAL(0, storeUpdateAndFetchLO(cache_db, MDBM_INSERT, _largeObjSize));

    mdbm_close(cache_db);
}

// Store some record (LO), delete it and store new record
void BackingStoreFuncTestSuite::test_mdbm_bs_08() {
    const string bs_name  = _testDir + "/bs08.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache08.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record. Delete same record and verify that its getting
    // deleted correctly
    CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(cache_db, MDBM_INSERT, _largeObjSize));

    mdbm_close(cache_db);
}

// Store some record (LO), replace it with new LO and verify the new value
void BackingStoreFuncTestSuite::test_mdbm_bs_09() {
    const string bs_name  = _testDir + "/bs09.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache09.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    datum k, v1, v2, value;
    char* large_obj = new char[_largeObjSize];

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v1.dptr = large_obj;
    v1.dsize = strlen(v1.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_INSERT));

    // Create large value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'b';
    }
    large_obj[_largeObjSize-1] = '\0';
    v2.dptr = large_obj;
    v2.dsize = strlen(v2.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v2, MDBM_REPLACE));

    // Fetch and verify that correct values are stored
    value = mdbm_fetch(cache_db, k);

    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v2.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(value.dsize, v2.dsize);

    mdbm_close(cache_db);
    delete[] large_obj;
}


// Store some record (LO), replace it with non-LO it and verify new value
void BackingStoreFuncTestSuite::test_mdbm_bs_10() {
    const string bs_name  = _testDir + "/bs10.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache10.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    datum k, v1, v2, value;
    char* large_obj = new char[_largeObjSize];

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v1.dptr = large_obj;
    v1.dsize = strlen(v1.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_INSERT));

    v2.dptr = (char*)("small value");
    v2.dsize = strlen(v2.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v2, MDBM_REPLACE));

    // Fetch and verify that correct values are stored
    value = mdbm_fetch(cache_db, k);

    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v2.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(value.dsize, v2.dsize);

    mdbm_close(cache_db);
    delete[] large_obj;
}


// Store some record (non- LO), replace it with LO it and verify new value
void BackingStoreFuncTestSuite::test_mdbm_bs_11() {
    const string bs_name  = _testDir + "/bs11.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache11.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    datum k, v1, v2, value;
    char* large_obj = new char[_largeObjSize];

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    v1.dptr = (char*)("small value");
    v1.dsize = strlen(v1.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_INSERT));

    // Create large value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v2.dptr = large_obj;
    v2.dsize = strlen(v2.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v2, MDBM_REPLACE));

    // Fetch and verify that correct values are stored
    value = mdbm_fetch(cache_db, k);

    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v2.dptr, value.dsize));
    CPPUNIT_ASSERT_EQUAL(value.dsize, v2.dsize);

    mdbm_close(cache_db);
    delete[] large_obj;
}


// Store multiple records (LO) with same key, and verify the values
void BackingStoreFuncTestSuite::test_mdbm_bs_12() {
    const string bs_name  = _testDir + "/bs12.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache12.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,
        _initialDbSize, _limitSize)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize,
        _limitSize, _spillSize, _windowSize)) != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    datum k, v1, v2, value;
    char* large_obj = new char[_largeObjSize];

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'a';
    }
    large_obj[_largeObjSize-1] = '\0';
    v1.dptr = large_obj;
    v1.dsize = strlen(v1.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_INSERT));

    // Create large value
    for (int i = 0; i < _largeObjSize; i++) {
        large_obj[i] = 'b';
    }
    large_obj[_largeObjSize-1] = '\0';
    v2.dptr = large_obj;
    v2.dsize = strlen(v2.dptr);

    // Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v2, MDBM_INSERT_DUP));

    // Fetch and verify that correct values are stored
    value = mdbm_fetch(cache_db, k);

    if (memcmp(value.dptr, v1.dptr, value.dsize) == 0)
        CPPUNIT_ASSERT_EQUAL(value.dsize, v1.dsize);
    else {
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v2.dptr, value.dsize));
        CPPUNIT_ASSERT_EQUAL(value.dsize, v2.dsize);
    }

    mdbm_close(cache_db);
    delete[] large_obj;
}

// Test case - Create small cache and configure backing store. Try to store records
void BackingStoreFuncTestSuite::test_mdbm_bs_13() {
    const string bs_name  = _testDir + "/bs13.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache13.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createCacheMdbm(cache_mdbm, flags, 512,
        512, 512)) != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, 1024,
        1024, 1024, _windowSize)) != NULL);

    //CPPUNIT_ASSERT((bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize,
    //   _initialDbSize, _limitSize, _windowSize)) != NULL);

   // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // Store and fetch the record
    //CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT));

    datum k, v, value;
    for (int i = 0; i  < 900; i++) {
       // Create key-value pair
       k.dptr = reinterpret_cast<char*>(&i);
       k.dsize = sizeof(i);
       v.dptr = reinterpret_cast<char*>(&i);
       v.dsize = sizeof(i);

       // Store new records
       CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v, MDBM_INSERT));

       // Fetch previously stored records
       value = mdbm_fetch(cache_db, k);
       CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
    }

    mdbm_close(cache_db);
}

