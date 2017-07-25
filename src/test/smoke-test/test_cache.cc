/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-cache.cc
// Purpose: Smoke tests for mdbm cache functionality

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

#include "mdbm.h"
#include "TestBase.hh"

using namespace std;

static const char *LARGE_OBJ_KEY = "LargeObjKey";
static const int NUM_DATA_ITEMS = 456;
static const char * PREFIX  = "PRFX";
static const int STARTPOINT = 12345;

class SmokeTestCacheSuite : public CppUnit::TestFixture, public TestBase {
public:
    SmokeTestCacheSuite(int vFlag) : TestBase(vFlag, "SmokeTestCacheSuite") {}
    void setUp();
    void tearDown();
    void initialSetup();
    // Smoke tests in this suite
    void smoke_test_cache_01();  //        TC 01
    void smoke_test_cache_02();  //        TC 02
    void smoke_test_cache_03();  //        TC 03
    void smoke_test_cache_04();  //        TC 04
    void smoke_test_cache_05();  //        TC 05
    void smoke_test_cache_06();  //        TC 06
    void smoke_test_cache_07();  //        TC 07
    void smoke_test_cache_08();  //        TC 08
    void smoke_test_cache_09();  //        TC 09
    void smoke_test_cache_10();  //        TC 10
    void smoke_test_cache_11();  //        TC 11
    void smoke_test_cache_12();  //        TC 12

    int  StoreCacheLargeObj(MDBM *cache_db, int xobjNum = 0);
    void FetchCacheLargeObj(MDBM *cache_db, int key);
    void DeleteCacheLargeObj(MDBM *cache_db, int key);
    void cache_common_tests(MDBM *cache_db, bool modify);
    MDBM* create_mdbm_cache(const string& cache_name, int openflags, int mode);
    MDBM* create_mdbm_cache_40m(const string& cache_name, int openflags, int mode);
private:
    static string _basePath;
    static int _pageSize;
    static int _limitSize;
};

string SmokeTestCacheSuite::_basePath = "";
int SmokeTestCacheSuite::_pageSize = 64*1024;
int SmokeTestCacheSuite::_limitSize = 256;

void SmokeTestCacheSuite::setUp() {
}


void SmokeTestCacheSuite::tearDown() {
}

// Initial setup for all mdbm smoke tests
void SmokeTestCacheSuite::initialSetup() {
    GetTmpDir(_basePath);
}

void SmokeTestCacheSuite::smoke_test_cache_01() {
    string trprefix = "TC 01: Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_01.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;
    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    // fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
}

void SmokeTestCacheSuite::smoke_test_cache_02() {
    string trprefix = "TC 02: Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_02.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
    //Modify data
    CPPUNIT_ASSERT(0 == InsertData(cache_db, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT, true, 0 , MDBM_REPLACE, 1));
    //fetch and verify data
    CPPUNIT_ASSERT(0 == VerifyData(cache_db,DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT, true, 0, 1));
}

void SmokeTestCacheSuite::smoke_test_cache_03() {
    string trprefix = "TC 03 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_03.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    /*common tests */
    cache_common_tests(cache_db, false);
}

void SmokeTestCacheSuite::smoke_test_cache_04() {
    string trprefix = "TC 04 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_04.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;
    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //common tests
    cache_common_tests(cache_db, true);
}

void SmokeTestCacheSuite::smoke_test_cache_05() {
    string trprefix = "TC 05 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_05.mdbm";
    int ret = -1, i;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //store large objects
    for (i = 0; i < 10; ++i) {
       ret = StoreCacheLargeObj(cache_db, i);
       CPPUNIT_ASSERT_EQUAL(0, ret);
    }

    //fetch and validate large objects
    for (i = 0; i < 10; ++i) {
       FetchCacheLargeObj(cache_db, i);
    }
}


void SmokeTestCacheSuite::smoke_test_cache_06() {
    string trprefix = "TC 06 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_06.mdbm";
    int ret = -1, i;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;
    kvpair kv;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //store large objects
    for (i = 0; i < 10; ++i) {
       ret = StoreCacheLargeObj(cache_db, i);
       CPPUNIT_ASSERT_EQUAL(0, ret);
    }

   //delete the records
    for (i = 0; i < 10; ++i) {
       DeleteCacheLargeObj(cache_db, i);
    }

    //verify that no entries present
    kv = mdbm_first(cache_db);
    CPPUNIT_ASSERT(kv.key.dsize == 0);
    CPPUNIT_ASSERT(kv.key.dptr == NULL);
    CPPUNIT_ASSERT(kv.val.dsize == 0);
    CPPUNIT_ASSERT(kv.val.dptr == NULL);

    //store the records again
    for (i = 0; i < 10; ++i) {
       ret = StoreCacheLargeObj(cache_db, i);
       CPPUNIT_ASSERT_EQUAL(0, ret);
    }

    //fetch and validate large objects
    for (i = 0; i < 10; ++i) {
       FetchCacheLargeObj(cache_db, i);
    }
}

void SmokeTestCacheSuite::smoke_test_cache_07() {
    string trprefix = "TC 07 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_07.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;

    //create  mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_LRU);
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
}

void SmokeTestCacheSuite::smoke_test_cache_08() {
    string trprefix = "TC 08 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_08.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_LFU);
    //common tests
    cache_common_tests(cache_db, false);
}

void SmokeTestCacheSuite::smoke_test_cache_09() {
    string trprefix = "TC 09 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    const string mdbm_name  = _basePath + "/smoke_test_cache_09.mdbm";
    int ret = -1;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //get the cache mode
    ret = mdbm_get_cachemode(cache_db);

    stringstream cmss;
    cmss << prefix << "mdbm_get_cachemode returned=" << ret
         << " which should have matched MDBM_CACHEMODE_GDSF=" << MDBM_CACHEMODE_GDSF << endl;

    CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == MDBM_CACHEMODE_GDSF));
}

void SmokeTestCacheSuite::smoke_test_cache_10() {
    string trprefix = "TC 10 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    const string mdbm_name  = _basePath + "/smoke_test_cache_10.mdbm";
    int ret = -1;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(mdbm_name, openflags, MDBM_CACHEMODE_GDSF);
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //change the cache mode. It should return error
    CPPUNIT_ASSERT((ret = mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_LRU)) != 0);
}

void SmokeTestCacheSuite::smoke_test_cache_11() {
    string trprefix = "TC 11 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);

    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    const string mdbm_name  = _basePath + "/smoke_test_cache_11.mdbm";
    const char* mdbm_cache = mdbm_name.c_str();
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY;
    int ret = -1;

    //create mdbm as cache
    MdbmHolder cache_db(mdbm_open(mdbm_cache, flags, 0644, _pageSize, 0));
    CPPUNIT_ASSERT(NULL != (MDBM *)cache_db);
    CPPUNIT_ASSERT((ret = mdbm_sethash(cache_db, MDBM_HASH_MD5)) == 1);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(cache_db, static_cast<size_t>(_limitSize), NULL, NULL)) == 0);

    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //set the cache mode. It whould return error
    CPPUNIT_ASSERT((ret = mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_LRU)) != 0);
}

void SmokeTestCacheSuite::smoke_test_cache_12() {
    string trprefix = "TC 12 : Smoke Test DB Caching: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name  = _basePath + "/smoke_test_cache_12.mdbm";
    int ret = -1, i;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_CREATE_V3 | MDBM_CACHE_MODIFY | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;

    //create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache_40m(mdbm_name, openflags, MDBM_CACHEMODE_LRU);

    //store large objects
    for (i = 0; i < 1024; ++i) {
       ret = StoreCacheLargeObj(cache_db, i);
       if (ret == 0) {
           CPPUNIT_ASSERT_EQUAL(0, ret);
           FetchCacheLargeObj(cache_db, i);
       } else {
           CPPUNIT_ASSERT_EQUAL(-1, ret);
       }
    }
}

void SmokeTestCacheSuite::cache_common_tests(MDBM* cache_db, bool modify) {
    kvpair kv;

    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //delete data
    CPPUNIT_ASSERT(0 == DeleteData(cache_db));

    //verify that no entries present
    kv = mdbm_first(cache_db);
    CPPUNIT_ASSERT(kv.key.dsize == 0);
    CPPUNIT_ASSERT(kv.key.dptr == NULL);
    CPPUNIT_ASSERT(kv.val.dsize == 0);
    CPPUNIT_ASSERT(kv.val.dptr == NULL);

    //store data again
    CPPUNIT_ASSERT(0 == InsertData(cache_db));

    if (!modify)
      //fetch data and verify
      CPPUNIT_ASSERT(0 == VerifyData(cache_db));
    else
    {
      //Modify data
      CPPUNIT_ASSERT(0 == InsertData(cache_db, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT, true, 0 , MDBM_REPLACE, 1));
      //fetch and verify data
      CPPUNIT_ASSERT(0 == VerifyData(cache_db,DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT, true, 0, 1));
    }
}

MDBM* SmokeTestCacheSuite::create_mdbm_cache(const string& cache_name, int openflags, int mode) {
    MDBM *cache_db = NULL;
    int ret = -1;

    CPPUNIT_ASSERT((cache_db = mdbm_open(cache_name.c_str(), openflags, 0644, _pageSize, 0)) != NULL);

    if (!cache_db) {
        stringstream msg;
        msg << cache_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(cache_db);
        return NULL;
    }

    CPPUNIT_ASSERT((ret = mdbm_sethash(cache_db, MDBM_HASH_MD5)) == 1);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(cache_db, static_cast<size_t>(_limitSize), NULL, NULL)) == 0);
    CPPUNIT_ASSERT((ret = mdbm_set_cachemode(cache_db, mode)) == 0);

    return cache_db;
}

MDBM* SmokeTestCacheSuite::create_mdbm_cache_40m(const string& cache_name, int openflags, int mode) {
    MDBM *cache_db = NULL;
    int ret = -1;

    CPPUNIT_ASSERT((cache_db = mdbm_open(cache_name.c_str(), openflags, 0644, 4096, 40)) != NULL);

    if (!cache_db) {
        stringstream msg;
        msg << cache_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(cache_db);
        return NULL;
    }

    CPPUNIT_ASSERT((ret = mdbm_sethash(cache_db, MDBM_HASH_MD5)) == 1);
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(cache_db, mdbm_get_size(cache_db) / mdbm_get_page_size(cache_db), NULL, NULL)) == 0);
    CPPUNIT_ASSERT((ret = mdbm_set_cachemode(cache_db, mode)) == 0);

    return cache_db;
}

int SmokeTestCacheSuite::StoreCacheLargeObj(MDBM *cache_db, int objNum) {
    char kbuf[32], buf[256];
    string largeObj;
    datum k, dta;

    snprintf(kbuf, sizeof(kbuf), "%s%d", LARGE_OBJ_KEY, objNum);

    k.dptr = kbuf;
    k.dsize = strlen(kbuf);

    for (int i = 0; i < NUM_DATA_ITEMS*10; ++i ) {
        snprintf(buf, sizeof(buf),
                 "%s%s%d", PREFIX, PREFIX, STARTPOINT + i);
        largeObj += buf;

    }

    dta.dptr  = (char *) largeObj.c_str();
    dta.dsize = largeObj.size();

    return mdbm_store(cache_db, k, dta, MDBM_REPLACE);
}


void SmokeTestCacheSuite::FetchCacheLargeObj(MDBM *cache_db, int key) {
    char key1[32];
    datum fetched1;

    snprintf(key1, sizeof(key1), "%s%d", LARGE_OBJ_KEY, key);
    const datum k1 = { key1, (int)strlen(key1) };
    errno = 0;
    // Fetch the value for stored key
    fetched1 = mdbm_fetch(cache_db, k1);
    CPPUNIT_ASSERT_EQUAL(0, errno);
    CPPUNIT_ASSERT(fetched1.dptr != NULL);
}

void SmokeTestCacheSuite::DeleteCacheLargeObj(MDBM *cache_db, int key) {
    char key1[32];
    int ret = -1;
    snprintf(key1, sizeof(key1), "%s%d", LARGE_OBJ_KEY, key);
    const datum k1 = { key1, (int)strlen(key1) };
    ret = mdbm_delete(cache_db, k1);
    CPPUNIT_ASSERT_EQUAL(0, ret);
}


class MdbmSmokeTest : public SmokeTestCacheSuite {
    CPPUNIT_TEST_SUITE(MdbmSmokeTest);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(smoke_test_cache_01);   // TC 01
      CPPUNIT_TEST(smoke_test_cache_02);   // TC 02
      CPPUNIT_TEST(smoke_test_cache_03);   // TC 03
      CPPUNIT_TEST(smoke_test_cache_04);   // TC 04
      CPPUNIT_TEST(smoke_test_cache_05);   // TC 05
//    CPPUNIT_TEST(smoke_test_cache_06);   // TC 06
      CPPUNIT_TEST(smoke_test_cache_07);   // TC 07
      CPPUNIT_TEST(smoke_test_cache_08);   // TC 08
      CPPUNIT_TEST(smoke_test_cache_09);   // TC 09
      CPPUNIT_TEST(smoke_test_cache_10);   // TC 10
      CPPUNIT_TEST(smoke_test_cache_11);   // TC 11
      CPPUNIT_TEST(smoke_test_cache_12);   // TC 12
    CPPUNIT_TEST_SUITE_END();

public:
    MdbmSmokeTest() : SmokeTestCacheSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmSmokeTest);


