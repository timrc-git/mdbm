/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-backstore.cc
// Purpose: Smoke tests for mdbm backing store functionality

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

class SmokeTestBackStoreSuite : public CppUnit::TestFixture, public TestBase {
public:
    SmokeTestBackStoreSuite(int vFlag) : TestBase(vFlag, "SmokeTestBackStoreSuite") {}
    void setUp();
    void tearDown();
    void initialSetup();
    // Smoke tests in this suite
    void smoke_test_backstore_01();  //        TC 01
    void smoke_test_backstore_02();  //        TC 02
    void smoke_test_backstore_03();  //        TC 03
    void smoke_test_backstore_04();  //        TC 04
    void smoke_test_backstore_05();  //        TC 05
    void smoke_test_backstore_06();  //        TC 06
    void smoke_test_backstore_07();  //        TC 07
    void smoke_test_backstore_08();  //        TC 08
    void smoke_test_backstore_09();  //        TC 09
    void smoke_test_backstore_10();  //        TC 10

    MDBM* create_mdbm_cache(const string& mdbm_cache_name, int openflags);
    MDBM* create_mdbm_backstore(const string& mdbm_cache_name, int openflags);
    void backstore_common_tests(MDBM* cache_db, bool reinsert);
    int  StoreLargeObj(MDBM *cache_db, int xobjNum = 0);
    void FetchLargeObj(MDBM *cache_db, int key);

private:
    static string _basePath;
    static int _pageSize;
    static int _limitSize;
    static int _windowSize;
};

string SmokeTestBackStoreSuite::_basePath = "";
int SmokeTestBackStoreSuite::_pageSize = 64*1024;
int SmokeTestBackStoreSuite::_limitSize = 256;
int SmokeTestBackStoreSuite::_windowSize = (64*1024)*4;

void SmokeTestBackStoreSuite::setUp() {
}

void SmokeTestBackStoreSuite::tearDown() {
}

// Initial setup for all mdbm smoke tests
void SmokeTestBackStoreSuite::initialSetup() {
    GetTmpDir(_basePath);
}

void SmokeTestBackStoreSuite::smoke_test_backstore_01() {
    string trprefix = "TC 01: Smoke Test DB Backing Store: Insert record: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_01.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_01.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    // fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
}

void SmokeTestBackStoreSuite::smoke_test_backstore_02() {
    string trprefix = "TC 02: Smoke Test DB Backing Store: Modify record: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_02.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_02.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    //Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
    //Modify data
    CPPUNIT_ASSERT(0 == InsertData(cache_db, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT, true, 0 , MDBM_REPLACE, 1));
    //fetch and verify data
    CPPUNIT_ASSERT(0 == VerifyData(cache_db,DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT, true, 0, 1));
}

void SmokeTestBackStoreSuite::smoke_test_backstore_03() {
    string trprefix = "TC 03: Smoke Test DB Backing Store: Insert and Delete record: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_03.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_03.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing sckstore_common_teststore
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    /*common tests */
    backstore_common_tests(cache_db, false);
}

void SmokeTestBackStoreSuite::smoke_test_backstore_04() {
    string trprefix = "TC 04: Smoke Test DB Backing Store: Insert, delete and Insert record: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_04.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_04.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    /*common tests */
    backstore_common_tests(cache_db, true);
}

void SmokeTestBackStoreSuite::smoke_test_backstore_05() {

    string trprefix = "TC 05: Smoke Test DB Backing Store: Insert Large Object record: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_05.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_05.mdbm";
    int ret = -1, i;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    //store large objects
    for (i = 0; i < 10; ++i) {
       ret = StoreLargeObj(cache_db, i);
       CPPUNIT_ASSERT_EQUAL(0, ret);
    }

    //fetch and validate large objects
    for (i = 0; i < 10; ++i) {
       FetchLargeObj(cache_db, i);
    }
}

void SmokeTestBackStoreSuite::smoke_test_backstore_06() {
    string trprefix = "TC 06: Smoke Test DB Backing Store: Empty database: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_06.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_06.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    datum k, v, value;
    k.dptr = (char*)"";
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)"";
    v.dsize = strlen(v.dptr);
    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    // fetch data and verify
    value = mdbm_fetch(cache_db, k);
    // Verify that the returned value is NULL
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
}

void SmokeTestBackStoreSuite::smoke_test_backstore_07() {
#if 0
FIX: mdbm_set_backingstore: using MDBM_BSOPS_FILE always fails to create Back Store File
    string trprefix = "TC 07: Smoke Test DB Backing Store: Create backing store using mdbm_bsops_file: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_07.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_07.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_FILE, bs_db, 0));
#endif
}

void SmokeTestBackStoreSuite::smoke_test_backstore_08() {
    string trprefix = "TC 08: Smoke Test DB Backing Store: Reopen cache DB with same Backing Store: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_08.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_08.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MDBM *cache_db = NULL;
    MDBM* bs_db = NULL;

    //Create mdbm as cache. since closing the cache db and reopen it, do not use mdbmholder here
    CPPUNIT_ASSERT((cache_db = mdbm_open(cache_name.c_str(), openflags, 0644, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(cache_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    //Create mdbm as backing store
    CPPUNIT_ASSERT((bs_db = mdbm_open(mdbm_name.c_str(), openflags, 0600, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(bs_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(bs_db, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(bs_db, static_cast<size_t>(_windowSize)));

    //Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
    //close the DB cache
    mdbm_close(cache_db);

    //Reopen the cache and backing store mdbm
    CPPUNIT_ASSERT((cache_db = mdbm_open(cache_name.c_str(), MDBM_O_RDWR, 0644, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT((bs_db = mdbm_open(mdbm_name.c_str(), MDBM_O_RDWR, 0600, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    //fetch and verfify the previously stored data
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));

    mdbm_close(cache_db);
}

void SmokeTestBackStoreSuite::smoke_test_backstore_09() {
    string trprefix = "TC 09: Smoke Test DB Backing Store: Create new DB with same Backing store: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_09.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_09.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MDBM *cache_db = NULL, *cache_dbnew = NULL;
    MDBM* bs_db = NULL;

    //Create mdbm as cache. since closing the cache db and reopen it, do not use mdbmholder here
    CPPUNIT_ASSERT((cache_db = mdbm_open(cache_name.c_str(), openflags, 0644, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(cache_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    //Create mdbm as backing store
    CPPUNIT_ASSERT((bs_db = mdbm_open(mdbm_name.c_str(), openflags, 0600, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(bs_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(bs_db, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(bs_db, static_cast<size_t>(_windowSize)));

    //Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));
    //close the DB cache
    mdbm_close(cache_db);

    //open a new cache and reopen the previous backing store
    CPPUNIT_ASSERT((cache_dbnew = mdbm_open(cache_name.c_str(), openflags, 0644, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(cache_dbnew, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_dbnew, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_dbnew, MDBM_CACHEMODE_GDSF));

    CPPUNIT_ASSERT((bs_db = mdbm_open(mdbm_name.c_str(), MDBM_O_RDWR, 0600, _pageSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_dbnew, MDBM_BSOPS_MDBM, bs_db, 0));
    //fetch and verfify the previously stored data. It should success
    CPPUNIT_ASSERT(0 == VerifyData(cache_dbnew));
    mdbm_close(cache_dbnew);
}

void SmokeTestBackStoreSuite::smoke_test_backstore_10() {
    string trprefix = "TC 10: Smoke Test DB Backing Store: More than one backing store configuration: ";
    TRACE_TEST_CASE(trprefix);
    const string cache_name = _basePath + "/smoke_test_cache_10.mdbm";
    const string mdbm_name  = _basePath + "/smoke_test_backstore_10.mdbm";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    //Create mdbm as cache
    MdbmHolder cache_db = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db = NULL;
    bs_db = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db));

    //create 2nd mdbm backing store configuration
    //Create mdbm as cache
    MdbmHolder cache_db2 = create_mdbm_cache(cache_name, openflags);
    //Create another Mdbm as backing store
    MDBM *bs_db2 = NULL;
    bs_db2 = create_mdbm_backstore(mdbm_name, openflags);
    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db2, MDBM_BSOPS_MDBM, bs_db2, 0));
    //store data
    CPPUNIT_ASSERT(0 == InsertData(cache_db2));
    //fetch data and verify
    CPPUNIT_ASSERT(0 == VerifyData(cache_db2));
}

void SmokeTestBackStoreSuite::backstore_common_tests(MDBM* cache_db, bool reinsert) {
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

    if (reinsert) {
       //store data again
       CPPUNIT_ASSERT(0 == InsertData(cache_db));
        //fetch data and verify
       CPPUNIT_ASSERT(0 == VerifyData(cache_db));
    }
}

//Function to create Mdbm as cache
MDBM* SmokeTestBackStoreSuite::create_mdbm_cache(const string& mdbm_cache_name, int openflags) {
    MDBM *cache_db = NULL;

    CPPUNIT_ASSERT((cache_db = mdbm_open(mdbm_cache_name.c_str(), openflags, 0644, _pageSize, 0)) != NULL);

    if (!cache_db) {
        stringstream msg;
        msg << mdbm_cache_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(cache_db);
        return NULL;
    }

    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(cache_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));
    return cache_db;
}

//Function to create Mdbm as a backing store
MDBM* SmokeTestBackStoreSuite::create_mdbm_backstore(const string& mdbm_bs_name, int openflags) {
    MDBM* bs_db = NULL;

    CPPUNIT_ASSERT((bs_db = mdbm_open(mdbm_bs_name.c_str(), openflags, 0600, _pageSize, 0)) != NULL);

    if (!bs_db) {
        stringstream msg;
        msg << mdbm_bs_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(bs_db);
        return NULL;
    }

    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(bs_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(bs_db, static_cast<size_t>(_limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(bs_db, static_cast<size_t>(_windowSize)));
    return bs_db;
}

int SmokeTestBackStoreSuite::StoreLargeObj(MDBM *cache_db, int objNum) {
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

void SmokeTestBackStoreSuite::FetchLargeObj(MDBM *cache_db, int key) {
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

class MdbmSmokeTest : public SmokeTestBackStoreSuite { 
    CPPUNIT_TEST_SUITE(MdbmSmokeTest);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(smoke_test_backstore_01);   // TC 01
      CPPUNIT_TEST(smoke_test_backstore_02);   // TC 02
      CPPUNIT_TEST(smoke_test_backstore_03);   // TC 03
      CPPUNIT_TEST(smoke_test_backstore_04);   // TC 04
      CPPUNIT_TEST(smoke_test_backstore_05);   // TC 05
      CPPUNIT_TEST(smoke_test_backstore_06);   // TC 06
      CPPUNIT_TEST(smoke_test_backstore_07);   // TC 07
      CPPUNIT_TEST(smoke_test_backstore_08);   // TC 08
      CPPUNIT_TEST(smoke_test_backstore_09);   // TC 09
      CPPUNIT_TEST(smoke_test_backstore_10);   // TC 10
    CPPUNIT_TEST_SUITE_END();

public:
    MdbmSmokeTest() : SmokeTestBackStoreSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmSmokeTest);


