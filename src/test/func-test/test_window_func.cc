/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-window-func.cc
// Purpose: Functional tests for windowing functionality

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

class WindowingFuncTestSuite : public CppUnit::TestFixture, public TestBase {
public:
    WindowingFuncTestSuite(int vFlag) : TestBase(vFlag, "WindowingFuncTestSuite") {}

    void setUp();
    void tearDown();

    void setEnv();

    void test_window_func_01();
    void test_window_func_02();
    void test_window_func_03();
    void test_window_func_04();
    void test_window_func_05();
    void test_window_func_06();
    void test_window_func_07();
    void test_window_func_08();
    void test_window_func_09();
    void test_window_func_10();
    void test_window_func_11();
    void test_window_func_12();
    void test_window_func_13();
    void test_window_func_14();
    void test_window_func_15();
    void test_window_func_16();
    void test_window_func_17();
    void test_window_func_18();
    void test_window_func_19();
    void test_window_func_20();

private:
    static int _pageSize;
    static int _initialDbSize;
    static int _limitSize;
    static int _largeObjSize;
    static int _spillSize;
    static int _windowSize;
    static int _minWindowSize;
    static int _maxWindowSize;
    static string _testDir;
    static int flags;
    static int flagsLO;
};

class WindowingFuncTest : public WindowingFuncTestSuite {
    CPPUNIT_TEST_SUITE(WindowingFuncTest);

    CPPUNIT_TEST(setEnv);

#ifdef HAVE_WINDOWED_MODE
    CPPUNIT_TEST(test_window_func_01);
    CPPUNIT_TEST(test_window_func_02);
    CPPUNIT_TEST(test_window_func_03);
    CPPUNIT_TEST(test_window_func_04);
    CPPUNIT_TEST(test_window_func_05);
    CPPUNIT_TEST(test_window_func_06);
    CPPUNIT_TEST(test_window_func_07);
    CPPUNIT_TEST(test_window_func_08);
    CPPUNIT_TEST(test_window_func_09);
    CPPUNIT_TEST(test_window_func_10);
    CPPUNIT_TEST(test_window_func_11);
    CPPUNIT_TEST(test_window_func_12);
    CPPUNIT_TEST(test_window_func_13);
    CPPUNIT_TEST(test_window_func_14);
    CPPUNIT_TEST(test_window_func_15);
    CPPUNIT_TEST(test_window_func_16);
    CPPUNIT_TEST(test_window_func_17);
    CPPUNIT_TEST(test_window_func_18);
    CPPUNIT_TEST(test_window_func_19);
    CPPUNIT_TEST(test_window_func_20);
#endif

    CPPUNIT_TEST_SUITE_END();

public:
    WindowingFuncTest() : WindowingFuncTestSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(WindowingFuncTest);

int WindowingFuncTestSuite::_pageSize = 4096;
int WindowingFuncTestSuite::_initialDbSize = 2*64*4096;
int WindowingFuncTestSuite::_limitSize = 256;
int WindowingFuncTestSuite::_largeObjSize = 3073;
int WindowingFuncTestSuite::_spillSize = 3072;
int WindowingFuncTestSuite::_windowSize = 2*128*4096;
int WindowingFuncTestSuite::_minWindowSize = 2*64*4096;
int WindowingFuncTestSuite::_maxWindowSize = 2*256*4096;
string WindowingFuncTestSuite::_testDir = "";
int WindowingFuncTestSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_CREATE_V3|MDBM_OPEN_WINDOWED;
int WindowingFuncTestSuite::flagsLO = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_CREATE_V3|MDBM_OPEN_WINDOWED|MDBM_LARGE_OBJECTS;

void WindowingFuncTestSuite::setUp() {
}

void WindowingFuncTestSuite::tearDown() {
}

void WindowingFuncTestSuite::setEnv() {
  GetTmpDir(_testDir);
}

/*
  Cache + BS
*/


// Test case - Set up cache and backing store with minimum window
// size. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_01() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string bs_name  = _testDir + "/bs01.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache01.mdbm";
    const char* cache_mdbm = cache_name.c_str();
    // Create a Mdbm as cache
    MDBM *cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize, _initialDbSize, _limitSize);
    CPPUNIT_ASSERT(cache_db != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize, _initialDbSize, _limitSize, _minWindowSize);
    CPPUNIT_ASSERT(bs_db != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT_DUP));
    mdbm_close(cache_db);
}


// Test case - Set up cache and backing store with minimum window
// size and with LO support. Store and fetch some record with very large value
void WindowingFuncTestSuite::test_window_func_02() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string bs_name  = _testDir + "/bs02.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache02.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize, _initialDbSize, _limitSize);
    CPPUNIT_ASSERT(cache_db != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize,_initialDbSize, _limitSize,
        _spillSize, _minWindowSize);
    CPPUNIT_ASSERT(bs_db != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(cache_db, MDBM_INSERT_DUP, _largeObjSize));
    mdbm_close(cache_db);
}


// Test case - Set up cache and backing store with max (whole Db size) window
// size. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_03() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string bs_name  = _testDir + "/bs03.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache03.mdbm";
    const char* cache_mdbm = cache_name.c_str();
    // Create a Mdbm as cache
    MDBM *cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize, _initialDbSize, _limitSize);
    CPPUNIT_ASSERT(cache_db != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = createBSMdbmNoLO(bs_mdbm, flags, _pageSize, _initialDbSize, _limitSize, _maxWindowSize);
    CPPUNIT_ASSERT(bs_db != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT_DUP));
    mdbm_close(cache_db);
}


// Test case - Set up cache and backing store with max (whole Db size) window
// size and with LO support. Store and fetch some record with very large value
void WindowingFuncTestSuite::test_window_func_04() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string bs_name  = _testDir + "/bs04.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache04.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize,  _initialDbSize, _limitSize);
    CPPUNIT_ASSERT(cache_db != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize, _limitSize, _spillSize, _maxWindowSize);
    CPPUNIT_ASSERT(bs_db != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(cache_db, MDBM_INSERT_DUP, _largeObjSize));
    mdbm_close(cache_db);
}



// Test case - Set up cache and backing store with window size = n pages
// with LO support. Store and fetch some record with size = window size. It sould fail
void WindowingFuncTestSuite::test_window_func_05() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string bs_name  = _testDir + "/bs05.mdbm";
    const char* bs_mdbm = bs_name.c_str();
    const string cache_name  = _testDir + "/cache05.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = createCacheMdbm(cache_mdbm, flagsLO, _pageSize, _initialDbSize, _limitSize);
    CPPUNIT_ASSERT(cache_db != NULL);

    // Create another Mdbm as backing store
    MDBM *bs_db = createBSMdbmWithLO(bs_mdbm, flagsLO, _pageSize, _initialDbSize, _limitSize, _spillSize, _windowSize);
    CPPUNIT_ASSERT(bs_db != NULL);

    // Link cache and backing store
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

    // This should return -1
    CPPUNIT_ASSERT_EQUAL(-1, storeAndFetchLO(cache_db, MDBM_INSERT_DUP, _windowSize));
    mdbm_close(cache_db);
}

/*
      Cache only
*/


// Test case - Create cache Mdbm with minimum window
// size. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_06() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_01.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbmNoLO(cache_mdbm, flags, _pageSize,
        _initialDbSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _minWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT_DUP));
    mdbm_close(cache_db);
}


// Test case - Create cache Mdbm with minimum window size and with LO support.
// Store and fetch some record with very large value
void WindowingFuncTestSuite::test_window_func_07() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_02.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _spillSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _minWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(cache_db, MDBM_INSERT_DUP, _largeObjSize));
    mdbm_close(cache_db);
}


// Test case - Create cache Mdbm with max (whole Mdbm) window
// size and with LO support. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_08() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_03.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _spillSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _maxWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT_DUP));
    mdbm_close(cache_db);
}

// Test case - Create cache Mdbm with max (whole Mdbm) window
// size and with LO support. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_09() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_04.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _spillSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _maxWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(cache_db, MDBM_INSERT_DUP, _largeObjSize));
    mdbm_close(cache_db);
}


// Test case - Create cache Mdbm with window size = n pages
// with LO support. Store and fetch some record with size = window size
// This should fail
void WindowingFuncTestSuite::test_window_func_10() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_06.mdbm";
    const char* cache_mdbm = cache_name.c_str();

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _spillSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _windowSize));

    // This should return -1
    CPPUNIT_ASSERT_EQUAL(-1, storeAndFetchLO(cache_db, MDBM_INSERT_DUP, _windowSize));
    mdbm_close(cache_db);
}


// Test case - Create cache Mdbm with max window size with LO support
// Store and fetch some record. Change window size = n pages. Store and fetch record
// Change window size = min window size and store, fetch record again
void WindowingFuncTestSuite::test_window_func_11() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_08.mdbm";
    const char* cache_mdbm = cache_name.c_str();
    int i = 0;
    datum k, v1, value;

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _spillSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _maxWindowSize));

    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Store few records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_INSERT_DUP));
    }

    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _windowSize));

    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(cache_db, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Store new records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_REPLACE));
    }

/*
    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _maxWindowSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, (16*128*4096)));

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(cache_db, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }

*/
    mdbm_close(cache_db);
}

// Test case - Create cache Mdbm with min window size with LO support
// Store and fetch some record. Change window size = n pages. Store and fetch record
// Change window size = max window size and store, fetch record again
void WindowingFuncTestSuite::test_window_func_12() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string cache_name  = _testDir + "/cache_09.mdbm";
    const char* cache_mdbm = cache_name.c_str();
    int i = 0;
    datum k, v1, value;

    // Create a Mdbm as cache
    MDBM *cache_db = NULL;
    CPPUNIT_ASSERT((cache_db = createMdbm(cache_mdbm, flagsLO, _pageSize,
        _initialDbSize, _spillSize)) != NULL);

    // Set cache mode
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(cache_db, MDBM_CACHEMODE_GDSF));

    // Set limit size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(cache_db, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _minWindowSize));

    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Store few records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_INSERT_DUP));
    }

    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _windowSize));

    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(cache_db, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Store new records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(cache_db, k, v1, MDBM_REPLACE));
    }

/*
    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(cache_db, _maxWindowSize));

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(cache_db, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }
*/

    mdbm_close(cache_db);
}

/*
      Persistant store
*/

// Test case - Create with minimum window size. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_13() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db01.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _minWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm with minimum (1 page) window size and with LO support.
// Store and fetch some record with very large value
void WindowingFuncTestSuite::test_window_func_14() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db02.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _minWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm with max (whole Db size) window
// size. Store and fetch some record
void WindowingFuncTestSuite::test_window_func_15() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db03.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _maxWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm with max (whole Db size) window
// size and with LO support. Store and fetch some record with very large value
void WindowingFuncTestSuite::test_window_func_16() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db04.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _maxWindowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm with window size = n pages
// with LO support. Store and fetch some record with size = window size. It should fail
void WindowingFuncTestSuite::test_window_func_17() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db05.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _windowSize));

    // This should return -1
    CPPUNIT_ASSERT_EQUAL(-1, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _windowSize));
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm with max window size with LO support
// Store and fetch some record. Change window size = n pages. Store and fetch record
// Change window size = min window size and store, fetch record again
void WindowingFuncTestSuite::test_window_func_18() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db08.mdbm";
    const char* db_name = mdbm_name.c_str();
    int i = 0;
    datum k, v1, value;

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _maxWindowSize));

    // Change window size
    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Store few records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT_DUP));
    }

    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _windowSize));

    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(mdbm, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Store new records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_REPLACE));
    }

/*
    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _minWindowSize));

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(mdbm, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }
*/

    mdbm_close(mdbm);
}

// Test case - Create a Mdbm with min window size with LO support
// Store and fetch some record. Change window size = n pages. Store and fetch record
// Change window size = max window size and store, fetch record again
void WindowingFuncTestSuite::test_window_func_19() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db09.mdbm";
    const char* db_name = mdbm_name.c_str();
    int i = 0;
    datum k, v1, value;

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _minWindowSize));

    // Change window size
    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Store few records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_INSERT_DUP));
    }

    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _windowSize));

    for (i = 0; i  < 100; i++) {
        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&i);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&i);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(mdbm, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Store new records
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v1, MDBM_REPLACE));
    }

/*
    // Change window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _maxWindowSize));

    for (i = 0; i  < 100; i++) {
        int j = i+1000;

        // Create key-value pair
        k.dptr = reinterpret_cast<char*>(&j);
        k.dsize = sizeof(k.dptr);
        v1.dptr = reinterpret_cast<char*>(&j);
        v1.dsize = sizeof(v1.dptr);

        // Fetch previously stored records
        value = mdbm_fetch(mdbm, k);
        CPPUNIT_ASSERT_EQUAL(v1.dsize, value.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v1.dptr, value.dsize));
    }
*/

    mdbm_close(mdbm);
}


// Test case - To check mdbm_window_stats function. Revisit this
void WindowingFuncTestSuite::test_window_func_20() {
    SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
    //RETURN_IF_WINMODE_LIMITED

    const string mdbm_name  = _testDir + "/db10.mdbm";
    const char* db_name = mdbm_name.c_str();
    int _flagsPartition = flags|MDBM_PARTITIONED_LOCKS;
    mdbm_window_stats_t _windowStats;
    memset(&_windowStats, 0, sizeof(mdbm_window_stats_t));
    size_t offset = sizeof(mdbm_window_stats_t);

    // Create a Mdbm as cache
    MDBM *mdbm = createMdbmNoLO(db_name, _flagsPartition, _pageSize, _initialDbSize);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Set max Db size and window size
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, _windowSize));

    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_window_stats(mdbm, &_windowStats, offset));

    cout<<"\n1. the remapped count -> "<<_windowStats.w_num_remapped<<endl;
    cout<<"1. the reused count -> "<<_windowStats.w_num_reused<<endl;

    // This should return -1
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_window_stats(mdbm, &_windowStats, offset));

    cout<<"2. the remapped count -> "<<_windowStats.w_num_remapped<<endl;
    cout<<"2. the reused count -> "<<_windowStats.w_num_reused<<endl;

    datum k, v, value;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Fetch the value for stored key
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_window_stats(mdbm, &_windowStats, offset));

    cout<<"3. the remapped count -> "<<_windowStats.w_num_remapped<<endl;
    cout<<"3. the reused count -> "<<_windowStats.w_num_reused<<endl;

    // Fetch the value for stored key
    value = mdbm_fetch(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_window_stats(mdbm, &_windowStats, offset));

    cout<<"4. the remapped count -> "<<_windowStats.w_num_remapped<<endl;
    cout<<"4. the reused count  -> "<<_windowStats.w_num_reused<<endl;


    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_window_stats(mdbm, &_windowStats, offset));

    cout<<"5. the remapped count -> "<<_windowStats.w_num_remapped<<endl;
    cout<<"5. the reused count -> "<<_windowStats.w_num_reused<<endl;

    mdbm_close(mdbm);
}

