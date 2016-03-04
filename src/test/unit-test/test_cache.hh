/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_set_cachemode, mdbm_get_cachemode, mdbm_get_cachemode_name,
#ifndef TEST_CACHE_HH
#define TEST_CACHE_HH

#ifdef FREEBSD
#include <sys/types.h>
#endif
#include <stddef.h>
#include <stdint.h>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "TestBase.hh"

class CacheBaseTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    void setUp();
    void tearDown() { }

    // Test Cases

    // mdbm_set_cachemode
    void CacheAddDataThenSetValidModeA1();
    void CacheStoreLargeObjA2();
    void CacheAllModesModifyNewKeyA3();
    void CacheAllModesModifyOldKeyA4();
    void CacheNoModeModifyNewKeyA5();
    void CacheNoModeModifyOldKeyA6();
    void CacheModifyInsertNewKeyA7();
    void CacheModifyInsertOldKeyA8();
    void CacheAllModesCacheOnlyNewKeyA9();
    void CacheAllModesCacheOnlyOldKeyA10();
    void CacheNoModeCacheOnlyNewKeyA11();
    void CacheNoModeCacheOnlyOldKeyA12();
    void CacheOnlyInsertNewKeyA13();
    void CacheOnlyInsertOldKeyA14();

    // mdbm_get_cachemode
    void CacheNoSetModeThenGetModeB1();
    void CacheAllValidModesSetAndGetModeB2();
    void CacheAllInvalidModesSetAndGetModeB3();

    // mdbm_get_cachemode_name
    void CacheAllValidModesSetAndGetModeNameC1();
    void CacheAllInvalidModesSetAndGetModeNameC2();

    void CacheChurn();

    void CacheLimitSize();

protected:
    CacheBaseTestSuite(int vFlag) : TestBase(vFlag, "Caching TestSuite") {}

    void cacheAllModesModifyNewKey(string &prefix, int storeFlags, int expectedStoreRet);
    void cacheAllModesModifyOldKey(string &prefix, int storeFlags, int expectedStoreRet);
    void setCacheModifyNewKey(string &prefix, int cacheFlags, int storeFlags, int expectedStoreRet, bool expectStored);
    void setCacheModifyOldKey(string &prefix, int cacheFlags, int storeFlags, int expectedStoreRet);

    // return true if the mode matches correctly to the associated cacheMode
    static bool matchModeString(const char *mode, int cacheMode);

    static int _ValidCacheModes[];
    static int _ValidCacheModeLen;

    static int _InvalidCacheModes[];
    static int _InvalidCacheModeLen;

    static string _ModeNone;
    static string _ModeLFU;
    static string _ModeLRU;
    static string _ModeGDSF;
    static string _ModeCLEAN;

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST

};

class CacheTsV3 : public CacheBaseTestSuite
{
    CPPUNIT_TEST_SUITE(CacheTsV3);

    CPPUNIT_TEST(CacheAddDataThenSetValidModeA1);
    CPPUNIT_TEST(CacheStoreLargeObjA2);
    CPPUNIT_TEST(CacheAllModesModifyNewKeyA3);
    CPPUNIT_TEST(CacheAllModesModifyOldKeyA4);
    CPPUNIT_TEST(CacheNoModeModifyNewKeyA5);
    CPPUNIT_TEST(CacheNoModeModifyOldKeyA6);
    CPPUNIT_TEST(CacheModifyInsertNewKeyA7);
    CPPUNIT_TEST(CacheModifyInsertOldKeyA8);
    CPPUNIT_TEST(CacheAllModesCacheOnlyNewKeyA9);
    CPPUNIT_TEST(CacheAllModesCacheOnlyOldKeyA10);
    CPPUNIT_TEST(CacheNoModeCacheOnlyNewKeyA11);
    CPPUNIT_TEST(CacheNoModeCacheOnlyOldKeyA12);
    CPPUNIT_TEST(CacheOnlyInsertNewKeyA13);
    CPPUNIT_TEST(CacheOnlyInsertOldKeyA14);

    CPPUNIT_TEST(CacheNoSetModeThenGetModeB1);
    CPPUNIT_TEST(CacheAllValidModesSetAndGetModeB2);
    CPPUNIT_TEST(CacheAllInvalidModesSetAndGetModeB3);

    CPPUNIT_TEST(CacheAllValidModesSetAndGetModeNameC1);
    CPPUNIT_TEST(CacheAllInvalidModesSetAndGetModeNameC2);
    //CPPUNIT_TEST(CacheLimitSize);

    CPPUNIT_TEST(CacheChurn);

    CPPUNIT_TEST_SUITE_END();
public:
    CacheTsV3() : CacheBaseTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(CacheTsV3);


#endif
