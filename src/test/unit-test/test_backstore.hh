/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_set_backingstore, mdbm_set_window_size, mdbm_get_window_stats
#ifndef TEST_BACKSTORE_HH
#define TEST_BACKSTORE_HH

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

class BackStoreTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    void setUp();
    void tearDown() { }

    // Test Cases
    void TestWindowedMode();

    // mdbm_set_backingstore
    void BsSetWithNullParamA2();
    void BsSetMdbmMultipleTimesThenCloseA3();
    void BsSetUsingSameDbAsCacheAndBsA4();
    void BsSetBsWithoutCacheModeA5();
    void BsUserDefinedBsFunctionsA6();
    void BsStoreFetchLargeObjectA7();
    void BsUseFileForBsA8();
    void BsUseNullForBsA8();
    void BsUseInvalidForBsA8();
    void BsUseInvalid2ForBsA8();
    void BsCacheModeLruStoreCacheOnlyA9();
    void BsCacheModeNoneStoreCacheOnlyA10();
    void BsCacheModeLruStoreModifyNewKeyA11();
    void BsCacheModeLruStoreModifyOldKeyA12();
    void BsSameWindowSizeCacheAndBsA13();
    void BsSmallWindSizeCacheAndBigWindSizeBsA14();
    void BsBigWindSizeCacheAndSmallWindSizeBsA15();
    void BsLimitSizeSameCacheAndBsA16();
    void BsLimitSizeBigCacheAndSmallBsA17();
    void BsForEachPSizeOpenCacheForEachPSizeOpenBsLimitAndFillA18();
    void BsUseFileBsFillDbCloseDbNewDbUseFileBsA19();
    void BsUseMdbmBsFillDbCloseDbNewDbUseMdbmBsA20();
    void BsUseMdbmBsFillDbCloseDbSameDbUseMdbmBsA21();
    void BsStoreDataSetBsWithNewDbStoreNewA22();
    void BsStoreDataSetReadOnlyBsWithNewDbStoreNewA23();
    void BsTestReplaceA24();
    void BsTestReplaceWithCache();
    void BsUseMdbmBsInsertDataDeleteDataA25();
    void OpenTooSmallReopenWindowed();
    void BsTestMisc();
    void BsTestInvalid();

    // mdbm_set_window_size
    void BsCreateDbNoWindowFlagSetWindowSizeZeroB1();
    void BsCreateDbNoWindowFlagSetWindowSizeB2();
    void BsSetWindowSizeMaxIntB3();
    void BsSetWindowSizeMinusOneB4();
    void BsSetWindowSizeZeroB5();
    void BsCheckSeveralDiffPageSizesSetWinSizeB6();
    void BsCheckDiffPageSizesAgainstDiffWinSizesB7();
    void BsNPageDbSetWinSizeStoreDataSetWinSize0B8();
    void BsNPageDbSetWinSize3timesPsizeStoreDataSetWinSize2timesPsizeB9();
    void BsNPageDbSetWinSize3timesPsizeStoreDataSetWinSize4timesPsizeB10();
    void BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeLessNB11();
    void BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeEqualNB12();
    void BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeGreaterNB13();
    void BsSetWindowIncreaseNumPagesFillDbB14();

    // mdbm_get_window_stats
    void BsUseNullStatsPointerC1();
    void BsUseTooSmallSizeC2();
    void BsUseBigSizeC3();
    void BsUseInBetweenSizeC4();
    void BsCheckVariousWinSizesInLoopC5();
    void BsNoWinModeFillDbGetStatsC6();
    void BsWinModeNpagesFillDbNplusNpagesGetStatsC7();
    void BsWinModeNpagesFillDbNpagesSetWinSize0GetStatsC8();

protected:
    BackStoreTestSuite(int vFlag) : TestBase(vFlag, "Backing Store TestSuite") {}

    void simpleFileBsTest(MDBM *dbh, bool store, const char *location);
    void bsCacheModeStore(string &prefix, string &baseName, int cacheMode, int storeFlag, bool expectSuccess);
    void bsWindowsAndFill(string &prefix, string &baseName, size_t cachewsize, size_t bswsize, mdbm_ubig_t cacheNumPages, mdbm_ubig_t bsNumPages, int pageSize);
    void bsStoreDataSetBsWithNewDbStoreNew(string &prefix, string &baseName, int openBSflag, int fillDBrecCnt);
    void UseFileForBs(const string &prefix, const char *bsFileName);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
};

class BackStoreTsV3 : public BackStoreTestSuite
{
    CPPUNIT_TEST_SUITE(BackStoreTsV3);

    CPPUNIT_TEST(BsSetWithNullParamA2);
    CPPUNIT_TEST(BsSetMdbmMultipleTimesThenCloseA3);
    // TODO BZ 5718172, invalid test, shouldn't allow cache and bs to be the same MDBM
    //CPPUNIT_TEST(BsSetUsingSameDbAsCacheAndBsA4);
    CPPUNIT_TEST(BsSetBsWithoutCacheModeA5);
    CPPUNIT_TEST(BsUserDefinedBsFunctionsA6);
    CPPUNIT_TEST(BsStoreFetchLargeObjectA7);
    CPPUNIT_TEST(BsUseFileForBsA8);
    //CPPUNIT_TEST(BsUseNullForBsA8);
    CPPUNIT_TEST(BsUseInvalidForBsA8);
    CPPUNIT_TEST(BsUseInvalid2ForBsA8);
    CPPUNIT_TEST(BsCacheModeLruStoreCacheOnlyA9);
    CPPUNIT_TEST(BsCacheModeNoneStoreCacheOnlyA10);
    CPPUNIT_TEST(BsCacheModeLruStoreModifyNewKeyA11);
#ifdef HAVE_WINDOWED_MODE
// Windowed mode...
    CPPUNIT_TEST(TestWindowedMode);
    CPPUNIT_TEST(BsCacheModeLruStoreModifyOldKeyA12);
    CPPUNIT_TEST(BsSameWindowSizeCacheAndBsA13);
    CPPUNIT_TEST(BsSmallWindSizeCacheAndBigWindSizeBsA14);
    CPPUNIT_TEST(BsBigWindSizeCacheAndSmallWindSizeBsA15);
    CPPUNIT_TEST(BsLimitSizeSameCacheAndBsA16);
    CPPUNIT_TEST(BsLimitSizeBigCacheAndSmallBsA17);
    CPPUNIT_TEST(BsForEachPSizeOpenCacheForEachPSizeOpenBsLimitAndFillA18);
    CPPUNIT_TEST(BsUseFileBsFillDbCloseDbNewDbUseFileBsA19);
    CPPUNIT_TEST(BsUseMdbmBsFillDbCloseDbNewDbUseMdbmBsA20);
    CPPUNIT_TEST(BsUseMdbmBsFillDbCloseDbSameDbUseMdbmBsA21);
    CPPUNIT_TEST(BsStoreDataSetBsWithNewDbStoreNewA22);
    CPPUNIT_TEST(BsStoreDataSetReadOnlyBsWithNewDbStoreNewA23);
    CPPUNIT_TEST(BsTestReplaceA24);
    CPPUNIT_TEST(BsTestReplaceWithCache);
    CPPUNIT_TEST(BsUseMdbmBsInsertDataDeleteDataA25);
    CPPUNIT_TEST(OpenTooSmallReopenWindowed);
    CPPUNIT_TEST(BsTestMisc);
    CPPUNIT_TEST(BsTestInvalid);

    // mdbm_set_window_size
    CPPUNIT_TEST(BsCreateDbNoWindowFlagSetWindowSizeZeroB1);
    CPPUNIT_TEST(BsCreateDbNoWindowFlagSetWindowSizeB2);
    CPPUNIT_TEST(BsSetWindowSizeMaxIntB3);
    CPPUNIT_TEST(BsSetWindowSizeMinusOneB4);
    CPPUNIT_TEST(BsSetWindowSizeZeroB5);
    CPPUNIT_TEST(BsCheckSeveralDiffPageSizesSetWinSizeB6);
    CPPUNIT_TEST(BsCheckDiffPageSizesAgainstDiffWinSizesB7);
    CPPUNIT_TEST(BsNPageDbSetWinSizeStoreDataSetWinSize0B8);
    CPPUNIT_TEST(BsNPageDbSetWinSize3timesPsizeStoreDataSetWinSize2timesPsizeB9);
    CPPUNIT_TEST(BsNPageDbSetWinSize3timesPsizeStoreDataSetWinSize4timesPsizeB10);
    CPPUNIT_TEST(BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeLessNB11);
    CPPUNIT_TEST(BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeEqualNB12);
    CPPUNIT_TEST(BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeGreaterNB13);
    CPPUNIT_TEST(BsSetWindowIncreaseNumPagesFillDbB14);

    // mdbm_get_window_stats
    CPPUNIT_TEST(BsUseNullStatsPointerC1);
    CPPUNIT_TEST(BsUseTooSmallSizeC2);
    CPPUNIT_TEST(BsUseBigSizeC3);
    CPPUNIT_TEST(BsUseInBetweenSizeC4);
    CPPUNIT_TEST(BsCheckVariousWinSizesInLoopC5);
    CPPUNIT_TEST(BsNoWinModeFillDbGetStatsC6);
    CPPUNIT_TEST(BsWinModeNpagesFillDbNplusNpagesGetStatsC7);
    CPPUNIT_TEST(BsWinModeNpagesFillDbNpagesSetWinSize0GetStatsC8);
#endif

    CPPUNIT_TEST_SUITE_END();
public:
    BackStoreTsV3() : BackStoreTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackStoreTsV3);


#endif
