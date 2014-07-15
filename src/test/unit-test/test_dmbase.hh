/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_compress_tree mdbm_stat_header mdbm_stat_all_page mdbm_chk_error
//   mdbm_chk_page mdbm_chk_all_page mdbm_truncate mdbm_prune mdbm_purge
//   mdbm_set_cleanfunc mdbm_clean
#ifndef TEST_DMBASE_HH
#define TEST_DMBASE_HH

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

typedef int (*shakernew_funcp_t)(MDBM *, const datum*, const datum*, mdbm_shake_data_v3 *);

class DataMgmtBaseTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    void setUp();
    void tearDown() { }

    // Test Cases

    // mdbm_compress_tree
    void EmptyDbCompressA1();
    void PartFilledSinglePagedCompressA2();
    void FilledMultiPageCompressA3();
    void FilledMultiPageLoopDeleteAndCompressA4();
    void FilledMultiPageAllNullValueEntriesA5();
    void FilledMultiPageMaxNumPagesA6();
    void MultiPageSizesNoLargeObjFlagA7();
    void MultiPageSizesLargeObjFlagA8();
    void MultiPageSizesMemoryOnlyA9();

    // mdbm_stat_header
    void EmptyDbNonDefaultsStatB1();
    void MultiPageDbNonDefaultsStatB2();
    void OpenDbNOLOCKflagTheStatB3();

    // mdbm_stat_all_page
    void EmptyDbStatAllC1();
    void FilledSinglePagedDbStatAllC2();
    void FilledMultiPageDbStatAllC3();
    void AddLargeObjectStatAllC4();

    // mdbm_chk_error - no-op both v2 and v3

    // mdbm_chk_all_page
    void EmptyDbChkAllD1();
    void PartFilledDbChkAllD2();
    void FilledSinglePagedDbChkAllD3();
    void FilledMultiPageDbChkAllD4();
    void PartFilledMultiPageDbChkAllD5();

    // mdbm_chk_page
    void EmptyDbChkPage0E1();
    void PartFilledDbChkPage0E2();
    void FilledSinglePagedDbChkPage0E3();
    void FilledMultiPageDbChkAllPagesE4();
    void PartFilledMultiPageDbChkAllPagesE5();

    // mdbm_truncate
    void EmptyDbNonDefaultsTruncF1();
    void PartFilledDbNonDefaultsTruncF2();
    void FilledSinglePagedDbNonDefaultsTruncF3();
    void FilledMultiPageDbNonDefaultsTruncF4();
    //void FilledSinglePagedDbNonDefsAndShakeFuncTruncF5();
    void FilledMultiPageDbFileSizeTruncF6();
    void TestTruncateWarningMsgs();

    // mdbm_purge
    void EmptyDbNotDefaultsPurgeG1();
    void PartFilledDbNotDefaultsPurgeG2();
    void FilledSinglePagedDbNotDefaultsPurgeG3();
    void FilledMultiPagedDbNotDefaultsPurgeG4();
    void FilledSinglePagedDbNonDefsAndShakeFuncPurgeG5();

    // mdbm_prune
    void AddUniqueKeyPrunerMatchesH1();
    void AddDuplicateKeysPrunerMatchesH2();
    void AddUniqueValuePrunerMatchesH3();
    void FilledSinglePageDbAddDupValPrunerMatchesH4();
    void FilledMultiPageDbAddDupValPrunerMatchesH5();
    void FilledMultiPageDbPrunerDestroyAllH6();

    // mdbm_set_cleanfunc
    void SetCleanerCleansAllThenLimitSizeDbI1();
    void FillDbCleanerCleansNothingStoreDataI2();
    void FillDbCleanerCleanKeyQuitStoreSameSizedDataI3();
    void FillDbCleanerCleanKeyQuitStoreBiggerSizedDataI4();
    void CreateDbNODIRTYflagCleanerCleanKeyQuitStoreDataI5();
    void FillMultiPageDbUniqueKeysCleanKeyQuitStoreKeyI6();
    void FillMultiPageDbDuplicateKeysCleanKeyPageStoreDupKeysI7();
    void FillMultiPageDbDuplicateKeysCleanKeyPageInsertUniqueKeyI8();
    void FillMultiPageDbDuplicateKeysCleanKeyPageStoreDupNewKeyI9();
    void FillMultiPageDbLimitWithShakerSetCleanStoreBigDataI10();

    // mdbm_clean
    void FillDbDontSetCleanerFuncThenCleanAllJ1();
    void FillDb45pagesCleanPage32J2();
    void EmptyDbCleanerCleansAnyCleanPage0J3();
    void MultiPageDbCleanerCleansAnyThenLoopAndCleanJ4();
    void FillDbCleanerCleansAnyCleanAllRefillDbJ5();
    void FillDbCleanerCleansNothingCleanAllJ6();
    void FillMultiPageDbCleanerCleansKeyCleanAllStoreKeyJ7();
    void FillMultiPageDbCleanerCleansKeyCleanUsingGetPageKeyStoreKeyJ8();
    void FillDbDuplicateKeysUniqueValsCleanerCleansValsStoreJ9();
    void FillDb0SizedValsCleanerCleansKeysStoreValSizeN_J10();
    void FillDbDuplicateKeysMostValsSizeNminus1_StoreCleanSizeNStoreSizeN_J11();
    void FillDbDuplicateKeysMostValsSizeNminus1_StoreCleanSizeNStoreSizeNplus1_J12();
    void CreateDbNODIRTYflagCleanerCleansAllCleanPage0J13();

    void ErrorCasesMdbmSetCleanFun();
protected:
    DataMgmtBaseTestSuite(int vFlag) : TestBase(vFlag, "DataManagementTestSuite") {}
    int v2orV3flag() { return TestBase::versionFlag; }

    off_t getFileSize(const string &dbName); // return file size of specified db file
    // RETURN: number of records added to the DB
    int createAndAddData(MdbmHolder &dbh, int numPages, const string &keyBaseName, const char *value, int &defAlignVal, int &defHashID, int extraFlags);
    string makeKeyName(int incr, const string &keyBaseName);
    int deleteAllEntriesOnPage(MDBM *dbh);
    //void verifyDefaultConfig(MdbmHolder &dbh, const string &prefix, bool truncateDB, int defAlignVal, int defHashID, shakernew_funcp_t shaker = 0, int npagesLimit = -1);
    void verifyDefaultConfig(MdbmHolder &dbh, const string &prefix, bool truncateDB, int defAlignVal, int defHashID, int npagesLimit = -1, shakernew_funcp_t shaker=NULL, void* shakerData=NULL);

    // can only set the cache flag before data is added, thus a special needs method to fill the
    // DB without going into an infinite loop
    // RETURN: number of entries added to the DB
    int createCacheModeDB(string &prefix, MdbmHolder &dbh, string &keyBaseName, string &value, int limitNumPages, mdbm_clean_func cleanerfunc=0, void *cleanerdata=0);

    void parseHeaderOutput(stringstream &istrm, const string &prefix, int expectedNumPages, int expectedPageSize, const string &hashFunc, bool lockingEnabled, int pid);
    void parseStatAllPageOutput(stringstream &istrm, const string &prefix, const string &dbName, int expectedNumEntries, int avgBytesPerRecord, int numLargeObjEntries, int largeObjAvgDataSize, int pid);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _PageSizeRange[];
};


class DataMgmtV3 : public DataMgmtBaseTestSuite
{
    CPPUNIT_TEST_SUITE(DataMgmtV3);

    // FIX mdbm_compress_tree not implemented in V3 - v3 calls abort()
    // FIX CPPUNIT_TEST(MultiPageSizesNoLargeObjFlagA7);
    // FIX CPPUNIT_TEST(MultiPageSizesLargeObjFlagA8);
    // FIX CPPUNIT_TEST(MultiPageSizesMemoryOnlyA9)

    CPPUNIT_TEST(EmptyDbChkAllD1);
    CPPUNIT_TEST(PartFilledDbChkAllD2);
    CPPUNIT_TEST(FilledSinglePagedDbChkAllD3);
    CPPUNIT_TEST(FilledMultiPageDbChkAllD4);
    CPPUNIT_TEST(PartFilledMultiPageDbChkAllD5);

    CPPUNIT_TEST(EmptyDbChkPage0E1);
    CPPUNIT_TEST(PartFilledDbChkPage0E2);
    CPPUNIT_TEST(FilledSinglePagedDbChkPage0E3);
    CPPUNIT_TEST(FilledMultiPageDbChkAllPagesE4);
    CPPUNIT_TEST(PartFilledMultiPageDbChkAllPagesE5);

    CPPUNIT_TEST(EmptyDbNonDefaultsTruncF1);
    CPPUNIT_TEST(PartFilledDbNonDefaultsTruncF2);
    CPPUNIT_TEST(FilledSinglePagedDbNonDefaultsTruncF3);
    CPPUNIT_TEST(FilledMultiPageDbNonDefaultsTruncF4);
    //CPPUNIT_TEST(FilledSinglePagedDbNonDefsAndShakeFuncTruncF5);
    CPPUNIT_TEST(FilledMultiPageDbFileSizeTruncF6);
    CPPUNIT_TEST(TestTruncateWarningMsgs);

    CPPUNIT_TEST(EmptyDbNotDefaultsPurgeG1);
    CPPUNIT_TEST(PartFilledDbNotDefaultsPurgeG2);
    CPPUNIT_TEST(FilledSinglePagedDbNotDefaultsPurgeG3);
    CPPUNIT_TEST(FilledMultiPagedDbNotDefaultsPurgeG4);
    CPPUNIT_TEST(FilledSinglePagedDbNonDefsAndShakeFuncPurgeG5);

    CPPUNIT_TEST(AddUniqueKeyPrunerMatchesH1);
    CPPUNIT_TEST(AddDuplicateKeysPrunerMatchesH2);
    CPPUNIT_TEST(AddUniqueValuePrunerMatchesH3);
    CPPUNIT_TEST(FilledSinglePageDbAddDupValPrunerMatchesH4);
    CPPUNIT_TEST(FilledMultiPageDbAddDupValPrunerMatchesH5);
    CPPUNIT_TEST(FilledMultiPageDbPrunerDestroyAllH6);

    CPPUNIT_TEST(SetCleanerCleansAllThenLimitSizeDbI1);
    CPPUNIT_TEST(FillDbCleanerCleansNothingStoreDataI2);
    CPPUNIT_TEST(FillDbCleanerCleanKeyQuitStoreSameSizedDataI3);
    CPPUNIT_TEST(FillDbCleanerCleanKeyQuitStoreBiggerSizedDataI4);
    CPPUNIT_TEST(CreateDbNODIRTYflagCleanerCleanKeyQuitStoreDataI5);
    CPPUNIT_TEST(FillMultiPageDbUniqueKeysCleanKeyQuitStoreKeyI6);
    CPPUNIT_TEST(FillMultiPageDbDuplicateKeysCleanKeyPageStoreDupKeysI7);
    CPPUNIT_TEST(FillMultiPageDbDuplicateKeysCleanKeyPageInsertUniqueKeyI8);
    CPPUNIT_TEST(FillMultiPageDbDuplicateKeysCleanKeyPageStoreDupNewKeyI9);
    CPPUNIT_TEST(FillMultiPageDbLimitWithShakerSetCleanStoreBigDataI10);

    CPPUNIT_TEST(FillDbDontSetCleanerFuncThenCleanAllJ1);
    CPPUNIT_TEST(FillDb45pagesCleanPage32J2);
    CPPUNIT_TEST(EmptyDbCleanerCleansAnyCleanPage0J3);
    CPPUNIT_TEST(MultiPageDbCleanerCleansAnyThenLoopAndCleanJ4);
    CPPUNIT_TEST(FillDbCleanerCleansAnyCleanAllRefillDbJ5);
    CPPUNIT_TEST(FillDbCleanerCleansNothingCleanAllJ6);
    CPPUNIT_TEST(FillMultiPageDbCleanerCleansKeyCleanAllStoreKeyJ7);
    CPPUNIT_TEST(FillMultiPageDbCleanerCleansKeyCleanUsingGetPageKeyStoreKeyJ8);
    CPPUNIT_TEST(FillDbDuplicateKeysUniqueValsCleanerCleansValsStoreJ9);
    CPPUNIT_TEST(FillDb0SizedValsCleanerCleansKeysStoreValSizeN_J10);
    CPPUNIT_TEST(FillDbDuplicateKeysMostValsSizeNminus1_StoreCleanSizeNStoreSizeN_J11);
    CPPUNIT_TEST(FillDbDuplicateKeysMostValsSizeNminus1_StoreCleanSizeNStoreSizeNplus1_J12);
    CPPUNIT_TEST(CreateDbNODIRTYflagCleanerCleansAllCleanPage0J13);
    CPPUNIT_TEST(ErrorCasesMdbmSetCleanFun);

    CPPUNIT_TEST_SUITE_END();

public:
    DataMgmtV3() : DataMgmtBaseTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(DataMgmtV3);


#endif
