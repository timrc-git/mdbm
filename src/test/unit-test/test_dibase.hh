/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_page mdbm_dump_all_page mdbm_dump_page
//   mdbm_save mdbm_restore
#ifndef TEST_DIBASE_H
#define TEST_DIBASE_H

#include <vector>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "TestBase.hh"
#include "test_lockbase.hh"

class DataInterchangeBaseTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    virtual ~DataInterchangeBaseTestSuite() {}
    void setUp();
    void tearDown() { }

    // Test Cases

    // mdbm_get_page
    void EmptyDBgetpageA1();
    void GetpageNullKeyA2();
    void PartialDBgetPageValidKeyA3();
    void PartialDBgetPageInvalidKeysA4();
    void FullDBgetPageValidKeysA5();
    void FullDBgetPageInvalidKeysA6();
    void VariousHashFunctionsValidKeyA7();
    void FullMultiPageDBgetPageKeyCountA8();
    void AlternateValidInvalidKeysA9();
    void MultiPageDBalternateValidInvalidKeysA10();
    void TestNullGetPage();

    // mdbm_save
    void PartialDbSaveToNewB1();
    void PartialDbSaveToNewFileModeReadOnlyB2();
    void PartialDbSaveToNewNoCreateFlagB3();
    void PartialDbSaveToOldMultiPageB4();
    void PartialDbSaveToOldMultiPageNoTruncFlagB5();
    void PartialDbSaveToNewUseDbVersionFlagB6();
    void PartialDbSaveToInvalidPathB7();
    void EmptyDbSaveToNewB8();
    void EmptyDbSaveToNewFileModeReadOnlyB9();
    void EmptyDbSaveToToOldMultiPageB10();
    void FullDbSaveToNewB11();
    void FullDbSaveToNewFileModeMinPermsB12();
    void FullDbUseBadCompressionLevelB13();
    void DbFullDupDataCompareToDbFullUniqueDataB14();

    // mdbm_restore
    void SaveUsingV3FlagThenRestoreC1();
    void RestoreFromNonExistentFileC2();
    void RestoreFromDBthatWasNot_mdbm_savedC3();
    void SaveWithChmod0001_ThenRestoreC4();
    void OpenDBwithDataRestoreFromDBwithDiffDataC5();
    void SaveThenRestoreUsingSameDbHandleC6();
    void SaveEmptyDbRestoreToFullDbC7();
    void SaveDbDiffCompressionLevelsRestoreC8();
    void SaveUsingBinaryKeysAndDataThenRestoreC9();

    // mdbm_dump_all_page
    void DumpAllEmptyDbD1();
    void DumpAllPartialFilledDbD2();
    void DumpAllSinglePageFilledDbD3();
    void DumpAllMultiPageFilledDbD4();
    void DumpAllCacheDbD5();
    void DumpAllCacheDbD6();

    // mdbm_dump_page
    void DumpPage0EmptyDbE1();
    void DumpBadPageEmptyDbE2();
    void DumpPage0PartFilledDbE3();
    void DumpBadPagePartFilledDbE4();
    void DumpPage0SinglePageFilledDbE5();
    void DumpBadPageSinglePageFilledDbE6();
    void DumpPage1MultiPageFilledDbE7();
    void DumpBadPageMultiPageFilledDbE8();
    void DumpAllPagesMultiPageFilledDbE9();
    void SaveLargeDB();
protected:
    DataInterchangeBaseTestSuite(int vFlag) : TestBase(vFlag, "DataInterchangeTestSuite") {}

    // dbName : create DB with the specified name
    // numPages : 0 means create with 1 page and store only one key/value pair
    //          : > 0 means create with numPages and fill with key/value pairs
    // keyBaseName : use specified name as base of key name to store the value
    // value : use specified value to be stored
    // numRecsAdded : number of value/keys added to the DB
    MDBM* createDbAndAddData(MdbmHolder &dbh, int numPages, const string &keyBaseName, const string &value);
    MDBM* createDbAndAddData(MdbmHolder &dbh, int numPages, const string &keyBaseName, const string &value, int &numRecsAdded);
    string makeKeyName(int incr, const string &keyBaseName);
    void getpage(const string &tcmsg, MDBM *dbh, datum *key, bool expectOK);
    MDBM* addData(MDBM *dbh, int numPages, const string &keyBaseName, const string &value, int &numRecsAdded);
    int statFile(const string &dbname, struct stat &statbuf);
    void verifyData(string &keyBaseName, MDBM *newdbh, const string &saveFile, int numRecs, const string &prefix);
    void parseDumpOutput(stringstream &istrm, const string &prefix, int expectedNumPages, int expectedNumEntries, int pid);
    void parseDumpV3Output(stringstream &istrm, const string &prefix, int expectedNumPages, int expectedNumEntries, int pid);
    int parseNumber(string &token, const char *delim); // ex: <token>=nnn so delim is "="
    void forkAndParseDump(const string &prefix, MdbmHolder &db, int numRecsAdded, int numPages);
    void forkAndParseDumpPages(const string &prefix, MdbmHolder &db, vector<int> &pages, int expectedNumEntriesOnPage, bool expectSuccess = true);

    int *_hashIDs; // hash ID list - to be set by the child class
    int  _hashIDlen; // the number elements in _hashIDs - to be set by the child class

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST

    // These tests fail when redirecting output, so MDBM_UNITTEST_LEVEL=1,2,3 is set to disable them
    bool runStdoutTest() {
        int ret = GetUnitTestLevelEnv();
        if (!ret || (ret >= RunAllTestCases)) {
            return true;
        }
        return false;
    }
};


class DataInterchangeV3 : public DataInterchangeBaseTestSuite
{
    CPPUNIT_TEST_SUITE(DataInterchangeV3);
    CPPUNIT_TEST(EmptyDBgetpageA1);
    CPPUNIT_TEST(GetpageNullKeyA2);
    CPPUNIT_TEST(PartialDBgetPageValidKeyA3);
    CPPUNIT_TEST(PartialDBgetPageInvalidKeysA4);
    CPPUNIT_TEST(FullDBgetPageValidKeysA5);
    CPPUNIT_TEST(FullDBgetPageInvalidKeysA6);
    CPPUNIT_TEST(VariousHashFunctionsValidKeyA7);
    CPPUNIT_TEST(FullMultiPageDBgetPageKeyCountA8);
    CPPUNIT_TEST(AlternateValidInvalidKeysA9);
    CPPUNIT_TEST(MultiPageDBalternateValidInvalidKeysA10);

    CPPUNIT_TEST(DumpAllPartialFilledDbD2);
    CPPUNIT_TEST(DumpAllSinglePageFilledDbD3);
    CPPUNIT_TEST(DumpAllMultiPageFilledDbD4);
    CPPUNIT_TEST(DumpAllCacheDbD5);
    CPPUNIT_TEST(DumpAllCacheDbD6);

    CPPUNIT_TEST(TestNullGetPage);

    CPPUNIT_TEST_SUITE_END();
public:
    DataInterchangeV3() : DataInterchangeBaseTestSuite(MDBM_CREATE_V3) {}
    void setUp();
private:
    static int _DIv3HashIDs[];
    static int _DIv3HashIDlen;
};
CPPUNIT_TEST_SUITE_REGISTRATION(DataInterchangeV3);

#endif
