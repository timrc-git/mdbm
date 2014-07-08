/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_magic_number

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include <iostream>
#include <vector>
#include <algorithm>

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

/* Fixed the following bugzilla tickets:
 *   BZ ticket 5283034: V2 : mdbm_get_magic_number: returned error before fix. Now works.
 */
class GetMagVersTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    GetMagVersTestSuite(int vFlag) : TestBase(vFlag, "GetMagicVersionTestSuite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    void OpenPremadeFilledMag();  // TC E-1
    void OpenPremadeFilledVer();  // TC K-1
    void TruncPremadeFilledMag();  // TC E-3
    void TruncPremadeFilledVer();  // TC K-5
    void TruncNewDbMag();  // TC E-4
    void TruncNewDbVer();  // TC K-4
    void ReplaceNewWithPremadeMag();  // TC E-7
    void ReplaceNewWithPremadeVer();  // TC K-7
    void ReplaceNewWithPremadeOppositeVer();  // TC K-10
    void CreateMag();  // E-13
    void CreateVer();  // K-12
    void CreateV2withLOmag();  // E-12
private:
    // function objects for get magic number and get version
    //
    struct GetMagicNumFO
    {
        GetMagicNumFO(const string prefix, int v2orV3flag) :
           _prefix(prefix), _v2orV3flag(v2orV3flag) {}
        virtual ~GetMagicNumFO() {}

        virtual int operator() (MDBM *dbh, uint32_t &magicNum) {
            int ret = mdbm_get_magic_number(dbh, & magicNum);
            if (ret != 0)
                return ret;

            if (_v2orV3flag == MDBM_CREATE_V3)
                return int(magicNum) == (_MagNumV3 ? 0 : -4);

            if (magicNum == _MagNumV2a || magicNum == _MagNumV2b)
                return 0;

            return -4;
        }

        string  _prefix;
        int     _v2orV3flag;

        static uint32_t _MagNumV2a;
        static uint32_t _MagNumV2b;
        static uint32_t _MagNumV3;
    };
    struct GetVersNumFO : public GetMagicNumFO
    {
        GetVersNumFO(const string prefix, int v2orV3flag) :
            GetMagicNumFO(prefix, v2orV3flag) {}

        int operator() (MDBM *dbh, uint32_t &num) {
            num = mdbm_get_version(dbh);
            if (_v2orV3flag == MDBM_CREATE_V3)
                return int(num) == (_VersNumV3 ? 0 : -1);

            return int(num) == (_VersNumV2 ? 0 : -1);
        }

        static uint32_t _VersNumV2;
        static uint32_t _VersNumV3;
    };

    void openPremadeFilled(GetMagicNumFO & getFO, const string &tcprefix);  // TC E-1
    void truncPremadeFilled(GetMagicNumFO & getFO, const string &tcprefix);  // TC E-3, K-3

    void truncNewDb(GetMagicNumFO & getFO, const string &tcprefix);

    void replaceNewWithPremade(GetMagicNumFO & getFO, const string &tcprefix, int v2orV3flagPreDB);
    void createDB(GetMagicNumFO & getFO, const string &tcprefix);  // E-11

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
};

int GetMagVersTestSuite::_setupCnt = 0;
uint32_t GetMagVersTestSuite::GetMagicNumFO::_MagNumV2a = _MDBM_MAGIC_NEW; // 0x01023963
uint32_t GetMagVersTestSuite::GetMagicNumFO::_MagNumV2b = _MDBM_MAGIC; // 0x01023962
uint32_t GetMagVersTestSuite::GetMagicNumFO::_MagNumV3  = _MDBM_MAGIC_NEW2; // 0x01023964;
uint32_t GetMagVersTestSuite::GetVersNumFO::_VersNumV2 = 2;
uint32_t GetMagVersTestSuite::GetVersNumFO::_VersNumV3 = 3;


void GetMagVersTestSuite::setUp()
{
    if (_setupCnt++ == 0) {
        cout << endl << "Get Magic & Version Number Test Suite Beginning..." << endl << flush;
    }
}

/*
   Open pre-made v2 DB.
*/
void GetMagVersTestSuite::openPremadeFilled(GetMagicNumFO &getFO, const string &tcprefix)   // TC E-1, K-1
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|versionFlag));

    // 2. Call {mdbm_get_magic_number or mdbm_get_version}:
    uint32_t num = 0;
    int ret = (getFO)(dbh, num);

    stringstream msgss;
    msgss << prefix
          << " : Expected to Succeed retrieving and matching number. Return code=" << ret
          << " and Value retrieved(hex)=" << hex << num << endl;
    CPPUNIT_ASSERT_MESSAGE(msgss.str(), ret == 0);

}

void GetMagVersTestSuite::OpenPremadeFilledMag()   // TC E-2
{
    string prefix = "TC E-1/2: OpenPremadeFilledMag: mdbm_get_magic_number: ";
    TRACE_TEST_CASE(prefix);

    GetMagicNumFO magFO(prefix, versionFlag);
    openPremadeFilled(magFO, prefix);
}

void GetMagVersTestSuite::OpenPremadeFilledVer()   // TC K-2
{
    string prefix = "TC K-1/2: OpenPremadeFilledV3ver: mdbm_get_version: ";
    TRACE_TEST_CASE(prefix);
    GetVersNumFO versFO(prefix, versionFlag);
    openPremadeFilled(versFO, prefix);
}

/*
   Truncate pre-made v2 DB.
*/
void GetMagVersTestSuite::truncPremadeFilled(GetMagicNumFO &getFO, const string &tcprefix)   // TC E-3, K-3
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|versionFlag));

    // 2. Call mdbm_truncate
    mdbm_truncate(dbh);

    // 3. Call {mdbm_get_magic_number or mdbm_get_version}: m
    uint32_t num = 0;
    int ret = (getFO)(dbh, num);

    stringstream msgss;
    msgss << prefix
          << " : Expected to Succeed retrieving and matching number. Return code=" << ret
          << " and Value retrieved(hex)=" << hex << num << endl;
    CPPUNIT_ASSERT_MESSAGE(msgss.str(), ret == 0);
}

/*
   Truncate new v2 DB.
*/
void GetMagVersTestSuite::truncNewDb(GetMagicNumFO &getFO, const string &tcprefix)
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags));

    // 2. Call mdbm_store to place some data in DB
    InsertData(dbh, 7, 7, 3);

    // 3. Call {mdbm_get_magic_number, mdbm_get_version}: v;
    uint32_t num = 0;
    int ret = (getFO)(dbh, num);

    // 4. Expected result: v == 2;
    stringstream msgss;
    msgss << prefix
          << " : Expected to Succeed retrieving and matching number. Return code=" << ret
          << " and Value retrieved(hex)=" << hex << num << endl;
    CPPUNIT_ASSERT_MESSAGE(msgss.str(), ret == 0);

    // 5. Call mdbm_truncate
    mdbm_truncate(dbh);

    // 6. Call {mdbm_get_magic_number or mdbm_get_version}: m
    num = 0;
    ret = (getFO)(dbh, num);

    // 7. Expected result: v == 2;
    stringstream msgss2;
    msgss2 << prefix
           << " : Expected to Succeed retrieving and matching number. Return code=" << ret
           << " and Value retrieved(hex)=" << hex << num << endl;
    CPPUNIT_ASSERT_MESSAGE(msgss2.str(), ret == 0);
}

void GetMagVersTestSuite::TruncPremadeFilledMag()   // TC E-5
{
    string prefix = "TC E-3/5: TruncPremadeFilledMag: mdbm_get_magic_number: ";
    TRACE_TEST_CASE(prefix);

    GetMagicNumFO magFO(prefix, versionFlag);
    truncPremadeFilled(magFO, prefix);
}
void GetMagVersTestSuite::TruncPremadeFilledVer()   // TC K-5
{
    string prefix = "TC K-3/5: TruncPremadeFilledVer: mdbm_get_version: ";
    TRACE_TEST_CASE(prefix);

    GetVersNumFO versFO(prefix, versionFlag);
    truncPremadeFilled(versFO, prefix);
}

void GetMagVersTestSuite::TruncNewDbMag()   // TC E-6
{
    string prefix = "TC E-4/6: TruncNewDbMag: mdbm_get_magic_number: ";
    TRACE_TEST_CASE(prefix);

    GetMagicNumFO magFO(prefix, versionFlag);
    truncNewDb(magFO, prefix);
}
void GetMagVersTestSuite::TruncNewDbVer()   // TC K-6
{
    string prefix = "TC K-4/6: TruncNewDbVer: mdbm_get_version: ";
    GetVersNumFO versFO(prefix, versionFlag);
    truncNewDb(versFO, prefix);
}

/*
   Replace v2 DB with v2 DB File.
*/
void GetMagVersTestSuite::replaceNewWithPremade(GetMagicNumFO &getFO, const string &tcprefix,
                      int v2orV3flagPreDB)
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << " Premade DB version=3";
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create a DB with typical defaults; Make copy of the premade DB and open

    // 1.a. Setup DB: Open and create DB with typical defaults
    string newDbName;
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_LARGE_OBJECTS | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, -1, -1, &newDbName));

    // 1.b. Setup DB: Make copy of the premade DB and open
    string preMadeDbName;
    MdbmHolder preMadeDB(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|v2orV3flagPreDB, 0644, -1, -1, &preMadeDbName));

    // 2. Call mdbm_replace_db with the open DB handle and the copied file
    int ret = mdbm_replace_db(dbh, preMadeDB);
    stringstream repmsg;
    repmsg <<  prefix << " Replacement of file=" << newDbName
           << " with file=" << preMadeDbName << " should ";
    if (ret == 0) {
        if (versionFlag != v2orV3flagPreDB) { // replace should have fail
            cout << repmsg.str() << " FAIL but didn't, so we will continue to verify..." << endl;
        }
    } else if (versionFlag != v2orV3flagPreDB) { // replace should fail
        cout << repmsg.str() << " FAIL so ignore test and return success. Completed" << endl;
        return;
    } else {
        cout << repmsg.str() << " Succeed but didn't, so we will continue to verify..." << endl;
    }

    // 3. Call {mdbm_get_magic_number, mdbm_get_version}: m
    uint32_t num = 0;
    ret = (getFO)(dbh, num);

    // 4. Expected result: v == 2;
    stringstream msgss;
    msgss << prefix
          << " : Expected to Succeed retrieving and matching number. Return code=" << ret
          << " and Value retrieved(hex)=" << hex << num << endl;
    CPPUNIT_ASSERT_MESSAGE(msgss.str(), ret == 0);
}

void GetMagVersTestSuite::ReplaceNewWithPremadeMag()   // TC E-9
{
    string prefix = "TC E-7/9: ReplaceNewWithPremadeMag: mdbm_get_magic_number: ";
    TRACE_TEST_CASE(prefix);

    GetMagicNumFO magFO(prefix, versionFlag);
    replaceNewWithPremade(magFO, prefix, versionFlag);
}
void GetMagVersTestSuite::ReplaceNewWithPremadeVer()   // TC K-9
{
    string prefix = "TC K-7/9: ReplaceNewWithPremadeVer: mdbm_get_version: ";
    TRACE_TEST_CASE(prefix);

    GetVersNumFO verFO(prefix, versionFlag);
    replaceNewWithPremade(verFO, prefix, versionFlag);
}

/*
   Create new database.
*/
void GetMagVersTestSuite::createDB(GetMagicNumFO &getFO, const string &tcprefix)  // E-11
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags));

    // 2. Call mdbm_get_magic_number: m;
    uint32_t num = 0;
    int ret = (getFO)(dbh, num);

    stringstream msgss;
    msgss << prefix
          << " : Expected to Succeed retrieving and matching number. Return code=" << ret
          << " and Value retrieved(hex)=" << hex << num << endl;
    CPPUNIT_ASSERT_MESSAGE(msgss.str(), ret == 0);
}

void GetMagVersTestSuite::CreateV2withLOmag()  // E-12
{
    string prefix = "TC E-12: CreateV2withLOmag: mdbm_get_magic_number: ";
    GetMagicNumFO magFO(prefix, versionFlag | MDBM_LARGE_OBJECTS);
    createDB(magFO, prefix);
}
void GetMagVersTestSuite::CreateMag()  // E-13
{
    string prefix = "TC E-11/13: CreateV3mag: mdbm_get_magic_number: ";
    TRACE_TEST_CASE(prefix);

    GetMagicNumFO magFO(prefix, versionFlag);
    createDB(magFO, prefix);
}
void GetMagVersTestSuite::CreateVer()  // K-12
{
    string prefix = "TC K-11/12: CreateV3ver: mdbm_get_version: ";
    TRACE_TEST_CASE(prefix);
    GetVersNumFO verFO(prefix, versionFlag);
    createDB(verFO, prefix);
}



class GetMagVersTestSuiteV3 : public GetMagVersTestSuite
{
    CPPUNIT_TEST_SUITE(GetMagVersTestSuiteV3);
    CPPUNIT_TEST(OpenPremadeFilledMag);
    CPPUNIT_TEST(OpenPremadeFilledVer);
    CPPUNIT_TEST(CreateMag);  // E-13
    CPPUNIT_TEST(CreateVer);  // K-12
    CPPUNIT_TEST(TruncPremadeFilledMag);
    CPPUNIT_TEST(TruncPremadeFilledVer);
    CPPUNIT_TEST(TruncNewDbMag);
    CPPUNIT_TEST(TruncNewDbVer);
    CPPUNIT_TEST(ReplaceNewWithPremadeMag);
    CPPUNIT_TEST(ReplaceNewWithPremadeVer);
    CPPUNIT_TEST_SUITE_END();
  public:
    GetMagVersTestSuiteV3() : GetMagVersTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(GetMagVersTestSuiteV3);

