/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_limit_dir_size

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <errno.h>

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


class LimitDirSizeTestBase : public CppUnit::TestFixture, public TestBase
{
public:
    LimitDirSizeTestBase(int vFlag, const string& vString) : versionFlag(vFlag), versionString(vString) { SetSuiteName("Limit Directory Size Test Suite: "); }
    void setUp();
    void tearDown();

    // unit tests in this suite
    //
    void ValidDirSizes();  // TC J-2
    void InvalidDirSizes();  // TC J-3
    void LimPagesSetDirSmaller();  // TC J-4
    void LimPagesSetDirBigger();  // TC J-5
    void LimPagesSetDirSame();  // TC J-6

protected:
    void limitDirSize(int numPages, int expectedRetCode, const char * tcprefix);
    void limPagesSetDirSize(int limPages, int dirPages, int expectedRetCode, const char * tcprefix);
    void verifyMaxDirWidth(MDBM *dbh, int dirPages, const string &prefix);

    int versionFlag;
    string versionString;

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _MaxDirShift;
};


int LimitDirSizeTestBase::_setupCnt = 0;

// MDBM_DIRSHIFT_MAX is defined internally so cant use the macro here
int LimitDirSizeTestBase::_MaxDirShift = 24;


void LimitDirSizeTestBase::setUp()
{
    if (_setupCnt++ == 0) {
        cout << endl << "Limit Directory Size Test Suite Beginning..." << endl << flush;
    }
}
void LimitDirSizeTestBase::tearDown() { }

void LimitDirSizeTestBase::verifyMaxDirWidth(MDBM *dbh, int dirPages, const string &prefix)
{
    // get db info
    mdbm_db_info_t dbinfo;
    int ret = mdbm_get_db_info(dbh, &dbinfo);
    if (ret == 0) {
        //int calcMaxDirPages = (int)pow(2, dbinfo.db_max_dir_shift);
        int calcMaxDirPages = 1 << dbinfo.db_max_dir_shift;

        GetLogStream() << endl << prefix << "Database Info: Number pages=" << dbinfo.db_num_pages
            << " Maximum pages=" << dbinfo.db_max_pages
            << " Number Directory pages=" << dbinfo.db_num_dir_pages
            << " Directory width=" << dbinfo.db_dir_width
            << " Maximum Directory width=" << dbinfo.db_max_dir_shift
            << " Calculated Number of Directory pages=" << calcMaxDirPages << endl << endl << flush;

        // dirPages <= 2 ^ dbinfo.db_max_dir_shift
        stringstream dbmsg;
        dbmsg << prefix
              << "Expected Number directory pages=" << dirPages
              << " to be >= 2 ^ MAX(" << dirPages
              << ") directory width. Actual MAX directory width 2 ^ " << dbinfo.db_max_dir_shift
              << " which=" << calcMaxDirPages << endl;
        CPPUNIT_ASSERT_MESSAGE(dbmsg.str(), (dirPages >= calcMaxDirPages));
    }
}

void LimitDirSizeTestBase::limitDirSize(int numPages, int expectedRetCode, const char * tcprefix)
{
    stringstream premsg;
    premsg << tcprefix << " page-count-limit=" << numPages << ": ";
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);
    prefix = SUITE_PREFIX() + prefix;

    // Call mdbm_open; Create new DB using page size = p
    MdbmHolder dbh = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag);

    int ret = mdbm_limit_dir_size(dbh, numPages);

    stringstream ssmsg;
    ssmsg << prefix << "Expected return code=" << expectedRetCode
          << " but received=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (ret == expectedRetCode));

    if ((expectedRetCode != -1) && (versionFlag & MDBM_CREATE_V3)) {
        verifyMaxDirWidth(dbh, numPages, prefix);
    }

}
//  There is an internal maximum for directory size of MDBM_DIRSHIFT_MAX = 24.
//  So a set of invalid numbers can be : n in 1, 7, MDBM_DIRSHIFT_MAX

//  1. Call mdbm_open; create new v3 DB using defaults
//  2. Call mdbm_limit_dir_size: n
//  3. Expected result: success
void LimitDirSizeTestBase::ValidDirSizes()  // TC J-2
{
    limitDirSize(1, 0, "TC J-2: ValidDirSizes: mdbm_limit_dir_size: ");
    limitDirSize(3, 0, "TC J-2: ValidDirSizes: mdbm_limit_dir_size: ");
    limitDirSize(4, 0, "TC J-2: ValidDirSizes: mdbm_limit_dir_size: ");
    limitDirSize(7, 0, "TC J-2: ValidDirSizes: mdbm_limit_dir_size: ");
    limitDirSize(16, 0, "TC J-2: ValidDirSizes: mdbm_limit_dir_size: ");

    // MDBM_DIRSHIFT_MAX is defined internally so cant use the macro here
    limitDirSize(_MaxDirShift, 0, "TC J-2: ValidDirSizes: mdbm_limit_dir_size: ");
}
//  As an int, dir size could be negative. Also there is an internal maximum
//  for directory size of MDBM_DIRSHIFT_MAX = 24. So a set of invalid numbers
//  can be : n in -1, 0, 25

//    1. Call mdbm_open; create new v3 DB using defaults
//    2. Call mdbm_limit_dir_size: n
//    3. Expected result: FAIL and sets errno
void LimitDirSizeTestBase::InvalidDirSizes()  // TC J-3
{
    // BZ ticket 5285592
    limitDirSize(-1, -1, "TC J-3: InvalidDirSizes: mdbm_limit_dir_size: ");

    // MDBM_DIRSHIFT_MAX is defined internally so cant use the macro here
    limitDirSize(1<<(_MaxDirShift + 1), -1, "TC J-3: InvalidDirSizes: mdbm_limit_dir_size: ");

    limitDirSize(0, -1, "TC J-3: InvalidDirSizes: mdbm_limit_dir_size: ");
}


void LimitDirSizeTestBase::limPagesSetDirSize(int limPages, int dirPages, int expectedRetCode, const char * tcprefix)
{
    stringstream premsg;
    premsg << GetSuiteName() << tcprefix
           << " Number pages to limit directory size=" << dirPages
           << ": Number pages to limit database size=" << limPages << ": ";
    string prefix = premsg.str();
    TRACE_TEST_CASE(prefix);
    prefix = SUITE_PREFIX() + prefix;

    // Call mdbm_open; Create new DB using page size = p
    MdbmHolder dbh = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag);


    int ret = mdbm_limit_size_v3(dbh, limPages, 0, 0);
    stringstream lsmsg;
    lsmsg << prefix
          << "Expected mdbm_limit_size to succeed setting pages=" << limPages << endl;
    CPPUNIT_ASSERT_MESSAGE(lsmsg.str(), (ret == 0));

    ret = mdbm_limit_dir_size(dbh, dirPages);

    stringstream dsmsg;
    dsmsg << prefix
          << "Expected return code=" << expectedRetCode
          << " but received=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(dsmsg.str(), (ret == expectedRetCode));

    // get db info
    if (versionFlag & MDBM_CREATE_V3)
        verifyMaxDirWidth(dbh, dirPages, prefix);

}
// Limit the DB size(number of pages) then set directory size to support less pages than the limit.
//   1. Call mdbm_open; create v3 DB using defaults
//   2. Call mdbm_limit_size; number of pages p = 16
//   3. Call mdbm_limit_dir_size; pages p = 8
//   4. Call get_db_info; get structure(mdbm_db_info_t) of internal data fields
//   5. Expected result: mdbm_db_info_t.db_max_dir_shift == 2
void LimitDirSizeTestBase::LimPagesSetDirSmaller()  // TC J-4
{
    limPagesSetDirSize(16, 8, 0, "TC J-4: LimPagesSetDirSmaller: mdbm_limit_dir_size: ");
}
// Limit the DB size(number of pages) then set directory size to support more pages than the limit.
//   1. Call mdbm_open; create v3 DB using defaults
//   2. Call mdbm_limit_size; number of pages p = 1
//   3. Call mdbm_limit_dir_size; dir size d = 24
//   4. Expected result: FAIL and errno set
void LimitDirSizeTestBase::LimPagesSetDirBigger()  // TC J-5
{
    limPagesSetDirSize(1, 24, 0, "TC J-5: LimPagesSetDirBigger: mdbm_limit_dir_size: ");
}

// Limit the DB size(number of pages) then set directory size to support same pages as the limit.
//   1. Call mdbm_open; create v3 DB using defaults
//   2. Call mdbm_limit_size; number of pages p = 8
//   3. Call mdbm_limit_dir_size; pages p = 8
//   4. Call get_db_info; get structure(mdbm_db_info_t) of internal data fields
//   5. Expected result: mdbm_db_info_t.db_max_dir_shift == 2
void LimitDirSizeTestBase::LimPagesSetDirSame()  // TC J-6
{
    limPagesSetDirSize(8, 8, 0, "TC J-6: LimPagesSetDirSame: mdbm_limit_dir_size: ");
}



class LimitDirSizeTestSuiteV3 : public LimitDirSizeTestBase
{
    CPPUNIT_TEST_SUITE(LimitDirSizeTestSuiteV3);
    CPPUNIT_TEST(ValidDirSizes);
    CPPUNIT_TEST(InvalidDirSizes);
    CPPUNIT_TEST(LimPagesSetDirSmaller);
    CPPUNIT_TEST(LimPagesSetDirBigger);
    CPPUNIT_TEST(LimPagesSetDirSame);
    CPPUNIT_TEST_SUITE_END();

public:
    LimitDirSizeTestSuiteV3() : LimitDirSizeTestBase(MDBM_CREATE_V3, "v3") {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(LimitDirSizeTestSuiteV3);

