/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_size

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
//#include "configstoryutil.hh"

/*
    bugzilla tickets:
    BZ ticket 5249534: V2: mdbm_open hangs upon presize = invalid negative number
*/
class GetSizeTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    GetSizeTestSuite(int vFlag) : TestBase(vFlag, "GetSizeTestSuite") {}
    void setUp();
    void tearDown();

    // unit tests in this suite
    void CreateDefaultDB();  // Test Case B-1
    void CreateInitSizeDefaultPgSz(); // Test Case B-2
    void OpenPreMadeDB(); // Test Case B-3
    void CreateValidSeriesInitSizes(); // Test Case B-4
    void CreateMaxInitSizeV3(); // Test Case B-5

private:
    // function objects defined for loops
    // having a page size and given a number of pages, check the size of the created DB
    //struct CheckSize : public ConfigStoryUtil::FunctionObj
    //{
    //    CheckSize(int psize, int v2orV3flag, ConfigStoryUtil * cfgStory, const char * prefix) :
    //         ConfigStoryUtil::FunctionObj(v2orV3flag, cfgStory, prefix), _pgsize(psize) {}
    //    void operator() (int numPages);

    //    int _pgsize;
    //};
    void CheckSize(int numPages, int psize, const string& prefix);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _PageSizeSeries[];
    static int _NumPagesSeries[];
    static uint64_t _MegDivider;
};


int GetSizeTestSuite::_setupCnt = 0;

int GetSizeTestSuite::_PageSizeSeries[] = { 128, 256, 512, 1024, 2048, 4096, 8192, (40*1024), (48*1024), (63 * 1024), (64 * 1024) };

int GetSizeTestSuite::_NumPagesSeries[] = { 1, 2, 3, (int)TestBase::GetMaxNumPages(MDBM_CREATE_V3) };
// internal define not accessable -> MDBM_NUMPAGES_MAX(1<<MDBM_DIRSHIFT_MAX(24)) };

// MB = 1024 * 1024 = 1048576
uint64_t GetSizeTestSuite::_MegDivider = 1048576ULL;


void GetSizeTestSuite::setUp()
{
    if (_setupCnt++ == 0) {
        cout << endl << "Get DB Size Test Suite Beginning..." << endl << flush;
    }
}

void GetSizeTestSuite::tearDown() { }

void GetSizeTestSuite::CheckSize(int numPages, int psize, const string& tcprefix)
{
    if (GetTestLevel() < RunAllTestCases) {
        if (numPages > 100000 || psize >= (64*1024)) { return; }
    }

    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix
           << "CheckSize: num-pages=" << numPages
           << " page-size=" << psize;
    string prefix = premsg.str();

    GetLogStream() <<  endl << prefix << " Starting"  << endl << flush;

    // lets detect overflow
    if (numPages > 0 && psize > 0) {
        stringstream ofmsg;
        ofmsg << prefix
              << " Overflow detected: num-pages > " << (ULLONG_MAX / (uint64_t)psize)
              << endl;

        // valid for ULLONG_MAX >= numPages * psize
        // valid for ULLONG_MAX / psize >= numPages
        CPPUNIT_ASSERT_MESSAGE(ofmsg.str(), ((uint64_t)numPages <= ULLONG_MAX / (uint64_t)psize));
    }

    uint64_t dbSize    = (uint64_t)psize * (uint64_t)numPages;
    uint64_t maxDBsize = GetMaxDBsize(versionFlag);

    if (dbSize < MDBM_MINPAGE || dbSize > maxDBsize) {
        GetLogStream() << prefix << "Warning: MDBM limits: maximum allowed size="
            << maxDBsize << " and minimum allowed size=" << MDBM_MINPAGE
            << ": DB requested size=" << dbSize << " is NOT valid" << endl << flush;
    }

    // verify system limits before trying to open the requested DB size
    //
    try {
        uint64_t maxFSsize = GetMaxFileSize();
        uint64_t maxVAsize = GetMaxVirtualAddress();

        // lets take the smallest of the system limits
        //
        maxVAsize = maxVAsize > maxFSsize ? maxFSsize : maxVAsize;

        stringstream maxmsg;
        maxmsg << prefix
               << "Cannot TEST - Beyond system limits: requested size=" << dbSize
               << " > system allowed size=" << maxVAsize
               << endl;
        CPPUNIT_ASSERT_MESSAGE(maxmsg.str(), (dbSize <= maxVAsize));
    } catch (const char * errmsg) {
        cout << prefix << "Failed to get system limits for maximum size, continuing with DB size="
             << dbSize << endl << flush;
    }

    // Call mdbm_open; Create new DB using page size = p
    // If dbSize > INT_MAX, use MB's instead
    // flag and reinterpret presize to be in MB's
    //
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int presize = (int)dbSize;
    if (dbSize > INT_MAX) { // detect overflow
        openFlags |= MDBM_DBSIZE_MB;  // V3 only flag
        presize = (int)(dbSize / _MegDivider);
    }

    GetLogStream() << prefix << "TRACE: presize(" << presize << ")." << endl << flush;
    //MDBM *dbh = EnsureTmpMdbm(prefix, openFlags, 0644, psize, presize);
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, psize, presize));

    GetLogStream() << prefix << "TRACE: after assert open : presize(" << presize << ")." << endl << flush;

    // Call mdbm_get_size : z
    uint64_t DB_size = mdbm_get_size(dbh);

    // calculate expected DB size for a specified size
    //
    uint64_t expected_DB_size = (uint64_t)GetExpectedPageSize(versionFlag, psize) * (uint64_t)numPages;

    DB_size = openFlags & MDBM_DBSIZE_MB ? (DB_size * _MegDivider) : DB_size;

    stringstream ssmsg;
    ssmsg << prefix
          << "Expected DB size=" << expected_DB_size
          << " presize=" << presize
          << " mdbm_get_size=" << DB_size
          << "  MB_DBSIZE flag set=" << (openFlags & MDBM_DBSIZE_MB ? "on" : "off" )
          << endl;

    // Expected results:
    //   For p = 128, 256, 512, 1024, 2048, 4096, 8192, v2 max=(64 * 1024)
    //     For number pages n = 1, 2, 3, Max num pages=MDBM_NUMPAGES_MAX(2^24)
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (DB_size >= expected_DB_size));

    GetLogStream() << prefix << " Ending"  << endl << flush;
}


void GetSizeTestSuite::CreateDefaultDB()
{
    string tcprefix = "TC B-1: CreateDefaultDBv2: mdbm_get_size";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // Create new DB with defaults.
    // 1. Call mdbm_open; Create new DB using defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2. Call mdbm_get_page_size: p
    int page_size = mdbm_get_page_size(dbh);

    // 3. Call mdbm_get_size: z
    int DB_size = mdbm_get_size(dbh);

    stringstream ssmsg;
    ssmsg << prefix
          << "Expected DB size " << page_size
          << " but DB size=" << DB_size
          << endl;

    // 4. Expected result: z >= p
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (DB_size >= page_size));
}

void GetSizeTestSuite::CreateInitSizeDefaultPgSz()
{
    string tcprefix = "TC B-2: CreateInitSizeDefaultPgSz: mdbm_get_size";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // Create new DB specifying initial DB size and default page size.
    // 1. Call mdbm_open; Create new DB using pagesize = 0, presize = s(4096 * n)
    int preSize = MDBM_PAGESIZ * 2;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag, 0644, 0, preSize));

    // 2. Call mdbm_get_size: z
    int DB_size = mdbm_get_size(dbh);

    stringstream ssmsg;
    ssmsg << prefix
          << "Expected DB size=" << preSize
          << " but DB size=" << DB_size
          << " for MDBM version=" << mdbm_get_version(dbh)
          << endl;

    // 3. Expected result: z >= s
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (DB_size >= preSize));
}

void GetSizeTestSuite::OpenPreMadeDB()
{
    string tcprefix = "TC B-3: OpenPreMadeDB: mdbm_get_size";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Create premade DB
    // 2. Open pre-made database that was created with limits set.
    int openFlags = MDBM_O_RDWR|MDBM_O_CREAT|versionFlag;
    MdbmHolder dbh(GetTmpPopulatedMdbm(tcprefix, openFlags, 0644, -1, -1));

    // 3. Calculate known size: c = known page size * known number of pages
    //int dbSize = ConfigStoryUtil::getPremadeDBpageSize();
    int dbSize = DEFAULT_PAGE_SIZE;

    // 4. Call mdbm_get_size: z
    int DB_size = mdbm_get_size(dbh);

    stringstream ssmsg;
    ssmsg << prefix
          << " Expected DB size=" << dbSize
          << " but DB size=" << DB_size
          << " for MDBM version=" << versionFlag
          << endl;

    // 5. Expected result: z >= c
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (DB_size >= dbSize));

    GetLogStream() << prefix << " Ending"  << endl << flush;
}

void GetSizeTestSuite::CreateValidSeriesInitSizes() // Test Case B-4
{
    string prefix = "TC B-4: CreateValidSeriesInitSizes:";
    TRACE_TEST_CASE(prefix);

    int plen = sizeof(_PageSizeSeries)/sizeof(int);
    int nlen = sizeof(_NumPagesSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
      for (int n=0; n<nlen; ++n) {
        CheckSize(_NumPagesSeries[n], _PageSizeSeries[p], prefix);
      }
    }
}


void GetSizeTestSuite::CreateMaxInitSizeV3() // Test Case B-5
{
    string prefix = "TC B-5: CreateMaxInitSize: Starting";
    TRACE_TEST_CASE(prefix);

    int maxPageSizeSeries[] = { (15 * 1024 * 1024), MDBM_MAXPAGE }; // (16 * 1024 * 1024)

    int mlen = sizeof(maxPageSizeSeries)/sizeof(int);
    int nlen = sizeof(_NumPagesSeries)/sizeof(int);

    for (int m=0; m<mlen; ++m) {
      for (int n=0; n<nlen; ++n) {
        CheckSize(_NumPagesSeries[n], maxPageSizeSeries[m], prefix);
      }
    }
}



class GetSizeTestSuiteV3 : public GetSizeTestSuite
{
    CPPUNIT_TEST_SUITE(GetSizeTestSuiteV3);
    CPPUNIT_TEST(CreateDefaultDB);
    CPPUNIT_TEST(CreateInitSizeDefaultPgSz);
    CPPUNIT_TEST(OpenPreMadeDB);
    CPPUNIT_TEST(CreateValidSeriesInitSizes);
    CPPUNIT_TEST(CreateMaxInitSizeV3);
    CPPUNIT_TEST_SUITE_END();

public:
    GetSizeTestSuiteV3() : GetSizeTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(GetSizeTestSuiteV3);

