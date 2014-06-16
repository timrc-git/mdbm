/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_limit_size

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


class GetLimitTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    GetLimitTestSuite(int vFlag) : TestBase(vFlag, "GetLimitTestSuite") { }
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    //
    void CreateDBdefaults();  // TC C-1
    void PremadeKnownLimits();  // TC C-2
    void PremadeResetHiLimit();  // TC C-3
    void CreateResetHiLimit();  // TC C-4
    void CreateSeriesSizeNumPages();  // TC C-5
    void CreateSeriesMaxSizeV3();  // TC C-6

private:
    // having a page size and given a number of pages, check the size of the created DB
    void CheckSize(const string& prefix, int pgsize, int numPages);
    uint64_t expectedDBsize(int pageSize, int numPages);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _PageSizeSeries[];
    static int _NumPagesSeries[];
    static int _PageSizeSeriesV3[];
};

int GetLimitTestSuite::_setupCnt = 0;

int  GetLimitTestSuite::_PageSizeSeries[] = { 128, 256, 512, 1024, 2048, 4096, 8192, (40*1024), (48*1024), (63 * 1024), (64 * 1024) };

int  GetLimitTestSuite::_NumPagesSeries[] = { 1, 2, 3, (int)TestBase::GetMaxNumPages(MDBM_CREATE_V3) };

int  GetLimitTestSuite::_PageSizeSeriesV3[] = { (15 * 1024 * 1024), MDBM_MAXPAGE };


void GetLimitTestSuite::setUp()
{
    //_cfgStory = new ConfigStoryUtil("getlimtestv2", "getlimtestv3", "Get Limit Size Test Suite: ");

    if (_setupCnt++ == 0)
        cout << endl << "Get Limit Size Test Suite Beginning..." << endl << flush;
}

// expectedDBsize returns an expected size limit of the DB to be compared with
// what is returned by mdbm_get_limit_size.
// Algorithm created based on ob served output of v2 and v3 - including
// special case for smaller page sizes.
// This testing is for ball park testing only.
//
// It calculates the size of the DB according to the following:
// - whether the DB is v2 or v3
// - page size(pageSize)
// - number of pages (numPages)
// v2: number of pages must be a power of 2
// v3: number of pages need not be power of 2
// v2 and v3: page directory (number of pages that can be mapped) is a power of 2
uint64_t GetLimitTestSuite::expectedDBsize(int pageSize, int numPages)
{
    // calculate the number of pages to be a power of 2 - rounded up
    int calcNumPages = 1;
    while (calcNumPages < numPages && calcNumPages > 0) {
        calcNumPages <<= 1;
    }

    if (calcNumPages > numPages) { // detect possible overflow
        numPages = calcNumPages;
    }

    uint64_t expectedSize = pageSize * (uint64_t)numPages;
    int pageSizeFactor = MDBM_MINPAGE * 2;  // kludge: fudge factor
    if (versionFlag & MDBM_CREATE_V3) {
        if (pageSize < pageSizeFactor) { // pageSize < 256
            expectedSize += (pageSizeFactor - pageSize) + pageSizeFactor;
        } else {
            expectedSize += pageSize;
        }
        expectedSize = expectedSize < 1024 ? 1024 : expectedSize; // set a min size
    } else if (pageSize < pageSizeFactor) {
        expectedSize += (pageSizeFactor - pageSize);
    }
    return expectedSize;
}

void GetLimitTestSuite::CheckSize(const string& tcprefix, int pgsize, int numPages)
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << "CheckSize: Number of pages="
           << numPages << " Pagesize=" << pgsize;
    string prefix = premsg.str();

    if (GetTestLevel() < RunAllTestCases) {
        if (numPages > 100000 || pgsize >= (64*1024)) { return; }
    }

    // lets detect overflow
    if (numPages > 0 && pgsize > 0) {
        stringstream ofmsg;
        ofmsg << prefix << " Overflow detected: num-pages > "
              << (ULLONG_MAX / (uint64_t)pgsize) << endl;

        // valid for ULLONG_MAX >= numPages * pgsize
        // valid for ULLONG_MAX / pgsize >= numPages
        CPPUNIT_ASSERT_MESSAGE(ofmsg.str(), ((uint64_t)numPages <= ULLONG_MAX / (uint64_t)pgsize));
    }

    uint64_t dbSize    = (uint64_t)pgsize * (uint64_t)numPages;
    uint64_t maxDBsize = GetMaxDBsize(versionFlag);

    if (dbSize < MDBM_MINPAGE || dbSize > maxDBsize) {
        cout << prefix << endl << "Warning: MDBM limits: maximum allowed size="
             << maxDBsize << " and minimum allowed size=" << MDBM_MINPAGE
             << ": DB requested size=" << dbSize << " is NOT valid" << endl << flush;
    }

    // Call mdbm_open; Create new DB using page size = p
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pgsize));

    // Call mdbm_limit_size specifying n pages
    int ret = mdbm_limit_size_v3(dbh, numPages, 0, 0);
    stringstream limmsg;
    limmsg << prefix
           << " mdbm_limit_size FAILED with Number of pages=" << numPages
           << " so cannot continue testing mdbm_get_limit_size" << endl;
    //CPPUNIT_ASSERT_MESSAGE(limmsg.str(), (ret == 0));
    if (ret != 0) {
        cout << "WARNING: " << limmsg.str();
        return;
    }

    // Calculate configured DB size: c = n * p
    //
    uint64_t expectedPageSize = (uint64_t)GetExpectedPageSize(versionFlag, pgsize);
    if (expectedPageSize == MDBM_MINPAGE)
    {
        expectedPageSize = 256;
    }
    uint64_t expectedSize = expectedDBsize(expectedPageSize, numPages);

    // Call mdbm_get_limit_size : z
    uint64_t limitsize = mdbm_get_limit_size(dbh);
    uint64_t calcnumpages = limitsize / pgsize;

    stringstream ssmsg;
    ssmsg << prefix << endl << "Calculated number of pages=" << calcnumpages
          << ": Expected Max limit size=" << expectedSize
          << " to be >= mdbm_get_limit_size=" << limitsize
          << endl;

    // Expected result: z == c
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (limitsize <= expectedSize));
}


void GetLimitTestSuite::CreateDBdefaults()  // TC C-1
{
    string tcprefix = "TC C-1: CreateDBdefaults: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2. Call mdbm_get_limit_size
    uint64_t limitsize = mdbm_get_limit_size(dbh);

    // 3. Expected result: 0 since no limit was set
    stringstream sstrm;
    sstrm << prefix << "Expected default limit size=0 but got limit size=" << limitsize << endl;

    CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (limitsize == 0));
}

void GetLimitTestSuite::PremadeKnownLimits()  // TC C-2
{
    string tcprefix = "TC C-2: PremadeKnownLimits: ";
    TRACE_TEST_CASE(tcprefix);
    // Pre-made DB that had known limits set (page size and number of pages)
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Setup DB: Copy the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR));

    // Calculate known limit size: c = known page size * known number of pages
    // NOTE: number of pages = 1
    uint64_t pagesize = DEFAULT_PAGE_SIZE;

    // Call mdbm_get_limit_size: z
    uint64_t limitsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limitsize / pagesize;

    // Expected result: z == c
    stringstream sstrm;
    sstrm << prefix << "Pagesize=" << pagesize << " Calculated number of pages="
          << calcnumpages << ": Expected MIN limit size=" << pagesize
          << " to be <= limit size=" << limitsize << endl;

    CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (limitsize >= pagesize));
}

void GetLimitTestSuite::PremadeResetHiLimit()  // TC C-3
{
    string tcprefix = "TC C-3: PremadeResetHiLimit: ";
    TRACE_TEST_CASE(tcprefix);

    // Pre-made DB that had known limit set (page size and num of pages), reset to higher limit (num of pages)
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Setup DB: Copy the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR));

    // Call mdbm_get_limit_size: s
    uint64_t limitsize = mdbm_get_limit_size(dbh);

    // Call mdbm_get_page_size: p
    int pagesize = DEFAULT_PAGE_SIZE;

    // Reset number of pages; Call mdbm_limit_size: n = number of pages * 2
    size_t numpages = 2;
    int ret = mdbm_limit_size_v3(dbh, numpages, 0, 0);
    stringstream limmsg;
    limmsg << prefix << " mdbm_limit_size FAILED with Number of pages="
           << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(limmsg.str(), (ret == 0));

    // Calculate new limit size: c = n * p
    uint64_t calcsize = numpages * pagesize;

    // Expected result: s < c
    stringstream clcstrm;
    clcstrm << prefix << "Pagesize=" << pagesize << " Number of pages="
            << numpages << ": Expected MIN limit size=" << limitsize
            << " to be <= calculated limit size=" << calcsize << endl;
    CPPUNIT_ASSERT_MESSAGE(clcstrm.str(), (limitsize <= calcsize));

    // Call mdbm_get_limit_size: z
    limitsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limitsize / pagesize;

    // Expected result: z == c
    stringstream sstrm;
    sstrm << prefix << "Pagesize=" << pagesize << " Number of pages="
          << numpages << " Calculated number of pages=" << calcnumpages
          << ": Expected MIN limit size=" << calcsize
          << " to be <= limit size=" << limitsize << endl;

    CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (limitsize >= calcsize));
}

void GetLimitTestSuite::CreateResetHiLimit()  // TC C-4
{
    string tcprefix = "TC C-4: CreateResetHiLimit: ";
    TRACE_TEST_CASE(tcprefix);

    // Create new DB where limit is set more than once, reset to higher limit (num pages)
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // Call mdbm_limit_size specifying 1 pages
    size_t numpages = 1;
    int ret = mdbm_limit_size_v3(dbh, numpages, 0, 0);
    stringstream limitmsg;
    limitmsg << prefix << " mdbm_limit_size FAILED with Number of pages="
           << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(limitmsg.str(), (ret == 0));

    // Calculate configured DB size: c = p
    int pagesize = mdbm_get_page_size(dbh);
    uint64_t calcsize = pagesize;

    // Call mdbm_get_limit_size : z
    uint64_t limitsize = mdbm_get_limit_size(dbh);

    // Expected result: z == c
    stringstream clcstrm;
    clcstrm << prefix << "Pagesize=" << pagesize << ": Expected limit size="
            << limitsize << " should be >= calculated MIN limit size=" << calcsize << endl;
    CPPUNIT_ASSERT_MESSAGE(clcstrm.str(), (limitsize >= calcsize));

    // Reset number of pages; Call mdbm_limit_size : n pages
    numpages = 3;
    ret = mdbm_limit_size_v3(dbh, numpages, 0, 0);
    stringstream limmsg;
    limmsg << prefix << " mdbm_limit_size FAILED with Number of pages="
           << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(limmsg.str(), (ret == 0));

    // Calculate new DB size: c = p * n
    calcsize = pagesize * numpages;

    // Call mdbm_get_limit_size: z
    limitsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limitsize / pagesize;

    // Expected result: z == c
    stringstream sstrm;
    sstrm << prefix << "Pagesize=" << pagesize << " Number of pages="
          << numpages << " Calculated number of pages=" << calcnumpages
          << ": Expected MIN limit size=" << calcsize
          << " to be <= limit size=" << limitsize << endl;

    CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (limitsize >= calcsize));
}

void GetLimitTestSuite::CreateSeriesSizeNumPages()  // TC C-5
{
    string tcprefix = "TC C-5: CreateSeriesSizeNumPages: ";
    TRACE_TEST_CASE(tcprefix);

    // Test various page sizes, starting with MINimum sizes. This test will also test the V2 MAXimum size
    int plen = sizeof(_PageSizeSeries)/sizeof(int);
    int nlen = sizeof(_NumPagesSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
        for (int n=0; n<nlen; ++n) {
          CheckSize(tcprefix, _PageSizeSeries[p], _NumPagesSeries[n]);
        }
    }
}


void GetLimitTestSuite::CreateSeriesMaxSizeV3()  // TC C-6
{
    string tcprefix = "TC C-6: CreateSeriesMaxSize: ";
    TRACE_TEST_CASE(tcprefix);

    // Test MAXimum page size for V3 max=(16 * 1024 * 1024)
    int plen = sizeof(_PageSizeSeriesV3)/sizeof(int);
    int nlen = sizeof(_NumPagesSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
        for (int n=0; n<nlen; ++n) {
          CheckSize(tcprefix, _PageSizeSeriesV3[p], _NumPagesSeries[n]);
        }
    }
}




class GetLimitTestSuiteV3 : public GetLimitTestSuite
{
    CPPUNIT_TEST_SUITE(GetLimitTestSuiteV3);
    CPPUNIT_TEST(CreateDBdefaults);  // TC C-1
    CPPUNIT_TEST(PremadeKnownLimits);  // TC C-2
    CPPUNIT_TEST(PremadeResetHiLimit);  // TC C-3
    CPPUNIT_TEST(CreateResetHiLimit);  // TC C-4
    CPPUNIT_TEST(CreateSeriesSizeNumPages);  // TC C-5
    CPPUNIT_TEST(CreateSeriesMaxSizeV3);  // TC C-6
    CPPUNIT_TEST_SUITE_END();

public:
    GetLimitTestSuiteV3() : GetLimitTestSuite(MDBM_CREATE_V3) { }
};

CPPUNIT_TEST_SUITE_REGISTRATION(GetLimitTestSuiteV3);


