/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_page_size

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
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


class GetPageSizeTestBase : public CppUnit::TestFixture, public TestBase
{
public:
    GetPageSizeTestBase(int vFlag) : TestBase(vFlag, "PageSize Test Suite") { }
    void setUp();
    void tearDown();

    // unit tests in this suite
    //
    void OpenPreMade(); // Test Case A-1
    void CreateValidSeriesPageSizes(); // Test Case A-2
    void CreateMaxPageSize(); // Test Case A-3

protected:
    void CheckPageSize(const string& prefix, int psize);

    static int _PageSizeSeries[];
    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
};

int GetPageSizeTestBase::_setupCnt = 0;
int GetPageSizeTestBase::_PageSizeSeries[] = { 0, 128, 256, 512, 1024, 2048, 4096, 8192, (40*1024), (48*1024), (63 * 1024), (64 * 1024) };


void GetPageSizeTestBase::setUp()
{
    if (_setupCnt++ == 0)
        cout << endl << "Page Size Test Suite Beginning..." << endl << flush;
}
void GetPageSizeTestBase::tearDown() {
}

//  Open pre-made DB with known page size.
void GetPageSizeTestBase::OpenPreMade()  // TC A-1
{
    string tcprefix = "TC A-1: OpenPreMade: mdbm_get_page_size: ";
    TRACE_TEST_CASE(tcprefix);
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix;
    string prefix = premsg.str();


    // Call mdbm_open specifying pre-made DB using defaults
    MdbmHolder dbh = GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|versionFlag);

    // Call mdbm_get_page_size: page size
    int page_size = mdbm_get_page_size(dbh);

    int premade_psize = DEFAULT_PAGE_SIZE;
    stringstream ssmsg;
    ssmsg << prefix << "Expected pagesize=" << premade_psize << " but DB has pagesize=" << page_size << " for MDBM version=" << versionString << endl;

    // Expected results: page size == pre-made DB page size
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (page_size == premade_psize));
}

void GetPageSizeTestBase::CheckPageSize(const string& prefix, int psize)
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << prefix << "CheckPageSize: page-size=" << psize;
    string prefix2 = premsg.str();

    // Call mdbm_open; Create new DB using page size = p
     MdbmHolder dbh = EnsureTmpMdbm(prefix2, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |versionFlag, 0644, psize, psize*4);

    // Call mdbm_get_page_size : page size
    int page_size = mdbm_get_page_size(dbh);

    // calculate expected page size for a specified size
    int expected_page_size = GetExpectedPageSize(versionFlag, psize);

    stringstream ssmsg;
    ssmsg << prefix << "Expected pagesize=" << expected_page_size << " but DB has pagesize=" << page_size << " for MDBM version=" << mdbm_get_version(dbh) << endl;

    // Expected results: for p=0 expect page_size=4096; for all other tests expect page_size==p
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (page_size == expected_page_size));
}


// Create DB's for a series of valid page sizes, using default(0) and MINimum(128)
// page size. There is no difference for the MINimum value allowed between V2 and
// V3. This test will also test the V2 MAXimum size which is (64 * 1024).
// For page size p = 0, 128, 256, 512, 1024, 2048, 4096, 8192, (63 * 1024), V2 max=(64 * 1024):
void GetPageSizeTestBase::CreateValidSeriesPageSizes()
{
    TRACE_TEST_CASE("TC A-2: CreateValidSeriesPageSizes mdbm_get_page_size: ")

    int len = sizeof(_PageSizeSeries)/sizeof(int);
    for (int i=0; i<len; ++i) {
      CheckPageSize("TC A-2: ", _PageSizeSeries[i]);
    }
}


// Create DB's for MAXimum page size for V3. This V3 specific test will use the
// range: For page size p = (15 * 1024 * 1024), V3 max=(16 * 1024 * 1024).
void GetPageSizeTestBase::CreateMaxPageSize()
{
    TRACE_TEST_CASE("TC A-3: CreateMaxPageSize: mdbm_get_page_size: ");
    int maxPageSizeSeries [] = { (15 * 1024 * 1024), MDBM_MAXPAGE }; // (16 * 1024 * 1024)

    int len = sizeof(maxPageSizeSeries)/sizeof(int);
    for (int i=0; i<len; ++i) {
      CheckPageSize("TC A-3: ", maxPageSizeSeries[i]);
    }
}


class GetPageSizeTestSuiteV3 : public GetPageSizeTestBase
{
    CPPUNIT_TEST_SUITE(GetPageSizeTestSuiteV3);
    CPPUNIT_TEST(OpenPreMade);
    CPPUNIT_TEST(CreateValidSeriesPageSizes);
    CPPUNIT_TEST(CreateMaxPageSize);
    CPPUNIT_TEST_SUITE_END();

public:
    GetPageSizeTestSuiteV3() : GetPageSizeTestBase(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(GetPageSizeTestSuiteV3);


