/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_pre_split

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

//  bugzilla tickets:
//  BZ ticket 5271857: V3: mdbm_pre_split: invalid num pages(0) specified, no error returned
//  BZ ticket 5273139: V3: mdbm_pre_split: invalid num pages (-1) causes a hang
class PreSplitTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    PreSplitTestSuite(int vFlag) : TestBase(vFlag, "PreSplitTestSuite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    //
    void SplitPremadeDB();
    void CreateSplitInvalid();
    void CreatePresizeInvalidSplitA();
    void CreatePresizeInvalidSplitB();
    void CreateMaxPages();
    void VerifyPageRounding();
    void LimitSizeSplitSame();
    void LimitSizeSplitBigger();
    void SplitLimitSame();
    void SplitLimitBigger();
    void SplitLimitSmaller();
    void PresizeSplitBigger();
    void PresizeSplitSmaller();
    void PresplitMultiPageSizesNoLargeObj();
    void PresplitMultiPageSizesWithLargeObj();
    void PresplitMultiPageSizesMemoryOnly();

private:
    void createPresizeInvalidSplit(const string& tcprefix, mdbm_ubig_t splitNum);  // TC G-3
    void Splitter(const string& tcprefix, int numPages);  // TC G-5

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _NumPagesSeries[];
    static int _PageSizeRange[];
};

const uint64_t V2MaxNumPages = (1U<<31);

int PreSplitTestSuite::_setupCnt = 0;
int PreSplitTestSuite::_NumPagesSeries[] = { 1, 2, 3,
  (int)(TestBase::GetMaxNumPages(MDBM_CREATE_V3)-1), (int)(V2MaxNumPages-1) };

int PreSplitTestSuite::_PageSizeRange[] = { 256, 2048, 4096, 16384, 65536 };


void PreSplitTestSuite::setUp()
{
    //_cfgStory = new ConfigStoryUtil("presptestv2", "presptestv3", "Presplit DB Test Suite: ");
    if (_setupCnt++ == 0) {
        cout << endl << "Presplit DB Test Suite Beginning..." << endl << flush;
    }
}

// Open pre-made DB containing data, then try to split
void PreSplitTestSuite::SplitPremadeDB()  // TC G-1
{
    string tcprefix = "TC G-1: SplitPremadeDB: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open premade DB
    int flags = versionFlag | MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC;
    MdbmHolder dbh(GetTmpMdbm(flags, 0644, 512, 512));
    //MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR));
    mdbm_limit_size_v3(dbh, 1, 0, 0); // set to 1 page and no shake function
    FillDb(dbh);

    // 2. Calculate number of pages c: c = mdbm_get_size / mdbm_get_page_size
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t calcnumpages = dbSize / pgsize;

    // 3. Call mdbm_pre_split: c + 1
    errno = 0;
    int ret = mdbm_pre_split(dbh, calcnumpages + 1);
    int errnum = errno;

    // 4. Expected results: FAIL since shouldnt split DB that contains data and errno is set
    stringstream psss;
    psss << prefix << "Presplit to Number pages=" << (calcnumpages + 1)
         << " Expected to FAIL since data has already been inserted."
         << " mdbm_pre_split returned=" << ret << " and errno=" << errnum << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == -1));

    // 5. Calculate number of pages n: n = mdbm_get_size / mdbm_get_page_size
    dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pgsize;

    // 6. Expected results: n == c
    stringstream ncss;
    ncss << prefix << "Original calculated Number pages=" << calcnumpages
         << " Expected to be same as current Number pages=" << curNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (curNumPages == calcnumpages));
}

// Choose invalid number of pages = 0
void PreSplitTestSuite::CreateSplitInvalid()  // TC G-2
{
// TODO FIXME BZ 5271857
    return;

#if 0
    string tcprefix = "TC G-2: CreateSplitInvalid: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags));

    // Calculate number of pages c: c = mdbm_get_size / mdbm_get_page_size
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t calcnumpages = dbSize / pgsize;

    // Call mdbm_pre_split: 0
    errno = 0;
    int ret = mdbm_pre_split(dbh, 0);
    int errnum = errno;

    // Expected results: FAIL and errno is set
    stringstream psss;
    psss << prefix << "Presplit to Number pages=0"
         << " Expected to FAIL since invalid number of pages."
         << " mdbm_pre_split returned=" << ret << " and errno=" << errnum << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == -1));

    // Calculate number of pages n: n = mdbm_get_size / mdbm_get_page_size
    dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pgsize;

    // Expected results: n == c
    stringstream ncss;
    ncss << prefix << "Original calculated Number pages=" << calcnumpages
         << " Expected to be same as current Number pages=" << curNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (curNumPages == calcnumpages));
#endif
}

// Pre-split using invalid number of pages = 0 with db of specified presize
void PreSplitTestSuite::createPresizeInvalidSplit(const string& tcprefix, mdbm_ubig_t splitNum)  // TC G-3
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=(5*512)
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    int preSize  = 5*512;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // Calculate number of pages c: c = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t calcnumpages = dbSize / pageSize;

    // Call mdbm_pre_split: 0
    errno = 0;
    int ret = mdbm_pre_split(dbh, splitNum);
    int errnum = errno;

    // Expected results: FAIL and errno is set
    stringstream psss;
    psss << prefix << "Presplit to Number pages=" << splitNum
         << " Expected to FAIL since invalid number of pages."
         << " mdbm_pre_split returned=" << ret << " and errno=" << errnum << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == -1));

    // Calculate number of pages n: n = mdbm_get_size / 512
    dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expected results: n == c
    stringstream ncss;
    ncss << prefix << "Original calculated Number pages=" << calcnumpages
         << " Expected to be same as current Number pages=" << curNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (curNumPages == calcnumpages));
}
void PreSplitTestSuite::CreatePresizeInvalidSplitA()  // TC G-3
{
    // FIX BZ 5273139: - V2, V3: mdbm_pre_split: invalid num pages (-1) causes a hang
    mdbm_ubig_t splitNum = (mdbm_ubig_t)-1; // 0 no error returned; -1 v2 hangs
    createPresizeInvalidSplit("TC G-3: CreatePresizeInvalidSplitV2a: mdbm_pre_split: ", splitNum);
}
void PreSplitTestSuite::CreatePresizeInvalidSplitB()  // TC G-3
{
    // FIX BZ 5271857
    mdbm_ubig_t splitNum = 0;
    createPresizeInvalidSplit("TC G-3: CreatePresizeInvalidSplitV2b: mdbm_pre_split: ", splitNum);
}

void PreSplitTestSuite::CreateMaxPages()  // TC G-4
{
    string tcprefix = "TC G-4: CreateMaxPages: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=128(MIN page size), presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 128;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Call mdbm_pre_split: n = MDBM_NUMPAGES_MAX
    uint32_t maxPages = GetMaxNumPages(versionFlag);

    GetLogStream() << prefix << " Split using MAX Number pages=" << maxPages << endl << flush;

    int ret = mdbm_pre_split(dbh, maxPages);

    // Expect results: success
    stringstream psss;
    psss << prefix << "Presplit to Number pages=" << maxPages
         << " Expected to Fail for page size=" << pageSize
         << " and Number of pages=" << maxPages << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == -1));

    // Calculate number of pages c: c = mdbm_get_size / 128
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;
    mdbm_ubig_t expectedDefNumPages = 1;

    // Expect results: c == n
    stringstream ncss;
    ncss << prefix << "Presplit to Number pages=" << maxPages
         << " using page size=" << pageSize
         << " : Expected current Number pages=" << curNumPages
         << " to be > default Number pages=" << expectedDefNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (expectedDefNumPages <= curNumPages));
}

//  Verify rounding up of number of pages requested.
//  Choose N not a multiple of 2 to verify it will be rounded up.
//    * For a series of odd numbers : n = 1, 3, 5, 7, MDBM_NUMPAGES_MAX - 1
void PreSplitTestSuite::Splitter(const string& tcprefix, int numPages)  // TC G-5
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << "Number Pages=" << numPages << " : ";
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=128(MIN page size), presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 128;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Call mdbm_pre_split: n
    int ret = mdbm_pre_split(dbh, numPages);

    // Calculate number of pages c: c = mdbm_get_size / 128
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t calcnumpages = dbSize / pageSize;

    // Expected results: c == (n + 1) / 2 * 2
    mdbm_ubig_t rndNumPages = mdbm_ubig_t((numPages + 1) / 2.0 * 2);

    stringstream ncss;
    ncss << prefix << "Current calculated Number pages=" << calcnumpages
         << " Expected to be same as rounded Number pages=" << rndNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (ret == 0 && calcnumpages == rndNumPages));
}

void PreSplitTestSuite::VerifyPageRounding()  // TC G-5
{
    string tcprefix = "TC G-5: VerifyPageRounding: ";
    int plen = sizeof(_NumPagesSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
        Splitter(tcprefix, _NumPagesSeries[p]);
    }
}

// Limit number of pages. Pre-split to same limit.
void PreSplitTestSuite::LimitSizeSplitSame()  // TC G-6
{
    string tcprefix = "TC G-6: LimitSizeSplitSame: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Call mdbm_limit_size: p = 2
    int numPages = 2;
    int ret = mdbm_limit_size_v3(dbh, numPages, 0, 0);

    // Call mdbm_pre_split: s = 2
    ret = mdbm_pre_split(dbh, numPages);

    // Expect results: success
    stringstream psss;
    psss << prefix << "Limited size to Number pages=" << numPages
         << " Expected to Succeed when Pre split Number pages=" << numPages
         << " with Page size=" << pageSize << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == 0));

    // Calculate number of pages n: n = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expect results: n == s
    stringstream ncss;
    ncss << prefix << "Presplit to Number pages=" << numPages
         << " Expected to be same as current Number pages=" << curNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), ((mdbm_ubig_t)numPages <= curNumPages));
}

// Limit number of pages. Pre-split to bigger than limit previously set.
void PreSplitTestSuite::LimitSizeSplitBigger()  // TC G-7
{
    string tcprefix = "TC G-7: LimitSizeSplitBigger: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, pageSize));

    // Call mdbm_limit_size: p = 2
    int numPages = 2;
    int ret = mdbm_limit_size_v3(dbh, numPages, 0, 0);

    // Call mdbm_pre_split: s = 4
    int splitPages = numPages + 2;
    ret = mdbm_pre_split(dbh, splitPages);

    // Expect results: FAIL since mdbm_limit_size restricts the split size and errno is set
    stringstream psss;
    psss << prefix << "Limited to Number pages=" << numPages
         << " Expected to Fail for split Number pages=" << splitPages
         << " with Page size=" << pageSize << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == -1));

    // Calculate number of pages n: n = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expect results: n == p
    stringstream ncss;
    ncss << prefix << "Presplit to Number pages=" << splitPages
         << " : Expected limit Number pages=" << numPages
         << " to be same as current Number pages=" << curNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), ((mdbm_ubig_t)numPages <= curNumPages));
}

void PreSplitTestSuite::SplitLimitSame()  // TC G-8
{
    string tcprefix = "TC G-8: SplitLimitSame: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Call mdbm_pre_split: s = 2
    int numPages = 2;
    int ret = mdbm_pre_split(dbh, numPages);

    // Call mdbm_limit_size: p = 2
    ret = mdbm_limit_size_v3(dbh, numPages, 0, 0);

    // Expect results: success
    stringstream psss;
    psss << prefix << "Presplit to Number pages=" << numPages
         << " Expected to Succeed when limit size Number pages=" << numPages
         << " with Page size=" << pageSize << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == 0));

    // Calculate number of pages n: n = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expect results: n == s
    stringstream ncss;
    ncss << prefix << "Presplit to Number pages=" << numPages
         << " Expected to be same as current Number pages=" << curNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), ((mdbm_ubig_t)numPages <= curNumPages));
}

// Pre-split the DB. Limit num pages to bigger than split.
void PreSplitTestSuite::SplitLimitBigger()  // TC G-9
{
    string tcprefix = "TC G-9: SplitLimitBigger: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Call mdbm_pre_split: s = 2
    int splitPages = 2;
    int ret = mdbm_pre_split(dbh, splitPages);

    // Call mdbm_limit_size: p = 4
    int numPages = splitPages + 2;
    ret = mdbm_limit_size_v3(dbh, numPages, 0, 0);

    // Expect results: success
    stringstream psss;
    psss << prefix << "Split Number pages=" << splitPages
         << " Expected to Succeed for limiting Number pages=" << numPages
         << " with Page size=" << pageSize << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == 0));

    // Calculate number of pages n: n = mdbm_get_limit_size / 512
    uint64_t dbSize  = mdbm_get_limit_size(dbh);
    mdbm_ubig_t limNumPages = dbSize / pageSize;

    // Expect results: n == p
    stringstream ncss;
    ncss << prefix << ": Expected limited Number pages=" << numPages
         << " to be > Pre split Number pages=" << splitPages
         << ": current limit Number pages=" << limNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (numPages > splitPages && limNumPages >= (mdbm_ubig_t)numPages));
}

// Pre-split the DB. Limit num pages to smaller than split.
void PreSplitTestSuite::SplitLimitSmaller()  // TC G-10
{
    string tcprefix = "TC G-10: SplitLimitSmaller: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=0
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Call mdbm_pre_split: s = 4
    int splitPages = 4;
    int ret = mdbm_pre_split(dbh, splitPages);

    // Call mdbm_limit_size: p = 2
    int numPages = splitPages - 2;
    errno = 0;
    ret = mdbm_limit_size_v3(dbh, numPages, 0, 0);
    int errnum = errno;

    // Expect results: FAIL since already split to larger size and errno is set
    stringstream psss;
    psss << prefix << "Split Number pages=" << splitPages
         << " Expected to FAIL limiting Number pages=" << numPages
         << " with retcode=" << ret << " and errno=" << errnum << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == -1));

    // Calculate number of pages n: n = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_limit_size(dbh);
    mdbm_ubig_t limNumPages = dbSize / pageSize;
    dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expect results: n == s
    stringstream ncss;
    ncss << prefix << ": Expected Pre split Number pages=" << splitPages
         << " to be == current Number pages=" << curNumPages
         << ": current limit Number pages=" << limNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (curNumPages >= (mdbm_ubig_t)splitPages));
}

// Create with presize < num pages to pre-split.
// Set presize to pagesize * n; pre-split to n+2 pages
void PreSplitTestSuite::PresizeSplitBigger()  // TC G-11
{
    string tcprefix = "TC G-11: PresizeSplitBigger: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=512*2
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    int preSize  = pageSize * 2;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // Call mdbm_pre_split: s = 4
    int splitPages = 4;
    int ret = mdbm_pre_split(dbh, splitPages);

    // Expect results: success
    stringstream psss;
    psss << prefix << "Expected to Succeed Pre splitting Number pages=" << splitPages
         << " with Page size=" << pageSize << " and presize=" << preSize << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == 0));

    // Calculate number of pages n: n = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expect results: n == s
    stringstream ncss;
    ncss << prefix << ": Expected current Number pages=" << curNumPages
         << " to be same as Pre split Number pages=" << splitPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (curNumPages >= (mdbm_ubig_t)splitPages));
}

// Create with presize > num pages to pre-split.
// Set presize to pagesize * n; pre-split to n-1 pages.
void PreSplitTestSuite::PresizeSplitSmaller()  // TC G-12
{
    string tcprefix = "TC G-12: PresizeSplitSmaller: mdbm_pre_split: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB: page size=512, presize=512*4
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    int preSize  = pageSize * 4;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // Call mdbm_pre_split: s = 3
    int splitPages = 3;
    errno = 0;
    int ret = mdbm_pre_split(dbh, splitPages);
    int errnum = errno;

    // Expect results: FAIL and errno is set
    stringstream psss;
    psss << prefix << "Expected to Succeed Pre splitting to smaller Number pages=" << splitPages
         << " : Using Page size=" << pageSize << " and presize=" << preSize
         << ": retcode=" << ret << " errno=" << errnum << endl;
    CPPUNIT_ASSERT_MESSAGE(psss.str(), (ret == 0));

    // Calculate number of pages c: c = mdbm_get_size / 512
    uint64_t dbSize  = mdbm_get_size(dbh);
    mdbm_ubig_t curNumPages = dbSize / pageSize;

    // Expect results: c == s + 1
    stringstream ncss;
    ncss << prefix << ": Expected current Number pages=" << curNumPages
         << " to be > Pre split Number pages=" << splitPages << endl;
    CPPUNIT_ASSERT_MESSAGE(ncss.str(), (curNumPages > (mdbm_ubig_t)splitPages));
}

void PreSplitTestSuite::PresplitMultiPageSizesNoLargeObj()
{
    TRACE_TEST_CASE(__func__)
    SKIP_IF_FAST_VALGRIND()

    string tcprefix = "TC G-13: PresplitMultiPageSizesNoLargeObj: No large object flag: ";
    vector<int> pagerange(_PageSizeRange, _PageSizeRange + sizeof(_PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(versionFlag, pagerange, 32, false, tcprefix);
}
void PreSplitTestSuite::PresplitMultiPageSizesWithLargeObj()
{
    TRACE_TEST_CASE(__func__)
    SKIP_IF_FAST_VALGRIND()

    string tcprefix = "TC G-14: PresplitMultiPageSizesWithLargeObj: Large object flag: ";
    vector<int> pagerange(_PageSizeRange, _PageSizeRange + sizeof(_PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(versionFlag|MDBM_LARGE_OBJECTS, pagerange, 32, false, tcprefix);
}
void PreSplitTestSuite::PresplitMultiPageSizesMemoryOnly()
{
    string tcprefix = "TC G-15: PresplitMultiPageSizesMemoryOnly: Memory-Only: ";
    vector<int> pagerange(_PageSizeRange, _PageSizeRange + sizeof(_PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(0, pagerange, 32, false, tcprefix);
}

class PreSplitTestSuiteV3 : public PreSplitTestSuite
{
    CPPUNIT_TEST_SUITE(PreSplitTestSuiteV3);
    CPPUNIT_TEST(SplitPremadeDB);
    CPPUNIT_TEST(CreateSplitInvalid);
    CPPUNIT_TEST(CreatePresizeInvalidSplitA);
    CPPUNIT_TEST(CreatePresizeInvalidSplitB);
    CPPUNIT_TEST(CreateMaxPages);
// OBS    CPPUNIT_TEST(VerifyPageRounding);
    CPPUNIT_TEST(LimitSizeSplitSame);
    CPPUNIT_TEST(LimitSizeSplitBigger);
    CPPUNIT_TEST(SplitLimitSame);
    CPPUNIT_TEST(SplitLimitBigger);
    CPPUNIT_TEST(SplitLimitSmaller);
    CPPUNIT_TEST(PresizeSplitBigger);
    CPPUNIT_TEST(PresizeSplitSmaller);
    CPPUNIT_TEST(PresplitMultiPageSizesNoLargeObj);
    CPPUNIT_TEST(PresplitMultiPageSizesWithLargeObj);
    CPPUNIT_TEST(PresplitMultiPageSizesMemoryOnly);
    CPPUNIT_TEST(PreSplitPopulatedDb);
    CPPUNIT_TEST_SUITE_END();
public:
    void PreSplitPopulatedDb();
    PreSplitTestSuiteV3() : PreSplitTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(PreSplitTestSuiteV3);

void
PreSplitTestSuiteV3::PreSplitPopulatedDb()
{
    string prefix = string("PreSplitPopulatedDb") + versionString + ":";   //

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix,
                    MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    errno = 0;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_pre_split(mdbm, 16));   // Should fail on a populated db
    CPPUNIT_ASSERT_EQUAL(EFBIG, errno);
}



