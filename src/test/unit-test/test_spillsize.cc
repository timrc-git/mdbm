/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_setspillsize

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

/*
    bugzilla tickets:
    BZ ticket 5280242: V2 and V3: mdbm_setspillsize: allows invalid spillsize = -1
*/
class SpillSizeTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    SpillSizeTestSuite(int vFlag) : TestBase(vFlag, "SpillSizeTestSuite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    void PresizeDefThresh();  // TC I-1
    void PresizeSmallThresh();  // TC I-2
    void PresizeAddLgAddSm();  // TC I-3
    void SeriesValidThresholds();  // TC I-4
    void SeriesValidThresholdsB();  // TC I-4
    void SeriesInvalidThresholds();  // TC I-5
    void AddLOresetSpillsize();  // TC I-6
    void LimitThenAddVL();  // TC I-7
    void PremadeAddLO();  // TC I-8
    void AddLOthenLowerSpill();  // TC I-9
    void ErrorCaseMdbmSetSpillSize(); //TC I-10

    // perform assert if assertMsg is not null
    int assertSetSpillSize(MDBM *dbh, int spillSize, const char *assertMsg=0);
    int storeLargeObj(MDBM *dbh, int largeObjSize, const char *keyName, bool expectSuccess, const char * assertMsg);

private:
    struct CleanupMem {
        const char *_array;
        CleanupMem(const char *array) : _array(array) {}
        ~CleanupMem() {
          if (_array) { delete[] _array; _array = 0; }
        }
    };

    // having a page size and given a spill size, verify it succeeds
    void CheckSpillSize(const string& tcprefix, int pgSize, int spillSize); // TC I-4
    // having a page size and given a spill size, verify it fails
    void CheckInvalidSpillSize(const string& tcprefix, int pgSize, int spillSize);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _PageSizeSeries[];
    static int _PageSizeSeriesV3[];
    static int _SpillSizeSeries[];
    static int _SpillSizeSeriesV3[];
    static int _InvalidSpillSizeSeries[];
};


int SpillSizeTestSuite::_setupCnt = 0;

int SpillSizeTestSuite::_PageSizeSeries[] = { 128, 256, 512, 1024, 2048, 4096, 8192, (40*1024), (48*1024), (63 * 1024), (64 * 1024) };
int SpillSizeTestSuite::_PageSizeSeriesV3[] = { (15 * 1024 * 1024), MDBM_MAXPAGE };
int SpillSizeTestSuite::_SpillSizeSeries[] = { 2, 7, 13, 29, (63 * 1024), (64 * 1024) };
int SpillSizeTestSuite::_SpillSizeSeriesV3[] = { (MDBM_MAXPAGE - 1), MDBM_MAXPAGE };

// -1, 0, (pagesize + 1), (2 * pagesize)
//int SpillSizeTestSuite::_InvalidSpillSizeSeries[] = { -1, 0, 128, 257, 513, 1025, 4097, (48*1024 + 1), (64*1024 + 1) };
int SpillSizeTestSuite::_InvalidSpillSizeSeries[] = { -1, 0 };


void SpillSizeTestSuite::setUp()
{
    //_cfgStory = new ConfigStoryUtil("spilltestv2", "spilltestv3", "Spill Size Test Suite: ");
    if (_setupCnt++ == 0)
        cout << endl << "Spill Size Test Suite Beginning..." << endl << flush;
}

int SpillSizeTestSuite::assertSetSpillSize(MDBM *dbh, int spillSize, const char *assertMsg)
{
    int ret = mdbm_setspillsize(dbh, spillSize);
    if (assertMsg) {
        stringstream ssmsg;
        ssmsg << assertMsg << " Expect to set Spill size=" << spillSize << endl;
        CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (ret == 0));
    }

    return ret;
}

int SpillSizeTestSuite::storeLargeObj(MDBM *dbh, int largeObjSize, const char *keyName, bool expectSuccess, const char *assertMsg)
{
    char * largeObj = new char[largeObjSize];
    CleanupMem raii(largeObj);
    memset(largeObj, 'a', largeObjSize);
    largeObj[largeObjSize-1] = 0;
    if (expectSuccess) {
        string msg = assertMsg;
        msg += " Expected store to Succeed: ";
        return store(dbh, keyName, largeObj, msg.c_str());
    }

    // otherwise we expect it to fail
    int ret = store(dbh, keyName, largeObj);
    if (assertMsg) {
        stringstream ssmsg;
        ssmsg << assertMsg << " Expected store to FAIL for object size="
              << largeObjSize << " : key=" << keyName <<" : ret=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (ret == -1));
    }

    return ret;
}

// Verify default: Insert large object according to default threshold
// Test boundary case for default threshold. For a given page size and presize,
// verify the DB grows after large object added.
// Add object size = 3/4 * page size = default threshold
//
void SpillSizeTestSuite::PresizeDefThresh()  // TC I-1
{
    string tcprefix = "TC I-1: PresizeDefThresh: mdbm_setspillsize: ";
    TRACE_TEST_CASE(tcprefix);
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix;
    string prefix = premsg.str();

    // 1. Call mdbm_open; create DB with page size = 512; presize=512
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    int pageSize = 512;
    int preSize  = pageSize;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // 2. Call mdbm_get_size: z is the initial size
    uint64_t dbSize  = mdbm_get_size(dbh);

    // 3. Call mdbm_store; Add a large object of size = 0.75 * 512 = 128*3 = 384
    int largeObjSize = pageSize - 1;  // FIX this works for v2 and v3 : int(pageSize * 0.76);
    const char * keyName = "spsztc1";
    premsg << " DB created with page size=" << pageSize << " and presize ="
           << preSize << " ";
    prefix = premsg.str();
    storeLargeObj(dbh, largeObjSize, keyName, true, prefix.c_str());

    // verify it was stored - notice funniness in v2
    char * retval = 0;
    int ret = fetch(dbh, keyName, retval, prefix.c_str());
    CPPUNIT_ASSERT_MESSAGE(prefix.c_str(), (ret > 0 && retval));

    // 4. Call mdbm_get_size: n is the new size
    uint64_t newDbSize  = mdbm_get_size(dbh);

    // 5. Expected result: n > z
    stringstream erss;
    erss << prefix << "Stored a large object size=" << largeObjSize
         << " : Expected original DB size=" << dbSize
         << " < post-store DB size=" << newDbSize << endl;
    CPPUNIT_ASSERT_MESSAGE(erss.str(), (newDbSize > dbSize));
}

// Specify small threshold size; where threshold much smaller than page size
//
void SpillSizeTestSuite::PresizeSmallThresh()  // TC I-2
{
    // V3 only
    string tcprefix = "TC I-2: PresizeSmallThresh: mdbm_setspillsize: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with page size = 512; presize=512
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    int pageSize = 512;
    int preSize  = pageSize;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // 2. Call mdbm_setspillsize: 10
    int spillSize = 10;
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 3. Call mdbm_get_size: z is the initial size
    uint64_t dbSize  = mdbm_get_size(dbh);

    // 4. Call mdbm_store; Add a large object of size = 11
    int largeObjSize = spillSize + 1;
    const char * keyName = "spsztc2";
    storeLargeObj(dbh, largeObjSize, keyName, true, prefix.c_str());

    // 5. Call mdbm_get_size: n is the new size
    uint64_t newDbSize  = mdbm_get_size(dbh);

    // 6. Expected result: n > z
    stringstream erss;
    erss << prefix << "Set Spill size=" << spillSize
         << " and Stored a large object size=" << largeObjSize
         << " : Expected original DB size=" << dbSize
         << " < post-store DB size=" << newDbSize << endl;
    CPPUNIT_ASSERT_MESSAGE(erss.str(), (newDbSize > dbSize));
}

// Add large object; verify DB does not grow after adding non-large object
//
void SpillSizeTestSuite::PresizeAddLgAddSm()  // TC I-3
{
    string tcprefix = "TC I-3: PresizeAddLgAddSm: mdbm_setspillsize: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with page size = 512; presize=512
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    int pageSize = 512;
    int preSize  = pageSize;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // 2. Call mdbm_setspillsize: 10
    int spillSize = 10;
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 3. Call mdbm_store; Add a large object of size = 11
    int largeObjSize = spillSize + 1;
    const char * keyName1 = "spsztc3a";
    storeLargeObj(dbh, largeObjSize, keyName1, true, prefix.c_str());

    // 4. Call mdbm_get_size: z
    uint64_t dbSize  = mdbm_get_size(dbh);

    // 5. Call mdbm_store; insert non-large object size = 9
    int nonLargeObjSize = spillSize - 1;
    const char * keyName2 = "spsztc3b";
    storeLargeObj(dbh, nonLargeObjSize, keyName2, true, prefix.c_str());

    // 6. Call mdbm_get_size: n
    uint64_t newDbSize  = mdbm_get_size(dbh);

    // 7. Expected result: n == z
    stringstream erss;
    erss << prefix << "Set Spill size=" << spillSize
         << " and Stored a large object size=" << largeObjSize
         << " and Stored a regular object size=" << nonLargeObjSize
         << " : Expected post-store large-object DB size=" << dbSize
         << " == post-store regular-object DB size=" << newDbSize << endl;
    CPPUNIT_ASSERT_MESSAGE(erss.str(), (newDbSize == dbSize));
}

void SpillSizeTestSuite::SeriesValidThresholds()  // TC I-4
{
    string prefix = "TC I-4: SeriesValidThresholds: ";

    int plen = sizeof(_PageSizeSeries)/sizeof(int);
    int slen = sizeof(_SpillSizeSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
        for (int s=0; s<slen; ++s) {
            CheckSpillSize(prefix, _PageSizeSeries[p], _SpillSizeSeries[s]);
        }
    }
}
void SpillSizeTestSuite::SeriesValidThresholdsB()  // TC I-4
{
    // V3 only
    string prefix = "TC I-4: SeriesValidThresholdsB: ";

    int plen = sizeof(_PageSizeSeries)/sizeof(int);
    int slen = sizeof(_SpillSizeSeriesV3)/sizeof(int);
    for (int p=0; p<plen; ++p) {
        for (int s=0; s<slen; ++s) {
            CheckSpillSize(prefix, _PageSizeSeries[p], _SpillSizeSeriesV3[s]);
        }
    }
}

//  Test series of valid spill sizes relative to page size
// It is unknown what is a valid MINimum spill size, and the valid MAXimum size is pagesize.
//    * For series of page sizes: p in 128, 256, 512, 1024, 2048, 4096, 8192,
//            V2 max=(64 * 1024), V3 max=(16 * 1024 * 1024)
//        * For a series of spill sizes: s in 2, 7, 13, 29, (pagesize - 1), pagesize
void SpillSizeTestSuite::CheckSpillSize(const string& tcprefix, int pgSize, int spillSize) // TC I-4
{
    // since checking valid sizes only, ignore invalid sizes: spillsize > pagesize
    if (spillSize > pgSize) { // ignore invalid sizes
        return;
    }

    CleanupTmpDirLocks();

    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << "CheckSpillSize: "
           << " Page size=" << pgSize << " Spill Size=" << spillSize;
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with page size = p
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pgSize));

    // 2. Call mdbm_setspillsize: s
    // 3. Expected result : success
    assertSetSpillSize(dbh, spillSize, prefix.c_str());
}

//  Test series of invalid spill sizes relative to page size
//    * For series of page sizes: p in 128, 256, 512, 1024, 2048, 4096, 8192, V2 max=(64 * 1024), V3 max=(16 * 1024 * 1024)
//        * For a series of spill sizes: s in -1, 0, (pagesize + 1), (2 * pagesize)
void SpillSizeTestSuite::SeriesInvalidThresholds()  // TC I-5
{
    // FIX BZ 5280242: V2 and V3
    string prefix = "TC I-5: SeriesInvalidThresholds: ";
    int plen = sizeof(_PageSizeSeries)/sizeof(int);
    int slen = sizeof(_InvalidSpillSizeSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
      for (int s=0; s<slen; ++s) {
        CheckInvalidSpillSize(prefix, _PageSizeSeries[p], _InvalidSpillSizeSeries[s]);
      }
    }
}

void SpillSizeTestSuite::CheckInvalidSpillSize(const string& tcprefix, int pgSize, int spillSize)
{
    TRACE_TEST_CASE(tcprefix)
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << "CheckInvalidSpillSize: DB version="
           << " Page size=" << pgSize << " Spill Size=" << spillSize;
    string prefix = premsg.str();

    // 1. Call mdbm_open; create DB with page size = p
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pgSize));

    // 2. Call mdbm_setspillsize: s
    int ret = mdbm_setspillsize(dbh, spillSize);

    // 3. Expected result : FAIL and errno set
    stringstream efss;
    efss << prefix << " Expected FAILure for setting invalid spill size" << endl;
    CPPUNIT_ASSERT_MESSAGE(efss.str(), (ret == -1));
}

// Reset spill size to lower size after large objects had been added
void SpillSizeTestSuite::AddLOresetSpillsize()  // TC I-6
{
    string tcprefix = "TC I-6: AddLOresetSpillsize: mdbm_setspillsize: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with page size = 512; presize=512
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    int pageSize = 512;
    int preSize  = pageSize;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, preSize));

    // 2. Call mdbm_setspillsize: 1/2 of page size = 256
    int spillSize = pageSize / 2;
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 3. Call mdbm_store; Add a large object of size = 256; key="largeobj256"
    int largeObjSize1 = spillSize;
    const char * keyName1 = "largeobj256";
    storeLargeObj(dbh, largeObjSize1, keyName1, true, prefix.c_str());

    // 4. Call mdbm_setspillsize: 1/4 of original page size = 128
    spillSize = pageSize / 2;
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 5. Call mdbm_store; Add a large object of size = 128; key="largeobj128"
    int         largeObjSize2 = spillSize;
    const char *keyName2      = "largeobj128";
    storeLargeObj(dbh, largeObjSize2, keyName2, true, prefix.c_str());

    // 6. Call mdbm_fetch; key="largeobj256"
    char *foundval = 0;
    int ret = fetch(dbh, keyName1, foundval);

    // 7. Expected result: success
    stringstream ffss;
    ffss << prefix << "Expected to Succeed to fetch the stored large object size=" << largeObjSize1
        << " using key=" << keyName1 << " from the DB." << endl;
    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret > 0 && foundval) );

    // 8. Call mdbm_fetch; key="largeobj128"
    foundval = 0;
    ret = fetch(dbh, keyName2, foundval);

    // 9. Expected result: success
    stringstream ffss2;
    ffss2 << prefix << "Expected to Succeed to fetch the stored large object size=" << largeObjSize2
        << " using key=" << keyName2 << " from the DB." << endl;
    CPPUNIT_ASSERT_MESSAGE(ffss2.str(), (ret > 0 && foundval) );
}

// Limit the DB size then add large objects bigger than DB size
void SpillSizeTestSuite::LimitThenAddVL()  // TC I-7
{
    // V3 only
    string tcprefix = "TC I-7: LimitThenAddVL: mdbm_setspillsize: ";
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix;
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with page size = 128
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    int pageSize = 128;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize, pageSize));

    // 2. Call mdbm_limit_size: 1 page
    mdbm_ubig_t numPages = 1;
    premsg << " Limiting DB number pages=" << numPages;
    prefix = premsg.str();
    mdbm_limit_size_v3(dbh, numPages, 0, 0);

    // 3. Call mdbm_setspillsize: 64
    int spillSize = pageSize / 2;
    stringstream ssss1;
    ssss1 << prefix << " Set Spill Size=" << spillSize << " : ";
    prefix = ssss1.str();
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 4. Call mdbm_store; Add a large object of size = 65
    // 5. Expected result: failure
    int          largeObjSize = spillSize + 1;
    const char * keyName      = "sstc7";
    storeLargeObj(dbh, largeObjSize, keyName, false, prefix.c_str());

    // 6. Call mdbm_store; Add a large object of size = 64
    // 7. Expected result: FAIL and errno set
    keyName      = "sstc7b";
    storeLargeObj(dbh, spillSize, keyName, false, prefix.c_str());
}

// Limit DB size; fill with normal data; then try to add large object
void SpillSizeTestSuite::PremadeAddLO()  // TC I-8
{
    string tcprefix = "TC I-8: PremadeAddLO: mdbm_setspillsize: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Get premade DB
    int flags = versionFlag | MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_NOLOCK;
    MdbmHolder dbh(GetTmpMdbm(flags, 0644, 512, 512));
    //MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | versionFlag));

    mdbm_limit_size_v3(dbh, 1, 0, 0); // set to 1 page and no shake function
    FillDb(dbh);

    // 2. Call mdbm_setspillsize: 256, expect fail
    int spillSize = 256;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_setspillsize(dbh, spillSize));

    // 3. Call mdbm_store; Add a large object of size = 256
    // 4. Expected result: FAIL and errno set
    int          largeObjSize = 256;
    const char * keyName      = "sstc8";
    storeLargeObj(dbh, largeObjSize, keyName, false, prefix.c_str());
}

//  Lower spill size less than existing data of non-large objects
// Add data of size = s; lower spill size = s - 1; add data of size = s - 1
//
void SpillSizeTestSuite::AddLOthenLowerSpill()  // TC I-9
{
    string tcprefix = "TC I-9: AddLOthenLowerSpill: mdbm_setspillsize: ";
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix;
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);

    // 1. Call mdbm_open; create DB with page size = 512
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    int pageSize = 512;
    premsg << " Page size=" << pageSize;
    prefix = premsg.str();
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // 2. Call mdbm_setspillsize: 256
    int spillSize = pageSize / 2;
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 3. Call mdbm_store; Add a non-large object of size = 255; key="nlo255"
    int          largeObjSize1 = spillSize - 1;
    const char * keyName1      = "nlo255";
    storeLargeObj(dbh, largeObjSize1, keyName1, true, prefix.c_str());

    // 4. Call mdbm_setspillsize: 128
    spillSize = spillSize / 2;
    assertSetSpillSize(dbh, spillSize, prefix.c_str());

    // 5. Call mdbm_store; Add a non-large object of size = 127
    int          largeObjSize2 = spillSize - 1;
    const char * keyName2      = "nlo127";
    storeLargeObj(dbh, largeObjSize2, keyName2, true, prefix.c_str());

    // 6. Call mdbm_fetch; key="nlo255"
    char * foundval = 0;
    int ret = fetch(dbh, keyName1, foundval);

    // 7. Expected result: success
    stringstream ffss;
    ffss << prefix << " : Expected to Succeed to fetch the stored large object size=" << largeObjSize1
        << " using key=" << keyName1 << " from the DB." << endl;
    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret > 0 && foundval) );

    foundval = 0;
    ret = fetch(dbh, keyName2, foundval);

    stringstream ffss2;
    ffss2 << prefix << " : Expected to Succeed to fetch the stored large object size=" << largeObjSize2
        << " using key=" << keyName2 << " from the DB." << endl;
    CPPUNIT_ASSERT_MESSAGE(ffss2.str(), (ret > 0 && foundval) );
}

// Error case in mdbm_setspillsize()
//
void SpillSizeTestSuite::ErrorCaseMdbmSetSpillSize()  // TC I-10
{
    string tcprefix = "TC I-10: ErrorCaseMdbmSetSpillSize: mdbm_setspillsize: ";
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix;
    string prefix = premsg.str();
    TRACE_TEST_CASE(tcprefix);
    int pageSize = 512;
    int spillSize=pageSize;

    //Error Case: Passing MDBM as NULL
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_setspillsize(NULL,spillSize));
    // Call mdbm_open; create DB with page size = 512
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_LARGE_OBJECTS;
    premsg << " Page size=" << pageSize;
    prefix = premsg.str();
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pageSize));

    // Error case: spillsize > page size
    spillSize = pageSize * 2;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_setspillsize(dbh,spillSize));
}

class SpillSizeTestSuiteV3 : public SpillSizeTestSuite
{
    CPPUNIT_TEST_SUITE(SpillSizeTestSuiteV3);
    CPPUNIT_TEST(PresizeDefThresh);
    CPPUNIT_TEST(PresizeSmallThresh);
    CPPUNIT_TEST(PresizeAddLgAddSm);
    CPPUNIT_TEST(SeriesValidThresholds);
    CPPUNIT_TEST(SeriesValidThresholdsB);
    CPPUNIT_TEST(SeriesInvalidThresholds);
    CPPUNIT_TEST(AddLOresetSpillsize);
    CPPUNIT_TEST(LimitThenAddVL);
    CPPUNIT_TEST(PremadeAddLO);
    CPPUNIT_TEST(AddLOthenLowerSpill);
    CPPUNIT_TEST(ErrorCaseMdbmSetSpillSize);
    CPPUNIT_TEST_SUITE_END();
public:
    SpillSizeTestSuiteV3() : SpillSizeTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(SpillSizeTestSuiteV3);

