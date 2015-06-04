/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  File: test_open.cc
//  Purpose: Unit tests of all the flags mdbm_open()

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h>

#include <iostream>
#include <sstream>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <mdbm.h>
#include "mdbm_internal.h"

#include "TestBase.hh"

using namespace std;

static const int PAGESZ = 1024;
static const int STARTPOINT = 12345;    // Should generate 2 pages worth of data
static const int NUM_DATA_ITEMS = 456;
static const char * PREFIX  = "PRFX";
static const char * MODIFIED_DATA = "stuffing";  // Coded around turkey day
static const char *LARGE_OBJ_KEY = "LargeObjKey";

static const int MEGBT = 1024 * 1024;

static const int PAGESZ_1K  =  1024;
static const int BIGGER_PAGE_SIZE      =   8192;    // Try Bigger size
static const int TEST_PAGE_SMALL_V3   =   384;      // Try smaller size
static const int MAX_PAGE_V2 = ( 1 << 16 );    // 2^16 = 65536

/* Maximum page size is 16MB-64bytes, because any page size above that would be rounded to 16MB,
 * which does not fit in the 24 bits allocated for the in-page offset to an object */
static const int MAX_PAGE_V3 = ( (1 << 24) - MDBM_PAGE_ALIGN);

// Predefined (for tests A12, A13) MDBM sizes, in MegaBytes
#ifdef __x86_64__
static const int dbSizes[] = {0, 1, 2, 4, 5, 10, 16,32, 50, 64, 100, 128, 250, 256, 500, 512, 1000, 1024, 2000, 2048, 4000, 4096 };
#else
static const int dbSizes[] = {0, 1, 2, 4, 5, 10, 16,32, 50, 64, 100, 128, 250, 256, 500, 512, 1000, 1024, 2000};
#endif

static const int pageSizesV2[] = {MAX_PAGE_V3, MAX_PAGE_V3*2, MAX_PAGE_V2+1, MAX_PAGE_V2*2, 1, 64, 100, 127, 0, -1, -120, -65336, 129, 259, 1023, 16385 };
static const int pageSizesV2ArrayLen = sizeof(pageSizesV2) / sizeof(int);
static const vector<int> PageSizesV2(pageSizesV2, pageSizesV2+pageSizesV2ArrayLen);

// Test page sizes that are too large, too small, zero, negative, and size rounding
static const int pageSizesV3[] = {MAX_PAGE_V3+1, MAX_PAGE_V3*2, 1, 64, 100, 127, 0, -1, -120, -65336, 129, 191, 259, 104860, MAX_PAGE_V3-100 };
static const int pageSizesV3ArrayLen = sizeof(pageSizesV3) / sizeof(int);
static const vector<int> PageSizesV3(pageSizesV3, pageSizesV3+pageSizesV3ArrayLen);

static const int V3_ROUNDING_FACTOR = 64;

static const int UNITTEST_LEVEL_LONG =  4;  // When MDBM_UNITTEST_LEVEL is 4, run long open tests
  // Default case, when MDBM_UNITTEST_LEVEL!=4, run up to 5MB V2 tests only and up to 10MB V3 tests
static const int MDBM_FEWER_UTESTS    =  5;

extern "C" {
int open_tmp_test_file(const char *file, uint32_t siz, char *buf);
}

class MdbmOpenUnitTest : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmOpenUnitTest(int vFlag) : TestBase(vFlag, "MdbmOpenUnitTest") {}

    void setUp();     // Per-test initialization - applies to every test
    void tearDown();  // Per-test cleanup - applies to every test

    void initialSetup();
    void test_mdbm_openA1();
    void test_mdbm_openA2();
    void test_mdbm_openA3();
    void test_mdbm_openA4();
    void test_mdbm_openA5();
    void test_mdbm_openA6();
    void test_mdbm_openA7();
    void test_mdbm_openA8();
    void test_mdbm_openA9();
    void test_mdbm_openA10();
    void test_mdbm_openA11();
    void test_mdbm_openA12();
    void test_mdbm_openA13();
    void test_mdbm_openA14();
    void test_mdbm_openA15();
    void test_mdbm_openA16();
    void test_mdbm_openA17();
    void test_mdbm_openA18();
    void test_mdbm_openA19();
    void test_mdbm_openA20();
    void test_mdbm_openA21();
    void test_mdbm_openA22();
    void test_mdbm_openA23();
    void test_mdbm_openA24();
    void test_mdbm_openA25();
    void test_mdbm_openA26();
    void test_mdbm_openA27();
    void test_mdbm_openA28();
    void test_mdbm_openA29();
    void test_readonly_protection();

    // Helper functions
    void testA29Helper(const string &fname, int flags);
    void  changeDatum(MDBM **m, int dataNum, int flags = 0);
    int   writeDatum(MDBM *m, int dataNum);
    void  createData(int align = 0, int pagesz = PAGESZ);
    void  readBackData(int openFlags );
    u_int readBackModifiedData();
    u_int checkKeyValue(MDBM * mdbm, int key, const char * otherValue = 0);
    void  deleteFailed( MDBM * mdbm, int key );
    void testA9Helper(bool allowLargeObj);
    int  storeLargeObj(MDBM *db, int xobjNum = 0, int *sizePtr = 0);
    void getLargeObj(MDBM * mdbm, int key, int size = 0);
    void testProtectionSegv(int flags);
    void testProtectedLocked(int flags);
    int remapIsLimited();   // Use remap_file_pages() to test whether we're on RHEL6 or not

    string fileName;
    string basePath;
};


void MdbmOpenUnitTest::setUp()  {
  CPPUNIT_ASSERT(0 == GetTmpDir(basePath));
  fileName = basePath + "/opentest" + versionString;
}
void MdbmOpenUnitTest::tearDown() {
}

// Initial setup for all mdbm_open() unit tests
void
MdbmOpenUnitTest::initialSetup()
{
    TRACE_TEST_CASE(__func__);
    // Create the data and copy to the _cp* files
    createData();

    string moveTo = fileName + "_cp";
    CPPUNIT_ASSERT_EQUAL(0, rename(fileName.c_str(), moveTo.c_str() ));

    // Clean up .protect files, just in case they are there
    string toDelete = fileName + ".protect";
    unlink(toDelete.c_str());
}

//   mdbm_open() - Unit Test #A1, for MDBM version 2 and 3
//   Open pre-populated MDBM. Change data. Close.
//   Verify data is unchanged from original pre-populated MDBM.
void
MdbmOpenUnitTest::test_mdbm_openA1()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(fileName);

    MDBM *mdbm = 0;
    for (int i = 1; i <= 100; ++i) {
        changeDatum(&mdbm, STARTPOINT + i);
    }
    mdbm_close(mdbm);

    u_int unmodified = readBackModifiedData();
    printf("Percentage of %s data changes stored back w/o sync option: %d%%\n",
        versionString.c_str(), 100-unmodified);
}


//   mdbm_open() - Unit Test #A2, for MDBM version 2 and 3
//   Open pre-populated MDBM. Open with MDBM_O_FSYNC and change data. Close.
//   Verify data is unchanged from original pre-populated MDBM.
void
MdbmOpenUnitTest::test_mdbm_openA2()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(fileName);

    MDBM *mdbm = 0;
    for (int i = 1; i <= 100; ++i) {
        changeDatum(&mdbm, STARTPOINT + i, MDBM_O_FSYNC);
    }
    mdbm_close(mdbm);

    int unmodified = readBackModifiedData();
    CPPUNIT_ASSERT_EQUAL( 0, unmodified );
}

//   mdbm_open() - Unit Test #A3, for MDBM version 2 and 3
//   Open pre-populated MDBM. Open with MDBM_O_ASYNC and change data. Close.
//   Verify data is unchanged from original pre-populated MDBM.
void
MdbmOpenUnitTest::test_mdbm_openA3()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(fileName);

    MDBM *mdbm = 0;
    for (int i = 1; i <= 100; ++i)
        changeDatum(&mdbm, STARTPOINT + i, MDBM_O_ASYNC);
    mdbm_close(mdbm);

    u_int unmodified = readBackModifiedData();
    printf("Percentage of %s data changes stored back with O_ASYNC option: %d%%\n",
        versionString.c_str(), 100-unmodified);
}

// mdbm_open() - Unit Test #A4, for MDBM version 2
// This test does not behave as expected, meaning that O_WRONLY actually opens for RDWR.
// See bugzilla bug 5195012 - according to Steve Carney, we should just document behavior.
void
MdbmOpenUnitTest::test_mdbm_openA4()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(fileName);

    u_int failures = 0;
    MDBM *mdbm = 0;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_O_WRONLY, 0200, PAGESZ, 0)) != NULL);

    CPPUNIT_ASSERT(0 == VerifyData(mdbm));
    mdbm_close(mdbm);
    printf("Failed to read %d of entries with O_WRONLY\n", failures);

}

//   mdbm_open() - Unit Test #A5, for MDBM version 2 and 3
//   Call mdbm_open() with MDBM_O_WRONLY, perform writes, close.  Reopen with O_RDONLY
//   and make sure data is read back correctly.
void
MdbmOpenUnitTest::test_mdbm_openA5()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(fileName);

    int unmodified = 0;
    MDBM *mdbm = 0;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_O_WRONLY | MDBM_O_FSYNC, 0200, PAGESZ, 0)) != NULL);

    for (int i = 1; i <= 100; ++i) {
        changeDatum(&mdbm, STARTPOINT + i, MDBM_O_WRONLY);
        unmodified += checkKeyValue(mdbm, STARTPOINT + i, MODIFIED_DATA );
    }
    mdbm_close(mdbm);
    mdbm = 0;

    CPPUNIT_ASSERT((mdbm = mdbm_open(fileName.c_str(), MDBM_O_RDONLY, 0200, PAGESZ, 0)) != NULL);
    for (int i = 1; i <= 100; ++i) {
        unmodified += checkKeyValue(mdbm, STARTPOINT + i, MODIFIED_DATA );
    }
    mdbm_close(mdbm);

    CPPUNIT_ASSERT_EQUAL(0, unmodified);
}


//   mdbm_open() - Unit Test #A6, for MDBM version 2 and 3
//   Make sure MDBM opened with O_RDONLY cannot be written to.
void
MdbmOpenUnitTest::test_mdbm_openA6()
{
    TRACE_TEST_CASE(__func__);
    MDBM * mdbm = 0;

    CopyFile(fileName);
    readBackData(MDBM_O_RDONLY);
    CPPUNIT_ASSERT((mdbm = mdbm_open(fileName.c_str(), MDBM_O_RDONLY, 0644, PAGESZ, 0)) != NULL);

    int off = (versionFlag & MDBM_CREATE_V3) ? 2 : 1;
    // Below should be failing, so it should not return a zero
    int retval = !(writeDatum(mdbm, STARTPOINT+off));

    CPPUNIT_ASSERT(0 == retval);
    mdbm_close(mdbm);
}


//   mdbm_open() - Unit Test #A7, for MDBM version 2 and 3
//   Make sure MDBM opened with O_RDONLY cannot be deleted from.
void
MdbmOpenUnitTest::test_mdbm_openA7()
{
    TRACE_TEST_CASE(__func__);
    MDBM * mdbm = 0;
    CopyFile(fileName);

    readBackData(MDBM_O_RDONLY);

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_O_RDONLY, 0644, PAGESZ, 0)) != NULL);
    deleteFailed(mdbm, STARTPOINT+100);
    mdbm_close(mdbm);
}


//   mdbm_open() - Unit Test #A8, for MDBM version 2 and 3
//   Add data to newly created MDBM opened with O_CREAT.  Read back data to make sure
//   it is all there.
void
MdbmOpenUnitTest::test_mdbm_openA8()
{
    TRACE_TEST_CASE(__func__);
    CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
    MDBM * mdbm = 0;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_O_CREAT | MDBM_O_RDWR, 0644, PAGESZ, 0)) != NULL);

    // Test writes
    CPPUNIT_ASSERT(0 == InsertData(mdbm));
    // Test reads
    CPPUNIT_ASSERT(0 == VerifyData(mdbm));

    mdbm_close(mdbm);
}

//   mdbm_open() - Unit Test #A9, for MDBM version 2 and 3
//   Make sure existing MDBM re-opened with MDBM_O_CREAT does not erase data the data that
//   has been previously written to the MDBM.  Test other things like alignment, page-size...
//   Use the testA9Helper to run for both regular and large objects, for both V2 and V3.
void
MdbmOpenUnitTest::test_mdbm_openA9()
{
    TRACE_TEST_CASE(__func__);
    // Create data files for both V2 and V3 with alignment=32bit.  We want to make
    // sure mdmb_get_alignment() returns the non-default (zero) alignment value.
    // MDBM_ALIGN_32_BITS is a bitmask, whose value is 3 decimal.
    createData(MDBM_ALIGN_32_BITS);
    testA9Helper(false);   // no large objects
    testA9Helper(true);    // large objects
}

void
MdbmOpenUnitTest::testA9Helper(bool allowLargeObj)
{
    MDBM * mdbm1 = 0;
    string filename;
    const char *fname = fileName.c_str();

    if (allowLargeObj) {
        // Large Objects + Make sure all data is on disk
        int openFlags = MDBM_O_FSYNC | MDBM_LARGE_OBJECTS | versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;
        // Create file and assign fname for large objects
        filename = basePath + "/lopentest"+versionString;
        fname = filename.c_str();

        MDBM * mdbm = EnsureNamedMdbm(fname, openFlags, 0644, PAGESZ, 0);
        CPPUNIT_ASSERT(mdbm != NULL);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_set_alignment(mdbm, MDBM_ALIGN_16_BITS));
        //CPPUNIT_ASSERT_EQUAL(0, populateMdbm(mdbm, NUM_DATA_ITEMS, STARTPOINT, PREFIX));
        CPPUNIT_ASSERT(0 == InsertData(mdbm));

        storeLargeObj( mdbm );

        mdbm_close(mdbm);
    }

    CPPUNIT_ASSERT( (mdbm1 = mdbm_open(fname, MDBM_O_RDONLY, 0444, PAGESZ, 0)) != NULL);

    TestStats stats1;    // Get stats before the MDBM_O_CREAT
    GetTestStats(mdbm1, &stats1);

    mdbm_close(mdbm1);
    mdbm1 = 0;

    // Use a different page size, 2K, and a different DB size, 15MB, to open
    MDBM * mdbm2 = 0;
    CPPUNIT_ASSERT( (mdbm2 = mdbm_open(fname, MDBM_O_CREAT | MDBM_O_RDWR, 0644, 2048, 15*1024*1024)) != NULL);

    TestStats stats2;
    GetTestStats(mdbm2, &stats2);

    CPPUNIT_ASSERT_EQUAL(stats1.psize, stats2.psize);
    CPPUNIT_ASSERT_EQUAL(stats1.fileSize, stats2.fileSize);
    CPPUNIT_ASSERT_EQUAL(stats1.hashFunc, stats2.hashFunc);
    CPPUNIT_ASSERT_EQUAL(stats1.alignment, stats2.alignment);
    CPPUNIT_ASSERT_EQUAL(stats1.largeObj, stats2.largeObj);

    // Try writing, should work
    CPPUNIT_ASSERT_EQUAL(0, writeDatum(mdbm2, STARTPOINT+25));

    checkKeyValue(mdbm2, STARTPOINT+28);  // Try reading, should work

    mdbm_close(mdbm2);
    unlink(fname);
}

// mdbm_open() - Unit Test #A10, for MDBM version 2 and 3
// Open brand new MDBM without the MDBM_O_CREAT flag. Verify mdbm_open() fails to open
// and returns an error.
//
// First test (missing O_RDWR) generates the following error:
// E 19-141951.296518 16077 opentestV3.mdbm: Must set MDBM_O_RDWR when using MDBM_O_CREAT: ...
//
// The second test's (missing O_CREAT) error message looks like:
//  E 19-113622.017096  6954 opentest.mdbm: mdbm file open failure: No such file or directory
//
// The final implementation of Bugzilla bug 5267466 allows for MDBM_O_CREAT|MDBM_O_RDONLY
// by opening with RDWR, closing, and reopening with RDONLY.  RDONLY is an empty bitmask
// so MDBM_O_CREAT|MDBM_CREATE_V3 behaves the same as  MDBM_O_CREAT|MDBM_O_RDONLY|MDBM_CREATE_V3

void
MdbmOpenUnitTest::test_mdbm_openA10()
{
    TRACE_TEST_CASE(__func__);
    const char *fname = fileName.c_str();
    unlink(fname);
    // First try without O_RDWR
    {
      MdbmHolder db(mdbm_open(fname, MDBM_O_CREAT | versionFlag, 0644, PAGESZ, 0));

      if (versionFlag & MDBM_CREATE_V3) {
        CPPUNIT_ASSERT(NULL != (MDBM*)db);
      } else {
        CPPUNIT_ASSERT(NULL == (MDBM*)db);
        CPPUNIT_ASSERT_EQUAL(EINVAL, errno);
      }
    }
    unlink(fname);  // cleanup, just in case

    unlink(fname);  // cleanup, just in case
    CPPUNIT_ASSERT(mdbm_open(fname, MDBM_O_RDWR | versionFlag, 0644, PAGESZ, 0) == NULL);
    CPPUNIT_ASSERT_EQUAL(ENOENT, errno);
    unlink(fname);  // cleanup
}


// mdbm_open() - Unit Test #A11, for MDBM version 2 and 3
// Open pre-existing MDBM with MDBM_O_TRUNC flag. Verify old data is missing.
void
MdbmOpenUnitTest::test_mdbm_openA11()
{
    TRACE_TEST_CASE(__func__);
    int flags =  MDBM_O_TRUNC | MDBM_O_RDWR | versionFlag;
    MDBM* mdbm = 0;
    CopyFile(fileName);

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), flags, 0644, PAGESZ, 0)) != NULL);

    char key1[32];
    datum fetched1;
    snprintf(key1, sizeof(key1), "%s%d", PREFIX, STARTPOINT+3);
    const datum k1 = { key1, (int)strlen(key1) };
    errno = 0;
    fetched1 = mdbm_fetch(mdbm, k1);

    CPPUNIT_ASSERT(errno != 0);     // fetch sets errno to 2 to indicate failure
    CPPUNIT_ASSERT(fetched1.dptr == NULL);
    errno = 0;
    mdbm_close(mdbm);
}


//  mdbm_open() - Unit Test #A12, for MDBM version 2 and 3
//  Open pre-existing MDBM with MDBM_O_TRUNC flag. Verify old data is missing.
void
MdbmOpenUnitTest::test_mdbm_openA12()
{
    TRACE_TEST_CASE(__func__);
    stringstream msg;
    int presize;
    uint64_t presizeLarge, mdbmSize;
    MDBM * mdbm;
    int curSize, testLevel = GetTestLevel();
    int flags = MDBM_O_CREAT | MDBM_O_RDWR | versionFlag;
    int smallCap = (versionFlag & MDBM_CREATE_V3) ? 10 : MDBM_FEWER_UTESTS;

    for (u_int i=0; i < (sizeof(dbSizes) / sizeof(int)); ++i) {
        curSize = dbSizes[i];
        // testLevel !=4 means don't spend too much time here, it takes a lot
        // of time to preallocate large MDBMs.
        if ((testLevel != UNITTEST_LEVEL_LONG) && curSize > smallCap) {
            break;
        } else if (curSize > 2047) {  // API uses int, which cannot fit more than 2048MB-1
            break;
        }
        CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
        presize = curSize * MEGBT;
        presizeLarge = presize;
        CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), flags, 0644, 0, presize)) != NULL);
        mdbmSize = mdbm_get_size(mdbm);
        msg << "Preallocated size " << presizeLarge << " must be less than or equal to " << mdbmSize;
        CPPUNIT_ASSERT_MESSAGE(msg.str(), presizeLarge <= mdbmSize );
        mdbm_close(mdbm);
    }
}

//   mdbm_open() - Unit Test #A13, for MDBM 3
//   Open with presize and MDBM_DBSIZE_MB option.  Verify size in megabytes afterwards.
void
MdbmOpenUnitTest::test_mdbm_openA13()
{
    TRACE_TEST_CASE(__func__);
    stringstream msg;
    uint64_t curSize, presizeLarge, mdbmSize;
    MDBM * mdbm;
    int testLevel = GetTestLevel();
    int flags = MDBM_O_CREAT | MDBM_O_RDWR | MDBM_DBSIZE_MB | versionFlag;

    for (u_int i=0; i < (sizeof(dbSizes) / sizeof(int)); ++i) {
        curSize = dbSizes[i];
        // testLevel !=4 means don't spend too much time here, it takes a lot
        // of time to preallocate large MDBMs.
        if ((testLevel != UNITTEST_LEVEL_LONG) && curSize > 10) {
            break;
        }
        CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
        presizeLarge = curSize * MEGBT;
        CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), flags, 0644, 0, curSize)) != NULL);
        mdbmSize = mdbm_get_size(mdbm);
        msg << "Preallocated size " << presizeLarge << " must be less than or equal to " << mdbmSize;
        CPPUNIT_ASSERT_MESSAGE(msg.str(), presizeLarge <= mdbmSize );
        mdbm_close(mdbm);
    }
}

//  mdbm_open() - Unit Test #A14, for MDBM version 2 and 3
//  Open MDBM with the MDBM_LARGE_OBJECTS flag set.
//  Verify that reading and writing large objects to the MDBM succeeds.
// Create MDBM with regular and large objects.  Make sure you have large objects
// in DB after creation by calling mdbm_get_stats() and checking large_page_count
void
MdbmOpenUnitTest::test_mdbm_openA14()
{
    TRACE_TEST_CASE(__func__);
    MDBM *mdbm;
    int flags = versionFlag | MDBM_LARGE_OBJECTS | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_O_TRUNC;

    mdbm = EnsureNamedMdbm(fileName.c_str(), flags, 0644, PAGESZ, 0);;
    CPPUNIT_ASSERT(mdbm != NULL);
    CPPUNIT_ASSERT(0 == InsertData(mdbm));
    //CPPUNIT_ASSERT_EQUAL(0, populateMdbm(mdbm, 10, 30, PREFIX));  // Enter a few small objects

    int i, ret;
    for (i = 0; i < 10; ++i) {
        ret = storeLargeObj( mdbm, i );
        CPPUNIT_ASSERT_EQUAL(0, ret);
    }

    char key1[32];
    snprintf(key1, sizeof(key1), "%s2", LARGE_OBJ_KEY);
    const datum k1 = { key1, (int)strlen(key1) };
    ret = mdbm_delete(mdbm, k1);    // Delete 2nd item to test deletes
    CPPUNIT_ASSERT_EQUAL(0, ret);

    for (i = 0; i < 10; ++i) {
        if (i != 2) {
            getLargeObj(mdbm, i);
        }
    }

    mdbm_stats_t stats;
    memset(&stats, 0, sizeof(stats));
    mdbm_get_stats(mdbm, &stats, sizeof(stats));
    CPPUNIT_ASSERT(stats.s_large_page_count != 0);

    mdbm_close(mdbm);
}


// mdbm_open() - Unit Test #A15, for MDBM version 2 and 3
// Open MDBM w/o CREATE_LARGE_OBJECTS: writing large objects should fail
// Also, MDBM code prints "too big for a single db page" to stderr
void
MdbmOpenUnitTest::test_mdbm_openA15()
{
    TRACE_TEST_CASE(__func__);
    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_O_TRUNC;
    MDBM* mdbm = EnsureNamedMdbm(fileName, flags, 0644, PAGESZ, 0);
    CPPUNIT_ASSERT(mdbm != NULL);
    int retval = storeLargeObj(mdbm, 0);

    CPPUNIT_ASSERT(retval != 0);  // Should fail: retval=0 means success
    mdbm_close(mdbm);
}


//  mdbm_open() - Unit Test #A16, for MDBM version 2 and 3
//  After opening existing MDBM with MDBM_PROTECT, reading, deleting and writing
//  fails without a lock.
//  V2 doesn't support MDBM_PROTECT, so check RDONLY in A16v2 and RDWR in A17v2
//  MDBM_PROTECT test A16: also prints the following message to stdout:
//  protect feature enabled -- performance might be degraded
void
MdbmOpenUnitTest::test_mdbm_openA16()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

    if (versionFlag & MDBM_CREATE_V3) {
      // V3 is simple...
      testProtectionSegv(MDBM_PROTECT | MDBM_O_RDWR);
      return;
    }

    CopyFile(fileName);
    MDBM *mdbm;

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_PROTECT | MDBM_O_RDONLY, 0644, 0, 0)) != NULL);

    //checkKeyValue(mdbm, STARTPOINT+33);  // Reading works with MDBM_PROTECT
    CPPUNIT_ASSERT(0 == VerifyData(mdbm));

    int ret = writeDatum(mdbm, STARTPOINT+6);
    CPPUNIT_ASSERT_EQUAL(-1, ret);   // Writing should fail and return -1

    // Try updating
    char buf[32], buf2[32];
    snprintf(buf, sizeof(buf), "%s%d", PREFIX, 25);
    const datum ky = { buf, (int)strlen(buf) };
    memset(buf2, '\0', sizeof(buf2));
    strncpy(buf2, MODIFIED_DATA, sizeof(buf2)-1);
    const datum newValue = { buf2, (int)strlen(buf2) };
    ret = mdbm_store( mdbm, ky, newValue, MDBM_REPLACE);
    CPPUNIT_ASSERT_EQUAL(-1, ret);   // Updating should fail and return -1

    mdbm_close(mdbm);
}


void
MdbmOpenUnitTest::testProtectionSegv(int flags)
{
    const char *fname = fileName.c_str();
    flags = flags | versionFlag;

    CopyFile(fname);

    int status, retsig = 0;
    pid_t mypid = fork();

    if (mypid == 0) { // Child
        MdbmHolder mdbm(mdbm_open(fname, flags, 0644, 0, 0));
        CPPUNIT_ASSERT( NULL != (MDBM*)mdbm);

        char key1[32];
        datum fetched1;
        snprintf(key1, sizeof(key1), "%s%d", PREFIX, STARTPOINT+2);
        const datum k1 = { key1, (int)strlen(key1) };
        errno = 0;

        fetched1 = mdbm_fetch(mdbm, k1);   // Should cause a SEGV
        //mdbm_close(mdbm);   // In case it doesn't SEGV
        exit(1);
    } else {  // Parent
        CPPUNIT_ASSERT(mypid >= 0);
        waitpid(mypid, &status, 0);
        if (WIFSIGNALED(status)) {
            retsig = WTERMSIG(status);
        }
    }

    CPPUNIT_ASSERT_EQUAL(SIGSEGV, retsig);
}


// mdbm_open() - Unit Test #A17, for MDBM version 2 and 3
//
// Verify that after opening an MDBM with MDBM_O_PROTECT and setting a lock,
// data can be read, updated and deleted.
//
// V2 doesn't support MDBM_PROTECT, so check RDONLY in A16v2 and RDWR in A17v2
//
// MDBM_PROTECT test: prints the following message to stdout for V3 tests:
// protect feature enabled -- performance might be degraded
void
MdbmOpenUnitTest::test_mdbm_openA17()
{
    TRACE_TEST_CASE(__func__);
    string buf(fileName);
    buf += ".protect";
    unlink(buf.c_str());  // Make sure there's no .protect file

    testProtectedLocked(MDBM_PROTECT | MDBM_O_RDWR);
}

void
MdbmOpenUnitTest::testProtectedLocked(int flags)
{
    const char *fname = fileName.c_str();
    flags |= versionFlag;

    CopyFile(fname);
    MDBM *mdbm;

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname, flags, 0644, 0, 0)) != NULL);

    mdbm_lock(mdbm);

    //checkKeyValue(mdbm, STARTPOINT+40);  // Reading should work
    CPPUNIT_ASSERT(0 == VerifyData(mdbm));

    int ret = writeDatum(mdbm, STARTPOINT+16);
    CPPUNIT_ASSERT_EQUAL(0, ret);   // Writing should work

    // Updating should work
    char buf[32], buf2[32];
    snprintf(buf, sizeof(buf), "%s%d", PREFIX, 80);
    const datum ky = { buf, (int)strlen(buf) };
    memset(buf2, '\0', sizeof(buf2));
    strncpy(buf2, MODIFIED_DATA, sizeof(buf2)-1);
    const datum newValue = { buf2, (int)strlen(buf2) };
    ret = mdbm_store( mdbm, ky, newValue, MDBM_REPLACE);
    CPPUNIT_ASSERT_EQUAL(0, ret);

    mdbm_unlock(mdbm);
    mdbm_close(mdbm);
}


//  mdbm_open() - Unit Test #A18, for MDBM version 2 and 3
//
//  File name of NULL should tell MDBM to maintain DB in memory only.  It fails, and
//  generates the following error message:
//    E 13-115844.600674  9858 (null): mdbm file open failure: Bad address
void
MdbmOpenUnitTest::test_mdbm_openA18()
{
    TRACE_TEST_CASE(__func__);
    MDBM *mdbm;
    CPPUNIT_ASSERT_EQUAL((int *)NULL, (int *)(mdbm=mdbm_open(NULL, MDBM_O_RDWR, 0644, 0, 0)));
//    CPPUNIT_ASSERT( (mdbm = mdbm_open(NULL, MDBM_O_RDWR, 0644, 0, 0)) == NULL);
}


//   mdbm_open() - Unit Test #A19, for MDBM version 2 and 3
//
//   Open pre-populated MDBM with the MDBM_HEADER_ONLY option.
//   Writes fail, as they should.  V3 does not allow data access with HEADER_ONLY.
//   The MDBM_HEADER_ONLY option is for internal use only: collect header statistics only.
void
MdbmOpenUnitTest::test_mdbm_openA19()
{
    TRACE_TEST_CASE(__func__);
    createData(0, PAGESZ_1K);
    MDBM * mdbm = mdbm_open(fileName.c_str(), MDBM_HEADER_ONLY, 0644, PAGESZ_1K, 0);
    CPPUNIT_ASSERT(mdbm != NULL);

    if (versionFlag & MDBM_CREATE_V3) {   // V3 won't let you access data, so:
        CPPUNIT_ASSERT_EQUAL(3U, mdbm_get_version(mdbm));  // see that you can get MDBM version
        mdbm_close(mdbm);
        return;
    }
    // Writing should fail, so it should not return a zero
    CPPUNIT_ASSERT_EQUAL(0, (int) !writeDatum(mdbm, STARTPOINT+1));
    mdbm_close(mdbm);
}


//   mdbm_open() - Unit Test #A20, for MDBM version 2 and 3
//
//   Open brand new MDBM with the MDBM_HEADER_ONLY option.
//   Writes fail, as they should.  V3 does not allow data access with HEADER_ONLY.
//   The MDBM_HEADER_ONLY option is for internal use only: collect header statistics only.
void
MdbmOpenUnitTest::test_mdbm_openA20()
{
    TRACE_TEST_CASE(__func__);
    CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
    int flags = MDBM_HEADER_ONLY | MDBM_O_CREAT | MDBM_O_RDWR | versionFlag;
    MDBM *mdbm = mdbm_open(fileName.c_str(), flags, 0644, BIGGER_PAGE_SIZE, 0);

    if (versionFlag & MDBM_CREATE_V3) {  // mdbm_open should fail under MDBM V3
        CPPUNIT_ASSERT(NULL == mdbm);
        return;
    }

    CPPUNIT_ASSERT(NULL != mdbm);
    CPPUNIT_ASSERT(0 == InsertData(mdbm));
    //CPPUNIT_ASSERT_EQUAL(0, populateMdbm(mdbm, NUM_DATA_ITEMS, STARTPOINT, PREFIX));

    //for (int intKey = STARTPOINT; intKey < STARTPOINT+NUM_DATA_ITEMS; ++intKey) {
    //    checkKeyValue(mdbm, intKey);
    //}
    CPPUNIT_ASSERT(0 == VerifyData(mdbm));

    mdbm_close(mdbm);
}


//   mdbm_open() - Unit Test #A21, for MDBM version 2 and 3
//
//  Open pre-populated MDBM with a different page size.
//  Call mdbm_get_page_size() to make sure pagesize did not change.
void
MdbmOpenUnitTest::test_mdbm_openA21()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(fileName);
    MDBM *mdbm;
    int size = (versionFlag&MDBM_CREATE_V3) ? TEST_PAGE_SMALL_V3 : BIGGER_PAGE_SIZE;

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_O_RDWR, 0644, size, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(PAGESZ, mdbm_get_page_size(mdbm));

    mdbm_close(mdbm);
}



int getGoodPageSize(int vflag, int size) {
    int goodSize = size;

    if (!size) { return MDBM_PAGESIZ; }
    if (vflag & MDBM_CREATE_V3) {
        if ((size > MAX_PAGE_V3) || (size < MDBM_MINPAGE)) {
            return -1;
        } else {
            goodSize += V3_ROUNDING_FACTOR;
            goodSize -= (goodSize % V3_ROUNDING_FACTOR);
        }
    } else { // V2
        if (size < MDBM_MINPAGE) {
            goodSize = MDBM_MINPAGE;
        } else if (size > MAX_PAGE_V2) {
            goodSize = MAX_PAGE_V2;
        } else {
            int j;
            for (j = MDBM_MIN_PSHIFT; 1<<j < size; ++j);
            goodSize = 1 << j;
         }
    }
    return goodSize;
}
// mdbm_open() - Unit Test #A22, for MDBM version 2 and 3
//
// Open brand new MDBM with invalid page sizes: too big (16MB+1, 32MB), too small
//  (1,64,100,127), zero (valid), negative. Document behavior:
//  Call mdbm_get_page_size() to get actual page size
//  If the pagesize parameter is set to zero, MDBM uses the default page size of 4K.
//  V2: Minimum page size is 256 (another document says 512). Maximum page size is 64K.
//    Pagesize is rounded to the next power of 2.
//  V3: Minimum page size is 128. Maximum page size is 16MB-64bytes. If pagesize < 128, actual
//    page size becomes 128. Pagesize is rounded to the next multiple of 64 bytes.
void
MdbmOpenUnitTest::test_mdbm_openA22()
{
    TRACE_TEST_CASE(__func__);
    MDBM *mdbm;
    int curSize, goodSize, flags = MDBM_O_RDWR | versionFlag | MDBM_O_CREAT;
    bool openShouldFail;
    const vector<int> &pageSizes = (versionFlag & MDBM_CREATE_V3) ? PageSizesV3 : PageSizesV2;

    for (u_int i=0; i<pageSizes.size(); ++i) {
        curSize = pageSizes[i];
        CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
        errno = 0;
        mdbm = mdbm_open(fileName.c_str(), flags, 0644, curSize, 0);
        goodSize = getGoodPageSize(versionFlag, curSize);
        openShouldFail = (goodSize<0) ? true : false;

        stringstream msg;
        msg << " open with pagesize=" << curSize
            << " shouldFail=" << openShouldFail ;
        CPPUNIT_ASSERT_MESSAGE(msg.str(), openShouldFail == (mdbm == NULL));
        if (mdbm) {
            int actual_page_size = mdbm_get_page_size(mdbm);
            msg << " actual-pagesize=" << actual_page_size;
            CPPUNIT_ASSERT_MESSAGE(msg.str(), goodSize == actual_page_size);
        }

        mdbm_close(mdbm);
        mdbm = 0;
    }
}


//  mdbm_open() - Unit Test #A23, for MDBM version 2 and 3
//  Open brand new MDBM with page sizes that are greater than presize (pre-allocated DB size)
//  parameter: should not cause an error, but initial size should be greater than pagesize.
//  Also test presize = MaximumPageSize (64K V2 and 16MB V3).
//
//  DB size seems to indicate that MDBM V2 allocates 1 page when presize is less than page size
//  DB size seems to indicate that MDBM V3 allocates 2 pages when presize is less than page size
void
MdbmOpenUnitTest::test_mdbm_openA23()
{
    TRACE_TEST_CASE(__func__);
    MDBM *mdbm;
    int dbsize, flags = MDBM_O_RDWR | MDBM_O_CREAT | versionFlag;
    int maxPage = (versionFlag & MDBM_CREATE_V3) ? MAX_PAGE_V3 : MAX_PAGE_V2;

    for (int sz=MDBM_MINPAGE; sz <= maxPage; sz += sz) {
        CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
        CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), flags, 0644, sz, sz-64)) != NULL);
        dbsize = mdbm_get_size(mdbm);
        CPPUNIT_ASSERT(dbsize >= sz);
        mdbm_close(mdbm);
    }

    // Test for presize=MaxPageSize
    CPPUNIT_ASSERT_EQUAL(0, unlink(fileName.c_str()));
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), flags, 0644, maxPage, maxPage)) != NULL);
    dbsize = mdbm_get_size(mdbm);
    CPPUNIT_ASSERT(dbsize >= maxPage);
    mdbm_close(mdbm);
}


// mdbm_open() - Unit Test #A24, for MDBM version 2 and 3
// Verify that after opening an MDBM with a protection file symbolic link
// (same behavior as MDBM_PROTECT) and setting a lock, data can be read and modified.
//
// Testcase also try to use a non-symbolic link in order to generate the following error:
// E 16-124018.218402  3836 opentest.mdbm.protect: WARNING: must be a symbolic link
void
MdbmOpenUnitTest::test_mdbm_openA24()
{
    TRACE_TEST_CASE(__func__);
    MDBM *mdbm;
    string cmd("touch ");

    CopyFile(fileName);
    cmd += fileName + ".protect";
    CPPUNIT_ASSERT_EQUAL(0, system(cmd.c_str()));  // touch
    // Should generate the warning message "Must be a symbolic link..."
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fileName.c_str(), MDBM_O_RDWR | versionFlag, 0644, PAGESZ, 0)) != NULL);
    mdbm_close(mdbm);
    cmd = string("rm ") + fileName + string(" ") + fileName + ".protect";
    CPPUNIT_ASSERT_EQUAL(0, system(cmd.c_str()));  // delete

    // Set protection file by creating a symbolic link
    cmd = string("ln -s /dev/null ") + fileName + string(".protect");
    CPPUNIT_ASSERT_EQUAL(0, system(cmd.c_str()));

    testProtectedLocked(MDBM_O_RDWR);

    cmd = string("rm ") + fileName + string(".protect");
    system(cmd.c_str());  // delete link
}


//  mdbm_open() - Unit Test #A25, for MDBM version 2 and 3
//  Verify that after opening an MDBM with a protection file symbolic link
//  (same behavior as MDBM_PROTECT) but without setting a lock, data access
//  causes a segmentation fault
void
MdbmOpenUnitTest::test_mdbm_openA25()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

    // Set protection file by creating a symbolic link
    string cmd = string("ln -s /dev/null ") + fileName + string(".protect");
    CPPUNIT_ASSERT_EQUAL(0, system(cmd.c_str()));

    testProtectionSegv(MDBM_O_RDWR);

    cmd = "rm " + fileName + string(" ") + fileName + ".protect";
    system(cmd.c_str());  // delete link
}


//  mdbm_open() - Unit Test #A26, for MDBM version 3 only
//  Verify that after opening an MDBM after MDBM_PROTECT_PATH_REGEX (same behavior as
//  MDBM_PROTECT) was set, and setting a lock, data can be read and modified
void
MdbmOpenUnitTest::test_mdbm_openA26()
{
    TRACE_TEST_CASE(__func__);
    string buf(fileName);
    buf += ".protect";
    unlink(buf.c_str());  // Make sure there's no .protect file

    setenv("MDBM_PROTECT_PATH_REGEX", "opent.*mdbm", 1);
    testProtectedLocked(MDBM_O_RDWR);
    unsetenv("MDBM_PROTECT_PATH_REGEX");
}


//  mdbm_open() - Unit Test #A27, for MDBM version 3 only
//  Verify that opening an MDBM after setting the MDBM_PROTECT_PATH_REGEX
//  environment variable and setting a lock, data access causes a segmentation fault.
void
MdbmOpenUnitTest::test_mdbm_openA27()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

    string buf =fileName + ".protect";
    unlink(buf.c_str());  // Make sure there's no .protect file

    setenv("MDBM_PROTECT_PATH_REGEX", "opentestv*", 1);
    testProtectionSegv(MDBM_O_RDWR);
    unsetenv("MDBM_PROTECT_PATH_REGEX");
    unlink(fileName.c_str());
}

//  mdbm_open() - Unit Test #A28, for MDBM version 3 only
//  Open brand new MDBM with MDBM_O_CREAT and MDBM_O_RDONLY flags. Verify mdbm_open() opens,
//  then all fetches return null values and writes fail.
void
MdbmOpenUnitTest::test_mdbm_openA28()
{
    const char *fname = fileName.c_str();
    unlink(fname);
    errno = 0;
    MDBM *mdbm = mdbm_open(fname, MDBM_O_RDONLY | MDBM_O_CREAT | versionFlag, 0644, PAGESZ, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    const char *fetched = mdbm_fetch_str(mdbm, "stuff");
    CPPUNIT_ASSERT(fetched == NULL);

    int retval = !(writeDatum(mdbm, STARTPOINT+1));
    CPPUNIT_ASSERT_EQUAL(0, retval);

    mdbm_close(mdbm);

}


//  mdbm_open() - Unit Test #A29, for MDBM version 2 and 3
//  Open existing MDBM with MDBM_O_CREAT and MDBM_O_RDONLY flags.
//  Verify mdbm_open() opens, then fetches succeed but writes fail.
void
MdbmOpenUnitTest::test_mdbm_openA29()
{
    testA29Helper(fileName, MDBM_O_RDONLY | MDBM_O_CREAT | versionFlag);
    unlink(fileName.c_str());
    string buf = fileName + string("_cp");
    unlink(buf.c_str());
}

void
MdbmOpenUnitTest::test_readonly_protection()
{
    const char *fname = fileName.c_str();
    unlink(fname);
    errno = 0;
    MDBM *db = mdbm_open(fname, MDBM_O_RDWR | MDBM_O_CREAT, 0644, PAGESZ, 0);
    CPPUNIT_ASSERT( db != NULL);

    mdbm_close(db);
    db = mdbm_open(fname, MDBM_O_RDONLY, 0644, PAGESZ, 0);

    {
      int status, retsig = 0;
      pid_t mypid = fork();
      if (mypid == 0) { // Child
          // make a pointer to the middle of the map
          char* p = db->db_base + (db->db_base_len/2);
          *p = 1;   // Should cause a SEGV
          mdbm_close(db);   // In case it doesn't SEGV
          exit(1);
      } else {  // Parent
          CPPUNIT_ASSERT(mypid >= 0);
          waitpid(mypid, &status, 0);
          if (WIFSIGNALED(status)) {
              retsig = WTERMSIG(status);
          }
      }

      CPPUNIT_ASSERT_EQUAL(SIGSEGV, retsig);
    }

    mdbm_close(db);
    unlink(fname);
}



// ------------------------------------
// Helper methods used by several tests
// ------------------------------------
//

void
MdbmOpenUnitTest::testA29Helper(const string &fname, int flags)
{
    CopyFile(fname);
    readBackData(flags);
    MDBM * mdbm = 0;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname.c_str(), MDBM_O_RDONLY, 0644, PAGESZ, 0)) != NULL);
    deleteFailed(mdbm, STARTPOINT+100);
    mdbm_close(mdbm);
}



void
MdbmOpenUnitTest::changeDatum(MDBM **m, int dataNum, int flags)
{
    const char *fname = fileName.c_str();
    int openFlags = MDBM_O_RDWR | flags | versionFlag;

    if (*m == NULL) {
        *m = mdbm_open(fname, openFlags, 0644, PAGESZ, 0);
        CPPUNIT_ASSERT( (*m)  != NULL);
    }

    string keyStr = GetDefaultKey(dataNum);
    char buf[32];
    const datum ky = { (char*)keyStr.data(), (int)keyStr.size() };
    memset(buf, '\0', sizeof(buf));
    strncpy(buf, MODIFIED_DATA, sizeof(buf)-1);
    const datum newValue = { buf, (int)strlen(buf) };
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store( *m, ky, newValue, MDBM_REPLACE));
}


int
MdbmOpenUnitTest::writeDatum(MDBM *m, int dataNum)
{
    char buf[32], buf2[32];
    snprintf(buf, sizeof(buf), "%s%d", PREFIX, dataNum);
    const datum ky = { buf, (int)strlen(buf) };
    strncpy(buf2, MODIFIED_DATA, sizeof(buf2)-1);
    buf2[sizeof(buf2) - 1] = '\0';
    const datum newValue = { buf2, (int)strlen(buf2) };
    return mdbm_store( m, ky, newValue, MDBM_REPLACE);
}


void
MdbmOpenUnitTest::createData(int align, int pagesz)
{
    int openFlags = MDBM_O_FSYNC | versionFlag;   // Make sure all data is on disk
    openFlags |= MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC;
    MDBM *mdbm = 0;

    CleanupTmpDirLocks();
    string dummy;
    GetTmpDir(dummy);

    unlink(fileName.c_str());
    // Create mdbm
    CPPUNIT_ASSERT((mdbm = EnsureNamedMdbm(fileName, openFlags, 0644, pagesz, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_alignment(mdbm, align));
    //CPPUNIT_ASSERT_EQUAL(0, populateMdbm(mdbm, NUM_DATA_ITEMS, STARTPOINT, PREFIX));
    CPPUNIT_ASSERT(0 == InsertData(mdbm));
    mdbm_close(mdbm);  // returns void, cannot test
}


void
MdbmOpenUnitTest::readBackData(int openFlags)
{
    const char *fname = fileName.c_str();
    MDBM* mdbm = 0;

    CPPUNIT_ASSERT((mdbm = mdbm_open(fname, openFlags|versionFlag, 0644, PAGESZ, 0)) != NULL);
    CPPUNIT_ASSERT(0 == VerifyData(mdbm));
    //for (int intKey = STARTPOINT; intKey < STARTPOINT+NUM_DATA_ITEMS; ++intKey) {
    //    checkKeyValue(mdbm, intKey);
    //}
    mdbm_close(mdbm);
}

u_int
MdbmOpenUnitTest::readBackModifiedData()
{
    const char *fname = fileName.c_str();
    MDBM* mdbm = 0;
    int openFlags = MDBM_O_RDWR | versionFlag;

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname, openFlags, 0644, PAGESZ, 0)) != NULL);
    checkKeyValue(mdbm, STARTPOINT);
    u_int unmodifiedCount = 0;
    int i = 1;
    for (; i <= 100; ++i) {
        unmodifiedCount += checkKeyValue(mdbm, STARTPOINT + i, MODIFIED_DATA );
    }
    for (int intKey = STARTPOINT+i; intKey < STARTPOINT+NUM_DATA_ITEMS; ++intKey) {
        checkKeyValue(mdbm, intKey);
    }

    mdbm_close(mdbm);
    return unmodifiedCount;
}

// If otherValue is specified, then the data on disk may be modified or not
// and checkKeyValue will return 1 (one) if the value on disk has not been
// modified, zero otherwise.

u_int
MdbmOpenUnitTest::checkKeyValue(MDBM * mdbm, int key, const char *otherValue)
{
    // Verify the the mdbm was stored correctly.
    // The the value should be the same as prefix+key.
    if (!otherValue) {
      return VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, 1, true, key);
    }

    string keyStr = GetDefaultKey(key);
    //char key1[32];
    datum fetched1;
    //snprintf(key1, sizeof(key1), "%s%d", PREFIX, key);
    //const datum k1 = { key1, strlen(key1) };
    const datum k1 = { (char*)keyStr.data(), (int)keyStr.size() };
    errno = 0;
    fetched1 = mdbm_fetch(mdbm, k1);
    CPPUNIT_ASSERT_EQUAL(0, errno);
    CPPUNIT_ASSERT(fetched1.dptr != NULL);
    if ( otherValue ) {
        int len = strlen(otherValue);
        if (len != fetched1.dsize)
            return 1;
        if (strncmp(fetched1.dptr, otherValue, fetched1.dsize) != 0)
            return 1;
    } else {
        CPPUNIT_ASSERT_EQUAL(k1.dsize, fetched1.dsize);
        CPPUNIT_ASSERT_EQUAL(0, strncmp(fetched1.dptr, k1.dptr, k1.dsize));
    }
    return 0;
}

void
MdbmOpenUnitTest::deleteFailed(MDBM *mdbm, int key)
{
    char key1[32];
    snprintf(key1, sizeof(key1), "%s%d", PREFIX, key);
    const datum k1 = { key1, (int)strlen(key1) };
    errno = 0;

    // Below should be failing and errno is set
    CPPUNIT_ASSERT( mdbm_delete(mdbm, k1) != 0 );

    CPPUNIT_ASSERT_EQUAL(1, errno);  // Errno equals 1
}

int
MdbmOpenUnitTest::storeLargeObj(MDBM *db, int xobjNum, int *sizePtr)
{
    char kbuf[32], buf[256];
    string largeObj;
    datum k, dta;

    snprintf(kbuf, sizeof(kbuf), "%s%d", LARGE_OBJ_KEY, xobjNum);

    k.dptr = kbuf;
    k.dsize = strlen(kbuf);

    for (int i = 0; i < NUM_DATA_ITEMS*10; ++i ) {
        snprintf(buf, sizeof(buf),
                 "%s%s%d", PREFIX, PREFIX, STARTPOINT + i);
        largeObj += buf;

    }
    if (sizePtr)
        *sizePtr = largeObj.size();
    dta.dptr  = (char *) largeObj.c_str();
    dta.dsize = largeObj.size();

    return mdbm_store(db, k, dta, MDBM_REPLACE);
}


void
MdbmOpenUnitTest::getLargeObj(MDBM * mdbm, int key, int size)
{
    char key1[32];
    datum fetched1;
    snprintf(key1, sizeof(key1), "%s%d", LARGE_OBJ_KEY, key);
    const datum k1 = { key1, (int)strlen(key1) };
    errno = 0;
    fetched1 = mdbm_fetch(mdbm, k1);
    CPPUNIT_ASSERT_EQUAL(0, errno);
    CPPUNIT_ASSERT(fetched1.dptr != NULL);

    if (size)
        CPPUNIT_ASSERT_EQUAL(size, fetched1.dsize);
}

#ifdef HAVE_WINDOWED_MODE
int
MdbmOpenUnitTest::remapIsLimited()
{
    long sys_pagesize = sysconf(_SC_PAGESIZE);
    int ret, fd = -1;
    uint8_t* map = NULL;
    uint32_t size = sys_pagesize + sys_pagesize; // 2 pages needed in total
    char fnbuf[MAXPATHLEN];  // File name buffer

    // create test file

    if ((fd = open_tmp_test_file("/tmp/testlimited", size, fnbuf)) < 0) {
        return -1;
    }

    map = (uint8_t*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        close(fd);
        unlink(fnbuf);
        return -2;
    }

    //int remap_file_pages(void *addr, size_t size, int prot, ssize_t pgoff, int flags);
    // map 2 pages first
    if (remap_file_pages((void*)map, size, 0, 0, 0) != 0) {
        munmap(map, size);
        close(fd);
        unlink(fnbuf);
        return -3;
    }

    // map 2nd file-page to offset 0
    if (remap_file_pages((void*)map, sys_pagesize, 0, 1, 0) != 0) {
        munmap(map, size);
        close(fd);
        unlink(fnbuf);
        return -4;
    }

    // Now try remapping both pages again - fails under RHEL6 but not RHEL4
    ret = remap_file_pages((void*)map, size, 0, 0, 0);

    if (ret != 0) {   // Success means ret=0 (RHEL4) - remap_file_pages is not limited
        ret = 1;  // RHEL6 - remap_file_pages is limited
    }

    munmap(map, size);
    close(fd);
    unlink(fnbuf);
    return ret;
}
#else
int
MdbmOpenUnitTest::remapIsLimited()
{
    return 0;
}
#endif


// For use in threaded test code
typedef struct ThreadInfo {
    pthread_t threadId;
    int threadNum;
    class MdbmOpenUnitTestV3 *thisPtr;
    string file;
} ThreadInfo;

class MdbmOpenUnitTestV3 : public MdbmOpenUnitTest
{
    CPPUNIT_TEST_SUITE(MdbmOpenUnitTestV3);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(test_mdbm_openA1);
      CPPUNIT_TEST(test_mdbm_openA2);
      CPPUNIT_TEST(test_mdbm_openA3);
      CPPUNIT_TEST(test_mdbm_openA5);
      CPPUNIT_TEST(test_mdbm_openA6);
      CPPUNIT_TEST(test_mdbm_openA7);
      CPPUNIT_TEST(test_mdbm_openA8);
      CPPUNIT_TEST(test_mdbm_openA9);
      CPPUNIT_TEST(test_mdbm_openA10);
      CPPUNIT_TEST(test_mdbm_openA11);
      CPPUNIT_TEST(test_mdbm_openA12);
      CPPUNIT_TEST(test_mdbm_openA13);   // Test A13 (only V3)
      CPPUNIT_TEST(test_mdbm_openA14);
      CPPUNIT_TEST(test_mdbm_openA15);
      CPPUNIT_TEST(test_mdbm_openA17);
      CPPUNIT_TEST(test_mdbm_openA18);
      CPPUNIT_TEST(test_mdbm_openA19);
      CPPUNIT_TEST(test_mdbm_openA20);
      CPPUNIT_TEST(test_mdbm_openA21);
      CPPUNIT_TEST(test_mdbm_openA22);
      CPPUNIT_TEST(test_mdbm_openA23);
      CPPUNIT_TEST(test_mdbm_openA24);
#ifndef __MACH__
// TODO: dig into platform differences. Suspect different signals generated (SIGBUS)
      CPPUNIT_TEST(test_mdbm_openA16);
      CPPUNIT_TEST(test_mdbm_openA25);
      CPPUNIT_TEST(test_mdbm_openA26);   // Test A26 (V3 only)
      CPPUNIT_TEST(test_mdbm_openA27);   // Test A27 (V3 only)
      CPPUNIT_TEST(test_mdbm_openA28);   // Test A28 (V3 only)
      CPPUNIT_TEST(test_mdbm_openA29);   // Test A29
      CPPUNIT_TEST(test_readonly_protection);
#endif
#ifdef HAVE_WINDOWED_MODE
      CPPUNIT_TEST(test_mdbm_openWindowedModeThreaded);  // V3 only - V2 doesn't support threads
      CPPUNIT_TEST(test_mdbm_remapIsLimited);
      CPPUNIT_TEST(test_mdbm_openWindowedMode);
      CPPUNIT_TEST(test_mdbm_openWindowedLarge);
#endif
      CPPUNIT_TEST(test_mdbm_open3Processes);
    CPPUNIT_TEST_SUITE_END();

  public:
    MdbmOpenUnitTestV3() : MdbmOpenUnitTest(MDBM_CREATE_V3) {}
    void test_mdbm_remapIsLimited();  // New Windowing test for RHEL6
    void test_mdbm_openWindowedMode();
    void test_mdbm_openWindowedLarge();
    void test_mdbm_openWindowedModeThreaded();
    void test_mdbm_open3Processes();

  protected:
    static void *threadWorkerFunc(void* obj);
    void *OpenMdbmInThread(ThreadInfo *info);
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmOpenUnitTestV3);

// New Windowing unit test: on RHEL6, Windowing should be disabled
//  This unit test tests the MDBM function: remap_is_limited()
void
MdbmOpenUnitTestV3::test_mdbm_remapIsLimited()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

    int remapLimitedStatus = remapIsLimited();
    CPPUNIT_ASSERT(remapLimitedStatus >= 0);
    CPPUNIT_ASSERT_EQUAL(remapLimitedStatus, remap_is_limited(sysconf(_SC_PAGESIZE)));
}

// New Windowing unit test: on RHEL6, Make sure Windowing is disabled
//  This unit test tests the MDBM function: mdbm_open(...,flags|MDBM_OPEN_WINDOWED,...)
//  On RHEL6, you should see the following log message:
// E 19-184920.693005 21922 /tmp/mdbm/kislikm-21922/mdbm-00001OpenWindowed: OS does not support windowed mode: Success

void
MdbmOpenUnitTestV3::test_mdbm_openWindowedMode()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

    int remapLimitedStatus = remapIsLimited();
    CPPUNIT_ASSERT(remapLimitedStatus >= 0);

    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | MDBM_OPEN_WINDOWED | versionFlag;
    string fname(GetTmpName(string("OpenWindowed")));
    MdbmHolder mdbm(fname);
    long pgsize = sysconf(_SC_PAGESIZE);
    int ret = mdbm.Open(openFlags, 0644, pgsize, 0);

    //if (remapLimitedStatus == 0) {  // RHEL4: Should work
        string msg("test_mdbm_openWindowedMode Open Failed");
        CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
        msg = "test_mdbm_openWindowedMode Set windowsize Failed";
        ret = mdbm_set_window_size(mdbm, pgsize * 8);
        CPPUNIT_ASSERT_MESSAGE(msg, ret == 0);
        msg = "test_mdbm_openWindowedMode InsertData Failed";
        ret = InsertData(mdbm);
        CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
        msg = "test_mdbm_openWindowedMode VerifyData Failed";
        ret = VerifyData(mdbm);
        CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
    //} else {   // RHEL6 - should fail
    //    CPPUNIT_ASSERT_EQUAL(ret, -1);
    //}
}

void
MdbmOpenUnitTestV3::test_mdbm_openWindowedLarge()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

//    int remapLimitedStatus = remapIsLimited();
//    CPPUNIT_ASSERT(remapLimitedStatus >= 0);

    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | MDBM_OPEN_WINDOWED | versionFlag | MDBM_DBSIZE_MB;
    string fname(GetTmpName(string("OpenWinLarge")));
    MdbmHolder mdbm(fname);
    long pgsize = 131072;   // 128K
    int presize = (sizeof(int) == 4) ? 2047 : 5859;
    int ret = mdbm.Open(openFlags, 0644, pgsize, presize);   // presize to 2047MB/5859MB

//    if (remapLimitedStatus == 0) {  // RHEL4: Should work
        string msg("test_mdbm_openWindowedLarge Open Failed");
        CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
        msg = "test_mdbm_openWindowedLarge Set windowsize Failed";
        ret = mdbm_set_window_size(mdbm, pgsize * 32);
        CPPUNIT_ASSERT_MESSAGE(msg, ret == 0);
        msg = "test_mdbm_openWindowedLarge InsertData Failed";
        ret = InsertData(mdbm);
        CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
        msg = "test_mdbm_openWindowedLarge VerifyData Failed";
        ret = VerifyData(mdbm);
        CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
//    } else {
//        CPPUNIT_ASSERT_EQUAL(ret, -1);
//    }
}

void*
MdbmOpenUnitTestV3::threadWorkerFunc(void* data) {
  CPPUNIT_ASSERT(data != NULL);
  ThreadInfo *info = ((ThreadInfo*) data);
  return info->thisPtr->OpenMdbmInThread(info);
}

void*
MdbmOpenUnitTestV3::OpenMdbmInThread(ThreadInfo *info)
{
    //bool windowed = false;
    int flags = MDBM_O_RDWR | versionFlag;
    int remapLimitedStatus = remapIsLimited();
    CPPUNIT_ASSERT(remapLimitedStatus >= 0);

    if ((info->threadNum % 2) == 0) {  // Test windowing every other time
        flags |= MDBM_OPEN_WINDOWED;
        //windowed = true;
    }

    MDBM *db = mdbm_open(info->file.c_str(), flags, 0644, 0, 0);
    //if (windowed && (remapLimitedStatus == 1)) {  // RHEL6 and windowed
    //    CPPUNIT_ASSERT(db == NULL);
    //} else {  // RHEL4 or not windowed
        CPPUNIT_ASSERT(db != NULL);
        mdbm_close(db);
    //}

    return NULL;
}


void
MdbmOpenUnitTestV3::test_mdbm_openWindowedModeThreaded()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_VALGRIND()

    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | versionFlag;
    string fname(GetTmpName(string("OpenWinThread")));
    MdbmHolder mdbm(fname);
    long pgsize = sysconf(_SC_PAGESIZE);
    int ret = mdbm.Open(openFlags, 0644, pgsize, 0);
    string msg("test_mdbm_openWindowedModeThreaded Open Failed");
    CPPUNIT_ASSERT_MESSAGE(msg, ret != -1);
    CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, DEFAULT_KEY_SIZE));
    mdbm.Close();

    const int MAX_THREADS = 200;
    ThreadInfo thrInfo[MAX_THREADS];
    pthread_attr_t attr;
    CPPUNIT_ASSERT_EQUAL(0, pthread_attr_init(&attr));

    for (int i = 0; i < MAX_THREADS; ++i ) {
        thrInfo[i].thisPtr = this;
        thrInfo[i].threadNum = i;
        thrInfo[i].file = fname;
        ret = pthread_create(&thrInfo[i].threadId, &attr, &threadWorkerFunc, &thrInfo[i]);
        msg = string("Unable to spawn thread ") + ToStr(i);
        CPPUNIT_ASSERT_MESSAGE(msg, ret == 0);
    }

    void *valuePtr;
    for (int i = 0; i < MAX_THREADS; ++i ) {
        ret = pthread_join(thrInfo[i].threadId, &valuePtr);
        if (ret != 0) {  // Do not use CPPUNIT_ASSERT: try to join with the rest
            fprintf(stderr, "\nError: Unable to join with thread #%d, thread_id=%ld\n",
                    thrInfo[i].threadNum, (long)thrInfo[i].threadId);
        }
    }

    pthread_attr_destroy(&attr);
}

// 3 processes open the same MDBM, then 2 perform writes (parent+child), and one performs only
// reads.  Adding new multi-process test for mdbm_open.
void
MdbmOpenUnitTestV3::test_mdbm_open3Processes()
{
    string fname, prefix("MdbmOpenUnitTestV3::open3Processes");
    int pgsize = 4096;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR, 0644, pgsize, 0, &fname));
    mdbm.Close();

    int status;
    pid_t mypid = fork();

    if (mypid == 0) { // Child#1 - do reads
        MdbmHolder mdbm2(mdbm_open(fname.c_str(), MDBM_O_RDONLY, 0644, 0, 0));
        CPPUNIT_ASSERT( NULL != (MDBM*)mdbm2);

        CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm2));
        CPPUNIT_ASSERT_EQUAL(0,
            VerifyData(mdbm2, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
        CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm2));

        usleep(1000);  // Sleep, to be last to exit
        exit(0);
    } else {  // Parent
        if (mypid < 0) {
            cerr << "Could not get child#1 process PID" << endl;
            exit(2);
        }
        // Open and do some more writes
        MdbmHolder mymdbm(mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, 0, 0));
        CPPUNIT_ASSERT( NULL != (MDBM*)mymdbm);
        CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mymdbm));
        CPPUNIT_ASSERT_EQUAL(0, InsertData(mymdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE/2,
                                           DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT));
        CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mymdbm));

        // Fork again
        int newstatus;
        pid_t mynewpid = fork();
        if (mynewpid == 0) { // Child#2 - do writes
            MdbmHolder mdbm3(mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, 0, 0));
            CPPUNIT_ASSERT( NULL != (MDBM*)mdbm3);
            CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm3));
            CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm3, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE/2,
                                               DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT));
            CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm3));
            exit(0);
        } else {
            if (mynewpid < 0) {
                cerr << "Could not get child#2 process PID" << endl;
                exit(3);
            }
            waitpid(mynewpid, &newstatus, 0);
        }

        waitpid(mypid, &status, 0);
    }
}


