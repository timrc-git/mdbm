/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: MDBM unit tests of mdbm_store*

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include <string>
#include <iostream>
#include <sstream>
#include <vector>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"

//#include "test_common.h"
#include "TestBase.hh"

// Constants for small-object tests and simple large object tests

static const int PAGESZ_TSML  = 8192;   // For small object tests
static const int PAGESZ_TLRG  = 1024;   // For large Object duplicate/missing key tests

static const int NUM_DATA_ITEMS = 80;
//static const int MAX_NEG_TEST    = 7;   // For negative tests
static const int MAX_EASY_TEST   = 15;  // For large Object easy/negative tests

static const int SMALL_OBJ_SIZE_LARGE_OBJ_TEST = 50;  // Small objects for 16MB tests

static const char *PREFIX  = "SPREFIX";             // Small object Key prefix
static const char *SIMPLE_KEY_PREFIX  = "STkey";      // Large object key prefix
static const char *SOME_TEST_VALUE  = "More Stuff";     // Small test object

static const size_t smallDataSizes[] = {24, 65, 96, 280, 1024, 1500 };
static const size_t dataSizeArrayLen = sizeof(smallDataSizes) / sizeof(size_t);

using namespace std;


// constants for large-object tests involving the following specifications:
// When fetching large objects of various value sizes:
// 1. Use various pages sizes
// 2. Ensure that some objects have values that are:
//    * Slightly smaller than the page size (ex., by 12 bytes)
//    * Exactly the page size
//    * Slightly larger than the page size
//    * Much larger than the page size (1000 bytes)
//    * Maximal case: for example, 16MB page size and 10x that page size for at least 1 large object

static const int MEGBT = 1024 * 1024;
static const int LARGEST_V2_PAGE = 65536;
static const int LARGEST_V3_PAGE = 1 << 24;

// mdbm_open fails to open when page size = exactly 16MB. Only 16MB-64bytes is OK
static const int OFFSET_TO_LARGEST       = 64;

static const int SLIGHTLY_SMALLER_DELTA  = -12;   // Slightly smaller than the page size
static const int SLIGHTLY_LARGER_DELTA   = 20;    // Slightly larger than the page size
static const int MUCH_LARGER_DATA   = 2000;      // Much larger than the page size

static const int additionalDataSizes[4] = { SLIGHTLY_SMALLER_DELTA, 0,
                                            SLIGHTLY_LARGER_DELTA, MUCH_LARGER_DATA };

static const int MAX_NUM_LDATA   = 10;  // For large Objects tests - must be 10 or less

static const int v2LargeDataPageSizes[] = {256, 2048, 8192, 16384, LARGEST_V2_PAGE };
static const int v2LargePageSizeArrayLen = sizeof(v2LargeDataPageSizes) / sizeof(int);
static const vector<int> LargeDataPageSizesVectV2(v2LargeDataPageSizes,
    v2LargeDataPageSizes+v2LargePageSizeArrayLen);

//static const int v3LargeDataPageSizes[] = {448, 18176, 65536, MEGBT, LARGEST_V3_PAGE / 8, LARGEST_V3_PAGE - OFFSET_TO_LARGEST};
static const int largestPageSize = IS_32BIT() ? LARGEST_V3_PAGE/2 : LARGEST_V3_PAGE-OFFSET_TO_LARGEST;
static const int v3LargeDataPageSizes[] = {448, 18176, 65536, MEGBT, LARGEST_V3_PAGE / 8, largestPageSize};
static const int v3LargePageSizeArrayLen = sizeof(v3LargeDataPageSizes) / sizeof(int);
static const vector<int> LargeDataPageSizesVectV3(v3LargeDataPageSizes,
    v3LargeDataPageSizes+v3LargePageSizeArrayLen);

// page size range ; used for testing memory only DB
static const int PageSizeRange[] = { 256, 2048, 4096, 16384, 65536 };


enum {
    SMALL_OBJ_TESTS = 0
};

typedef enum {
    DO_STORE = 0,
    DO_STORE_R = 1,
    DO_STORE_STR = 2
} StoreType;

struct StoreTestData {
    string basePath;
    string fileName;
    string fileNameLarge;   // Large Object duplicate/missing key tests
    vector<string> largeFiles;

    // variables to store data items and number of keys
    map<int, vector <string> > dataItems;  // for registerDatum
    map<int, vector <int> > howManyKeys;   // for registerDatum

    MDBM_ITER mIter;
};


class MdbmUnitTestStore : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestStore(int vflag, StoreTestData& dat) : TestBase(vflag, "MdbmUnitTestStore"), data(dat) {}

    void setUp();
    void tearDown() {}
    void test_StoreQ1();   // Test Q1
    void test_StoreQ2();   // Test Q2
    void test_StoreQ3();   // Test Q3
    void test_StoreQ4();   // Test Q4
    void test_StoreQ5();   // Test Q5
    void test_StoreQ6();   // Test Q6
    void test_StoreQ7();   // Test Q7
    void test_StoreQ8();   // Test Q8
    void test_StoreQ9();   // Test Q9
    void test_StoreQ10();   // Test Q10
    void test_StoreQ11();   // Test Q11
    void test_StoreQ12();   // Test Q12
    void test_StoreQ13();   // Test Q13
    void test_StoreQ14();   // Test Q14
    void test_StoreQ15();   // Test Q15
    void test_StoreR1();   // Test R1
    void test_StoreR2();   // Test R2
    void test_StoreR3();   // Test R3
    void test_StoreR4();   // Test R4
    void test_StoreR5();   // Test R5
    void test_StoreR6();   // Test R6
    void test_StoreR7();   // Test R7
    void test_StoreR8();   // Test R8
    void test_StoreR9();   // Test R9
    void test_StoreR10();   // Test R10
    void test_StoreR11();   // Test R11
    void test_StoreR12();   // Test R12
    void test_StoreR13();   // Test R13
    void test_StoreR14();   // Test R14
    void test_StoreR15();   // Test R15
    void test_StoreR16();   // Test R16
    void test_StoreR17();   // Test R17
    void test_StoreS1();   // Test S1
    void test_StoreS2();   // Test S2
    void test_StoreS3();   // Test S3
    void test_StoreS4();   // Test S4
    void test_StoreS5();   // Test S5
    void test_StoreS6();   // Test S6
    void test_StoreS7();   // Test S7
    void test_StoreS8();   // Test S8
    void test_StoreS9();   // Test S9
    void test_StoreS10();  // Test S10
    void test_StoreS11();   // Test S11
    void test_StoreS12();   // Test S12
    void test_StoreS13();   // Test S13
    void test_StoreS14();   // Test S14
    void test_StoreS15();   // Test S15
    void test_StoreT1();   // Test T1
    void test_StoreT2();   // Test T2
    void test_StoreT3();   // Test T3
    void test_StoreT4();   // Test T4
    void test_StoreDupesOverflowPage();   // Store too many duplicates

    void test_StoreChurnLob();
    void test_StoreChurnOversize();

    void initialSetup();
    void createFileNames(const string &basename);
    void finalCleanup();


    // Helper functions
    const vector<int>& GetLargeDataPageSizes();
    void  createSmallData(const string &fname, int pageszLrg,
                          StoreType storeType, int mode, int openFlags = 0);
    void storeTestData(MDBM *mdbm, int id, const string& testName, StoreType storeType, int mode);
    void checkData(const string &filename, const string& testName);
    void storeOrCheckData(MDBM *mdbm, const string& testName, bool store,
                          StoreType storeType, int mode, int id = 0);

    void testDuplicateInsert(const string &fname, StoreType storeType, char *data = NULL);
    int  doStore(MDBM *mdbm, datum key, datum val, int mode, StoreType storeType);
    void replaceExisting(const string &fname, const string& testName, StoreType storeType,
                         int mode = MDBM_REPLACE, bool useLonger = false);
    void doSmallDuplicateTest(const string &fname, StoreType storeType);
    datum fetchData(MDBM *mdbm, datum *keyPtr);
    void createLargeObjectMdbm(MDBM *mdbm, StoreType storeType = DO_STORE, int mode = MDBM_REPLACE);
    void addLargeDuplicates(const string &fname, StoreType storeType);
    void fetchLargeDups(const string &filename);
    void createBigDataV3(int mode, StoreType storeType);
    void createBigTestFiles(int openFlags, vector<string> &fnames, const vector<int>& pageSizes,
                            int mode, StoreType storeType);
    void compareLargeObjFiles(vector<string> &fnames, const vector<int>& pageSizes);
    void compareLargeData(MDBM *mdbm, datum *keyPtr, const datum &compareTo);
    void compareXLfile(bool useStoreStr);
    void replaceLargeObjects(vector<string> &fnames, const vector<int>& pageSizes,
                             StoreType storeType, int mode);
    void replaceTestXLobj(StoreType storeType, int mode);
    // test replacing large to small and vice versa
    void testLargeToSmallReplace(const string &fname, StoreType storeType);
    void testNonExistentStore(const string &fname, StoreType storeType);
    void modifyLargeObjects(vector<string> &fnames, const vector<int>& pageSizes, StoreType storeType);
    void modifyTestXLobj(StoreType storeType);
    void reserveAndStore(const string &fname, int openFlags);
    void reserveAndStoreLarge(const string &fname, int openFlags);

    StoreTestData &data;

    // TODO FIXME find something better to do with these
    void RegisterDatum(int id, const char *value, int numKeys);
    // Returns "how many" vector of integers
    vector<int> GetRegisteredData(int id, std::vector<std::string> & items);

};

void
MdbmUnitTestStore::RegisterDatum(int id, const char *value, int numKeys)
{
    data.dataItems[id].push_back(string(value));
    data.howManyKeys[id].push_back(numKeys);
}

vector<int>
MdbmUnitTestStore::GetRegisteredData(int id, std::vector<std::string> & items)
{
    vector<int> emptyVec;
    map<int, vector <string> >::iterator dataVecIt;
    map<int, vector <int> >::iterator numVecIt;

    if ((dataVecIt = data.dataItems.find(id)) == data.dataItems.end())
        return emptyVec;

    if ((numVecIt = data.howManyKeys.find(id)) == data.howManyKeys.end())
        return emptyVec;

    items = dataVecIt->second;
    return numVecIt->second;
}



void MdbmUnitTestStore::setUp()
{
    //createFileNames("store");
}

void
MdbmUnitTestStore::initialSetup()
{
    datum ky, val;
    createFileNames("store");
    MDBM_ITER_INIT(&data.mIter);

    for (size_t i = 0; i < dataSizeArrayLen ; ++i) {   // create test data
        ky = CreateTestValue("", smallDataSizes[i], val);
        RegisterDatum(SMALL_OBJ_TESTS, val.dptr, NUM_DATA_ITEMS+i);
    }
}

void
MdbmUnitTestStore::finalCleanup()
{
  CleanupTmpDir();
}

void
MdbmUnitTestStore::test_StoreQ1()
{
    TRACE_TEST_CASE(__func__);
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE, MDBM_INSERT, versionFlag);
    checkData(data.fileName, "Q1"+versionString);
}

void
MdbmUnitTestStore::test_StoreQ2()
{
    TRACE_TEST_CASE(__func__);
    testDuplicateInsert(data.fileName, DO_STORE);
}

void
MdbmUnitTestStore::test_StoreQ3()
{
    TRACE_TEST_CASE(__func__);
    const vector<int> &pageSizes = GetLargeDataPageSizes();

    createBigDataV3(MDBM_INSERT, DO_STORE);
    compareLargeObjFiles(data.largeFiles, pageSizes);
}


void
MdbmUnitTestStore::test_StoreQ4()
{
    TRACE_TEST_CASE(__func__);
    int openFlags = MDBM_LARGE_OBJECTS | versionFlag;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0);
    createLargeObjectMdbm(mdbm);

    datum key, value;
    key = CreateTestValue("", PAGESZ_TLRG + 15, value);
    testDuplicateInsert(data.fileNameLarge, DO_STORE, value.dptr);
    //mdbm_close(mdbm);
}


void
MdbmUnitTestStore::test_StoreQ5()
{
    TRACE_TEST_CASE(__func__);
    // Test creating brand new objects
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE, MDBM_REPLACE, versionFlag);
    checkData(data.fileName, "Q5"+versionString);
    replaceExisting(data.fileName, "Q5"+versionString, DO_STORE);   // Test replacing existing objs
}



void
MdbmUnitTestStore::test_StoreQ6()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST_VALGRIND()

    const vector<int> &pageSizes = GetLargeDataPageSizes();

    createBigDataV3(MDBM_REPLACE, DO_STORE);
    compareLargeObjFiles(data.largeFiles, pageSizes);
    compareXLfile(false);
    replaceTestXLobj(DO_STORE, MDBM_REPLACE);

    replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE, MDBM_REPLACE);
}


void
MdbmUnitTestStore::test_StoreQ7()
{
    TRACE_TEST_CASE(__func__);
    testLargeToSmallReplace(data.fileNameLarge, DO_STORE);    // Uses previously made file
}

void
MdbmUnitTestStore::test_StoreQ8()
{
    TRACE_TEST_CASE(__func__);
    // Insert initial data with INSERT_DUP
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE, MDBM_INSERT_DUP, versionFlag);
    checkData(data.fileName, "Q8"+versionString);
    doSmallDuplicateTest(data.fileName, DO_STORE);   // Insert duplicates
}

void
MdbmUnitTestStore::test_StoreQ9()
{
    TRACE_TEST_CASE(__func__);
    int openFlags = MDBM_LARGE_OBJECTS | versionFlag;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0);
    createLargeObjectMdbm(mdbm, DO_STORE, MDBM_INSERT_DUP);
    addLargeDuplicates(data.fileNameLarge, DO_STORE);
    fetchLargeDups(data.fileNameLarge);
}


void
MdbmUnitTestStore::test_StoreQ10()
{
    TRACE_TEST_CASE(__func__);
    // Create data file to modify later
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE, MDBM_INSERT, versionFlag);
    replaceExisting(data.fileName, "Q10"+versionString, DO_STORE, MDBM_MODIFY);
}


void
MdbmUnitTestStore::test_StoreQ11()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentStore(data.fileName, DO_STORE);
}

void
MdbmUnitTestStore::test_StoreQ12()
{
    TRACE_TEST_CASE(__func__);
    replaceExisting(data.fileName, "Q12"+versionString, DO_STORE, MDBM_MODIFY, true);
}

void
MdbmUnitTestStore::test_StoreQ13()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST_VALGRIND()

    const vector<int> &pageSizes = GetLargeDataPageSizes();
    createBigDataV3(MDBM_REPLACE, DO_STORE);
    modifyLargeObjects(data.largeFiles, pageSizes, DO_STORE);
    modifyTestXLobj(DO_STORE);
}


void
MdbmUnitTestStore::test_StoreQ14()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentStore(data.fileNameLarge, DO_STORE);
}


void
MdbmUnitTestStore::test_StoreQ15()
{
    TRACE_TEST_CASE(__func__);
    // const vector<int> &pageSizes = GetLargeDataPageSizes();

    // // Create large-object test files: mode=REPLACE is not important for this test
    // createBigTestFiles(MDBM_LARGE_OBJECTS|versionFlag, data.largeFiles, pageSizes, MDBM_REPLACE, DO_STORE);
    // replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE, MDBM_MODIFY);

    createBigDataV3(MDBM_REPLACE, DO_STORE);
    replaceTestXLobj(DO_STORE, MDBM_MODIFY);
    replaceLargeObjects(data.largeFiles, LargeDataPageSizesVectV3, DO_STORE, MDBM_MODIFY);
}

// mdbm_store_r() tests
void
MdbmUnitTestStore::test_StoreR1()
{
    TRACE_TEST_CASE(__func__);
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_R, MDBM_INSERT, versionFlag);
    checkData(data.fileName, "R1"+versionString);
}


void
MdbmUnitTestStore::test_StoreR2()
{
    TRACE_TEST_CASE(__func__);
    testDuplicateInsert(data.fileName, DO_STORE_R);
}


void
MdbmUnitTestStore::test_StoreR3()
{
    TRACE_TEST_CASE(__func__);
    const vector<int> &pageSizes = GetLargeDataPageSizes();
    createBigDataV3(MDBM_INSERT, DO_STORE_R);
    compareLargeObjFiles(data.largeFiles, pageSizes);
    compareXLfile(false);
}


void
MdbmUnitTestStore::test_StoreR4()
{
    TRACE_TEST_CASE(__func__);
    int openFlags = MDBM_LARGE_OBJECTS| versionFlag;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0);
    createLargeObjectMdbm(mdbm);

    datum key, value;
    key = CreateTestValue("", PAGESZ_TLRG + 15, value);
    testDuplicateInsert(data.fileNameLarge, DO_STORE_R, value.dptr);
}


void
MdbmUnitTestStore::test_StoreR5()
{
    // Test creating brand new objects
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_R, MDBM_REPLACE, versionFlag);
    checkData(data.fileName, "R5"+versionString);
    replaceExisting(data.fileName, "R5"+versionString, DO_STORE_R);   // Test replacing existing objs
}


void
MdbmUnitTestStore::test_StoreR6()
{
    TRACE_TEST_CASE(__func__);
    const vector<int> &pageSizes = GetLargeDataPageSizes();
    createBigDataV3(MDBM_REPLACE, DO_STORE_R);
    compareLargeObjFiles(data.largeFiles, pageSizes);
    compareXLfile(false);
    replaceTestXLobj(DO_STORE_R, MDBM_REPLACE);

    replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE_R, MDBM_REPLACE);
}

void
MdbmUnitTestStore::test_StoreR7()
{
    TRACE_TEST_CASE(__func__);
    testLargeToSmallReplace(data.fileNameLarge, DO_STORE_R);    // Uses previously made file
}

void
MdbmUnitTestStore::test_StoreR8()
{
    TRACE_TEST_CASE(__func__);
    // Insert initial data with INSERT_DUP
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_R, MDBM_INSERT_DUP, versionFlag);
    checkData(data.fileName, "R8"+versionString);
    doSmallDuplicateTest(data.fileName, DO_STORE_R);   // Insert duplicates
}

void
MdbmUnitTestStore::test_StoreR9()
{
    TRACE_TEST_CASE(__func__);
    int openFlags = MDBM_LARGE_OBJECTS | versionFlag;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0);
    createLargeObjectMdbm(mdbm, DO_STORE_R, MDBM_INSERT_DUP);
    addLargeDuplicates(data.fileNameLarge, DO_STORE_R);
    fetchLargeDups(data.fileNameLarge);
}


void
MdbmUnitTestStore::test_StoreR10()
{
    TRACE_TEST_CASE(__func__);
    // Create data file to modify later
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_R, MDBM_INSERT, versionFlag);
    replaceExisting(data.fileName, "R10"+versionString, DO_STORE_R, MDBM_MODIFY);
}


void
MdbmUnitTestStore::test_StoreR11()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentStore(data.fileName, DO_STORE_R);
}

void
MdbmUnitTestStore::test_StoreR12()
{
    TRACE_TEST_CASE(__func__);
    replaceExisting(data.fileName, "R12"+versionString, DO_STORE_R, MDBM_MODIFY, true);
}

void
MdbmUnitTestStore::test_StoreR13()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST_VALGRIND()

    const vector<int> &pageSizes = GetLargeDataPageSizes();
    createBigDataV3(MDBM_REPLACE, DO_STORE_R);
    modifyLargeObjects(data.largeFiles, pageSizes, DO_STORE_R);
    modifyTestXLobj(DO_STORE_R);
}

void
MdbmUnitTestStore::test_StoreR14()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentStore(data.fileNameLarge, DO_STORE_R);
}

void
MdbmUnitTestStore::test_StoreR15()
{
    TRACE_TEST_CASE(__func__);
    const vector<int> &pageSizes = GetLargeDataPageSizes();
    // // Create large-object test files: mode=REPLACE is not important for this test
    // int openFlags = MDBM_LARGE_OBJECTS | versionFlag;
    // createBigTestFiles(openFlags, data.largeFiles, pageSizes, MDBM_REPLACE, DO_STORE_R);
    // replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE_R, MDBM_MODIFY);

    createBigDataV3(MDBM_REPLACE, DO_STORE_R);
    replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE_R, MDBM_MODIFY);
    replaceTestXLobj(DO_STORE_R, MDBM_MODIFY);
}

void
MdbmUnitTestStore::test_StoreR16()
{
    TRACE_TEST_CASE(__func__);
    reserveAndStore(data.fileName, versionFlag);
    checkData(data.fileName, "R16"+versionString);
}

void
MdbmUnitTestStore::test_StoreR17()
{
    TRACE_TEST_CASE(__func__);
//    Bugzilla bug 5385076
    reserveAndStoreLarge(data.fileNameLarge, versionFlag);
}


// mdbm_store_str() tests
void
MdbmUnitTestStore::test_StoreS1()
{
    TRACE_TEST_CASE(__func__);
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_STR, MDBM_INSERT, versionFlag);
    checkData(data.fileName, "S1"+versionString);
}


void
MdbmUnitTestStore::test_StoreS2()
{
    TRACE_TEST_CASE(__func__);
    testDuplicateInsert(data.fileName, DO_STORE_STR);
}

void
MdbmUnitTestStore::test_StoreS3()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST()

    const vector<int> &pageSizes = GetLargeDataPageSizes();
    createBigDataV3(MDBM_INSERT, DO_STORE_STR);
    compareLargeObjFiles(data.largeFiles, pageSizes);
    compareXLfile(true);
}

void
MdbmUnitTestStore::test_StoreS4()
{
    TRACE_TEST_CASE(__func__);
    int openFlags = MDBM_LARGE_OBJECTS| versionFlag;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0);
    createLargeObjectMdbm(mdbm);

    datum key, value;
    key = CreateTestValue("", PAGESZ_TLRG + 20, value);
    testDuplicateInsert(data.fileNameLarge, DO_STORE_STR, value.dptr);
}

void
MdbmUnitTestStore::test_StoreS5()
{
    TRACE_TEST_CASE(__func__);
    // Test creating brand new objects
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_STR, MDBM_REPLACE, versionFlag);
    checkData(data.fileName, "S5"+versionString);
    replaceExisting(data.fileName, "S5"+versionString, DO_STORE_STR);   // Test replacing existing objs
}

void
MdbmUnitTestStore::test_StoreS6()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST()

    vector<int> pageSizes = GetLargeDataPageSizes();
    //if (IS_32BIT()) {
    //  // the largest size tries to allocate >2G and blows up in RHEL6-32-bit
    //  pageSizes.resize(pageSizes.size()-1);
    //}
    createBigDataV3(MDBM_REPLACE, DO_STORE_STR);
    compareLargeObjFiles(data.largeFiles, pageSizes);
    compareXLfile(true);
    replaceTestXLobj(DO_STORE_STR, MDBM_REPLACE);
    replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE_STR, MDBM_REPLACE);
}


void
MdbmUnitTestStore::test_StoreS7()
{
    TRACE_TEST_CASE(__func__);
    testLargeToSmallReplace(data.fileNameLarge, DO_STORE_STR);    // Uses previously made file
}


void
MdbmUnitTestStore::test_StoreS8()
{
    TRACE_TEST_CASE(__func__);
    // Insert initial data with INSERT_DUP
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_STR, MDBM_INSERT_DUP, versionFlag);
    checkData(data.fileName, "S8"+versionString);
    doSmallDuplicateTest(data.fileName, DO_STORE_STR);   // Insert duplicates
}


void
MdbmUnitTestStore::test_StoreS9()
{
    TRACE_TEST_CASE(__func__);
    int openFlags = MDBM_LARGE_OBJECTS | versionFlag;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0);
    createLargeObjectMdbm(mdbm, DO_STORE_STR, MDBM_INSERT_DUP);
    addLargeDuplicates(data.fileNameLarge, DO_STORE_STR);
    fetchLargeDups(data.fileNameLarge);
}


void
MdbmUnitTestStore::test_StoreS10()
{
    TRACE_TEST_CASE(__func__);
    // Create data file to modify later
    createSmallData(data.fileName, PAGESZ_TSML, DO_STORE_STR, MDBM_INSERT, versionFlag);
    replaceExisting(data.fileName, "S10"+versionString, DO_STORE_STR, MDBM_MODIFY);
}

void
MdbmUnitTestStore::test_StoreS11()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentStore(data.fileName, DO_STORE_STR);
}

void
MdbmUnitTestStore::test_StoreS12()
{
    TRACE_TEST_CASE(__func__);
    replaceExisting(data.fileName, "S12"+versionString, DO_STORE_STR, MDBM_MODIFY, true);
}


void
MdbmUnitTestStore::test_StoreS13()
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST_VALGRIND()

    vector<int> pageSizes = GetLargeDataPageSizes();
    //if (IS_32BIT()) {
    //  // the largest size tries to allocate too much memory and blows up in RHEL6-32-bit
    //  pageSizes.resize(pageSizes.size()-1);
    //}
    // Create files using MDBM_REPLACE and DO_STORE to improve performance
    createBigDataV3(MDBM_REPLACE, DO_STORE);
    modifyLargeObjects(data.largeFiles, pageSizes, DO_STORE_STR);
    modifyTestXLobj(DO_STORE_STR);
}


void
MdbmUnitTestStore::test_StoreS14()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentStore(data.fileNameLarge, DO_STORE_STR);
}


void
MdbmUnitTestStore::test_StoreS15()
{
    TRACE_TEST_CASE(__func__);
    const vector<int> &pageSizes = GetLargeDataPageSizes();
    // Create large-object test files: that fact that mode=REPLACE is not important for this test,
    // And using DO_STORE to improve performance by reducing calls to strlen()
    int openFlags = MDBM_LARGE_OBJECTS | versionFlag;
    createBigTestFiles(openFlags, data.largeFiles, pageSizes, MDBM_REPLACE, DO_STORE);
    replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE_STR, MDBM_MODIFY);

//    createBigDataV3(MDBM_REPLACE, DO_STORE_STR);
//    replaceLargeObjects(data.largeFiles, pageSizes, DO_STORE_STR, MDBM_MODIFY);
//    replaceTestXLobj(DO_STORE_STR, MDBM_MODIFY);
}

void
MdbmUnitTestStore::test_StoreT1()   // Test T1
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST_VALGRIND()

    string tcprefix = "TC T-1: test_StoreT1: Multi-page sizes, No large object flag: ";
    vector<int> pagerange(PageSizeRange, PageSizeRange + sizeof(PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(versionFlag, pagerange, 0, false, tcprefix);
}
void
MdbmUnitTestStore::test_StoreT2()   // Test T2
{
    TRACE_TEST_CASE(__func__);
    SKIP_IF_FAST_VALGRIND()

    string tcprefix = "TC T-2: test_StoreT2: Multi-page sizes, Large object flag: ";
    vector<int> pagerange(PageSizeRange, PageSizeRange + sizeof(PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(versionFlag|MDBM_LARGE_OBJECTS, pagerange, 0, false, tcprefix);
}
void
MdbmUnitTestStore::test_StoreT3()   // Test T3
{
    // Memory-Only db
    string tcprefix = "TC T-3: test_StoreT3: Multi-page sizes, Memory-Only: ";
    vector<int> pagerange(PageSizeRange, PageSizeRange + sizeof(PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(0, pagerange, 0, false, tcprefix);
}

void
MdbmUnitTestStore::test_StoreChurnLob()
{
    TRACE_TEST_CASE(__func__)

    // This is an attepmt to exercise some deeply nested functions like fixup_lob_pointer()

    char key[16];
    char val[3*4096];
    datum kdat = { key, 4 };
    datum vdat = { val, 4000 };
    int i, j,ret;
    string fname = GetTmpName();
    MdbmHolder db(fname);

    db.Open(versionFlag|MDBM_O_CREAT|MDBM_O_RDWR|MDBM_LARGE_OBJECTS, 0644, 4096, 0);
    CPPUNIT_ASSERT(NULL != (MDBM*)db);
    //CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 21, NULL, NULL));
    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 16, NULL, NULL));
    //CPPUNIT_ASSERT(0 == mdbm_pre_split(db, 20));

    memset(key, 0, 16);
    memset(val, '*', 3*4096);

    for (j=0; j<10; ++j) {
      CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 16+(int)(j*1.5), NULL, NULL));
      //vdat.dsize = 4000-j;
      vdat.dsize = 12000-j*1000;
      //for (i=0; i<5; ++i) {
      for (i=0; i<5+j/2; ++i) {
        //fprintf(stderr, "**** STORING ENTRY %d size:%d\n", i, vdat.dsize);
        kdat.dsize = 1+ snprintf(key, 16, "%d", i);
        //ret = mdbm_delete(db, kdat);
        ret = mdbm_store(db, kdat, vdat, MDBM_REPLACE);
        if (ret) {
          mdbm_dump_all_page(db);
        }
        CPPUNIT_ASSERT(0 == ret);
      }
    }

    //for (i=0; i<10; i+=2) {
    for (i=0; i<4; i+=2) {
      //fprintf(stderr, "**** DELETE ENTRY %d size:%d\n", i, vdat.dsize);
      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
      ret = mdbm_delete(db, kdat);
      if (ret) {
        mdbm_dump_all_page(db);
      }
      CPPUNIT_ASSERT(0 == ret);
    }
    // add normal (small) objects
    for (i=410; i>310; --i) {
      vdat.dsize = 200;
      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
      //if (i>=300) { fprintf(stderr, "STORING ENTRY %d\n", i); }
      if (i<=40) { fprintf(stderr, "STORING ENTRY %d\n", i); }
      int ret = mdbm_store(db, kdat, vdat, MDBM_REPLACE);
      if (ret) { fprintf(stderr, "FAILED STORING ENTRY %d\n", i); }
      CPPUNIT_ASSERT(0 == ret);
    }
    for (i=1; i<7; i+=2) {
      vdat.dsize = 12000;
      //fprintf(stderr, "**** TRIPLE-SIZE ENTRY %d size:%d\n", i, vdat.dsize);
      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
      ret = mdbm_store(db, kdat, vdat, MDBM_REPLACE);
      if (ret) {
        mdbm_dump_all_page(db);
      }
      CPPUNIT_ASSERT(0 == ret);
    }

    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 16+15, NULL, NULL));
    for (i=1; i<6; i+=2) {
      vdat.dsize = 16000;
      //fprintf(stderr, "**** QUAD-SIZE ENTRY %d size:%d\n", i, vdat.dsize);
      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
      ret = mdbm_store(db, kdat, vdat, MDBM_REPLACE);
      CPPUNIT_ASSERT(0 == ret);
    }
    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 16+19, NULL, NULL));
      //fprintf(stderr, "**** QUAD-SIZE ENTRY %d size:%d\n", i, vdat.dsize);
      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
      ret = mdbm_store(db, kdat, vdat, MDBM_REPLACE);
      CPPUNIT_ASSERT(0 == ret);

//    // fill with normal (small) objects
//    for (i=410; i>20; --i) {
//      vdat.dsize = 200;
//      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
//      //if (i>=300) { fprintf(stderr, "STORING ENTRY %d\n", i); }
//      if (i<=40) { fprintf(stderr, "STORING ENTRY %d\n", i); }
//      int ret = mdbm_store(db, kdat, vdat, MDBM_REPLACE);
//      CPPUNIT_ASSERT(0 == ret);
//    }


}

void
MdbmUnitTestStore::test_StoreT4()   // Test T4
{
    // Memory-Only db with large object
    string tcPrefix = "TC T-4: test_StoreT4: Large Object and Memory-Only db: ";
    int flags = MDBM_CREATE_V3|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_LARGE_OBJECTS;
    int pagesz = 512;
    MDBM *dbh = mdbm_open(NULL, flags, 0666, pagesz, 8*pagesz + 2000);
    CPPUNIT_ASSERT_MESSAGE(tcPrefix + "FAILed to open memory only db", (dbh != NULL));

    // Fails if presize dbsize was power of 2 so make a number that will allow
    // enough space to be used for our large object.
    // Else we will get a failure to store large obj: 12=Cannot allocate memory

    char largeObj[512];
    datum   key, val;
    key.dptr = (char*)"big obj";
    key.dsize = strlen(key.dptr);
    val.dptr = largeObj;
    val.dsize = sizeof(largeObj);
    strcpy(largeObj+4, "big oh boy");
    int ret = mdbm_store(dbh, key, val, MDBM_INSERT);
    CPPUNIT_ASSERT_MESSAGE(tcPrefix + "FAILed to store large object in memory only db", (ret == 0));

    datum dret = mdbm_fetch(dbh, key);
    CPPUNIT_ASSERT_MESSAGE(tcPrefix + "FAILed to fetch large object in memory only db", (dret.dsize > 0 && dret.dptr != NULL));

    //dump_mdbm_header(dbh);
    //dump_chunk_headers(dbh);
    //mdbm_dump_all_page(dbh);

    mdbm_close(dbh);
}


void
MdbmUnitTestStore::test_StoreChurnOversize()
{
    TRACE_TEST_CASE(__func__)

    // This is an attepmt to exercise some deeply nested (oversize) functions

    char key[16];
    char val[256];
    datum kdat = { key, 4 };
    datum vdat = { val, 256 };
    int i;
    string fname = GetTmpName();
    MdbmHolder db(fname);

    db.Open(versionFlag|MDBM_O_CREAT|MDBM_O_RDWR|MDBM_LARGE_OBJECTS, 0644, 4096, 0);
    CPPUNIT_ASSERT(NULL != (MDBM*)db);
    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 20, NULL, NULL));

    memset(val, '*', 256);
    kdat.dsize = 1+ snprintf(key, sizeof(key), "key");

    for (i=0; i<90; ++i) {
      //fprintf(stderr, "STORING ENTRY %d\n", i);
      CPPUNIT_ASSERT(0 == mdbm_store(db, kdat, vdat, MDBM_INSERT_DUP));
    }


    for (i=1; i<50; ++i) {
      CPPUNIT_ASSERT(0 == mdbm_delete(db, kdat));
    }

    { 
        // dump coverage... but to /dev/null
        StdoutDiverter diverter("/dev/null");
        mdbm_dump_all_page(db);
        mdbm_dump_page(db, 0);
    }

    for (i=1; i<50; ++i) {
      CPPUNIT_ASSERT(0 == mdbm_store(db, kdat, vdat, MDBM_INSERT_DUP));
    }

}

void
MdbmUnitTestStore::testDuplicateInsert(const string &fname, StoreType storeType, char *data)
{
    MDBM* mdbm =  mdbm_open(fname.c_str(), MDBM_O_RDWR , 0644, 0, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    if (data == NULL) {
        data = const_cast<char *> (SOME_TEST_VALUE);
    }

    int ret = 0;
    string key1, somekey("somekey");
    datum key, val;
    for (int i = 0; i < 7; ++i) {
        key1 = somekey + ToStr(i);  // Some non-existent key
        key.dsize = key1.size() + 1;
        key.dptr = const_cast<char *> (key1.c_str());
        val.dsize = strlen(SOME_TEST_VALUE) + 1;
        val.dptr = data;
        ret = doStore(mdbm, key, val, MDBM_INSERT, storeType);
        CPPUNIT_ASSERT_EQUAL(0, ret);
        //CPPUNIT_ASSERT_EQUAL (0, errno);
    }

    for (int i = 0; i < 7; ++i) {
        key1 = somekey + ToStr(i);  // Same key as above
        key.dsize = key1.size() + 1;
        key.dptr = const_cast<char *> (key1.c_str());
        val.dsize = strlen(SOME_TEST_VALUE) + 1;
        val.dptr = data;
        ret = doStore(mdbm, key, val, MDBM_INSERT, storeType);
        CPPUNIT_ASSERT(ret != 0);       // Supposed to fail
        if (ret == 0)
            CPPUNIT_ASSERT(errno != 0);   // can set errno if store doesn't fail
    }
    errno = 0;
    mdbm_close(mdbm);
}


int
MdbmUnitTestStore::doStore(MDBM *mdbm, datum key, datum val, int mode, StoreType storeType)
{
    int ret = -1;
    errno = 0;

    if (storeType == DO_STORE) {
        ret = mdbm_store(mdbm, key, val, mode);
    } else if (storeType == DO_STORE_R) {
        ret = mdbm_store_r(mdbm, &key, &val, mode, &data.mIter);
    } else if (storeType == DO_STORE_STR) {  // store_str()
        ret = mdbm_store_str(mdbm, key.dptr, val.dptr, mode);
    }

    return ret;
}


/// Replace existing data in file "fname" using the store type of storeType, useLonger=true
/// means take original data, add a string to what's there and replace the data with a longer data
/// item. "mode" is MDBM_REPLACE, MDBM_INSERT, MDBM_MODIFY...
void
MdbmUnitTestStore::replaceExisting(const string &fname, const string& testName, StoreType storeType,
                                   int mode, bool useLonger)
{
    datum ky, val, fetched;
    string key;
    vector<string> dataItems;
    string msg;
    MDBM *mdbm;
    CPPUNIT_ASSERT((mdbm = mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, 0, 0)) != NULL);

    vector<int> howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);
    CPPUNIT_ASSERT( dataItems.size() != 0);

    vector<string>::iterator datait = dataItems.begin();
    int testCount = 0;
    for (; datait != dataItems.end(); ++datait, ++testCount) {
        key = PREFIX + ToStr(0) + datait->c_str();  // replace first value
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        string newVal;
        if (useLonger) {
            newVal = *datait + SOME_TEST_VALUE;
        } else {
            newVal = ToStr(testCount);
        }
        val.dptr = const_cast<char *> (newVal.c_str());
        val.dsize = newVal.size() + 1;  // Store the null char

        msg = string("Storing ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
        msg += string(", errno is: ") + strerror(errno);
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), doStore(mdbm, ky, val, mode, storeType) == 0);
    }
    mdbm_close(mdbm);

    CPPUNIT_ASSERT((mdbm = mdbm_open(fname.c_str(), MDBM_O_RDONLY, 0444, 0, 0)) != NULL);
    datait = dataItems.begin();   // Start over and fetch
    int fetchCount = 0;
    for (; datait != dataItems.end(); ++datait, ++fetchCount) {
        key = PREFIX + ToStr(0) + datait->c_str();  // fetch back first value
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        fetched = fetchData(mdbm, &ky);
        string newVal;
        if (useLonger) {
            newVal = *datait + SOME_TEST_VALUE;
        } else {
            newVal = ToStr(fetchCount);
        }
        val.dptr = const_cast<char *> (newVal.c_str());
        val.dsize = newVal.size() + 1;  // Store the null char
        msg = string("Replace test ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(fetched.dptr, val.dptr, val.dsize) == 0);
    }
    mdbm_close(mdbm);

    GetLogStream() << "Test " << testName << " performed " << testCount << " replaces" << endl;
    GetLogStream() << "Test " << testName << " performed " << fetchCount << " tests" << endl;
}



/// Create full pathnames as initial setup for all unit tests
void
MdbmUnitTestStore::createFileNames(const string &basename)
{
    CPPUNIT_ASSERT(0 == GetTmpDir(data.basePath));

    data.fileName = data.basePath + "/" + basename + string("test")+versionString;
    data.fileNameLarge = data.basePath + "/" + basename + string("testL")+versionString;

    CPPUNIT_ASSERT(data.fileName != "");
    CPPUNIT_ASSERT(data.fileNameLarge != "");

    // File names for extensive large object tests
    string name;
    const vector<int> &pageSizes = GetLargeDataPageSizes();
    for (unsigned i = 0; i<pageSizes.size(); ++i) {
        name = data.basePath + "/" + basename + string("big") + versionString + "_" + ToStr(i);
        data.largeFiles.push_back(name);
        CPPUNIT_ASSERT(data.largeFiles[i] != "");
    }
}


const vector<int>& MdbmUnitTestStore::GetLargeDataPageSizes() {
    return LargeDataPageSizesVectV3;
}


/// Create test data made of small objects.  Then create large object data for duplicate and missing
/// key tests (mdbm3 and mdbm4).
//
void
MdbmUnitTestStore::createSmallData(const string &fname, int pageszSml,
                                   StoreType storeType, int mode, int openFlags)
{
    //datum ky, val;
    //size_t i;
    //static bool firstTime = true;

    //if (firstTime) {
    //    for (i = 0; i < dataSizeArrayLen ; ++i) {   // create test data
    //        ky = CreateTestValue("", smallDataSizes[i], val);
    //        RegisterDatum(SMALL_OBJ_TESTS, val.dptr, NUM_DATA_ITEMS+i);
    //    }
    //    firstTime = false;
    //}

    MdbmHolder mdbm1(EnsureNewNamedMdbm(fname, openFlags, 0644, pageszSml, 0));
    storeTestData(mdbm1, SMALL_OBJ_TESTS, "Small Objects", storeType, mode);
    mdbm1 = 0;
}


/// Stores test data: used by createSmallData() to create V2 and V3 small object test data
//
void
MdbmUnitTestStore::storeTestData(MDBM *mdbm, int id, const string& testName, StoreType storeType, int mode)
{
    storeOrCheckData(mdbm, testName, true, storeType, mode, id);
    mdbm_close(mdbm);
}

/// Test to see that data stored by createSmallData remains the same
//
void
MdbmUnitTestStore::checkData(const string &filename, const string& testName)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, 0, 0)) != NULL);

    storeOrCheckData(mdbm, testName, false, DO_STORE, SMALL_OBJ_TESTS);  // DO_STORE not used
    mdbm_close(mdbm);
}

/// Loop through and either store the data or fetch it
//
void
MdbmUnitTestStore::storeOrCheckData(MDBM *mdbm, const string& testName, bool store,
                                    StoreType storeType, int mode, int id)
{
    int numKys;
    datum ky, val, fetched;
    string key;
    int testCount = 0, fetchCount = 0;
    vector<string> dataItems;
    string msg;

    vector<int> howMany = GetRegisteredData(id, dataItems);
    CPPUNIT_ASSERT( dataItems.size() != 0);

    vector<string>::iterator datait = dataItems.begin();
    vector<int>::iterator numit = howMany.begin();
    for (; datait != dataItems.end(); ++datait, ++numit) {
        CPPUNIT_ASSERT(numit != howMany.end());   // stop if data error
        numKys = *numit;
        val.dptr = const_cast<char *> (datait->c_str());
        val.dsize = datait->size() + 1;  // Store the null char
        for (int j=0; j < numKys; ++j) {
            key = PREFIX + ToStr(j) + datait->c_str();
            ky.dptr = const_cast<char *> (key.c_str());
            ky.dsize = key.size() + 1;
            if (store) {
                msg = string("Storing ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
                msg += string(", errno is ") + strerror(errno);
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(), doStore(mdbm, ky, val, mode, storeType) == 0);
            } else {
                fetched = fetchData(mdbm, &ky);
                msg = string("Store test ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(fetched.dptr, val.dptr, val.dsize) == 0);
                ++fetchCount;
            }
            ++testCount;
        }
    }

    if (store) {
        GetLogStream() << "Test " << testName << " performed " << testCount << " stores" << endl;
    } else {
        GetLogStream() << "Test " << testName << " performed " << fetchCount << " fetches" << endl;
    }
}


/// Insert duplicates into the MDBM
//
void
MdbmUnitTestStore::doSmallDuplicateTest(const string &fname, StoreType storeType)
{
    string key, firstDup, secondDup;
    datum ky, val;
    vector<string> dataItems;
    vector<int> howMany;

    howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);

    vector<string>::iterator datait = dataItems.begin();
    CPPUNIT_ASSERT( datait != dataItems.end());  // stop if no data

    MDBM* mdbm =  mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    errno = 0;

    firstDup =  string("FirstData") + *datait + string("EndFirstData");
    secondDup = string("SecondData") + *datait + string("TailSecondData");

    // Insert the 2 duplicates for each data item size
    for (; datait != dataItems.end(); ++datait) {
        key = PREFIX + ToStr(0) + datait->c_str();
        ky.dptr = (char *) key.c_str();
        ky.dsize = key.size() + 1;
        val.dptr = (char *) firstDup.c_str();
        val.dsize = firstDup.size() + 1;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_INSERT_DUP, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        val.dptr = (char *) secondDup.c_str();
        val.dsize = secondDup.size() + 1;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_INSERT_DUP, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
    }

    mdbm_close(mdbm);
}


/// Wrapper to fetch data from the MDBM
//
datum
MdbmUnitTestStore::fetchData(MDBM *mdbm, datum *keyPtr)
{
    datum fetched;
    int ret = 0;
    string err;

    errno = 0;
    fetched.dptr = 0;
    fetched.dsize = 0;
    fetched = mdbm_fetch( mdbm, *keyPtr );

    err = string("Failed to find Key ") + keyPtr->dptr + ", errno set to " + ToStr(errno);
    //CPPUNIT_ASSERT_MESSAGE(err.c_str(), errno == 0);
    err += ", return value set to " + ToStr(ret);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);

    return (fetched);
}


/// Creates a "simple" large object MDBM, where we don't need to have several page sizes
/// for the purpose of the test (mainly negative tests).
void
MdbmUnitTestStore::createLargeObjectMdbm(MDBM *mdbm, StoreType storeType, int mode)
{
    char buf[256];
    // Keys must be smaller than the page size, so I won't use the ky return value
    datum ky, val;
    ky = CreateTestValue("",  MUCH_LARGER_DATA, val);
    ++val.dsize;  // store the null
    for (int i = 0; i < MAX_EASY_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
    }

    mdbm_close(mdbm);
}

/// Insert duplicates for duplicate tests
//
void
MdbmUnitTestStore::addLargeDuplicates(const string &fname, StoreType storeType)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT((mdbm = mdbm_open(fname.c_str(),  MDBM_O_RDWR, 0644, 0, 0)) != NULL);

    string buf;
    // Keys must be smaller than the page size, so I won't use the ky return value
    datum ky, val;
    ky = CreateTestValue("",  MUCH_LARGER_DATA*2, val);
    char *baseline = val.dptr;
    int baselineSize = val.dsize + 1;  // store the null
    for (int i = 0; i < MAX_EASY_TEST; ++i) {
        buf = SIMPLE_KEY_PREFIX + ToStr(i);
        ky.dptr = const_cast<char *> (buf.c_str());
        ky.dsize = buf.size() + 1;
        val.dptr = baseline;
        val.dsize = baselineSize;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_INSERT_DUP, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        val.dptr = const_cast<char *> (SOME_TEST_VALUE);
        val.dsize = strlen(SOME_TEST_VALUE) + 1;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_INSERT_DUP, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
    }

    mdbm_close(mdbm);
}

/// Check that values of large object duplicates correspond to what was stored
//
void
MdbmUnitTestStore::fetchLargeDups(const string &filename)
{
    char buf[256];
    datum ky, val;
    MDBM *mdbm;

    ky = CreateTestValue("",  MUCH_LARGER_DATA, val);
    string longstr1 ( val.dptr );
    ky = CreateTestValue("",  MUCH_LARGER_DATA*2, val);
    string longstr2 = ( val.dptr );

    CPPUNIT_ASSERT( (mdbm = mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, 0, 0)) != NULL);

    errno = 0;
    for (int i = 0; i < MAX_EASY_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        val = fetchData(mdbm, &ky);

        int found = (strcmp( val.dptr, SOME_TEST_VALUE) == 0);
        found = found || (longstr1 == val.dptr);
        found = found || (longstr2 == val.dptr);
        CPPUNIT_ASSERT(found != 0);
    }
    mdbm_close(mdbm);
}

/// Create large object data for V3 MDBMs
void
MdbmUnitTestStore::createBigDataV3(int mode, StoreType storeType)
{
    const vector<int> &pageSizes = GetLargeDataPageSizes();
    int openFlags = MDBM_LARGE_OBJECTS | MDBM_O_RDWR | MDBM_CREATE_V3;
    createBigTestFiles(openFlags, data.largeFiles, pageSizes, mode, storeType);

    // create 16MB page-size MDBM
    int lastEntry = pageSizes.size()-1;
    int pgsize = pageSizes[lastEntry];
    //MdbmHolder mdbm(EnsureNewNamedMdbm(data.largeFiles[lastEntry], openFlags, 0644, pgsize, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(data.largeFiles[lastEntry], openFlags, 0644, pgsize, 0);
    datum ky, val;
    ky = CreateTestValue("",  pgsize * 10, val);
    char *baselineData = val.dptr;
    char key[64];

    for (int i = 0; i < MAX_NUM_LDATA; ++i) {
        errno = 0;
        ky.dptr = key;   // Store small objects for test purposes
        ky.dsize = snprintf(key, sizeof(key), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        val.dsize = SMALL_OBJ_SIZE_LARGE_OBJ_TEST;
        val.dptr = baselineData;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
    }

//    if (getTestRunLevel() != RunAllTestCases)
//        mdbm_close(mdbm);
//        return;
//    }

    for (int i = 1; i < 3; ++i) {
        errno = 0;
        ky.dptr = key;  // Now create 2 very large objects
        ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, i, i) + 1;
        val.dsize = (pgsize * 10);  // No null stored
        val.dptr = baselineData;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
    }

    mdbm_close(mdbm);
}

/// Create large object test files: enter some small objects and some large ones
void
MdbmUnitTestStore::createBigTestFiles(int openFlags, vector<string> &fnames,
        const vector<int>& pageSizes, int mode, StoreType storeType)
{
    int pgsize;
    int maxData = pageSizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DATA;

    datum ky, val;
    ky = CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pageSizes[filei];
        MdbmHolder mdbm(EnsureNewNamedMdbm(fnames[filei], openFlags, 0644, pgsize, 0));

        string key, msg;
        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            key = SIMPLE_KEY_PREFIX + ToStr(j);
            ky.dptr = const_cast<char *> (key.c_str());   // Store small objects for test purposes
            ky.dsize = key.size() + 1;
            val.dsize = pgsize / 2;
            val.dptr = baselineData;
            CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
            //CPPUNIT_ASSERT_EQUAL(0, errno);

            key = SIMPLE_KEY_PREFIX + ToStr(j) + ToStr(j);
            ky.dptr = const_cast<char *> (key.c_str());  // Now create large objects
            ky.dsize = key.size() + 1;
            val.dptr = baselineData;
            int siz = val.dsize = pgsize + additionalDataSizes[j % 4];  // without null first
            char temp = baselineData[siz];    // Store original
            baselineData[siz] = '\0';    // Store null
            ++(val.dsize);
            CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
            msg = "Failed to store key " + key + " to file " + fnames[filei] +
                  " page size = " + ToStr(pgsize) + " errno = " + ToStr(errno);
            //CPPUNIT_ASSERT_MESSAGE(msg.c_str(), errno == 0);
            baselineData[siz] = temp; // restore original
        }
    }
}

/// Compare large objects in files passed in vector fnames to expected values
//
void
MdbmUnitTestStore::compareLargeObjFiles(vector<string> &fnames, const vector<int>& pageSizes)
{
    MDBM *mdbm;
    char key[64];
    int pgsize;
    int maxData = pageSizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DATA;

    datum ky, val;
    ky = CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pageSizes[filei];
        CPPUNIT_ASSERT((mdbm = mdbm_open(fnames[filei].c_str(), MDBM_O_RDONLY, 0444, pgsize, 0)) != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            ky.dptr = key;
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            val.dptr = baselineData;
            val.dsize = pgsize + additionalDataSizes[j % 4];
            int siz = val.dsize;
            char temp = baselineData[siz];    // Store original
            baselineData[siz] = '\0';    // Store null
            ++(val.dsize);
            compareLargeData(mdbm, &ky, val);
            baselineData[siz] = temp; // restore original
        }
        mdbm_close(mdbm);
    }
}

/// Retrieve Large Objects from the MDBM, then compare data to the compareTo datum
//

void
MdbmUnitTestStore::compareLargeData(MDBM *mdbm, datum *keyPtr, const datum &compareTo)
{
    datum fetched;
    int ret = 0;

    fetched.dptr = 0;
    fetched.dsize = 0;
    string err;

    errno = 0;
    fetched = mdbm_fetch( mdbm, *keyPtr );

    err = string("Failed to find Key ") + keyPtr->dptr + ", errno set to " + ToStr(errno);
    //CPPUNIT_ASSERT_MESSAGE(err.c_str(), errno == 0);
    err += ", return value set to " + ToStr(ret);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);

    err = string("Size of key ") + keyPtr->dptr + " is " + ToStr(fetched.dsize) +
          " but should be " + ToStr(compareTo.dsize);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), fetched.dsize == compareTo.dsize);
    err = string("Key ") + keyPtr->dptr + " contains data different than intended";
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), memcmp(fetched.dptr,compareTo.dptr, compareTo.dsize) == 0);
}

/// Test that 160MB obj on 16MB page-size MDBM data was stored correctly
//
void
MdbmUnitTestStore::compareXLfile(bool useStoreStr)
{
//    if (getTestRunLevel() != RunAllTestCases)
//        return;

    const vector<int> &pageSizes = GetLargeDataPageSizes();
    MDBM *mdbm;
    int lastEntry = pageSizes.size() -1;
    int pgsize = pageSizes[lastEntry];
    CPPUNIT_ASSERT((mdbm = mdbm_open(data.largeFiles[lastEntry].c_str(),  MDBM_O_RDONLY, 0644, pgsize, 0)) != NULL);
    datum ky, val;
    ky = CreateTestValue("",  pgsize * 10, val);
    char *baselineData = val.dptr;
    char key[64];
    errno = 0;
    ky.dptr = key;
    ky.dsize = snprintf(key, sizeof(key), "%s11", SIMPLE_KEY_PREFIX) + 1;
    val.dsize = (pgsize * 10);
    if (useStoreStr)
        ++val.dsize;
    val.dptr = baselineData;
    compareLargeData(mdbm, &ky, val);
    mdbm_close(mdbm);
}


/// Replace large object data and make sure they look good
//
void
MdbmUnitTestStore::replaceLargeObjects(vector<string> &fnames, const vector<int>& pageSizes,
                                       StoreType storeType, int mode)
{
    MDBM *mdbm;
    char key[64];
    int pgsize;
    int maxData = pageSizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DATA;

    datum ky, val;
    const char *prfx = "ReplacePrefix";
    int  plen = strlen(prfx);
    ky = CreateTestValue(prfx,  maxData, val);
    char *baselineData = ky.dptr;  // baseline is the key, since it includes the prefix

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pageSizes[filei];
        CPPUNIT_ASSERT((mdbm = mdbm_open(fnames[filei].c_str(),
                                         MDBM_O_RDWR | MDBM_LARGE_OBJECTS, 0644, 0, 0)) != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            ky.dptr = key;
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            val.dptr = baselineData;
            val.dsize = pgsize + additionalDataSizes[j % 4] + plen;
            int siz = val.dsize;
            char temp = baselineData[siz];    // Store original
            baselineData[siz] = '\0';    // Store null
            ++(val.dsize);
            CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
            //CPPUNIT_ASSERT_EQUAL(0, errno);
            compareLargeData(mdbm, &ky, val);
            baselineData[siz] = temp; // restore original
        }
        mdbm_close(mdbm);
    }
}

/// Replace and test that 160MB obj on 16MB page-size MDBM data was stored correctly
//
void
MdbmUnitTestStore::replaceTestXLobj(StoreType storeType, int mode)
{
//    if (getTestRunLevel() != RunAllTestCases)
//        return;

    const vector<int> &pageSizes = GetLargeDataPageSizes();
    MDBM *mdbm;
    int lastEntry = pageSizes.size()-1;
    int pgsize = pageSizes[lastEntry];
    CPPUNIT_ASSERT((mdbm = mdbm_open(data.largeFiles[lastEntry].c_str(),  MDBM_O_RDWR, 0644, pgsize, 0)) != NULL);

    datum ky, val;
    const char *prfx = "ReplaceHuge";
    int  plen = strlen(prfx);
    ky = CreateTestValue(prfx, pgsize * 10, val);
    char *baselineData = ky.dptr;
    string key(SIMPLE_KEY_PREFIX);
    key += "11";
    ky.dptr = const_cast<char *> (key.c_str());
    ky.dsize = key.size() + 1;
    val.dsize = (pgsize * 10) + plen + (storeType == DO_STORE_STR ? 1 : 0);
    val.dptr = baselineData;
    CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, mode, storeType));
    //CPPUNIT_ASSERT_EQUAL(0, errno);
    val.dptr = baselineData;
    val.dsize = (pgsize * 10) + plen + (storeType == DO_STORE_STR ? 1 : 0);
    compareLargeData(mdbm, &ky, val);
    mdbm_close(mdbm);
}


/// test replacing large to small and vice versa
//
void
MdbmUnitTestStore::testLargeToSmallReplace(const string &fname, StoreType storeType)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT((mdbm = mdbm_open(fname.c_str(),  MDBM_O_RDWR, 0644, 0, 0)) != NULL);

    string key, str;
    datum ky, val, fetched;
    // Convert large to small objects and check
    for (int i = 0; i < MAX_EASY_TEST; ++i) {
        key = SIMPLE_KEY_PREFIX + ToStr(i);
        str = string("Test Value ") + ToStr(i);
        ky.dptr = const_cast<char *>(key.c_str());
        ky.dsize = key.size() + 1;
        val.dptr = const_cast<char *>(str.c_str());
        val.dsize = str.size() + 1;
        CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_REPLACE, storeType));
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        fetched = fetchData(mdbm, &ky);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(fetched.dptr, str.c_str(), str.size() + 1));
    }

    createLargeObjectMdbm(mdbm, storeType);  // Now convert back to large objects
}


void
MdbmUnitTestStore::testNonExistentStore(const string &fname, StoreType storeType)
{
    MDBM * mdbm =  mdbm_open(fname.c_str(), MDBM_O_RDWR , 0644, 0, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    int ret = 0;
    string key1, somekey("someother");
    datum key, val;
    for (int i = 0; i < 7; ++i) {
        key1 = somekey + ToStr(i);  // Some non-existent key
        key.dsize = key1.size() + 1;
        key.dptr = const_cast<char *> (key1.c_str());
        val.dsize = strlen(SOME_TEST_VALUE) + 1;
        val.dptr = const_cast<char *> (SOME_TEST_VALUE);
        ret = doStore(mdbm, key, val, MDBM_MODIFY, storeType);
        CPPUNIT_ASSERT(ret != 0);       // Supposed to fail
        CPPUNIT_ASSERT( errno != 0 );   // Also sets errno
    }
    errno = 0;
    mdbm_close(mdbm);
}

/// Modify large object data and make sure they look good
void
MdbmUnitTestStore::modifyLargeObjects(vector<string> &fnames, const vector<int>& pageSizes, StoreType storeType)
{
    MDBM *mdbm;
    const int mod_offset = 2; // Create data 2 chars shorter than page
    char key[64];
    int maxData = pageSizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DATA;

    datum ky, val;
    ky = CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr + mod_offset;  // baseline is 2 chars shorter than data


    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        int pgsize = pageSizes[filei];
        CPPUNIT_ASSERT((mdbm = mdbm_open(fnames[filei].c_str(),
                                         MDBM_O_RDWR | MDBM_LARGE_OBJECTS, 0644, 0, 0)) != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            ky.dptr = key;  // Now create large objects
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            int siz = pgsize + additionalDataSizes[j % 4] - mod_offset;
            char temp = baselineData[siz];    // Store original
            if ((j % 2) == 0) {   // store small object: the key
                val.dsize = ky.dsize;
                val.dptr = ky.dptr;
            } else {              // store large object that is smaller
                val.dsize = siz;
                val.dptr = baselineData;
                baselineData[siz] = '\0';    // Store null
                ++(val.dsize);
            }
            CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_MODIFY, storeType));
            //CPPUNIT_ASSERT_EQUAL(0, errno);
            baselineData[siz] = temp; // restore original
        }
        mdbm_close(mdbm);
    }

    // Now retrieve and compare the data
    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        int pgsize = pageSizes[filei];
        CPPUNIT_ASSERT((mdbm = mdbm_open(fnames[filei].c_str(), MDBM_O_RDONLY, 0644, 0, 0)) != NULL);
        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            ky.dptr = key;  // Now create large objects
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            int siz = pgsize + additionalDataSizes[j % 4] - mod_offset;
            char temp = baselineData[siz];    // Store original
            if ((j % 2) == 0) {   // store small object: the key
                val.dsize = ky.dsize;
                val.dptr = ky.dptr;
            } else {              // store large object that is smaller
                val.dsize = siz;
                val.dptr = baselineData;
                baselineData[siz] = '\0';    // Store null
                ++(val.dsize);
            }
            compareLargeData(mdbm, &ky, val);
            baselineData[siz] = temp; // restore original
        }
        mdbm_close(mdbm);
    }
}


/// Modify and test that 160MB obj on 16MB page-size MDBM data was stored correctly
//
void
MdbmUnitTestStore::modifyTestXLobj(StoreType storeType)
{
//    if (getTestRunLevel() != RunAllTestCases)
//        return;

    const vector<int> &pageSizes = GetLargeDataPageSizes();
    MDBM *mdbm;
    int lastEntry = pageSizes.size() - 1;
    int pgsize = pageSizes[lastEntry];
    CPPUNIT_ASSERT((mdbm = mdbm_open(data.largeFiles[lastEntry].c_str(),  MDBM_O_RDWR, 0644, pgsize, 0)) != NULL);
    datum ky, val, cval;
    ky = CreateTestValue("", pgsize * 10, val);
    string key(SIMPLE_KEY_PREFIX);

    key += "0";
    char *baselineData = val.dptr + SLIGHTLY_SMALLER_DELTA;
    ky.dptr = const_cast<char *> (key.c_str());
    ky.dsize = key.size() + 1;
    val.dsize = (pgsize * 10) + SLIGHTLY_SMALLER_DELTA + (storeType == DO_STORE_STR ? 1 : 0);
    val.dptr = baselineData;
    CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_MODIFY, storeType));
    //CPPUNIT_ASSERT_EQUAL(0, errno);
    //val.dptr = baselineData;
    //val.dsize = (pgsize * 10) + SLIGHTLY_SMALLER_DELTA + (storeType == DO_STORE_STR ? 1 : 0);
    cval.dptr = val.dptr;
    cval.dsize = (storeType==DO_STORE_STR) ? strlen(val.dptr)+1 : val.dsize; 
    compareLargeData(mdbm, &ky, cval);

    key = SIMPLE_KEY_PREFIX;
    key += "11";
    val.dptr = ky.dptr = const_cast<char *> (key.c_str());
    val.dsize = ky.dsize = key.size() + 1;
    CPPUNIT_ASSERT_EQUAL(0, doStore( mdbm, ky, val, MDBM_MODIFY, storeType));
    //CPPUNIT_ASSERT_EQUAL(0, errno);
    val.dptr = ky.dptr = const_cast<char *> (key.c_str());
    val.dsize = ky.dsize = key.size() + 1;
    cval.dptr = val.dptr;
    cval.dsize = (storeType==DO_STORE_STR) ? strlen(val.dptr)+1 : val.dsize; 
    compareLargeData(mdbm, &ky, cval);

    mdbm_close(mdbm);
}


/// Use MDBM_RESERVE to reserve space for the data, then store the data
//
void
MdbmUnitTestStore::reserveAndStore(const string &fname, int openFlags)
{
    int numKys;
    datum ky, val;
    string key;
    vector<string> dataItems;
    string msg;
    //MdbmHolder mdbm(EnsureNewNamedMdbm(fname, openFlags, 0644, PAGESZ_TSML, 0));
    MDBM* mdbm = EnsureNewNamedMdbm(fname, openFlags, 0644, PAGESZ_TSML, 0);

    MDBM_ITER mIter;
    MDBM_ITER_INIT(&mIter);

    vector<int> howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);
    CPPUNIT_ASSERT( dataItems.size() != 0);

    vector<string>::iterator datait = dataItems.begin();
    vector<int>::iterator numit = howMany.begin();
    for (; datait != dataItems.end(); ++datait, ++numit) {
        CPPUNIT_ASSERT(numit != howMany.end());   // stop if data error
        numKys = *numit;
        val.dptr = const_cast<char *> (datait->c_str());
        val.dsize = datait->size() + 1;  // Store the null char
        for (int j=0; j < numKys; ++j) {
            key = PREFIX + ToStr(j) + datait->c_str();
            ky.dptr = const_cast<char *> (key.c_str());
            ky.dsize = key.size() + 1;
            errno = 0;
            msg = string("Reserving key= ") + ky.dptr + " val= " + val.dptr + string(", errno is: ") + strerror(errno);
            CPPUNIT_ASSERT_MESSAGE(msg.c_str(), mdbm_store_r(mdbm, &ky, &val, MDBM_RESERVE, &mIter) == 0);
            //CPPUNIT_ASSERT_EQUAL(0, errno);
            memcpy(val.dptr, datait->c_str(), datait->size() + 1);
        }
    }
    mdbm_close(mdbm);
}

/// Use MDBM_RESERVE to reserve space for large object data, then store the data
//
void
MdbmUnitTestStore::reserveAndStoreLarge(const string &fname, int openFlags)
{
    MdbmHolder mdbm(EnsureNewNamedMdbm(fname, openFlags|MDBM_LARGE_OBJECTS, 0644, PAGESZ_TLRG, 0));
    MDBM_ITER mIter;
    MDBM_ITER_INIT(&mIter);

    string key, msg;
    // Keys must be smaller than the page size, so I won't use the ky return value
    datum ky={0,0}, val={0,0};
    ky = CreateTestValue("",  MUCH_LARGER_DATA, val);
    int curSize = (++val.dsize);  // store the null
    char *baseline = val.dptr;
    for (int i = 0; i < MAX_EASY_TEST; ++i) {
        key = SIMPLE_KEY_PREFIX;
        key += ToStr(i);
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        errno = 0;
        msg = string("Reserving large object key= ") + ky.dptr + " val= " + val.dptr;
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), mdbm_store_r(mdbm, &ky, &val, MDBM_RESERVE, &mIter) == 0);
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        memcpy(val.dptr, baseline, curSize);
        baseline[(--curSize)] = '\0';  // shorten by 1
    }
}

// Store too many duplicates and don't use limit_size
void
MdbmUnitTestStore::test_StoreDupesOverflowPage()
{
    string prefix = "DupesFill";
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_LARGE_OBJECTS;
    int pagesize = 512;
    // value is 80 characters long
    string val("12345678901234567890123456789012345678901234567890123456789012345678901234567890");
    int maxCount = pagesize / val.size() + 10;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix, flags, 0644, pagesize, 0));

    string ky("Dupes");
    datum key, value;
    for (int i = 0; i < maxCount; ++i) {
        key.dptr = const_cast<char *> (ky.c_str());
        key.dsize = ky.size();
        value.dptr = const_cast<char *> (val.c_str());
        value.dsize = val.size();
        errno = 0;
        int ret = 0;
        if ((ret = mdbm_store(mdbm, key, value, MDBM_INSERT_DUP)) != 0) {
            ostringstream oss;
            oss << "mdbm_store failed to store duplicates, ret= " << ret << " errno = " << errno;
            CPPUNIT_FAIL(oss.str());
        }
    }
    datum fetched = mdbm_fetch( mdbm, key );
    CPPUNIT_ASSERT(NULL != fetched.dptr);
}



class MdbmUnitTestStoreV3 : public MdbmUnitTestStore
{
  CPPUNIT_TEST_SUITE(MdbmUnitTestStoreV3);
    CPPUNIT_TEST(test_StoreChurnLob);
    CPPUNIT_TEST(test_StoreChurnOversize);

    CPPUNIT_TEST(initialSetup);
    CPPUNIT_TEST(test_StoreQ1);
    CPPUNIT_TEST(test_StoreQ2);
    CPPUNIT_TEST(test_StoreQ3);
    CPPUNIT_TEST(test_StoreQ4);
    CPPUNIT_TEST(test_StoreQ5);
    CPPUNIT_TEST(test_StoreQ6);
    CPPUNIT_TEST(test_StoreQ7);
    CPPUNIT_TEST(test_StoreQ8);
    CPPUNIT_TEST(test_StoreQ9);
    CPPUNIT_TEST(test_StoreQ10);
    CPPUNIT_TEST(test_StoreQ11);
    CPPUNIT_TEST(test_StoreQ12);
    CPPUNIT_TEST(test_StoreQ13);
    CPPUNIT_TEST(test_StoreQ14);
    CPPUNIT_TEST(test_StoreQ15);
    CPPUNIT_TEST(test_StoreR1);
    CPPUNIT_TEST(test_StoreR2);
    CPPUNIT_TEST(test_StoreR3);
    CPPUNIT_TEST(test_StoreR4);
    CPPUNIT_TEST(test_StoreR5);
    CPPUNIT_TEST(test_StoreR6);
    CPPUNIT_TEST(test_StoreR7);
    CPPUNIT_TEST(test_StoreR8);
    CPPUNIT_TEST(test_StoreR9);
    CPPUNIT_TEST(test_StoreR10);
    CPPUNIT_TEST(test_StoreR11);
    CPPUNIT_TEST(test_StoreR12);
    CPPUNIT_TEST(test_StoreR13);
    CPPUNIT_TEST(test_StoreR14);
    CPPUNIT_TEST(test_StoreR15);
    CPPUNIT_TEST(test_StoreR16);
    CPPUNIT_TEST(test_StoreR17);
    CPPUNIT_TEST(test_StoreS1);
    CPPUNIT_TEST(test_StoreS2);
    CPPUNIT_TEST(test_StoreS3);
    CPPUNIT_TEST(test_StoreS4);
    CPPUNIT_TEST(test_StoreS5);
    CPPUNIT_TEST(test_StoreS6);
    CPPUNIT_TEST(test_StoreS7);
    CPPUNIT_TEST(test_StoreS8);
    CPPUNIT_TEST(test_StoreS9);
    CPPUNIT_TEST(test_StoreS10);
    CPPUNIT_TEST(test_StoreS11);
    CPPUNIT_TEST(test_StoreS12);
    CPPUNIT_TEST(test_StoreS13);
    CPPUNIT_TEST(test_StoreS14);
    CPPUNIT_TEST(test_StoreS15);
    CPPUNIT_TEST(test_StoreT1);
    CPPUNIT_TEST(test_StoreT2);
    CPPUNIT_TEST(test_StoreT3);
    CPPUNIT_TEST(test_StoreT4);
    CPPUNIT_TEST(test_StoreDupesOverflowPage);
    CPPUNIT_TEST(finalCleanup);
  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestStoreV3() : MdbmUnitTestStore(MDBM_CREATE_V3, dataV3) {}
    void setUp();
    void tearDown();
    static StoreTestData dataV3;
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestStoreV3);
StoreTestData MdbmUnitTestStoreV3::dataV3;
void MdbmUnitTestStoreV3::setUp() {
    TRACE_TEST_CASE(__func__);
    data = dataV3;
}
void MdbmUnitTestStoreV3::tearDown() {
    TRACE_TEST_CASE(__func__);
}


