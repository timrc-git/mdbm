/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of Data Access features, testing the mdbm_fetch* functions

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


// TODO FIXME, lots of duplicates with test_delete... can we coalesce?

// Constants for small-object tests and simple large object tests

static const int PAGESZ_TSML  = 8192;   // For small object tests
static const int PAGESZ_TLRG  = 1024;   // For large Object duplicate/missing key tests

//static const int NUM_DATA_ITEMS = 150;
static const int NUM_DATA_ITEMS = 120;
static const int MAX_DUP_TEST   = 20;  // For large Object duplicate/missing key tests

static const int SIZE_DUP_R_TEST   = 1500; // For large Object for fetch_dup_r tests
static const int SMALL_OBJ_SIZE_LARGE_OBJ_TEST = 1000;  // Small objects for large object tests

static const char *PREFIX  = "DPREFIX";             // Small object Key prefix
static const char *SIMPLE_KEY_PREFIX  = "Akey";     // Simple object key prefix
static const char *SOME_TEST_VALUE  = "Some Stuff";     // Small test object

static const size_t smallDataSizes[] = {25, 66, 100, 250, 1000, 1600 };
static const size_t dataSizeArrayLen = sizeof(smallDataSizes) / sizeof(size_t);

using namespace std;

//static string fileNameV2;
//static string fileNameV3;
//static string fileNameV2lrg;   // Large Object duplicate/missing key tests
//static string fileNameV3lrg;   // Large Object duplicate/missing key tests

// constants for large-object tests involving the following specifications:
// When fetching large objects of various value sizes:
//
//  1. Use various pages sizes
//  2. Ensure that some objects have values that are:
//         * Slightly smaller than the page size (ex., by 12 bytes)
//         * Exactly the page size
//         * Slightly larger than the page size
//         * Much larger than the page size (1000 bytes)
//         * Maximal case: for example, 16MB page size and 10x that page size for at least 1 large object

static const int MEGBT = 1024 * 1024;
static const int LARGEST_V2_PAGE = 65536;
static const int LARGEST_V3_PAGE = 1 << 24;

// mdbm_open fails to open when page size = exactly 16MB. Only 16MB-64bytes is OK
static const int OFFSET_TO_LARGEST       = 64;

static const int SLIGHTLY_SMALLER_DELTA  = -12;   // Slightly smaller than the page size
static const int SLIGHTLY_LARGER_DELTA   = 20;    // Slightly larger than the page size
static const int MUCH_LARGER_DELTA   = 1000;      // Much larger than the page size

static const int additionalDataSizes[4] = {SLIGHTLY_SMALLER_DELTA, 0, SLIGHTLY_LARGER_DELTA, MUCH_LARGER_DELTA };

static const int MAX_NUM_LDATA   = 10;  // For large Objects tests - must be 10 or less

static const int v2LargeDataPageSizes[] = {256, 1024, 4096, 16384, LARGEST_V2_PAGE };
static const int v2LargePageSizeArrayLen = sizeof(v2LargeDataPageSizes) / sizeof(int);
static const vector<int> LargeDataPageSizesVectV2(v2LargeDataPageSizes,
    v2LargeDataPageSizes+v2LargePageSizeArrayLen);

static const int v3LargeDataPageSizes[] = {384, 17152, 131072, MEGBT, LARGEST_V3_PAGE / 4, LARGEST_V3_PAGE - OFFSET_TO_LARGEST};
static const int v3LargePageSizeArrayLen = sizeof(v3LargeDataPageSizes) / sizeof(int);
static const vector<int> LargeDataPageSizesVectV3(v3LargeDataPageSizes,
    v3LargeDataPageSizes+v3LargePageSizeArrayLen);


enum {
    SMALL_OBJ_TESTS = 0,
    LARGE_OBJ_TESTS = 1
};

typedef enum {
    DO_STORE = 0,
    DO_FETCH = 1,
    DO_FETCH_R = 2,
    DO_FETCH_BUF = 3,
    DO_FETCH_DUP_R = 4,
    DO_FETCH_STR = 5,
    DO_FETCH_INFO = 6
} StoreFetchType;

struct FetchTestData {
    string basePath;
    string fileName;
    string fileNameLarge;   // Large Object duplicate/missing key tests
    vector<string> largeFiles;

    // variables to store data items and number of keys
    map<int, vector <string> > dataItems;  // for registerDatum
    map<int, vector <int> > howManyKeys;   // for registerDatum
};

class MdbmFetchUnitTest : public CppUnit::TestFixture, public TestBase
{
  public:
    //MdbmFetchUnitTest(FetchTestData& dat, int vFlag) : TestBase(vFlag, "MdbmFetchUnitTest"), data(dat) { }
    MdbmFetchUnitTest(FetchTestData& dat, int vFlag) : TestBase(vFlag, "MdbmFetchUnitTest") { }
    virtual ~MdbmFetchUnitTest() {}

    void setUp() ;    // Per-test initialization - applies to every test
    void tearDown() ; // Per-test cleanup - applies to every test

    void initialSetup();
    void test_FetchH1();
    void test_FetchH2();
    void test_FetchH3();
    void test_FetchH4();
    void test_FetchI1();
    void test_FetchI2();
    void test_FetchI3();
    void test_FetchI4();
    void test_FetchJ1();
    void test_FetchJ2();
    void test_FetchJ3();
    void test_FetchJ4();
    void test_FetchJ5();
    void test_FetchK1();
    void test_FetchK2();
    void test_FetchK3();
    void test_FetchL1();
    void test_FetchL2();
    void test_FetchL3();
    void test_FetchL4();
    void test_FetchM1_v2();
    void test_FetchM1();
    void test_FetchM2();
    void test_FetchM3();
    void test_FetchM4();
    void test_FetchM5();

    void finalCleanup();

    // Helper functions
    const vector<int>& GetLargeDataPageSizes();
    void createData(int pageszSml);
    void checkData(const string &filename, int pagesz, const string& testName, StoreFetchType fetchType);
    void storeOrCheckData(MDBM *mdbm, const string& testName, StoreFetchType storeFetch, int id = 0);

    void testNonExistentFetch(const string &fname);
    void testNonExistentFetchR(const string &fname);
    void testNonExistentFetchBuf(const string &fname);
    void testNonExistentFetchDupR(const string &fname);
    void testNonExistentFetchStr(const string &fname);
    void testAllBuffers(const string & fname);
    void runSmallDuplicateTest(const string &fname, StoreFetchType fetchType);
    datum fetchData(MDBM *mdbm, datum *keyPtr, StoreFetchType fetchType);
    void createLargeObjectMdbm(MDBM *mdbm);
    void fetchLargeDups(const string &filename, StoreFetchType fetchType);
    void createDupObjectMdbm(MDBM *mdbm, int size = 200);
    void getAllDuplicates(const string &filename, int size = 200);
    void createBigData();
    void createBigTestFiles(int openFlags, const vector<string> &fnames, const vector<int>& pagesizes);
    void compareLargeObjFiles(vector<string> &fnames, const vector<int>& pagesizes, StoreFetchType fetchType);

    void compareLargeData(MDBM *mdbm, datum *keyPtr, StoreFetchType fetchType, const datum &compareTo);
    void compareXLfile(StoreFetchType fetchType);

    FetchTestData data;

    // TODO FIXME find something better to do with these
    void RegisterDatum(int id, const char *value, int numKeys);
    // Returns "how many" vector of integers
    vector<int> GetRegisteredData(int id, std::vector<std::string> & items);
};


void
MdbmFetchUnitTest::RegisterDatum(int id, const char *value, int numKeys)
{
    data.dataItems[id].push_back(string(value));
    data.howManyKeys[id].push_back(numKeys);
}

vector<int>
MdbmFetchUnitTest::GetRegisteredData(int id, std::vector<std::string> & items)
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

const vector<int>& MdbmFetchUnitTest::GetLargeDataPageSizes() {
    return LargeDataPageSizesVectV3;
}

void MdbmFetchUnitTest::setUp() {
    //CPPUNIT_ASSERT( !createMdbmTestDir().empty() );
    CPPUNIT_ASSERT(!GetTmpDir(data.basePath));

    data.fileName = data.basePath + "/" + string("test")+versionString;
    data.fileNameLarge = data.basePath + "/" + string("testL")+versionString;

    CPPUNIT_ASSERT(data.fileName != "");
    CPPUNIT_ASSERT(data.fileNameLarge != "");

    // Files for extensive large object tests
    string name, err;
    const vector<int>& largePageArray = GetLargeDataPageSizes();
    for (unsigned i = 0; i < largePageArray.size(); ++i) {
        name = data.basePath + "/fetchbig" +versionString + "_" + ToStr(i);
        data.largeFiles.push_back(name);
        CPPUNIT_ASSERT(data.largeFiles[i] != "");
    }

    // Create the data and move to the *_cp files
    datum ky, val;
    size_t i;
    for (i = 0; i < dataSizeArrayLen ; ++i) {   // create test data
        ky = CreateTestValue("", smallDataSizes[i], val);
        RegisterDatum(SMALL_OBJ_TESTS, val.dptr, NUM_DATA_ITEMS+i);
    }
}
void MdbmFetchUnitTest::tearDown() {
}

/// Initial setup for all mdbm_fetch* unit tests
void
MdbmFetchUnitTest::initialSetup()
{
    //GetLogStream() << "MdbmFetchUnitTest::" <<__func__<< " calling createData()" << endl<<flush;
    createData(PAGESZ_TSML);

    ////GetLogStream() << "MdbmFetchUnitTest::" <<__func__<< " finished createData()" << endl<<flush;
    //string moveTo = data.fileName + "_cp";
    //CPPUNIT_ASSERT(0 == rename(data.fileName.c_str(), moveTo.c_str()) );

    //// Files for Large Object duplicate/missing key tests
    //moveTo = data.fileNameLarge + "_cp";
    //CPPUNIT_ASSERT(0 == rename(data.fileNameLarge.c_str(), moveTo.c_str()) );


    GetLogStream() << "MdbmFetchUnitTest::" <<__func__<< " calling createBigData()" << endl<<flush;
    createBigData();
    GetLogStream() << "MdbmFetchUnitTest::" <<__func__<< " finished createBigData()" << endl<<flush;
}

/// mdbm_fetch() - Unit Test #H1, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch() to retrieve values of various sizes.
/// Use keys of various sizes.
void
MdbmFetchUnitTest::test_FetchH1()
{
    string prefix = "H1"+versionString;
    TRACE_TEST_CASE(__func__)
    //CopyFile(data.fileName);   // For use by tests H1-H4
    createData(PAGESZ_TSML);
    checkData(data.fileName, PAGESZ_TSML, prefix, DO_FETCH);
}

/// mdbm_fetch() - Unit Test #H2, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch() to retrieve large objects of various sizes.
/// Use keys of various sizes
void
MdbmFetchUnitTest::test_FetchH2()
{
    TRACE_TEST_CASE(__func__)
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes(), DO_FETCH);
//    compareXLfile(DO_FETCH); // V3
}

/// mdbm_fetch() - Unit Test #H3, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch() to retrieve keys that do not exist in the MDBM.
/// No need to call CopyFile(fileName) because test H1 already copied the files
void
MdbmFetchUnitTest::test_FetchH3()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentFetch(data.fileName);

   // Copy file for use by tests H3-H4 and subsequent non-existent
   // and duplicate tests of large objects (generally I3-I4,J3-J4 ...)
    //CopyFile(data.fileNameLarge);
    createData(PAGESZ_TSML);
    testNonExistentFetch(data.fileNameLarge);
}

void
MdbmFetchUnitTest::testNonExistentFetch(const string &fname)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    char key1[32];
    datum fetched1, k1;
    for (int i = 0; i < 10; ++i) {
        k1.dsize = snprintf(key1, sizeof(key1), "more%d", i);  // Some non-existent key
        k1.dptr = key1;
        errno = 0;
        fetched1 = mdbm_fetch(mdbm, k1);

        CPPUNIT_ASSERT(errno != 0);   // fetch also sets errno
        CPPUNIT_ASSERT(fetched1.dptr == NULL);
    }
    errno = 0;
    mdbm_close(mdbm);
}

/// mdbm_fetch() - Unit Test #H4, for MDBM version 2 and 3
/// Populate MDBM with several records pointed to by a single key using MDBM_INSERT_DUP.
/// mdbm_fetch() should retrieve one of the duplicates.
/// No need to call CopyFile because test H1 already copied the files, but unlink when done
void
MdbmFetchUnitTest::test_FetchH4()
{
    TRACE_TEST_CASE(__func__);
    runSmallDuplicateTest(data.fileName, DO_FETCH);
    unlink(data.fileName.c_str());   // Completed mdbm_fetch() tests

    fetchLargeDups(data.fileNameLarge, DO_FETCH);
}


/// mdbm_fetch_r() - Unit Test #I1, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_r() to retrieve values of various sizes.
/// Use keys of various sizes.
void
MdbmFetchUnitTest::test_FetchI1()
{
    string prefix = "I1" + versionString;
    TRACE_TEST_CASE(__func__);
    //CopyFile(data.fileName);   // For use by tests I1-I4
    createData(PAGESZ_TSML);
    checkData(data.fileName, PAGESZ_TSML, prefix, DO_FETCH_R);
}


/// mdbm_fetch_r() - Unit Test #I2, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_r() to retrieve large objects of various sizes.
/// Use keys of various sizes
void
MdbmFetchUnitTest::test_FetchI2()
{
    TRACE_TEST_CASE(__func__);
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes(), DO_FETCH_R);
//    compareXLfile(DO_FETCH_R); // V3
}


/// mdbm_fetch_r() - Unit Test #I3, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_r() to retrieve keys that do not exist in the MDBM.
void
MdbmFetchUnitTest::test_FetchI3()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentFetchR(data.fileName);
    testNonExistentFetchR(data.fileNameLarge);
}

void
MdbmFetchUnitTest::testNonExistentFetchR(const string &fname)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    MDBM_ITER mIter;
    int ret = 0;
    char key1[32];
    datum fetched, k1;

    MDBM_ITER_INIT(&mIter);

    for (int i = 0; i < 10; ++i) {
        k1.dsize = snprintf(key1, sizeof(key1), "etc%d", i);  // Some non-existent key
        k1.dptr = key1;
        errno = 0;
        ret = mdbm_fetch_r(mdbm, &k1, &fetched, &mIter);

        CPPUNIT_ASSERT(ret != 0);
        CPPUNIT_ASSERT(errno != 0);   // fetch sets errno on failure
        CPPUNIT_ASSERT(fetched.dptr == NULL);
    }
    errno = 0;
    mdbm_close(mdbm);
}


/// mdbm_fetch_r() - Unit Test #I4, for MDBM version 2 and 3
/// Populate MDBM with several records pointed to by a single key using MDBM_INSERT_DUP.
/// mdbm_fetch_r() should retrieve one of the duplicates.
/// No need to call CopyFile because test I1 already copied the files, but unlink when done
void
MdbmFetchUnitTest::test_FetchI4()
{
    TRACE_TEST_CASE(__func__);
    runSmallDuplicateTest(data.fileName, DO_FETCH_R);
    unlink(data.fileName.c_str());   // Completed mdbm_fetch_r() tests
    fetchLargeDups(data.fileNameLarge, DO_FETCH_R);
}


/// mdbm_fetch_buf() - Unit Test #J1, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_buf() to retrieve small objects of various sizes.
/// Use keys of various sizes
void
MdbmFetchUnitTest::test_FetchJ1()
{
    string prefix = "J1" + versionString;
    //CopyFile(data.fileName);   // For use by tests J1-J4
    createData(PAGESZ_TSML);
    checkData(data.fileName, PAGESZ_TSML, prefix, DO_FETCH_BUF);
}


/// mdbm_fetch_buf() - Unit Test #J2, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_buf() to retrieve large objects of various sizes.
/// Use keys of various sizes
void
MdbmFetchUnitTest::test_FetchJ2()
{
    TRACE_TEST_CASE(__func__);
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes(), DO_FETCH_BUF);
//    compareXLfile(DO_FETCH_BUF); // V3
}


/// mdbm_fetch_buf() - Unit Test #J3, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_buf() to retrieve keys that do not exist in the MDBM.
void
MdbmFetchUnitTest::test_FetchJ3()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentFetchBuf(data.fileName);
    testNonExistentFetchBuf(data.fileNameLarge);
}

void
MdbmFetchUnitTest::testNonExistentFetchBuf(const string &fname)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    int ret = 0;
    char key1[32], *buf = (char*)malloc(PAGESZ_TSML); // must be malloc'ed, mdbm may realloc() it
    datum fetched, k1, bufd;

    CPPUNIT_ASSERT(buf != NULL);

    for (int i = 0; i < 10; ++i) {
        bufd.dptr = buf;
        bufd.dsize = PAGESZ_TSML;
        k1.dsize = snprintf(key1, sizeof(key1), "other%d", i);  // Some non-existent key
        k1.dptr = key1;
        errno = 0;
        ret = mdbm_fetch_buf(mdbm, &k1, &fetched, &bufd, 0);

        CPPUNIT_ASSERT(ret != 0);
        CPPUNIT_ASSERT(errno != 0);   // fetch also sets errno
        CPPUNIT_ASSERT(fetched.dptr == NULL);
    }
    errno = 0;
    mdbm_close(mdbm);
    free(buf);
}


/// mdbm_fetch_buf() - Unit Test #J4, for MDBM version 2 and 3
/// Populate MDBM with several records pointed to by a single key using MDBM_INSERT_DUP.
/// mdbm_fetch_buf() should retrieve one of the duplicates into a buffer.
void
MdbmFetchUnitTest::test_FetchJ4()
{
    TRACE_TEST_CASE(__func__);
    runSmallDuplicateTest(data.fileName, DO_FETCH_BUF);
    fetchLargeDups(data.fileNameLarge, DO_FETCH_BUF);
}


/// mdbm_fetch_buf() - Unit Test #J5, for MDBM version 2 and 3
/// Using a pre-populated MDBM, call mdbm_fetch_buf() to retrieve records into various buffer sizes,
/// including buffers with zero and negative sizes.
void
MdbmFetchUnitTest::test_FetchJ5()
{
    TRACE_TEST_CASE(__func__);
    testAllBuffers(data.fileName);
    unlink(data.fileName.c_str());   // Completed mdbm_fetch_buf() tests
}

/// Test buffers with zero and negative sizes and legal sizes.
/// bNew documentation says that this should succeed ecause mdbm_fetch_buf
/// calls realloc if the buffer is too small.
void
MdbmFetchUnitTest::testAllBuffers(const string &fname)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    vector<string> dataItems;
    vector<int> howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);
    vector<string>::iterator datait = dataItems.begin();

    CPPUNIT_ASSERT( datait != dataItems.end());
    string key = PREFIX + ToStr(0) + datait->c_str();

    int ret = 0;
    datum fetched, k1, bufd;

    for (int i = 0; i < 2; ++i) {
        bufd.dptr = (char*)malloc(PAGESZ_TSML); // must be malloc'ed, mdbm may realloc() it
        CPPUNIT_ASSERT(bufd.dptr != NULL); // Should be a valid ptr, otherwise will segv
        bufd.dsize = 0 - i;  // Try zero and -1
        k1.dsize = key.size() + 1;
        k1.dptr = (char*) key.c_str();
        errno = 0;
        ret = mdbm_fetch_buf(mdbm, &k1, &fetched, &bufd, 0);

        //CPPUNIT_ASSERT(errno != 0);   // fetch used to fail and sets errno
        CPPUNIT_ASSERT_EQUAL(0, ret);
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        free(bufd.dptr);
    }

    for (int siz = 10; siz < PAGESZ_TSML; siz *= 5) {
        bufd.dptr = (char*)malloc(PAGESZ_TSML); // must be malloc'ed, mdbm may realloc() it
        CPPUNIT_ASSERT(bufd.dptr != NULL); // Should be a valid ptr, otherwise will segv
        bufd.dsize = siz;
        k1.dsize = key.size() + 1;
        k1.dptr = (char*) key.c_str();
        errno = 0;
        ret = mdbm_fetch_buf(mdbm, &k1, &fetched, &bufd, 0);

        CPPUNIT_ASSERT_EQUAL(0, ret);
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        free(bufd.dptr);
    }

    errno = 0;
    mdbm_close(mdbm);
}

/// mdbm_fetch_dup_r() - Unit Test #K1, for MDBM version 2 and 3
/// Populate MDBM with duplicate values pointed to by the same key, then call mdbm_fetch_dup_r() to
/// retrieve values of various sizes. Use keys of various sizes.
void
MdbmFetchUnitTest::test_FetchK1()
{
    TRACE_TEST_CASE(__func__);
    MDBM *mdbm;
    int openFlags = MDBM_O_FSYNC | versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;
    // Create small object MDBM for dup_r testing
    mdbm = EnsureNamedMdbm(data.fileName, openFlags, 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT(mdbm != NULL);
    createDupObjectMdbm(mdbm);

    getAllDuplicates(data.fileName);
}


/// mdbm_fetch_dup_r() - Unit Test #K2, for MDBM version 2 and 3
/// Populate MDBM, call mdbm_fetch_dup_r() to retrieve large duplicate objects of various sizes.
/// Use records of various sizes per key: the same key may point to duplicates that are only large
/// records or to a mix of small and large records.
void
MdbmFetchUnitTest::test_FetchK2()
{
    TRACE_TEST_CASE(__func__);

    MDBM *mdbm;
    int openFlags = MDBM_O_FSYNC | MDBM_LARGE_OBJECTS | versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;
    // Create small object MDBM for dup_r testing
    mdbm = EnsureNamedMdbm(data.fileName, openFlags, 0644, PAGESZ_TLRG, 0);
    CPPUNIT_ASSERT(mdbm != NULL);
    createDupObjectMdbm(mdbm, SIZE_DUP_R_TEST);

    getAllDuplicates(data.fileName, SIZE_DUP_R_TEST);
}


/// mdbm_fetch_dup_r() - Unit Test #K3, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_dup_r() to retrieve keys that do not exist in the MDBM.
void
MdbmFetchUnitTest::test_FetchK3()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentFetchDupR(data.fileName);
    unlink(data.fileName.c_str());   // cleanup
}


void
MdbmFetchUnitTest::testNonExistentFetchDupR(const string &fname)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    MDBM_ITER mIter;
    int ret = 0;
    char key1[32];
    datum fetched, k1;

    MDBM_ITER_INIT(&mIter);

    for (int i = 0; i < 10; ++i) {
        k1.dsize = snprintf(key1, sizeof(key1), "yawn%d", i);  // Some non-existent key
        k1.dptr = key1;
        errno = 0;
        ret = mdbm_fetch_dup_r(mdbm, &k1, &fetched, &mIter);

        CPPUNIT_ASSERT(ret != 0);
        CPPUNIT_ASSERT(errno != 0);   // fetch also sets errno
        CPPUNIT_ASSERT(fetched.dptr == NULL);
    }
    errno = 0;
    mdbm_close(mdbm);
}


/// mdbm_fetch_str() - Unit Test #L1, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_str() to retrieve strings of various sizes.
void
MdbmFetchUnitTest::test_FetchL1()
{
    TRACE_TEST_CASE(__func__);
    string prefix = "L1" + versionString;
    //CopyFile(data.fileName);   // For use by tests L1-L4
    createData(PAGESZ_TSML);
    checkData(data.fileName, PAGESZ_TSML, prefix, DO_FETCH_STR);
}


/// mdbm_fetch_str() - Unit Test #L2, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_str() to retrieve large strings of various sizes.
void
MdbmFetchUnitTest::test_FetchL2()
{
    TRACE_TEST_CASE(__func__);
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes(), DO_FETCH_STR);
//    compareXLfile(DO_FETCH_STR); // V3
}


/// mdbm_fetch_str() - Unit Test #L3, for MDBM version 2 and 3
/// Populate MDBM, then call mdbm_fetch_str() to retrieve keys that do not exist in the MDBM.
void
MdbmFetchUnitTest::test_FetchL3()
{
    TRACE_TEST_CASE(__func__);
    testNonExistentFetchStr(data.fileName);
    testNonExistentFetchStr(data.fileNameLarge);
}

void
MdbmFetchUnitTest::testNonExistentFetchStr(const string &fname)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    char key1[32], *ret;
    for (int i = 0; i < 10; ++i) {
        snprintf(key1, sizeof(key1), "some%d", i);  // Some non-existent key
        errno = 0;
        ret = mdbm_fetch_str(mdbm, key1);

        CPPUNIT_ASSERT(errno != 0);   // fetch also sets errno
        CPPUNIT_ASSERT(ret == NULL);
    }
    errno = 0;
    mdbm_close(mdbm);
}


/// mdbm_fetch_str() - Unit Test #L4, for MDBM version 2 and 3
/// Populate MDBM with duplicate strings. mdbm_fetch_str() should retrieve one of the duplicates.
void
MdbmFetchUnitTest::test_FetchL4()
{
    TRACE_TEST_CASE(__func__);
    runSmallDuplicateTest(data.fileName, DO_FETCH_STR);
    unlink(data.fileName.c_str());   // done
    fetchLargeDups(data.fileNameLarge, DO_FETCH_STR);
}


/// mdbm_fetch_info() - Unit Test #M1, for MDBM version 2
/// Test M1, For MDBM version 2 is a negative test since mdbm_fetch_info should fail on a V2 DB
void
MdbmFetchUnitTest::test_FetchM1_v2()
{
    TRACE_TEST_CASE(__func__);
    // MDBM Version 2 does not support mdbm_fetch_info
    // Verify that mdbm_fetch_info does fail
    MDBM_ITER mIter;
    datum fetched, bufd;
    int ret = 0;
    char buf[PAGESZ_TSML];
    struct mdbm_fetch_info info;

    //CopyFile(data.fileName);   // For use by tests M1-M5
    createData(PAGESZ_TSML);

    MDBM * mdbm =  EnsureNamedMdbm(data.fileName.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    MDBM_ITER_INIT(&mIter);

    datum key = {(char*)"stuff", 5};
    fetched.dptr = 0;
    fetched.dsize = 0;
    bufd.dptr = buf;
    bufd.dsize = PAGESZ_TSML;

    ret = mdbm_fetch_info( mdbm, &key, &fetched, &bufd, &info, &mIter );

    CPPUNIT_ASSERT( ret != 0 );
    CPPUNIT_ASSERT( errno != 0 );
}

/// mdbm_fetch_info() - Unit Test #M1, for MDBM version 3
/// Populate MDBM, then call mdbm_fetch_info() to retrieve values of various sizes. Use keys of various sizes
void
MdbmFetchUnitTest::test_FetchM1()
{
    TRACE_TEST_CASE(__func__);
    //CopyFile(data.fileName);   // For use by tests M1-M5
    createData(PAGESZ_TSML);
    checkData(data.fileName, PAGESZ_TSML, "M1v3", DO_FETCH_INFO);
}


/// mdbm_fetch_info() - Unit Test #M2, for MDBM version 3
/// Populate MDBM, then call mdbm_fetch_info() to retrieve large objects of various sizes.
void
MdbmFetchUnitTest::test_FetchM2()
{
    TRACE_TEST_CASE(__func__);
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes(), DO_FETCH_INFO);
//    compareXLfile(DO_FETCH_INFO);
}


/// mdbm_fetch_info() - Unit Test #M3, for MDBM version 3
/// Populate MDBM, then call mdbm_fetch_info() to retrieve objects that do not exist in the MDBM.
void
MdbmFetchUnitTest::test_FetchM3()
{
    TRACE_TEST_CASE(__func__);
    MDBM * mdbm =  EnsureNamedMdbm(data.fileName.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    int ret = 0;
    char key1[32], buf[PAGESZ_TSML];
    datum fetched, k1, bufd;
    struct mdbm_fetch_info info;
    MDBM_ITER mIter;
    string msg;

    bufd.dptr = buf;
    bufd.dsize = PAGESZ_TSML;
    MDBM_ITER_INIT(&mIter);

    for (int i = 0; i < 10; ++i) {
        k1.dsize = snprintf(key1, sizeof(key1), "what%d", i);  // Some non-existent key
        k1.dptr = key1;
        errno = 0;
        ret = mdbm_fetch_info(mdbm, &k1, &fetched, &bufd, &info, &mIter);

        CPPUNIT_ASSERT(ret != 0);
        CPPUNIT_ASSERT(errno != 0);   // fetch also sets errno
        msg = string("Should not find key ") + key1;
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), fetched.dptr == NULL);
    }
    errno = 0;
    mdbm_close(mdbm);
}


/// mdbm_fetch_info() - Unit Test #M4, for MDBM version 3
/// Populate MDBM with duplicate records. Call mdbm_fetch_info() to retrieve one of the duplicates.
void
MdbmFetchUnitTest::test_FetchM4()
{
    TRACE_TEST_CASE(__func__);
    runSmallDuplicateTest(data.fileName, DO_FETCH_INFO);
}


/// mdbm_fetch_info() - Unit Test #M5, for MDBM version 3
/// Using a pre-populated MDBM, call mdbm_fetch_info() to retrieve records
/// into invalid buffer sizes. Buffers with zero and negative sizes.
/// New documentation says that this should succeed because
/// mdbm_fetch_info calls realloc if the buffer is too small.

void
MdbmFetchUnitTest::test_FetchM5()
{
    TRACE_TEST_CASE(__func__);
    MDBM * mdbm =  EnsureNamedMdbm(data.fileName.c_str(), MDBM_O_RDONLY , 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    vector<string> dataItems;
    vector<int> howMany;
    howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);
    vector<string>::iterator datait = dataItems.begin();

    CPPUNIT_ASSERT(datait != dataItems.end());
    string key = PREFIX + ToStr(0) + datait->c_str();

    int ret = 0;
    datum fetched, k1, bufd;
    struct mdbm_fetch_info info;
    MDBM_ITER mIter;
    MDBM_ITER_INIT(&mIter);

    for (int i = 0; i < 2; ++i) {
        bufd.dptr = (char*)malloc(PAGESZ_TSML); // must be malloc'ed, mdbm may realloc() it
        CPPUNIT_ASSERT(bufd.dptr != NULL); // Should be a valid ptr, otherwise will segv
        bufd.dsize = 0 - i;  // Try zero and -1
        k1.dsize = key.size() + 1;
        k1.dptr = (char*) key.c_str();
        errno = 0;
        ret = mdbm_fetch_info(mdbm, &k1, &fetched, &bufd, &info, &mIter);

        CPPUNIT_ASSERT_EQUAL(0, ret);
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        free(bufd.dptr);
    }

    for (int siz = 10; siz < PAGESZ_TSML; siz *= 5) {
        bufd.dptr = (char*)malloc(PAGESZ_TSML); // must be malloc'ed, mdbm may realloc() it
        CPPUNIT_ASSERT(bufd.dptr != NULL); // Should be a valid ptr, otherwise will segv
        bufd.dsize = siz;
        k1.dsize = key.size() + 1;
        k1.dptr = const_cast<char *> (key.c_str());
        errno = 0;
        ret = mdbm_fetch_info(mdbm, &k1, &fetched, &bufd, &info, &mIter);

        CPPUNIT_ASSERT_EQUAL(0, ret);
        //CPPUNIT_ASSERT_EQUAL(0, errno);
        free(bufd.dptr);
    }

    errno = 0;
    mdbm_close(mdbm);
}


void
MdbmFetchUnitTest::finalCleanup()
{
    TRACE_TEST_CASE(__func__);
    //string cmd ( "rm -f " );
    //cmd += mdbmBasepath() + string("/fetch*.mdbm*");
    //system(cmd.c_str());
    CleanupTmpDir();
}

/// ------------------------------------------------------------------------------
///
/// Helper methods
///
/// ------------------------------------------------------------------------------


/// Create test data made of small objects.  Then create large object data for duplicate and missing
/// key tests for the H3-L3 and H4-L4 series of tests (mdbm3 and mdbm4).
void
MdbmFetchUnitTest::createData(int pageszSml )
{
    // Make sure all data is on disk
    int openFlags = MDBM_O_FSYNC | versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;


    // Create mdbm
    {
        MdbmHolder mdbm(EnsureNamedMdbm(data.fileName, openFlags, 0644, pageszSml, 0));
        //CPPUNIT_ASSERT(mdbm != NULL);
        storeOrCheckData(mdbm, versionString+" small", DO_STORE, SMALL_OBJ_TESTS);
    }

    // Create mdbm for large object tests of duplicate keys and missing keys
    {
      openFlags |=  MDBM_LARGE_OBJECTS;
      MdbmHolder mdbm(EnsureNamedMdbm(data.fileNameLarge, openFlags, 0644, PAGESZ_TLRG, 0));
      //CPPUNIT_ASSERT(mdbm != NULL);
      createLargeObjectMdbm(mdbm);
    }
}

/// Test to see that data stored by createData remains the same: use various fetch* functions
void
MdbmFetchUnitTest::checkData(const string &filename, int pagesz, const string &testName, StoreFetchType fetchType)
{
    MdbmHolder mdbm(mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, pagesz, 0));
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);

    storeOrCheckData(mdbm, testName, fetchType);
}

/// Loop through and either store the data or fetch it via the various fetch* functions
void
MdbmFetchUnitTest::storeOrCheckData(MDBM *mdbm, const string& testName, StoreFetchType storeFetch, int id)
{
    int numKys;
    datum ky, val, fetched;
    string key;
    int testCount = 0;
    vector<string> dataItems;
    vector<int> howMany;
    string msg;
    int ret;

    howMany = GetRegisteredData(id, dataItems);
    CPPUNIT_ASSERT( dataItems.size() != 0);

    vector<string>::iterator datait = dataItems.begin();
    vector<int>::iterator numit = howMany.begin();
    for (; datait != dataItems.end(); ++datait, ++numit) {
        CPPUNIT_ASSERT(numit != howMany.end());   // stop if data error
        numKys = *numit;
        val.dptr = (char *) datait->c_str();
        val.dsize = datait->size() + 1;  // Store the null char
        for (int j=0; j < numKys; ++j) {
            key = PREFIX + ToStr(j) + datait->c_str();
            ky.dptr = (char *) key.c_str();
            ky.dsize = key.size() + 1;
            errno = 0;
            if (storeFetch == DO_STORE) {
//cerr << "Storing ksize=" << ky.dsize << " vsize=" << val.dsize << endl << flush;
                msg = string("Storing ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
                ret = mdbm_store( mdbm, ky, val, MDBM_REPLACE);
                if (ret) { cerr << "Error: " << msg << ", errno: "<< errno << " - " <<strerror(errno) << endl << flush; }
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(), ret == 0);
            } else {
                fetched = fetchData(mdbm, &ky, storeFetch);
                msg = string("Fetching test ") + testName + " key= " + ky.dptr;
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(),  fetched.dptr != NULL);
                msg += string(" val= ") + val.dptr;
                ret = memcmp(fetched.dptr, val.dptr, val.dsize);
                if (ret) { cerr << "Error: " << msg << endl << flush; }
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(), ret == 0);
            }
            ++testCount;
        }
    }

//    if (storeFetch) {
//        fprintf(stderr, "Test %s performed %d fetches\n", testName, testCount);
//    }
}

/// Insert duplicates and make sure you fetch one of the duplicates using the fetch* functions
void
MdbmFetchUnitTest::runSmallDuplicateTest(const string &fname, StoreFetchType fetchType)
{
    string key, firstDup, secondDup;
    datum ky, val, fetch;
    vector<string> dataItems;
    vector<int> howMany;

    howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);

    vector<string>::iterator datait = dataItems.begin();
    CPPUNIT_ASSERT( datait != dataItems.end());  // stop if no data

    MdbmHolder mdbm(mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, PAGESZ_TSML, 0));
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);

    errno = 0;

    firstDup =  string("BeginData") + *datait + string("EndData");
    secondDup = string("SecondDuplicate") + *datait + string("TailData");

    // Insert the 2 duplicates for each data item size
    for (; datait != dataItems.end(); ++datait) {
        key = PREFIX + ToStr(0) + datait->c_str();
        ky.dptr = (char *) key.c_str();
        ky.dsize = key.size() + 1;
        val.dptr = (char *) firstDup.c_str();
        val.dsize = firstDup.size() + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));
        val.dptr = (char *) secondDup.c_str();
        val.dsize = secondDup.size() + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));

        fetch = fetchData(mdbm, &ky, fetchType);

        // Fetch data back and see if it is one of the 3 possibilities:
        // *datait, firstDup, or secondDup
        int found = (memcmp( fetch.dptr, datait->c_str(), datait->size()) == 0);
        found = found || (firstDup == fetch.dptr);
        found = found || (secondDup == fetch.dptr);
        CPPUNIT_ASSERT(found != 0);
    }

}

/// Call the various fetch* functions to retrieve data from the MDBM
datum
MdbmFetchUnitTest::fetchData(MDBM *mdbm, datum *keyPtr, StoreFetchType fetchType)
{
    static bool firstTime = true;
    static MDBM_ITER mIter;
    datum fetched, bufd;
    int ret = 0;
    static char *buf = NULL; // TODO FIXME leak here
    char *str1;
    string err;
    struct mdbm_fetch_info info;

    if (firstTime) {
        buf = (char *)malloc(PAGESZ_TSML);
        CPPUNIT_ASSERT(buf != NULL);
        MDBM_ITER_INIT(&mIter);
        firstTime = false;
    }

    fetched.dptr = 0;
    fetched.dsize = 0;
    bufd.dptr = buf;
    bufd.dsize = PAGESZ_TSML;

    switch ( fetchType ) {
        case DO_FETCH:
            fetched = mdbm_fetch( mdbm, *keyPtr );
            break;
        case DO_FETCH_R:
            ret = mdbm_fetch_r( mdbm, keyPtr, &fetched, &mIter );
            break;
        case DO_FETCH_BUF:
            ret = mdbm_fetch_buf( mdbm, keyPtr, &fetched, &bufd, 0 );
            CPPUNIT_ASSERT_EQUAL(0, memcmp(fetched.dptr, bufd.dptr, fetched.dsize) );
            break;
        case DO_FETCH_STR:
            str1 = mdbm_fetch_str( mdbm, keyPtr->dptr );
            if (NULL != (fetched.dptr = str1)) {  // assign and check for NULL
                fetched.dsize = strlen(str1) + 1;
            } else {
                err = string("Cannot find key ") + keyPtr->dptr;
            }
            CPPUNIT_ASSERT_MESSAGE(err.c_str(), str1 != NULL);
            break;
        case DO_FETCH_INFO:
            ret = mdbm_fetch_info( mdbm, keyPtr, &fetched, &bufd, &info, &mIter );
            CPPUNIT_ASSERT_EQUAL(0, memcmp(fetched.dptr, bufd.dptr, fetched.dsize) );
            break;
        default:
            CPPUNIT_ASSERT_EQUAL( 0, 1 );
    }

    err = string("Failed to find Key ") + keyPtr->dptr + ", return value set to " + ToStr(ret);
    err += ", errno set to " + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);
    //err = string("Failed to find Key ") + keyPtr->dptr + ", errno set to " + ToStr(errno);
    //CPPUNIT_ASSERT_MESSAGE(err.c_str(), errno == 0);

    return (fetched);
}

// Creates a large object MDBM exclusively for the duplicate and missing key tests
void
MdbmFetchUnitTest::createLargeObjectMdbm(MDBM *mdbm)
{
    char buf[256];
    int i;
    // Keys must be smaller than the page size, so I won't use the ky return value
    datum ky, val;
    ky = CreateTestValue("",  2000, val);
    ++val.dsize;  // store the null
    for (i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
    }

    // Insert large duplicates
    ky = CreateTestValue("", 5000, val);
    ++val.dsize;  // store the null
    for (i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));
    }

    // Insert small duplicates
    val.dptr = const_cast<char *>(SOME_TEST_VALUE);
    val.dsize = strlen(SOME_TEST_VALUE) + 1;
    for (i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));
    }
}

// Make sure duplicates of large objects are fetched correctly
void
MdbmFetchUnitTest::fetchLargeDups(const string &filename, StoreFetchType fetchType)
{
    char buf[256];
    datum ky, val;
    int nullOffset = 1;

    if (fetchType == DO_FETCH_STR) {
        nullOffset = 0;
    }

    ky = CreateTestValue("",  2000, val);
    string longstr1 ( val.dptr );
    ky = CreateTestValue("",  5000, val);
    string longstr2 = ( val.dptr );

    MdbmHolder mdbm(mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, PAGESZ_TLRG, 0));
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);

    errno = 0;
    for (int i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + nullOffset;
        val = fetchData(mdbm, &ky, fetchType);

        int found = (strcmp( val.dptr, SOME_TEST_VALUE) == 0);
        found = found || (longstr1 == val.dptr);
        found = found || (longstr2 == val.dptr);
        CPPUNIT_ASSERT(found != 0);
    }

}

// Creates a small object MDBM exclusively for the mdbm_fetch_dup_r testing
void
MdbmFetchUnitTest::createDupObjectMdbm(MDBM *mdbm, int size)
{
    char key[64], buf[SIZE_DUP_R_TEST*2];
    int i, ditems;

    datum ky, val;
    // Keys must be smaller than the page size, so I won't use the ky return value
    ky = CreateTestValue("",  size, val);
    char *baselineData = val.dptr;

    errno = 0;
    for (ditems = 0; ditems < NUM_DATA_ITEMS; ++ditems) {
        ky.dptr = key;
        // The format is "DigitBaselineDigit", as in 0blahblah0, 1blahblah1...
        ky.dsize = snprintf(key, sizeof(key), "%s%d", SIMPLE_KEY_PREFIX, ditems) + 1;
        for (i=0; i < 10; ++i) {
            val.dsize = snprintf(buf, sizeof(buf), "%d%s%d", i, baselineData, i) + 1;  // store the null
            val.dptr = buf;
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));
            //CPPUNIT_ASSERT_EQUAL(0, errno);
        }
    }

    mdbm_close(mdbm);
}

void
MdbmFetchUnitTest::getAllDuplicates(const string &filename, int size)
{
    char key[64];
    string msg;
    int count, ditems;
    datum ky, val;
    MDBM_ITER iter;

    char beginChar, endChar;
    ky = CreateTestValue("",  size, val);
    char *baselineData = val.dptr;
    int baselen = strlen(val.dptr);

    MdbmHolder mdbm(mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, PAGESZ_TSML, 0));
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);
    errno = 0;

    for (ditems = 0; ditems < NUM_DATA_ITEMS; ++ditems) {
        ky.dptr = key;
        ky.dsize = snprintf(key, sizeof(key), "%s%d", SIMPLE_KEY_PREFIX, ditems) + 1;
        count = 0;
        MDBM_ITER_INIT(&iter);
        while (mdbm_fetch_dup_r(mdbm, &ky, &val, &iter) == 0) {
            // Verify correct length, then verify digits at the end, then the value's body
            msg = string("Invalid length ") + ToStr(val.dsize) + string(" for key ") + ky.dptr;
            CPPUNIT_ASSERT_MESSAGE(msg.c_str(), val.dsize == (baselen+3));
            beginChar = *(val.dptr);
            msg = string("Invalid first character ") + beginChar + string(" for key ") + ky.dptr;
            CPPUNIT_ASSERT_MESSAGE(msg.c_str(), beginChar >= '0' && beginChar <= '9');
            endChar = *(val.dptr + val.dsize - 1);
            msg = string("Invalid last character ") + endChar + string(" for key ") + ky.dptr;
            CPPUNIT_ASSERT_MESSAGE(msg.c_str(), endChar >= '0' || endChar <= '9');
            msg = string("Invalid value body ") + (val.dptr+1) + " for key " + ky.dptr;
            CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(val.dptr+1, baselineData, baselen) == 0);
            ++count;
        }
        msg = string("Found ") + ToStr(count) + string(" values for key ") + ky.dptr;
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), count == 10);
    }
}


// Create large object data for tests H2, I2, J2, L2 and M2
void
MdbmFetchUnitTest::createBigData()
{
    int openFlags = MDBM_LARGE_OBJECTS | MDBM_O_RDWR | MDBM_O_FSYNC | versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC;
    createBigTestFiles(openFlags, data.largeFiles, GetLargeDataPageSizes());

    if (versionFlag & MDBM_CREATE_V3) {
      // create 16MB page-size MDBM
      MDBM *mdbm;
      int lastEntry = v3LargePageSizeArrayLen - 1;
      int pgsize = v3LargeDataPageSizes[lastEntry];
      mdbm = EnsureNamedMdbm(data.largeFiles[lastEntry],  openFlags, 0644, pgsize, 0);
      //CPPUNIT_ASSERT(mdbm != NULL);
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
          CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
          ////CPPUNIT_ASSERT_EQUAL(0, errno);
      }

      errno = 0;
      ky.dptr = key;  // Now create the very large object
      ky.dsize = snprintf(key, sizeof(key), "%s11", SIMPLE_KEY_PREFIX) + 1;
      val.dsize = (pgsize * 10);  // No null stored
      val.dptr = baselineData;
      CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
      //CPPUNIT_ASSERT_EQUAL(0, errno);

      mdbm_close(mdbm);
    }
}


void
MdbmFetchUnitTest::createBigTestFiles(int openFlags, const vector<string> &fnames, const vector<int>& pagesizes)
{
    MDBM *mdbm;
    char key[64];
    int pgsize;
    int maxData = pagesizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DELTA;

    datum ky, val;
    ky = CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pagesizes[filei];
        int flags = openFlags | versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;
        mdbm = EnsureNamedMdbm(fnames[filei], flags, 0644, pgsize, 0);
        CPPUNIT_ASSERT(mdbm != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            errno = 0;
            ky.dptr = key;   // Store small objects for test purposes
            ky.dsize = snprintf(key, sizeof(key), "%s%d", SIMPLE_KEY_PREFIX, j) + 1;
            val.dsize = pgsize / 2;
            val.dptr = baselineData;
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, ky, val, MDBM_REPLACE));
            //CPPUNIT_ASSERT_EQUAL(0, errno);

            errno = 0;
            ky.dptr = key;  // Now create large objects
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            val.dptr = baselineData;
            val.dsize = pgsize + additionalDataSizes[j % 4];  // without null first
            int siz = val.dsize;
            char temp = baselineData[siz];    // Store original
            baselineData[siz] = '\0';    // Store null
            ++(val.dsize);
            string err = string("Failed to Store Large Obj: Key=") + ky.dptr
              + string(", out of ") + ToStr(MAX_NUM_LDATA)
              + string(" value_size=") + ToStr(siz)
              + string(" page_size=") + ToStr(pgsize)
              + string(" filename=") + fnames[filei];
            CPPUNIT_ASSERT_MESSAGE(err, 0 == mdbm_store(mdbm, ky, val, MDBM_REPLACE));
            //CPPUNIT_ASSERT_EQUAL(0, errno);
            baselineData[siz] = temp; // restore original
        }
        mdbm_close(mdbm);
    }
}

/// compare large object files passed in the vector fnames to the expected values,
/// using appropriate fetch type
void
MdbmFetchUnitTest::compareLargeObjFiles(vector<string> &fnames, const vector<int>& pagesizes, StoreFetchType fetchType)
{
    char key[64];
    int pgsize;
    int maxData = pagesizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DELTA;

    datum ky, val;
    ky = CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pagesizes[filei];

        MdbmHolder mdbm(mdbm_open(fnames[filei].c_str(), MDBM_O_RDONLY, 0644, pgsize, 0));
        CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            ky.dptr = key;
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            val.dptr = baselineData;
            val.dsize = pgsize + additionalDataSizes[j % 4];
            int siz = val.dsize;
            char temp = baselineData[siz];    // Store original
            baselineData[siz] = '\0';    // Store null
            ++(val.dsize);
            compareLargeData(mdbm, &ky, fetchType, val);
            baselineData[siz] = temp; // restore original
        }
    }
}

/// Call the various fetch* functions to retrieve Large Objects from the MDBM, then compare data to
/// the compareTo datum
void
MdbmFetchUnitTest::compareLargeData(MDBM *mdbm, datum *keyPtr, StoreFetchType fetchType, const datum &compareTo)
{
    static bool firstTime = true;
    static MDBM_ITER mIter;
    datum fetched, bufd;
    int ret = 0;
    static char *buf = NULL;
    char *str1;
    struct mdbm_fetch_info info;

    if (firstTime) {
        MDBM_ITER_INIT(&mIter);
        buf = (char *) malloc(LARGEST_V3_PAGE*10 + 1);
        firstTime = false;
    }
    CPPUNIT_ASSERT(buf != NULL);

    fetched.dptr = 0;
    fetched.dsize = 0;
    bufd.dptr = buf;
    bufd.dsize = LARGEST_V3_PAGE*10 + 1;
    string err;

    errno = 0;
    switch ( fetchType ) {
        case DO_FETCH:
            fetched = mdbm_fetch(mdbm, *keyPtr);
            break;
        case DO_FETCH_R:
            ret = mdbm_fetch_r(mdbm, keyPtr, &fetched, &mIter);
            break;
        case DO_FETCH_BUF:
            ret = mdbm_fetch_buf(mdbm, keyPtr, &fetched, &bufd, 0);
            CPPUNIT_ASSERT(fetched.dptr != NULL);
            CPPUNIT_ASSERT_EQUAL(0, memcmp(fetched.dptr, bufd.dptr, fetched.dsize));
            break;
        case DO_FETCH_STR:
            str1 = mdbm_fetch_str(mdbm, keyPtr->dptr);
            if (NULL != (fetched.dptr = str1)) {  // assign and check for NULL
                fetched.dsize = strlen(str1) + 1;
            } else {
                err = string("Cannot find key ") + keyPtr->dptr;
            }
            CPPUNIT_ASSERT_MESSAGE(err.c_str(), str1 != NULL);
            break;
        case DO_FETCH_INFO:
            ret = mdbm_fetch_info( mdbm, keyPtr, &fetched, &bufd, &info, &mIter );
            CPPUNIT_ASSERT(fetched.dptr != NULL);
            CPPUNIT_ASSERT_EQUAL(0, memcmp(fetched.dptr, bufd.dptr, fetched.dsize) );
            break;
        default:
            CPPUNIT_ASSERT_EQUAL( 0, 1 );
    }


    CPPUNIT_ASSERT(keyPtr);
    CPPUNIT_ASSERT(keyPtr->dptr);
    err = string("Failed to find Key ") + keyPtr->dptr + ", return value set to " + ToStr(ret);
    err += ", errno set to " + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);
    err = string("Failed to find Key ") + keyPtr->dptr + ", errno set to " + ToStr(errno);
    //CPPUNIT_ASSERT_MESSAGE(err.c_str(), errno == 0);

    err = string("Size of key ") + keyPtr->dptr + " is " + ToStr(fetched.dsize) +
          " but should be " + ToStr(compareTo.dsize);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), fetched.dsize == compareTo.dsize);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), fetched.dptr != NULL);
    err = string("Key ") + keyPtr->dptr + " contains data different than intended";
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), memcmp(fetched.dptr,compareTo.dptr, compareTo.dsize) == 0);
}


// Run fetch tests on 16MB page-size MDBM
void
MdbmFetchUnitTest::compareXLfile(StoreFetchType fetchType)
{
    int lastEntry = v3LargePageSizeArrayLen - 1;
    int pgsize = v3LargeDataPageSizes[lastEntry];

    MdbmHolder mdbm(mdbm_open(data.largeFiles[lastEntry].c_str(), MDBM_O_RDONLY, 0644, pgsize, 0));
    CPPUNIT_ASSERT(NULL != (MDBM*)mdbm);

    datum ky, val;
    ky = CreateTestValue("",  pgsize * 10, val);
    char *baselineData = val.dptr;
    char key[64];
    errno = 0;
    ky.dptr = key;
    ky.dsize = snprintf(key, sizeof(key), "%s11", SIMPLE_KEY_PREFIX) + 1;
    val.dsize = (pgsize * 10);  // No null stored,
    if (fetchType == DO_FETCH_STR)
        ++val.dsize;
    val.dptr = baselineData;
    compareLargeData(mdbm, &ky, fetchType, val);
    mdbm_close(mdbm);
}





class MdbmFetchUnitTestV3 : public MdbmFetchUnitTest
{
  CPPUNIT_TEST_SUITE(MdbmFetchUnitTestV3);
    CPPUNIT_TEST(initialSetup);
    CPPUNIT_TEST(test_FetchH1);
    CPPUNIT_TEST(test_FetchH2);
    CPPUNIT_TEST(test_FetchH3);
    CPPUNIT_TEST(test_FetchH4);
    CPPUNIT_TEST(test_FetchI1);
    CPPUNIT_TEST(test_FetchI2);
    CPPUNIT_TEST(test_FetchI3);
    CPPUNIT_TEST(test_FetchI4);
    CPPUNIT_TEST(test_FetchJ1);
    CPPUNIT_TEST(test_FetchJ2);
    CPPUNIT_TEST(test_FetchJ3);
    CPPUNIT_TEST(test_FetchJ4);
    CPPUNIT_TEST(test_FetchJ5);
    CPPUNIT_TEST(test_FetchK1);
    CPPUNIT_TEST(test_FetchK2);
    CPPUNIT_TEST(test_FetchK3);
    CPPUNIT_TEST(test_FetchL1);
    CPPUNIT_TEST(test_FetchL2);
    CPPUNIT_TEST(test_FetchL3);
    CPPUNIT_TEST(test_FetchL4);
    CPPUNIT_TEST(test_FetchM1);   // Actually test: V3 only
    CPPUNIT_TEST(test_FetchM2);   // Test M2 - V3 only
    CPPUNIT_TEST(test_FetchM3);   // Test M3 - V3 only
    CPPUNIT_TEST(test_FetchM4);   // Test M4 - V3 only
    CPPUNIT_TEST(test_FetchM5);   // Test M5 - V3 only
    CPPUNIT_TEST(finalCleanup);
  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmFetchUnitTestV3() : MdbmFetchUnitTest(GetDataV3(), MDBM_CREATE_V3) { }
    void setUp();
    void tearDown();
    //static FetchTestData dataV3;
    static FetchTestData& GetDataV3();
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmFetchUnitTestV3);
//FetchTestData MdbmFetchUnitTestV3::dataV3;
FetchTestData& MdbmFetchUnitTestV3::GetDataV3() {
  static FetchTestData dataV3;
  return dataV3;
}
void MdbmFetchUnitTestV3::setUp() {
    TRACE_TEST_CASE(__func__);
    MdbmFetchUnitTest::setUp();
    //data = dataV3;
}
void MdbmFetchUnitTestV3::tearDown() {
    TRACE_TEST_CASE(__func__);
}

