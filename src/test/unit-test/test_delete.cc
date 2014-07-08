/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: MDBM unit tests of mdbm_delete*

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
#include "TestBase.hh"
//#include "test_common.h"

// Constants for small-object tests and simple large object tests

static const int PAGESZ_TSML  = 8192;   // For small object tests
static const int PAGESZ_TLRG  = 1024;   // For large Object duplicate/missing key tests

static const int NUM_DATA_ITEMS = 120;
static const int MAX_DUP_TEST   = 12;  // For large Object duplicate/missing key tests

static const int SMALL_OBJ_SIZE_LARGE_OBJ_TEST = 1000;  // Small objects for 16MB object tests

static const char *PREFIX  = "DLPREFIX";             // Small object Key prefix
static const char *SIMPLE_KEY_PREFIX  = "Dkey";      // Large object key prefix
static const char *SOME_TEST_VALUE  = "More Stuff";     // Small test object

static const size_t smallDataSizes[] = {32, 75, 110, 300, 1024, 1700 };
static const size_t dataSizeArrayLen = sizeof(smallDataSizes) / sizeof(size_t);

using namespace std;


//  constants for large-object tests involving the following specifications:
//  When fetching large objects of various value sizes:
//   1. Use various pages sizes
//   2. Ensure that some objects have values that are:
//          * Slightly smaller than the page size (ex., by 12 bytes)
//          * Exactly the page size
//          * Slightly larger than the page size
//          * Much larger than the page size (1000 bytes)
//          * Maximal case: i.e. 16MB page size and 10x that page size for at least 1 large object
static const int MEGBT = 1024 * 1024;
static const int LARGEST_V2_PAGE = 65536;
static const int LARGEST_V3_PAGE = 1 << 24;

// mdbm_open fails to open when page size = exactly 16MB. Only 16MB-64bytes is OK
static const int OFFSET_TO_LARGEST       = 64;

static const int SLIGHTLY_SMALLER_DELTA  = -12;   // Slightly smaller than the page size
static const int SLIGHTLY_LARGER_DELTA   = 20;    // Slightly larger than the page size
static const int MUCH_LARGER_DELTA   = 2000;      // Much larger than the page size

static const int additionalDataSizes[4] = {SLIGHTLY_SMALLER_DELTA, 0, SLIGHTLY_LARGER_DELTA, MUCH_LARGER_DELTA };

static const int MAX_NUM_LDATA   = 10;  // For large Objects tests - must be 10 or less

static const int v2LargeDataPageSizes[] = {256, 2048, 8192, 16384, LARGEST_V2_PAGE };
static const int v2LargePageSizeArrayLen = sizeof(v2LargeDataPageSizes) / sizeof(int);
static const vector<int> LargeDataPageSizesVectV2(v2LargeDataPageSizes,
    v2LargeDataPageSizes+v2LargePageSizeArrayLen);

static const int v3LargeDataPageSizes[] = {448, 18176, 65536, MEGBT+128, LARGEST_V3_PAGE / 8, LARGEST_V3_PAGE - OFFSET_TO_LARGEST};
static const int v3LargePageSizeArrayLen = sizeof(v3LargeDataPageSizes) / sizeof(int);
static const vector<int> LargeDataPageSizesVectV3(v3LargeDataPageSizes,
    v3LargeDataPageSizes+v3LargePageSizeArrayLen);


enum {
    SMALL_OBJ_TESTS = 0
};

typedef enum {
    DO_DELETE = 0,
    DO_DELETE_R = 1,
    DO_DELETE_STR = 2
} DeleteType;

struct DeleteTestData {
    string basePath;
    string fileName;
    string fileNameLarge;   // Large Object duplicate/missing key tests
    vector<string> largeFiles;

    // variables to store data items and number of keys
    map<int, vector <string> > dataItems;  // for registerDatum
    map<int, vector <int> > howManyKeys;   // for registerDatum
};

class MdbmUnitTestDelete : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestDelete(DeleteTestData& dat, int vFlag) : TestBase(vFlag, "MdbmUnitTestDelete"), data(dat) {
      //TRACE_TEST_CASE(__func__);
    }
    void setUp();
    void tearDown();
    void initialSetup();
    void finalCleanup();

    void test_DeleteN1();   // Test N1
    void test_DeleteN2();   // Test N2
    void test_DeleteN3();   // Test N3
    void test_DeleteN4();   // Test N4
    void test_DeleteO1();   // Test O1
    void test_DeleteO2();   // Test O2
    void test_DeleteO3();   // Test O3
    void test_DeleteO4();   // Test O4
    void test_DeleteP1();   // Test P1
    void test_DeleteP2();   // Test P2
    void test_DeleteP3();   // Test P3
    void test_DeleteP4();   // Test P4

    const vector<int>& GetLargeDataPageSizes();
    void createInitialFiles(const string &basename);
    void finalClean(const string &basename);

    // Helper functions
    void createData(int pageszSml, int pageszLrg);
    void storeTestData(MDBM *mdbm, int id, const string& testName);
    void checkData(const string &filename, const string& testName);
    void storeOrCheckData(MDBM *mdbm, const string& testName, bool store, int id = 0);
    void deleteEveryOtherItem(const string &fname, const string &testName, DeleteType deleteType);
    void deleteRhelper(const string &fname, bool small = true);

    void testNonExistentDelete(const string &fname, bool useDeleteStr = false);
    void doSmallDuplicateTest(const string &fname, DeleteType deleteType);
    void performDelete(MDBM *mdbm, const string &key1, bool useDeleteStr, const char *filename = NULL);
    datum fetchData(MDBM *mdbm, datum key);
    void deleteEveryOtherDup(const string &fname, bool useDeleteStr);
    void createLargeObjectMdbm(MDBM *mdbm);
    void fetchLargeDups(const string &filename);
    void createBigData();
    void createBigTestFiles(int openFlags, const vector<string> &fnames, const vector<int> &pagesizes);
    void copyLargeFiles();
    void compareLargeObjFiles(vector<string> &fnames, const vector<int> &pagesizes);
    void compareLargeData(MDBM *mdbm, datum *keyPtr, const datum &compareTo);
    void deleteEveryOtherLargeObj( vector<string> &fnames, bool useDeleteStr);
    void deleteHugeData(bool useDeleteStr);

    DeleteTestData& data;
    //static map<string,DeleteTestData> DataByVersion;

    // TODO FIXME find something better to do with these
    void RegisterDatum(int id, const char *value, int numKeys);
    // Returns "how many" vector of integers
    vector<int> GetRegisteredData(int id, std::vector<std::string> & items);
};

void
MdbmUnitTestDelete::RegisterDatum(int id, const char *value, int numKeys)
{
    data.dataItems[id].push_back(string(value));
    data.howManyKeys[id].push_back(numKeys);
}

vector<int>
MdbmUnitTestDelete::GetRegisteredData(int id, std::vector<std::string> & items)
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

//map<string,DeleteTestData> MdbmUnitTestDelete::DataByVersion;

const vector<int>& MdbmUnitTestDelete::GetLargeDataPageSizes() {
    return LargeDataPageSizesVectV3;
}

void MdbmUnitTestDelete::setUp() {
    TRACE_TEST_CASE(__func__);
    //data = DataByVersion
}
void MdbmUnitTestDelete::tearDown() {
    TRACE_TEST_CASE(__func__);
}
void
MdbmUnitTestDelete::initialSetup()
{
    TRACE_TEST_CASE("initialSetup");
    createInitialFiles("delete");
}

void
MdbmUnitTestDelete::test_DeleteN1()
{
    TRACE_TEST_CASE(__func__);
    string prefix = "N1" + versionString;
    CopyFile(data.fileName);
    deleteEveryOtherItem(data.fileName, prefix, DO_DELETE);
    checkData(data.fileName, prefix);
}

void
MdbmUnitTestDelete::test_DeleteN2()
{
    TRACE_TEST_CASE(__func__);
// Taking too long: commented out
//    copyLargeFiles();

    createBigData();  // For both V2 and V3
    deleteEveryOtherLargeObj(data.largeFiles, false);
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes());
    if (versionFlag & MDBM_CREATE_V3) {
      deleteHugeData(false);
    }
}


void
MdbmUnitTestDelete::test_DeleteN3()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(data.fileName); // Copy files for use by tests N3,N4
    CopyFile(data.fileNameLarge);
    testNonExistentDelete(data.fileName);
    testNonExistentDelete(data.fileNameLarge);
}

void
MdbmUnitTestDelete::testNonExistentDelete(const string &fname, bool useDeleteStr)
{
    MDBM * mdbm =  EnsureNamedMdbm(fname.c_str(), MDBM_O_RDWR , 0644, 0, 0);
    CPPUNIT_ASSERT(mdbm != NULL);

    int ret = 0;
    char key1[32];
    datum key;
    for (int i = 0; i < 10; ++i) {
        key.dsize = snprintf(key1, sizeof(key1), "something%d", i);  // Some non-existent key
        key.dptr = key1;
        errno = 0;
        if (useDeleteStr) {   // use mdbm_delete_str()
             ret = mdbm_delete_str(mdbm, key1);
        } else {  // use mdbm_delete
             ret = mdbm_delete(mdbm, key);
        }
        CPPUNIT_ASSERT(ret != 0);       // Supposed to fail
        CPPUNIT_ASSERT(errno != 0);   // delete also sets errno
    }
    errno = 0;
    mdbm_close(mdbm);
}


void
MdbmUnitTestDelete::test_DeleteN4()
{
    TRACE_TEST_CASE(__func__);
//    Bugzilla bug 5366957 - both large and small obj tests don't work
    CopyFile(data.fileName);
    doSmallDuplicateTest(data.fileName, DO_DELETE);
    deleteEveryOtherDup(data.fileNameLarge, false);
    fetchLargeDups(data.fileNameLarge);
}


void
MdbmUnitTestDelete::test_DeleteO1()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(data.fileName);
    deleteRhelper(data.fileName);
}

void
MdbmUnitTestDelete::deleteRhelper(const string &fname, bool small)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, 0, 0)) != NULL);
    MDBM_ITER mIter;
    MDBM_ITER_INIT(&mIter);

    string ky;
    if (small) {
        vector<string> dataItems;
        vector<int> howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);
        vector<string>::iterator datait = dataItems.begin();

        CPPUNIT_ASSERT( datait != dataItems.end());
        ky = PREFIX + ToStr(0) + datait->c_str();
    } else {
        ky = SIMPLE_KEY_PREFIX;
        ky += ToStr(0);
    }

    datum val, key;

    key.dptr = const_cast<char *>(ky.c_str());
    key.dsize = ky.size() + 1;

    errno = 0;
    int ret = mdbm_fetch_r(mdbm, &key, &val, &mIter);  // get one datum

    CPPUNIT_ASSERT_EQUAL(0, ret );
    //CPPUNIT_ASSERT_EQUAL(0, errno );
    CPPUNIT_ASSERT(val.dptr != NULL);

    kvpair pair = mdbm_next_r(mdbm, &mIter);  // Skip to the next one
    (void)pair; // unused, make compiler happy

    errno = 0;
    while (mdbm_delete_r(mdbm, &mIter) == 0) {  // delete_r everything else
        if (errno != 0)
            break;
    }

    // Make sure key/value pair is still there
    datum val2, key2;

    key2.dptr = const_cast<char *>(ky.c_str());
    key2.dsize = ky.size() + 1;

    errno = 0;
    val2 = mdbm_fetch(mdbm, key2);  // get datum again

    //CPPUNIT_ASSERT_EQUAL(0, errno );
    CPPUNIT_ASSERT(val.dsize == val2.dsize);
    CPPUNIT_ASSERT(val.dptr != NULL);
    CPPUNIT_ASSERT(memcmp(val.dptr, val2.dptr, val.dsize) == 0);

    mdbm_close(mdbm);
}


void
MdbmUnitTestDelete::test_DeleteO2()
{
    TRACE_TEST_CASE(__func__);
    createBigData();  // For both V2 and V3
    unsigned len = data.largeFiles.size();
    if (versionFlag & MDBM_CREATE_V3) { len -= 1; }
    for (u_int i = 0; i < len; ++i) {
        deleteRhelper(data.largeFiles[i], false);
    }
}


void
MdbmUnitTestDelete::test_DeleteO3()
{
    TRACE_TEST_CASE(__func__);
    string fname = "deleteempty" + versionString;

    MDBM *mdbm;
    int openFlags = versionFlag | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;
    //string path(makeMdbmPath(fname));
    string path(data.basePath +"/"+fname);

    CPPUNIT_ASSERT( (mdbm = mdbm_open(path.c_str(), openFlags, 0644, 0, 0)) != NULL);

    errno = 0;
    int count = 0;
    MDBM_ITER mIter;
    MDBM_ITER_INIT(&mIter);
    while (mdbm_delete_r(mdbm, &mIter) == 0) {  // delete_r should delete nothing
        if (errno != 0)
            break;
        ++count;
    }

    CPPUNIT_ASSERT(errno != 0);
    CPPUNIT_ASSERT_EQUAL(0, count);

    mdbm_close(mdbm);
}

void
MdbmUnitTestDelete::test_DeleteO4()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(data.fileName);
    doSmallDuplicateTest(data.fileName, DO_DELETE_R);
    CopyFile(data.fileName);
    deleteRhelper(data.fileName);
    CopyFile(data.fileNameLarge);
    deleteRhelper(data.fileNameLarge, false);
}

void
MdbmUnitTestDelete::test_DeleteP1()
{
    TRACE_TEST_CASE(__func__);
    string prefix = "P1" + versionString;
    CopyFile(data.fileName);
    deleteEveryOtherItem(data.fileName, prefix, DO_DELETE_STR);
    checkData(data.fileName, prefix);
}


void
MdbmUnitTestDelete::test_DeleteP2()
{
    TRACE_TEST_CASE(__func__);
    createBigData();  // For both V2 and V3
    deleteEveryOtherLargeObj(data.largeFiles, true);
    compareLargeObjFiles(data.largeFiles, GetLargeDataPageSizes());
}

void
MdbmUnitTestDelete::test_DeleteP3()
{
    TRACE_TEST_CASE(__func__);
    CopyFile(data.fileName); // Copy files for use by tests P3,P4
    CopyFile(data.fileNameLarge);
    testNonExistentDelete(data.fileName, true);
    testNonExistentDelete(data.fileNameLarge, true);
}

void
MdbmUnitTestDelete::test_DeleteP4()
{
    TRACE_TEST_CASE(__func__);
//    Bugzilla bug 5366957 - both large and small obj tests don't work
    CopyFile(data.fileName);
    doSmallDuplicateTest(data.fileName, DO_DELETE_STR);
    deleteEveryOtherDup(data.fileNameLarge, true);
    fetchLargeDups(data.fileNameLarge);
}

void
MdbmUnitTestDelete::finalCleanup()
{
    finalClean("delete");
}


/// Initial setup for all unit tests
void
MdbmUnitTestDelete::createInitialFiles(const string &basename)
{

    //CPPUNIT_ASSERT( !createMdbmTestDir().empty() );
    int ret = GetTmpDir(data.basePath);
    if (ret) { cerr << "FAILED TO CREATE TMP DIR! :" << data.basePath << endl << flush; }
    CPPUNIT_ASSERT(ret == 0);

    //data.fileName = makeMdbmPath(basename + string("test"+versionString));
    data.fileName = data.basePath + "/" + basename + string("test"+versionString);
    //data.fileNameLarge = makeMdbmPath(basename + string("testL"+versionString));
    data.fileNameLarge = data.basePath + "/" + basename + string("testL"+versionString);


    CPPUNIT_ASSERT(data.fileName != "");
    CPPUNIT_ASSERT(data.fileNameLarge != "");

    // File names for extensive large object tests
    string name;
    const vector<int> &largePageSizes = GetLargeDataPageSizes();
    for (unsigned i = 0; i < largePageSizes.size(); ++i) {
        name = data.basePath + "/" + basename;
        name += string("big") +versionString + "_" + ToStr(i);
        //data.largeFiles.push_back(makeMdbmPath(name));
        GetLogStream() << "adding filename [" << (data.basePath+"/"+name) << "] to largeFiles." << endl;
        data.largeFiles.push_back(name);
        if (data.largeFiles[i]=="") { cerr << "bad largeFile name! :" << i << endl << flush; }
        CPPUNIT_ASSERT(data.largeFiles[i] != "");
    }


    // Create the data and move to the *_cp files
    createData(PAGESZ_TSML, PAGESZ_TLRG);
    string moveTo = data.fileName + "_cp";
    ret = rename(data.fileName.c_str(), moveTo.c_str());
    if (ret) { cerr << "FAILED TO rename normal! :" << data.fileName << endl << flush; }
    CPPUNIT_ASSERT(0 == ret);

    // Files for Large Object duplicate/missing key tests
    moveTo = data.fileNameLarge + "_cp";
    ret = rename(data.fileNameLarge.c_str(), moveTo.c_str());
    if (ret) { cerr << "FAILED TO rename normal! :" << data.fileNameLarge << endl << flush; }
    CPPUNIT_ASSERT(0 == ret);

// Renaming and copying after every test is taking too long: below commented out
#if 0
    for (u_int i = 0; i < data.largeFiles.size(); ++i) {
        moveTo = data.largeFiles[i] + "_cp";
        CPPUNIT_ASSERT_EQUAL(0, rename(data.largeFiles[i].c_str(), moveTo.c_str() ));
    }

    for (u_int i = 0; i < data.largeFiles.size(); ++i) {
        moveTo = data.largeFiles[i] + "_cp";
        CPPUNIT_ASSERT_EQUAL(0, rename(data.largeFiles[i].c_str(), moveTo.c_str() ));
    }
#endif
}

void
MdbmUnitTestDelete::copyLargeFiles()
{
    string cmd;

    for (u_int i = 0; i < data.largeFiles.size(); ++i) {
        cmd = string("cp ") + data.largeFiles[i] + "_cp " + data.largeFiles[i];
        CPPUNIT_ASSERT_EQUAL(0, system(cmd.c_str()));
    }
}

void
MdbmUnitTestDelete::finalClean(const string &basename)
{
    TRACE_TEST_CASE(__func__);
    //string cmd ( "rm -f " );
    //cmd += mdbmBasepath() + string("/") + basename + string("*.mdbm*");
    //system(cmd.c_str());
    CleanupTmpDir();
}


/// Create test data made of small objects.  Then create large object data for duplicate and missing
/// key tests (mdbm3 and mdbm4).
void
MdbmUnitTestDelete::createData(int pageszSml, int pageszLrg)
{
    // Make sure all data is on disk
    int openFlags = versionFlag | MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC;
    MDBM * mdbm1 = 0, * mdbm2 = 0;

    datum val;
    size_t i;
    for (i = 0; i < dataSizeArrayLen ; ++i) {   // create test data
        CreateTestValue("", smallDataSizes[i], val);
        RegisterDatum(SMALL_OBJ_TESTS, val.dptr, NUM_DATA_ITEMS+i);
    }

    // Create V2/V3 mdbm
    string prefix = versionString + " small";
    mdbm1 = EnsureNamedMdbm(data.fileName, openFlags, 0644, pageszSml,0);
    if (!mdbm1) { cerr << "FAILED TO OPEN MDBM! :" << data.fileName << endl << flush; }
    CPPUNIT_ASSERT(mdbm1 != NULL);
    storeTestData(mdbm1, SMALL_OBJ_TESTS, prefix);
    mdbm1 = 0;

    // Create V2/V3 mdbm for large object tests of duplicate keys and missing keys
    openFlags = openFlags | MDBM_LARGE_OBJECTS ;
    mdbm2 = EnsureNamedMdbm(data.fileNameLarge, openFlags, 0644, pageszLrg, 0);
    if (!mdbm2) { cerr << "FAILED TO OPEN LARGE MDBM! :" << data.fileNameLarge << endl << flush; }
    CPPUNIT_ASSERT(mdbm2 != NULL);
    createLargeObjectMdbm(mdbm2);
    mdbm2 = 0;
}

/// Stores test data: used by createData() to create V2 and V3 small object test data
void
MdbmUnitTestDelete::storeTestData(MDBM *mdbm, int id, const string& testName)
{
    storeOrCheckData(mdbm, testName, true, id);
    mdbm_close(mdbm);
}

/// Test to see that data stored by createData remains the same: use various fetch* functions
void
MdbmUnitTestDelete::checkData(const string &filename, const string& testName)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, 0, 0)) != NULL);

    storeOrCheckData(mdbm, testName, false, SMALL_OBJ_TESTS);
    mdbm_close(mdbm);
}

/// Loop through and either store the data or fetch it
void
MdbmUnitTestDelete::storeOrCheckData(MDBM *mdbm, const string& testName, bool store, int id)
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
        val.dptr = (char *) datait->c_str();
        val.dsize = datait->size() + 1;  // Store the null char
        for (int j=0; j < numKys; ++j) {
            key = PREFIX + ToStr(j) + datait->c_str();
            ky.dptr = (char *) key.c_str();
            ky.dsize = key.size() + 1;
            errno = 0;
            if (store) {
                msg = string("Storing ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
                int ret = mdbm_store( mdbm, ky, val, MDBM_REPLACE);
                if (ret) { cerr << "FAILED TO STORE! :::: " << msg << endl << flush;}
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(), ret == 0);
            } else {
                if ((testCount % 2) != 0) {
                    fetched = fetchData(mdbm, ky);
                    msg = string("Delete test ") + testName + " key= " + ky.dptr + " val= " + val.dptr;
                    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(fetched.dptr, val.dptr, val.dsize) == 0);
                    ++fetchCount;
                } else {  // supposed to fail because it was previously deleted
                    fetched = mdbm_fetch(mdbm, ky);
                    CPPUNIT_ASSERT( errno != 0);
                    CPPUNIT_ASSERT(fetched.dptr == NULL);
                }
            }
            ++testCount;
        }
    }

    if (!store) {
        GetLogStream() << "Test " << testName << " performed " << fetchCount << " fetches" << endl;
    }
}

/// Loop through and delete every other item - small objects
void
MdbmUnitTestDelete::deleteEveryOtherItem(const string &fname, const string& testName, DeleteType deleteType)
{
    int numKys;
    datum ky;
    string key;
    int testCount = 0, deleteCount = 0, ret = 0;
    vector<string> dataItems;
    MDBM *mdbm;

    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, 0, 0)) != NULL);

    vector<int> howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);
    CPPUNIT_ASSERT( dataItems.size() != 0);

    string msg, opType;
    vector<string>::iterator datait = dataItems.begin();
    vector<int>::iterator numit = howMany.begin();
    for (; datait != dataItems.end(); ++datait, ++numit) {
        CPPUNIT_ASSERT(numit != howMany.end());   // stop if data error
        numKys = *numit;
        for (int j=0; j < numKys; ++j) {
            key = PREFIX + ToStr(j) + datait->c_str();
            ky.dptr = const_cast<char *>( key.c_str() );
            ky.dsize = key.size() + 1;
            errno = 0;
            if ((testCount % 2) == 0) {
                if (deleteType == DO_DELETE) {
                    opType = "mdbm_delete";
                    ret = mdbm_delete(mdbm, ky);
                } else if (deleteType == DO_DELETE_STR) {
                    opType = "mdbm_delete_str";
                    ret = mdbm_delete_str(mdbm, key.c_str());
                } else {  // mdmb_delete_r is not supported
                    opType = "Invalid delete type";
                    ret = -1;
                }
                msg = opType + string(" test failed: ") + testName + " key= " + ky.dptr;
                CPPUNIT_ASSERT_MESSAGE(msg.c_str(), ret == 0);
                msg = string("Errno set for ") + msg;
                //CPPUNIT_ASSERT_MESSAGE(msg.c_str(), errno == 0);
                ++deleteCount;
            }
            ++testCount;
        }
    }

    mdbm_close(mdbm);
    GetLogStream() << "Test " << testName << " performed " << deleteCount << " deletes" << endl;
}


/// Insert duplicates into the MDBM
void
MdbmUnitTestDelete::doSmallDuplicateTest(const string &fname, DeleteType deleteType)
{
    string key, firstDup, secondDup;
    datum ky, val;
    vector<string> dataItems;
    vector<int> howMany;
    int ret;

    howMany = GetRegisteredData(SMALL_OBJ_TESTS, dataItems);

    vector<string>::iterator datait = dataItems.begin();
    CPPUNIT_ASSERT( datait != dataItems.end());  // stop if no data

    MDBM * mdbm =  mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, PAGESZ_TSML, 0);
    CPPUNIT_ASSERT( mdbm != NULL);

    errno = 0;

    firstDup =  string("SomeSmallData") + *datait + string("EndSmallData");
    secondDup = string("SecondDuplicateData") + *datait + string("TailDuplicateData");

    bool useDeleteStr = false;  // DO_DELETE by default
    bool doDelete = true;

    if (deleteType == DO_DELETE_STR) {
        useDeleteStr = true;
    } else if (deleteType == DO_DELETE_R) {
        doDelete = false;   // delete_r will do the deletes in another place
    }

    // Insert the 2 duplicates for each data item size
    for (; datait != dataItems.end(); ++datait) {
        key = PREFIX + ToStr(0) + datait->c_str();
        ky.dptr = (char *) key.c_str();
        ky.dsize = key.size() + 1;
        val.dptr = (char *) firstDup.c_str();
        val.dsize = firstDup.size() + 1;
        ret = mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP);
        if (ret) {
          fprintf(stderr, "FAILED TO STORE FIRST DUP! ksize:%d vsize:%d \n", ky.dsize, val.dsize);
        }
        CPPUNIT_ASSERT(0 == ret);
        val.dptr = (char *) secondDup.c_str();
        val.dsize = secondDup.size() + 1;
        ret = mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP);
        if (ret) {
          fprintf(stderr, "FAILED TO STORE SECOND DUP! ksize:%d vsize:%d \n", ky.dsize, val.dsize);
        }
        CPPUNIT_ASSERT(0 == ret);
        if (doDelete) {
            performDelete(mdbm, key, useDeleteStr, fname.c_str());
            performDelete(mdbm, key, useDeleteStr, fname.c_str());
            performDelete(mdbm, key, useDeleteStr, fname.c_str());
        }
    }

    mdbm_close(mdbm);
}


/// Perform delete, and check that it succeeds
void
MdbmUnitTestDelete::performDelete(MDBM *mdbm, const string &key1, bool useDeleteStr, const char *filename)
{
    string fname;
    if (filename) {
        fname = string(", file ") + filename;
    }

    int ret = 0;
    errno = 0;
    if (useDeleteStr) {   // use mdbm_delete_str()
        ret = mdbm_delete_str(mdbm, key1.c_str());
    } else {  // use mdbm_delete
        datum key;
        key.dptr = const_cast<char *>(key1.c_str());
        key.dsize = key1.size() + 1;
        ret = mdbm_delete(mdbm, key);
    }
    string err = string("Failed to delete key ") + key1 + ", errno set to " + ToStr(errno) + fname;
    //CPPUNIT_ASSERT_MESSAGE(err.c_str(), errno == 0);
    //err = string("Failed to delete key ") + key1 + fname;
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);
}

/// Call the various fetch* functions to retrieve data from the MDBM
datum
MdbmUnitTestDelete::fetchData(MDBM *mdbm, datum key)
{
    datum fetched;
    int ret = 0;
    string err;

    fetched.dptr = 0;
    fetched.dsize = 0;
    fetched = mdbm_fetch(mdbm, key);

    //err = string("Failed to find Key ") + key->dptr + ", errno set to " + ToStr(errno);
    //CPPUNIT_ASSERT_MESSAGE(err.c_str(), errno == 0);
    err = string("Failed to find Key ") + key.dptr + ", return value set to " + ToStr(ret);
    err += ", errno set to " + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);

    return (fetched);
}


/// Creates a large object MDBM exclusively for the duplicate and missing key tests.
/// These tests are: N3,N4,O3,O4,P3,P4
void
MdbmUnitTestDelete::createLargeObjectMdbm(MDBM *mdbm)
{
    char buf[256];
    // Keys must be smaller than the page size, so I won't use the ky return value
    datum ky, val;
    CreateTestValue("",  2000, val);
    ++val.dsize;  // store the null
    for (int i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
    }

    // Insert large duplicates
    CreateTestValue("", 5000, val);
    ++val.dsize;  // store the null
    for (int i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));
    }

    // Insert small duplicates
    val.dptr = const_cast<char *>(SOME_TEST_VALUE);
    val.dsize = strlen(SOME_TEST_VALUE) + 1;
    for (int i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_INSERT_DUP));
    }

    mdbm_close(mdbm);
}

/// delete some of the duplicates
void
MdbmUnitTestDelete::deleteEveryOtherDup(const string &fname, bool useDeleteStr)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, 0, 0)) != NULL);

    string ky;
    for (int i = 0; i < MAX_DUP_TEST; i += 2) {
        ky = SIMPLE_KEY_PREFIX + ToStr(i);
        performDelete(mdbm, ky, useDeleteStr, fname.c_str());
    }
    mdbm_close(mdbm);
}

/// After deleting some of the duplicates with deleteEveryOtherDup(), you don't really
/// know which dupes were deleted, so just check every possibility
void
MdbmUnitTestDelete::fetchLargeDups(const string &filename)
{
    char buf[256];
    datum ky, val;
    MDBM *mdbm;

    CreateTestValue("",  2000, val);
    string longstr1 ( val.dptr );
    CreateTestValue("",  5000, val);
    string longstr2 = ( val.dptr );

    CPPUNIT_ASSERT( (mdbm = mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, 0, 0)) != NULL);

    errno = 0;
    for (int i = 0; i < MAX_DUP_TEST; ++i) {
        ky.dptr = buf;
        ky.dsize = snprintf(buf, sizeof(buf), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
        val = fetchData(mdbm, ky);

        int found = (strcmp( val.dptr, SOME_TEST_VALUE) == 0);
        found = found || (longstr1 == val.dptr);
        found = found || (longstr2 == val.dptr);
        CPPUNIT_ASSERT(found != 0);
    }
    mdbm_close(mdbm);
}

// Create large object data for tests N2, O2, P2
void
MdbmUnitTestDelete::createBigData()
{
    int openFlags = MDBM_LARGE_OBJECTS | MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_FSYNC | versionFlag;
    CPPUNIT_ASSERT(data.largeFiles.size());
    createBigTestFiles(openFlags, data.largeFiles, GetLargeDataPageSizes());

    if (versionFlag & MDBM_CREATE_V3) {
        // create 16MB page-size MDBM
        MDBM *mdbm;
        int lastEntry = v3LargePageSizeArrayLen - 1;
        int pgsize = v3LargeDataPageSizes[lastEntry];
        GetLogStream() << "****** accessing large file " << lastEntry << " of " << data.largeFiles.size() << endl;
        mdbm = EnsureNamedMdbm(data.largeFiles[lastEntry], openFlags, 0644, pgsize, 0);
        //CPPUNIT_ASSERT(mdbm != NULL);
        datum ky, val;
        CreateTestValue("",  pgsize * 10, val);
        char *baselineData = val.dptr;
        char key[64];

        for (int i = 0; i < MAX_NUM_LDATA; ++i) {
            errno = 0;
            ky.dptr = key;   // Store small objects for test purposes
            ky.dsize = snprintf(key, sizeof(key), "%s%d", SIMPLE_KEY_PREFIX, i) + 1;
            val.dsize = SMALL_OBJ_SIZE_LARGE_OBJ_TEST;
            val.dptr = baselineData;
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
            //CPPUNIT_ASSERT_EQUAL(0, errno);
        }

        for (int i = 1; i < 3; ++i) {
            errno = 0;
            ky.dptr = key;  // Now create 2 very large objects
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, i, i) + 1;
            val.dsize = (pgsize * 10);  // No null stored
            val.dptr = baselineData;
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
            //CPPUNIT_ASSERT_EQUAL(0, errno);
        }

        mdbm_close(mdbm);
    }
}


void
MdbmUnitTestDelete::createBigTestFiles(int openFlags, const vector<string> &fnames, const vector<int> &pagesizes)
{
    MDBM *mdbm;
    char key[64];
    int pgsize;
    int maxData = pagesizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DELTA;

    //CPPUNIT_ASSERT(fnames.size());

    datum ky, val;
    CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pagesizes[filei];
        mdbm = EnsureNamedMdbm(fnames[filei], openFlags, 0644, pgsize, 0);
        //CPPUNIT_ASSERT(mdbm != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            errno = 0;
            ky.dptr = key;   // Store small objects for test purposes
            ky.dsize = snprintf(key, sizeof(key), "%s%d", SIMPLE_KEY_PREFIX, j) + 1;
            val.dsize = pgsize / 2;
            val.dptr = baselineData;
            CPPUNIT_ASSERT_EQUAL(0, mdbm_store( mdbm, ky, val, MDBM_REPLACE));
            //CPPUNIT_ASSERT_EQUAL(0, errno);

            errno = 0;
            ky.dptr = key;  // Now create large objects
            ky.dsize = snprintf(key, sizeof(key), "%s%d%d", SIMPLE_KEY_PREFIX, j, j) + 1;
            val.dptr = baselineData;
            int siz = val.dsize = pgsize + additionalDataSizes[j % 4];  // without null first
            char temp = baselineData[siz];    // Store original
            baselineData[siz] = '\0';    // Store null
            ++(val.dsize);
            int ret = mdbm_store( mdbm, ky, val, MDBM_REPLACE);
            string msg("createBigTestFiles failed to store key, errno= ");
            msg += strerror(errno);
            CPPUNIT_ASSERT_MESSAGE(msg, ret == 0);
            baselineData[siz] = temp; // restore original
        }
        mdbm_close(mdbm);
    }
}

/// Compare every other large object in files passed in vector fnames to expected values
void
MdbmUnitTestDelete::compareLargeObjFiles(vector<string> &fnames, const vector<int> &pagesizes)
{
    MDBM *mdbm;
    char key[64];
    int pgsize;
    int maxData = pagesizes[v2LargePageSizeArrayLen - 1] + MUCH_LARGER_DELTA;

    datum ky, val;
    CreateTestValue("",  maxData, val);
    char *baselineData = val.dptr;
    int count = 0;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        pgsize = pagesizes[filei];
        CPPUNIT_ASSERT((mdbm = mdbm_open(fnames[filei].c_str(), MDBM_O_RDONLY, 0444, pgsize, 0)) != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            if ((count % 2) == 0) {
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
            ++count;
        }
        mdbm_close(mdbm);
    }
}

/// Retrieve Large Objects from the MDBM, then compare data to the compareTo datum
void
MdbmUnitTestDelete::compareLargeData(MDBM *mdbm, datum *keyPtr, const datum &compareTo)
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
    err = string("Failed to find Key ") + keyPtr->dptr + ", return value set to " + ToStr(ret);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), ret == 0);

    err = string("Size of key ") + keyPtr->dptr + " is " + ToStr(fetched.dsize) +
          " but should be " + ToStr(compareTo.dsize);
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), fetched.dsize == compareTo.dsize);
    err = string("Key ") + keyPtr->dptr + " contains data different than intended";
    CPPUNIT_ASSERT_MESSAGE(err.c_str(), memcmp(fetched.dptr,compareTo.dptr, compareTo.dsize) == 0);
}


/// Delete every other large object
void
MdbmUnitTestDelete::deleteEveryOtherLargeObj(vector<string> &fnames, bool useDeleteStr)
{
    MDBM *mdbm;
    string ky;
    int count = 0;

    for (int filei = 0; filei < v2LargePageSizeArrayLen; ++filei ) {
        CPPUNIT_ASSERT((mdbm = mdbm_open(fnames[filei].c_str(), MDBM_O_RDWR, 0644, 0, 0)) != NULL);

        for (int j = 0; j < MAX_NUM_LDATA; ++j) {
            if ((count % 2) == 1) {
                ky = SIMPLE_KEY_PREFIX + ToStr(j) + ToStr(j);
                performDelete(mdbm, ky, useDeleteStr, fnames[filei].c_str());
            }
            ++count;
        }
        mdbm_close(mdbm);
    }
}

/// Run tests on 16MB page-size MDBM
void
MdbmUnitTestDelete::deleteHugeData(bool useDeleteStr)
{
    int lastEntry = v3LargePageSizeArrayLen - 1;
    int pgsize = v3LargeDataPageSizes[lastEntry];
    MDBM *mdbm = mdbm_open(data.largeFiles[lastEntry].c_str(),  MDBM_O_RDWR, 0644, pgsize, 0);
    CPPUNIT_ASSERT(mdbm != NULL);

    string key(SIMPLE_KEY_PREFIX);
    key += string("11");
    performDelete(mdbm, key, useDeleteStr, data.largeFiles[lastEntry].c_str());

    mdbm_close(mdbm);
}




class MdbmUnitTestDeleteV3 : public MdbmUnitTestDelete
{
  CPPUNIT_TEST_SUITE(MdbmUnitTestDeleteV3);
    CPPUNIT_TEST(initialSetup);
    CPPUNIT_TEST(test_DeleteN1);   // Test N1
    CPPUNIT_TEST(test_DeleteN2);   // Test N2
    CPPUNIT_TEST(test_DeleteN3);   // Test N3
    CPPUNIT_TEST(test_DeleteN4);   // Test N4
    CPPUNIT_TEST(test_DeleteO1);   // Test O1
    CPPUNIT_TEST(test_DeleteO2);   // Test O2
    CPPUNIT_TEST(test_DeleteO3);   // Test O3
    CPPUNIT_TEST(test_DeleteO4);   // Test O4
    CPPUNIT_TEST(test_DeleteP1);   // Test P1
    CPPUNIT_TEST(test_DeleteP2);   // Test P2
    CPPUNIT_TEST(test_DeleteP3);   // Test P3
    CPPUNIT_TEST(test_DeleteP4);   // Test P4
    CPPUNIT_TEST(finalCleanup);
  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestDeleteV3() : MdbmUnitTestDelete(dataV3, MDBM_CREATE_V3) { }
    void setUp();
    void tearDown();
    static DeleteTestData dataV3;
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestDeleteV3);
DeleteTestData MdbmUnitTestDeleteV3::dataV3;
void MdbmUnitTestDeleteV3::setUp() {
    TRACE_TEST_CASE(__func__);
    data = dataV3;
}
void MdbmUnitTestDeleteV3::tearDown() {
    TRACE_TEST_CASE(__func__);
}


