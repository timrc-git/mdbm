/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of Iteration features

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

using namespace std;

#define KEY_PREFIX          "KPrefix"
#define STARTPOINT          30
#define KEY_COUNT_DEFAULT   40

typedef enum {
    DO_FIRST = 0,
    DO_FIRST_R = 1,
    DO_FIRSTKEY = 2,
    DO_FIRSTKEY_R = 3
} FirstType;

typedef enum {
    DO_NOTHING = 0,
    DO_NEXT = 4,
    DO_NEXT_R = 5,
    DO_NEXTKEY = 6,
    DO_NEXTKEY_R = 7
} NextType;

typedef enum {
    DELETE_NONE = 0,
    DELETE_FIRST = 1,
    DELETE_EVERY_OTHER = 2,
    DELETE_ALL = 3
} DeleteWhat;


class MdbmUnitTestIter : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestIter(int tFlags) : TestBase(tFlags, "MdbmUnitTestIter") { testFlags = tFlags; }
    virtual ~MdbmUnitTestIter() {}

    void setUp() {}       // Per-test initialization - applies to every test
    void tearDown() {}    // Per-test cleanup - applies to every test

    void initialSetup();
    void test_IterT1();
    void test_IterT2();
    void test_IterT3();

    void test_IterU1();
    void test_IterU2();
    void test_IterU3();

    void test_IterV1();
    void test_IterV2();
    void test_IterV3();

    void test_IterW1();
    void test_IterW2();
    void test_IterW3();

    void test_IterX1();
    void test_IterX2();
    void test_IterX3();
    void test_IterX4();
    void test_IterX5();

    void test_IterY1();
    void test_IterY2();
    void test_IterY3();
    void test_IterY4();
    void test_IterY5();

    void test_IterZ1();
    void test_IterZ2();
    void test_IterZ3();
    void test_IterZ4();
    void test_IterZ5();

    void test_IterAA1();
    void test_IterAA2();
    void test_IterAA3();
    void test_IterAA4();
    void test_IterAA5();

    void finalCleanup();

    // Helper methods

    int getmdbmFlags() { return (testFlags | MDBM_O_RDWR); }

    void testFirstKV(MDBM *mdbm, FirstType firstType, bool shouldwork, NextType nextType = DO_NOTHING);
    void testFirstKey(MDBM *mdbm, FirstType firstType, bool shouldwork, NextType nextType = DO_NOTHING);

    MDBM *createLargeObjFile(const string &prefix, bool randomize = false,
                             int keyCount = KEY_COUNT_DEFAULT);
    string createEmptyMdbm(const string &prefix);
    bool checkMixedData(MDBM *mdbm, NextType nextType, DeleteWhat deleteWhat = DELETE_NONE,
                        int keyCount = KEY_COUNT_DEFAULT);

    protected:

    int getMixedSize();
    void addkey(datum ky, std::set<int> &keySet);
    void deleteOne(MDBM *mdbm, datum ky, std::set<int> &deleteSet);

    private:

    int testFlags;
};

void
MdbmUnitTestIter::initialSetup()
{
}

void
MdbmUnitTestIter::test_IterT1()
{
    string prefix = string("IterT1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags(), 0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST, true);
}

void
MdbmUnitTestIter::test_IterT2()
{
    string prefix = string("IterT2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createLargeObjFile(prefix));
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST, true);
}

void
MdbmUnitTestIter::test_IterT3()
{
    string prefix = string("IterT3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), MDBM_O_RDONLY | getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST, false);
}

void
MdbmUnitTestIter::test_IterU1()
{
    string prefix = string("IterU1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags());
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST_R, true);
}

void
MdbmUnitTestIter::test_IterU2()
{
    string prefix = string("IterU2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST_R, true);
}

void
MdbmUnitTestIter::test_IterU3()
{
    string prefix = string("IterU3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST_R, false);
}

void
MdbmUnitTestIter::test_IterV1()
{
    string prefix = string("IterV1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags());
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY, true);
}

void
MdbmUnitTestIter::test_IterV2()
{
    string prefix = string("IterV2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY, true);
}

void
MdbmUnitTestIter::test_IterV3()
{
    string prefix = string("IterV3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), MDBM_O_RDONLY | getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY, false);
}

void
MdbmUnitTestIter::test_IterW1()
{
    string prefix = string("IterW1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags());
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY_R, true);
}

void
MdbmUnitTestIter::test_IterW2()
{
    string prefix = string("IterW2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY_R, true);
}

void
MdbmUnitTestIter::test_IterW3()
{
    string prefix = string("IterW3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY_R, false);
}


void
MdbmUnitTestIter::test_IterX1()
{
    string prefix = string("IterX1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT));
}

void
MdbmUnitTestIter::test_IterX2()
{
    string prefix = string("IterX2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST, false, DO_NEXT);
}

void
MdbmUnitTestIter::test_IterX3()
{
    string prefix = string("IterX3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT, DELETE_FIRST));
}

void
MdbmUnitTestIter::test_IterX4()
{
    string prefix = string("IterX4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT, DELETE_EVERY_OTHER));
}

void
MdbmUnitTestIter::test_IterX5()
{
    string prefix = string("IterX5") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT, DELETE_ALL));
}

void
MdbmUnitTestIter::test_IterY1()
{
    string prefix = string("IterY1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT_R));
}

void
MdbmUnitTestIter::test_IterY2()
{
    string prefix = string("IterY2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), MDBM_O_RDONLY | getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKV(mdbm, DO_FIRST_R, false, DO_NEXT_R);
}

void
MdbmUnitTestIter::test_IterY3()
{
    string prefix = string("IterY3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT_R, DELETE_FIRST));
}

void
MdbmUnitTestIter::test_IterY4()
{
    string prefix = string("IterY4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT_R, DELETE_EVERY_OTHER));
}

void
MdbmUnitTestIter::test_IterY5()
{
    string prefix = string("IterY5") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXT_R, DELETE_ALL));
}

void
MdbmUnitTestIter::test_IterZ1()
{
    string prefix = string("IterZ1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY));
}

void
MdbmUnitTestIter::test_IterZ2()
{
    string prefix = string("IterZ2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY, false, DO_NEXTKEY);
}

void
MdbmUnitTestIter::test_IterZ3()
{
    string prefix = string("IterZ3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY, DELETE_FIRST));
}

void
MdbmUnitTestIter::test_IterZ4()
{
    string prefix = string("IterZ4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY, DELETE_EVERY_OTHER));
}

void
MdbmUnitTestIter::test_IterZ5()
{
    string prefix = string("IterZ5") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY, DELETE_ALL));
}

void
MdbmUnitTestIter::test_IterAA1()
{
    string prefix = string("IterAA1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY_R));
}

void
MdbmUnitTestIter::test_IterAA2()
{
    string prefix = string("IterAA2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm = mdbm_open(fileNameEmpty.c_str(), getmdbmFlags(), 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    testFirstKey(mdbm, DO_FIRSTKEY_R, false, DO_NEXTKEY_R);
}

void
MdbmUnitTestIter::test_IterAA3()
{
    string prefix = string("IterAA3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY_R, DELETE_FIRST));
}

void
MdbmUnitTestIter::test_IterAA4()
{
    string prefix = string("IterAA4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY_R, DELETE_EVERY_OTHER));
}

void
MdbmUnitTestIter::test_IterAA5()
{
    string prefix = string("IterAA5") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm = createLargeObjFile(prefix, true);  // randomize object size
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);
    CPPUNIT_ASSERT_EQUAL(true, checkMixedData(mdbm, DO_NEXTKEY_R, DELETE_ALL));
}


void
MdbmUnitTestIter::finalCleanup()
{
    TRACE_TEST_CASE(__func__);
    CleanupTmpDir();
    GetLogStream() << "Completed " << versionString << " Iteration test." << endl<<flush;
}

/// ------------------------------------------------------------------------------
///
/// Helper methods
///
/// ------------------------------------------------------------------------------


string
MdbmUnitTestIter::createEmptyMdbm(const string &prefix)
{
    string fname;
    MdbmHolder mdbm = EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT, 0644, DEFAULT_PAGE_SIZE, 0, &fname);
    return fname;
}


/// randomize=true means create a mixture of large and small objects
//
MDBM *
MdbmUnitTestIter::createLargeObjFile(const string &prefix, bool randomize, int keyCount)
{
    string fname;
    MDBM *mdbm = EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_LARGE_OBJECTS | MDBM_O_CREAT,
                               0644, DEFAULT_PAGE_SIZE, 0, &fname);

    if (!randomize) {  // Only large objects
        int ret = InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE+20);
        string msg = string("Cannot insert data into ") + fname + string(" ");
        msg += strerror(errno) + string(", retval= ") + ToStr(ret);
        if (ret != 0) {
            cerr << msg << endl << flush;
            abort();
        }
        return mdbm;
    }

    string key;
    for (int i = 0; i < keyCount; ++i) {
        datum val;
        int siz = getMixedSize();
        datum ky = CreateTestValue("",  siz, val);  // return value "ky" unused
        ++val.dsize;  // store the null
        key = KEY_PREFIX + ToStr(i + STARTPOINT);
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        string msg(prefix);
        msg += string(":createLargeObjFile: Cannot add key ") + key;
        CPPUNIT_ASSERT_MESSAGE(msg, mdbm_store( mdbm, ky, val, MDBM_REPLACE) == 0);
    }

    return mdbm;
}


void
MdbmUnitTestIter::testFirstKV(MDBM *mdbm, FirstType firstType, bool shouldwork, NextType nextType)
{
    kvpair kv;
    MDBM_ITER iter;

    if (firstType == DO_FIRST_R) {
        kv = mdbm_first_r(mdbm, &iter);
    } else {
        kv = mdbm_first(mdbm);
    }

    if (shouldwork) {
        CPPUNIT_ASSERT(kv.key.dsize != 0);
        CPPUNIT_ASSERT(kv.key.dptr != NULL);
        CPPUNIT_ASSERT(kv.val.dsize != 0);
        CPPUNIT_ASSERT(kv.val.dptr != NULL);
    } else {
        CPPUNIT_ASSERT_EQUAL(0, kv.key.dsize);
        CPPUNIT_ASSERT(kv.key.dptr == NULL);
        CPPUNIT_ASSERT_EQUAL(0, kv.val.dsize);
        CPPUNIT_ASSERT(kv.val.dptr == NULL);
    }

    if (!nextType) {
        return;
    }
    // Test a subsequent next/next_r
    else if (nextType == DO_NEXT) {
        kv = mdbm_next(mdbm);  // supposed to fail, asserts below
    }
    else if (nextType == DO_NEXT_R) {
        kv = mdbm_next_r(mdbm, &iter);  // supposed to fail, asserts below
    }
    CPPUNIT_ASSERT_EQUAL(0, kv.key.dsize);
    CPPUNIT_ASSERT(kv.key.dptr == NULL);
    CPPUNIT_ASSERT_EQUAL(0, kv.val.dsize);
    CPPUNIT_ASSERT(kv.val.dptr == NULL);
}


void
MdbmUnitTestIter::testFirstKey(MDBM *mdbm, FirstType firstType, bool shouldwork, NextType nextType)
{
    datum key;
    MDBM_ITER iter;

    if (firstType == DO_FIRSTKEY_R) {
        key = mdbm_firstkey_r(mdbm, &iter);
    } else {
        key = mdbm_firstkey(mdbm);
    }

    if (shouldwork) {
        CPPUNIT_ASSERT(key.dsize != 0);
        CPPUNIT_ASSERT(key.dptr != NULL);
    } else {
        CPPUNIT_ASSERT_EQUAL(0, key.dsize);
        CPPUNIT_ASSERT(key.dptr == NULL);
    }

    if (!nextType) {
        return;
    }
    // Test a subsequent nextkey/nextkey_r
    else if (nextType == DO_NEXTKEY) {
        key = mdbm_nextkey(mdbm);  // supposed to fail, asserts below
    }
    else if (nextType == DO_NEXTKEY_R) {
        key = mdbm_nextkey_r(mdbm, &iter);  // supposed to fail, asserts below
    }
    CPPUNIT_ASSERT_EQUAL(0, key.dsize);
    CPPUNIT_ASSERT(key.dptr == NULL);
}

int
MdbmUnitTestIter::getMixedSize()
{
    int value = (random() % (DEFAULT_PAGE_SIZE * 2)) - (DEFAULT_PAGE_SIZE * 3 / 4);

    if (value <= 0)
        return 1;
    return value;
}

void
MdbmUnitTestIter::addkey(datum ky, std::set<int> &keySet)
{
    int plen = strlen(KEY_PREFIX);
    string msg = string("Invalid key size ");// + ky.dsize;
    CPPUNIT_ASSERT_MESSAGE(msg, ky.dsize >= plen);

    msg = string("Invalid key ") + ky.dptr;
    CPPUNIT_ASSERT_MESSAGE(msg, memcmp(ky.dptr, KEY_PREFIX, plen) == 0);

    int keynum = atoi(ky.dptr + plen);
    keySet.insert(keynum);
}

void
MdbmUnitTestIter::deleteOne(MDBM *mdbm, datum ky, std::set<int> &deleteSet)
{
    errno = 0;
    string msg("deleteOne: Cannot delete key ");
    msg += ky.dptr + string(", errno= ") + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(msg, (mdbm_delete(mdbm, ky) == 0));

    addkey(ky, deleteSet);
}



bool
MdbmUnitTestIter::checkMixedData(MDBM *mdbm, NextType nextType, DeleteWhat deleteWhat, int keyCount)
{
    kvpair kv;
    MDBM_ITER iter;

    kv.key.dsize=0;
    kv.key.dptr=NULL;
    kv.val.dsize=0;
    kv.val.dptr=NULL;

    switch (nextType) {
        case DO_NEXT:
            kv = mdbm_first(mdbm);
            break;
        case DO_NEXT_R:
            kv = mdbm_first_r(mdbm, &iter);
            break;
        case DO_NEXTKEY:
            kv.key = mdbm_firstkey(mdbm);
            break;
        case DO_NEXTKEY_R:
            kv.key = mdbm_firstkey_r(mdbm, &iter);
            break;
        default:
            string msg("checkMixedData: invalid next types");
            CPPUNIT_ASSERT_MESSAGE(msg, 1);
    }
    std::set<int> keySet, deleteSet;
    if (deleteWhat == DELETE_FIRST) {
        deleteOne(mdbm, kv.key, deleteSet);
    }
    else {
        addkey(kv.key, keySet);
    }

    int count = 1;
    while (kv.key.dsize != 0) {
        switch (nextType) {
            case DO_NEXT:
                kv = mdbm_next(mdbm);
                break;
            case DO_NEXT_R:
                kv = mdbm_next_r(mdbm, &iter);
                break;
            case DO_NEXTKEY:
                kv.key = mdbm_nextkey(mdbm);
                break;
            case DO_NEXTKEY_R:
                kv.key = mdbm_nextkey_r(mdbm, &iter);
                break;
            default:
                return false;
        }
        if (!kv.key.dsize)  {
            break;
        }
        if ((deleteWhat == DELETE_ALL) || ((count % 2 == 1) && (deleteWhat == DELETE_EVERY_OTHER))) {
            deleteOne(mdbm, kv.key, deleteSet);
        }
        else {
            addkey(kv.key, keySet);
        }
        ++count;
    }

    std::set<int>::iterator it = keySet.begin();  // recombine deleted and "good" sets
    copy(deleteSet.begin(), deleteSet.end(), inserter(keySet, it));

    for (int i = 0; i < keyCount; ++i) {
        if (keySet.find(i + STARTPOINT) == keySet.end()) {
            cerr << "checkMixedData: Cannot find key " << KEY_PREFIX << i + STARTPOINT << endl;
            return false;
        }
    }
    return true;
}


/// MDBM V3 class

class MdbmUnitTestIterV3 : public MdbmUnitTestIter
{
  CPPUNIT_TEST_SUITE(MdbmUnitTestIterV3);

    CPPUNIT_TEST(initialSetup);

    CPPUNIT_TEST(test_IterT1);
    CPPUNIT_TEST(test_IterT2);
    CPPUNIT_TEST(test_IterT3);

    CPPUNIT_TEST(test_IterU1);
    CPPUNIT_TEST(test_IterU2);
    CPPUNIT_TEST(test_IterU3);

    CPPUNIT_TEST(test_IterV1);
    CPPUNIT_TEST(test_IterV2);
    CPPUNIT_TEST(test_IterV3);

    CPPUNIT_TEST(test_IterW1);
    CPPUNIT_TEST(test_IterW2);
    CPPUNIT_TEST(test_IterW3);

    CPPUNIT_TEST(test_IterX1);
    CPPUNIT_TEST(test_IterX2);
    CPPUNIT_TEST(test_IterX3);
    CPPUNIT_TEST(test_IterX4);
    CPPUNIT_TEST(test_IterX5);

    CPPUNIT_TEST(test_IterY1);
    CPPUNIT_TEST(test_IterY2);
    CPPUNIT_TEST(test_IterY3);
    CPPUNIT_TEST(test_IterY4);
    CPPUNIT_TEST(test_IterY5);

    CPPUNIT_TEST(test_IterZ1);
    CPPUNIT_TEST(test_IterZ2);
    CPPUNIT_TEST(test_IterZ3);
    CPPUNIT_TEST(test_IterZ4);
    CPPUNIT_TEST(test_IterZ5);

    CPPUNIT_TEST(test_IterAA1);
    CPPUNIT_TEST(test_IterAA2);
    CPPUNIT_TEST(test_IterAA3);
    CPPUNIT_TEST(test_IterAA4);
    CPPUNIT_TEST(test_IterAA5);

    CPPUNIT_TEST(finalCleanup);

  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestIterV3() : MdbmUnitTestIter(MDBM_CREATE_V3) { }
};

CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestIterV3);

