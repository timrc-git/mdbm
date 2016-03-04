/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test_close_sync_repl.cc
// Purpose: Unit tests of mdbm_close(), mdbm_sync() and mdbm_fsync(),
//          mdbm_close_fd(), mdbm_replace_file() and mdbm_replace_db()

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>

// for usage
#include <sys/time.h>
#include <sys/resource.h>

#include <iostream>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <mdbm.h>

#include "mdbm_util.h"
#include "TestBase.hh"

#define main mdbm_replace_main_wrapper
#define usage mdbm_replace_usage_wrapper
#include "../../tools/mdbm_replace.c"
#undef main
#undef usage

using namespace std;

static const int PAGESZ  = 8192;
//static const int STARTPOINT = 123;
static const int NUM_DATA_ITEMS = 45;
static const char* PREFIX = "STUFF";
static string TestPrefix = "MdbmCloseSyncUnitTest";

//void cleanup_mdbm(const string & fname)
//{
//  string lockName(fname);
//  lockName += "/tmp/.mlock-named";
//  lockName += fname;
//  lockName += "._int_";
//  unlink(fname.c_str());
//  unlink(lockName.c_str());
//}

class MdbmCloseSyncUnitTestBase : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmCloseSyncUnitTestBase(int vFlag)
          : TestBase(vFlag, "MdbmCloseSyncUnitTest") {}
    void setUp() {}    // Per-test initialization
    void tearDown() {} // Per-test cleanup

    void test_mdbm_close();
    void test_mdbm_sync();
    void test_mdbm_fsync();
    void test_mdbm_close_fd();
    void test_mdbm_replace_file();
    void test_mdbm_replace_db();
    void test_mdbm_fsync_error_cases();
    void test_mdbm_sync_error_cases();
    void test_mdbm_replace_db_error_case_1();
    void test_mdbm_replace_db_error_case_2();
    void test_mdbm_replace_file_error_case();

    void TestReplaceFileLarge();   // (Bug 6061776) Make sure it doesn't crash with 64 bit
    void TestMdbmReplaceMakeResident();

    // Helpers that do most of the work
    MDBM *createData();
    void closeAndReadBackData(MDBM *dataMdbm);
    MDBM *modifyItems(const string &fname);
    MDBM *prepareReplaceData(string &oldName);
    void verifyContents(string prefix = "");

public:
    string workingFile;
};


//
// Helper methods
//

MDBM* MdbmCloseSyncUnitTestBase::createData()
{
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    // Create MDBM, enter data and keep mdbm open.
    MDBM* mdbm = EnsureTmpMdbm(TestPrefix, openFlags, 0644, PAGESZ, 0, &workingFile);
    CPPUNIT_ASSERT(0 == InsertData(mdbm, 7, 7, NUM_DATA_ITEMS));

    return mdbm;
}


void MdbmCloseSyncUnitTestBase::closeAndReadBackData(MDBM *mdbm)
{
    mdbm_close(mdbm);  // returns void, cannot test
    verifyContents();
}

MDBM* MdbmCloseSyncUnitTestBase::modifyItems(const string &fname)
{
    MDBM *mdbm;
    CPPUNIT_ASSERT( (mdbm = mdbm_open(fname.c_str(), MDBM_O_RDWR, 0644, PAGESZ, 0)) != NULL);

    int keySize = 7;
    //char buf[32];
    char buf2[32];
    memset(buf2, '\0', sizeof(buf2));
    strncpy(buf2, "invalid data", sizeof(buf2)-1);
    datum key = {new char[keySize], keySize};

    for (int dataNum=5; dataNum < 20; ++dataNum) { // Modify some of the data
        //snprintf(buf, sizeof(buf), "%s%d", PREFIX, STARTPOINT + dataNum);
        //const datum ky = { buf, strlen(buf) };
        GenerateData(key.dsize, key.dptr, true, dataNum);
        const datum newValue = { buf2, (int)strlen(buf2) };
        //CPPUNIT_ASSERT(0 == mdbm_store( mdbm, ky, newValue, MDBM_REPLACE));
        CPPUNIT_ASSERT(0 == mdbm_store( mdbm, key, newValue, MDBM_REPLACE));
    }

    delete[] key.dptr;
    return mdbm;
}

// Helper method used by mdbm_replace_file/replace_db
//
// Performs steps 1-3 of mdbm_replace_file/replace_db
MDBM* MdbmCloseSyncUnitTestBase::prepareReplaceData(string &oldName)
{
    MDBM *mdbm = 0;
    int openFlags = MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_LARGE_OBJECTS;
    //string fname = GetTmpName("close_sync.mdbm");

    // create test data
    //CPPUNIT_ASSERT((mdbm = openNewDataFile(fname, openFlags, PAGESZ, 0)) != NULL);
    //CPPUNIT_ASSERT(0 == populateMdbm(mdbm, NUM_DATA_ITEMS, STARTPOINT, PREFIX));
    mdbm = EnsureTmpMdbm(TestPrefix, openFlags, 0644, PAGESZ, 0, &workingFile);
    CPPUNIT_ASSERT(0 == InsertData(mdbm, 7, 7, NUM_DATA_ITEMS));

    mdbm_close(mdbm);
    mdbm = 0;

    string cmd("cp ");
    oldName = workingFile + string(".old");
    cmd += workingFile + string(" ") + oldName;
    system(cmd.c_str());

    return modifyItems(workingFile);
}

void MdbmCloseSyncUnitTestBase::verifyContents(string prefix)
{
    int openFlags = O_RDWR | versionFlag;

    MDBM* mdbm = mdbm_open(workingFile.c_str(), openFlags, 0644, PAGESZ, 0);
    CPPUNIT_ASSERT(mdbm != NULL);

    // Verify the the mdbm was stored correctly.
    // The the value should be the same as the key.
    CPPUNIT_ASSERT(0 == VerifyData(mdbm, 7, 7, NUM_DATA_ITEMS));

    //char key1[32];
    //datum fetched1;
    //for (int intKey = STARTPOINT; intKey < STARTPOINT+NUM_DATA_ITEMS; ++intKey) {
    //    snprintf(key1, sizeof(key1), "%s%d", prefix.c_str(), intKey);
    //    const datum k1 = { key1, strlen(key1) };
    //    errno = 0;
    //    fetched1 = mdbm_fetch(mdbm, k1);
    //    CPPUNIT_ASSERT(0 == errno);
    //    CPPUNIT_ASSERT(fetched1.dptr != NULL);
    //    CPPUNIT_ASSERT(k1.dsize == fetched1.dsize);
    //    CPPUNIT_ASSERT(0 == strncmp(fetched1.dptr, k1.dptr, k1.dsize));
    //}

    // Close the new mdbm.
    mdbm_close(mdbm);
}

// Test the mdbm_close() call to make sure that all the data is stored to disk
// by creating, adding data, closing the DB, then reopening and making
// sure the data is the same
void MdbmCloseSyncUnitTestBase::test_mdbm_close()
{
    TRACE_TEST_CASE("test_mdbm_close");
    MDBM* mdbm = createData();
    closeAndReadBackData(mdbm);
}

// Test the mdbm_sync() call to make sure that all the data is stored to disk
// by creating, adding data, calling mdbm_sync, closing the DB, then reopening and making
// sure the data is the same
void MdbmCloseSyncUnitTestBase::test_mdbm_sync()
{
    TRACE_TEST_CASE("test_mdbm_sync");
    MDBM* mdbm = createData();
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(mdbm));
    closeAndReadBackData(mdbm);
}

// Test the mdbm_fsync() call to make sure that all the data is stored to disk
// by creating, adding data, fsyncing, closing the DB, then reopening and making
// sure the data is the same
void MdbmCloseSyncUnitTestBase::test_mdbm_fsync()
{
    TRACE_TEST_CASE("test_mdbm_fsync");
    MDBM* mdbm = createData();
    CPPUNIT_ASSERT_EQUAL(0, mdbm_fsync(mdbm));
    closeAndReadBackData(mdbm);
}

// V2 Unit test of mdbm_close_fd(), which returns void. mdbm_close_fd() sets the fd to -1.
// But V2 doesn't attempt to truncate if fd is negative, so it doesn't set errno
//
// V3 Unit test of mdbm_close_fd(), which returns void. mdbm_close_fd() sets the fd to -1.
// The idea here is that mdbm_truncate should fail and set errno.
// Testcase generates a "Bad file descriptor" error on NFS mounts
void MdbmCloseSyncUnitTestBase::test_mdbm_close_fd()
{
    TRACE_TEST_CASE("test_mdbm_close_fd");
    MDBM *mdbm = createData();
    mdbm_close_fd(mdbm);
    errno = 0;
    mdbm_truncate(mdbm);
    if (versionFlag == MDBM_CREATE_V3) {
      CPPUNIT_ASSERT_EQUAL(9, errno);  // 9=EBADF
    } else {
      CPPUNIT_ASSERT_EQUAL(0, errno);
    }
    mdbm_close(mdbm);
}


// Unit tests of mdbm_replace_file (Testcase E1) - V2 and V3
//
// Perform the following test:
// 1. Create test data
// 2. Copy it to another file named old_fileName.mdbm
// 3. Make changes to fileName and close it.
// 4. Call mdbm_replace_file to get the contents of old_fileName.mdbm
// 5. Make sure post-replace MDBM doesn't have the changes made in step 3.
void MdbmCloseSyncUnitTestBase::test_mdbm_replace_file()
{
    TRACE_TEST_CASE("test_mdbm_replace_file");
    string oldName;
    MDBM *mdbm = prepareReplaceData(oldName);

    mdbm_close(mdbm);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(workingFile.c_str(), oldName.c_str()));
    verifyContents(PREFIX);
}


// Unit tests of mdbm_replace_file (Testcase F1) - V2 and V3
//
// Perform the following test:
// 1. Create test data
// 2. Copy it to another file named old_fileName.mdbm
// 3. Make changes to fileName
// 4. Call mdbm_replace_db to get the contents of old_fileName.mdbm
// 5. Make sure post-replace MDBM doesn't have the changes made in step 3.
void MdbmCloseSyncUnitTestBase::test_mdbm_replace_db()
{
    TRACE_TEST_CASE("test_mdbm_replace_db");
    string oldName;
    MDBM *mdbm = prepareReplaceData(oldName);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_db(mdbm, oldName.c_str()));
    verifyContents(PREFIX);
    mdbm_close(mdbm);
}

// Test the error cases in mdbm_fsync()
void MdbmCloseSyncUnitTestBase::test_mdbm_sync_error_cases()
{
    TRACE_TEST_CASE("test_mdbm_sync_error_cases");
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_sync(mdbm));
}

// Test the error cases in mdbm_fsync()
void MdbmCloseSyncUnitTestBase::test_mdbm_fsync_error_cases()
{
    TRACE_TEST_CASE("test_mdbm_fsync_error_cases");
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_fsync(mdbm));
}

// Test the error cases in mdbm_replace_db()
void MdbmCloseSyncUnitTestBase::test_mdbm_replace_db_error_case_1()
{
    TRACE_TEST_CASE("test_mdbm_replace_db_error_case_1");
    string oldName;
    MDBM *mdbm = prepareReplaceData(oldName);

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_replace_db(NULL, oldName.c_str()));
    mdbm_close(mdbm);
}

// Test replace_db when newName doesn't exist. Prints the following error message:
// SomeNonExistentName: mdbm file open failure: No such file or directory

void MdbmCloseSyncUnitTestBase::test_mdbm_replace_db_error_case_2()
{
    TRACE_TEST_CASE("test_mdbm_replace_db_error_case_2");
    string newName("SomeNonExistentName");   // doesn't exist
    MDBM *mdbm = createData();

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_replace_db(mdbm, newName.c_str()));
    mdbm_close(mdbm);
}

// Test replace_file when newName doesn't exist. Prints the following error message:
// mdbm file open failure: No such file or directory

void MdbmCloseSyncUnitTestBase::test_mdbm_replace_file_error_case()
{
    TRACE_TEST_CASE("test_mdbm_replace_file_error_case");
    MdbmHolder db(createData());
    const char *name(db);
    db.Close();
    string newName("SomeOtherNonExistentName");   // doesn't exist

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_replace_file(name, newName.c_str()));
}

// (Bug 6061776) mdbm_replace_file uses HEADER_ONLY: make sure it doesn't crash with 64 bit
void
MdbmCloseSyncUnitTestBase::TestReplaceFileLarge()
{
    TRACE_TEST_CASE("TestReplaceFileLarge");
    SKIP_IF_FAST()

    string prefix("ReplaceLarge"), filename;
    int flags = MDBM_O_CREAT | MDBM_O_RDWR | MDBM_DBSIZE_MB | MDBM_LARGE_OBJECTS | versionFlag;
    int presize =  1024;
    MDBM *mdbm = EnsureTmpMdbm(prefix, flags, 0666, 4096, presize, &filename);   // Presize to 2GB
    InsertData(mdbm, DEFAULT_KEY_SIZE, 1024, 200, true);
    mdbm_close(mdbm);

    string replaceWith;  // Replace with slightly different file
    presize = 1000;
    MDBM *mdbm2 = EnsureTmpMdbm(prefix, flags, 0666, 4096, presize, &replaceWith);
    InsertData(mdbm2, DEFAULT_KEY_SIZE, 800, 250, true);
    mdbm_close(mdbm2);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(filename.c_str(), replaceWith.c_str()));
}

void
MdbmCloseSyncUnitTestBase::TestMdbmReplaceMakeResident()
{
    string prefix("MdbmReplaceMakeResident");
    TRACE_TEST_CASE(prefix);
    SKIP_IF_FAST()

    string firstFile;   // Store some data
    string copyName;     // Then copy it into copyName
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_FSYNC | MDBM_LARGE_OBJECTS;
    int pgsize = 16384, maxCount = 300000;
    MDBM *mdbm = EnsureTmpMdbm(prefix, openFlags, 0644, pgsize, 0, &firstFile);
    CPPUNIT_ASSERT(0 == InsertData(mdbm, 8, DEFAULT_PAGE_SIZE, maxCount, true));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(mdbm));

    copyName = firstFile + string(".orig");
    int fd = open(copyName.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    CPPUNIT_ASSERT(fd != -1);
    CPPUNIT_ASSERT(-1 != mdbm_fcopy(mdbm, fd, 0));
    close(fd);

    // modify some of the data
    kvpair kv;
    int counter = 0;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    for (kv=mdbm_first_r(mdbm, &iter);
         kv.key.dptr != NULL && counter < 10;
         kv=mdbm_next_r(mdbm, &iter)) {
       memcpy(kv.val.dptr, "any", 4);
       ++counter;
    }

    // Replace with the preload option
    const char *args[] = { "mdbm_replace", "--preload",
                           firstFile.c_str(), copyName.c_str(), NULL };
    reset_getopt();
    CPPUNIT_ASSERT_EQUAL(0,mdbm_replace_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args));

    struct rusage rusage1, rusage2;
    int who = RUSAGE_SELF;
    getrusage(who,&rusage1);
    // Verify that the "any" data modified has been replaced back to the "normal" VerifyData form
    CPPUNIT_ASSERT(0 == VerifyData(mdbm, 8, DEFAULT_PAGE_SIZE, maxCount, true));
    getrusage(who,&rusage2);
    // The value below is arbitrary, and depends on VM pressure and settings. At 5, it still
    // intermittently fails (and we can't control what other tasks co-run on CI machines).
    //CPPUNIT_ASSERT(10 >= (rusage2.ru_majflt - rusage1.ru_majflt));
    if (10 < (rusage2.ru_majflt - rusage1.ru_majflt)) {
      fprintf(stderr, "NOTE: mdbm_preload was less effective than expected (%d vs %ld)\n", 
          10, (long)(rusage2.ru_majflt - rusage1.ru_majflt));
    }
    mdbm_close(mdbm);
}


class MdbmCloseSyncUnitTestV3 : public MdbmCloseSyncUnitTestBase
{
    CPPUNIT_TEST_SUITE(MdbmCloseSyncUnitTestV3);
    CPPUNIT_TEST(test_mdbm_close);
    CPPUNIT_TEST(test_mdbm_sync);
    CPPUNIT_TEST(test_mdbm_fsync);
    CPPUNIT_TEST(test_mdbm_close_fd);
    CPPUNIT_TEST(test_mdbm_replace_file);
    CPPUNIT_TEST(test_mdbm_replace_db);
    CPPUNIT_TEST(test_mdbm_sync_error_cases);
    CPPUNIT_TEST(test_mdbm_fsync_error_cases);
    CPPUNIT_TEST(test_mdbm_replace_db_error_case_1);
    CPPUNIT_TEST(test_mdbm_replace_db_error_case_2);
    CPPUNIT_TEST(test_mdbm_replace_file_error_case);
    CPPUNIT_TEST(test_mdbm_sync_error_cases);
    CPPUNIT_TEST(test_mdbm_fsync_error_cases);
    CPPUNIT_TEST(test_mdbm_replace_db_error_case_1);
    CPPUNIT_TEST(TestReplaceFileLarge);
#ifdef __linux__
    CPPUNIT_TEST(TestMdbmReplaceMakeResident);
#endif
    CPPUNIT_TEST_SUITE_END();

public:
    MdbmCloseSyncUnitTestV3() : MdbmCloseSyncUnitTestBase(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmCloseSyncUnitTestV3);

