/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test_mash.cc
// Purpose: Unit tests for Mash (Mdbm Access SHell)

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>

#include <iostream>
#include <ostream>

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

#define main mash_main_wrapper
#include "../../tools/mash.cc"
#undef main

using namespace std;

class MdbmUnitTestMash : public CppUnit::TestFixture, public TestBase
{
    CPPUNIT_TEST_SUITE(MdbmUnitTestMash);

        CPPUNIT_TEST(initialSetup);
        CPPUNIT_TEST(TestCreate);
#ifdef HAVE_WINDOWED_MODE
        CPPUNIT_TEST(TestCreate2);
#endif
        CPPUNIT_TEST(TestCreateInitial);
        CPPUNIT_TEST(TestCreateFail);
        CPPUNIT_TEST(TestCreateFail2);
        CPPUNIT_TEST(TestDeleteLockfiles);
        CPPUNIT_TEST(TestMashExport);
        CPPUNIT_TEST(TestMashCdbExport);
        CPPUNIT_TEST(TestMashImport);
        CPPUNIT_TEST(TestMashCdbImport);
        CPPUNIT_TEST(TestStore);
        CPPUNIT_TEST(TestStoreHex);
        CPPUNIT_TEST(TestStoreLarge);
        CPPUNIT_TEST(TestLock);
        CPPUNIT_TEST(TestLockReadOnly);
        CPPUNIT_TEST(TestLockAndCopy);
        CPPUNIT_TEST(TestLockFork);
        CPPUNIT_TEST(TestUnlock);
        CPPUNIT_TEST(TestUnlockFail);
        CPPUNIT_TEST(TestMcheck);
        CPPUNIT_TEST(TestMcompress);
        CPPUNIT_TEST(TestMcompare);
        CPPUNIT_TEST(TestMpurge);
        CPPUNIT_TEST(TestMsync);
        CPPUNIT_TEST(TestMopen);
        CPPUNIT_TEST(TestMopenRO);
        CPPUNIT_TEST(TestMclose);
        CPPUNIT_TEST(TestMcloseNoUnlock);
        CPPUNIT_TEST(TestMcopy);
        CPPUNIT_TEST(TestMlockPrint);
        CPPUNIT_TEST(TestMlockReset);
        CPPUNIT_TEST(TestMdelete);
        CPPUNIT_TEST(TestMdump);
        CPPUNIT_TEST(TestMtrunc);
        CPPUNIT_TEST(TestMtruncLarge);
        CPPUNIT_TEST(TestFetch);
        CPPUNIT_TEST(TestLockFetch);
        CPPUNIT_TEST(TestFetchHex);
        CPPUNIT_TEST(TestFetchCdb);
        CPPUNIT_TEST(TestFetchDbdump);
        CPPUNIT_TEST(TestMdigest);

    CPPUNIT_TEST_SUITE_END();

public:
    MdbmUnitTestMash() : TestBase(MDBM_CREATE_V3, "MdbmUnitTestMash") {}
    void setUp()
    {
        string dir;
        GetTmpDir(dir);
        scriptFile = dir + "/mash_script";
        outputFile = dir + "/mash_output";
    }
    void tearDown() {}

    void initialSetup();
    // Tests
    void TestCreate();            // test mcreate
    void TestCreate2();           // test mcreate with different options
    void TestCreateInitial();     // test mcreate with initial size
    void TestCreateFail();        // test mcreate with bad options
    void TestCreateFail2();       // test mcreate with bad options
    void TestDeleteLockfiles();   // test mdelete_lockfiles
    void TestMashExport();        // Test mexport
    void TestMashCdbExport();     // Test mexport in cdb format
    void TestMashImport();        // Test mimport
    void TestMashCdbImport();     // Test mimport in cdb format
    void TestStore();             // Store test
    void TestStoreHex();          // Store test with hex format
    void TestStoreLarge();        // Store large object
    void TestLock();              // Basic mlock test
    void TestLockReadOnly();      // mlock in read-only mode (shared locking)
    void TestLockAndCopy();       // Combine locking and mcopy
    void TestLockFork();          // Make sure forked process is blocked
    void TestUnlock();            // Basic munlock test
    void TestUnlockFail();        // munlock failing
    void TestMcheck();            // test mcheck
    void TestMcompress();         // test mcompress
    void TestMcompare();          // test mcompare
    void TestMpurge();            // test mpurge
    void TestMsync();             // test msync
    void TestMopen();             // simple test of mopen
    void TestMopenRO();           // test mopen read-only
    void TestMclose();            // test of mclose
    void TestMcloseNoUnlock();    // test of mclose - no unlock
    void TestMcopy();             // test of mcopy
    void TestMlockPrint();        // test of mlock_print
    void TestMlockReset();        // test of mlock_reset
    void TestMdelete();           // test of mdelete
    void TestMdump();             // test of mdump
    void TestMtrunc();            // test of mtrunc (mdbm_trunc)
    void TestMtruncLarge();       // test mtrunc with post-trunc large objects
    void TestFetch();             // Basic fetch test
    void TestLockFetch();         // lock, then fetch
    void TestFetchHex();          // Fetch using unprintable "hex" format
    void TestFetchCdb();          // Fetch and output using CDB format
    void TestFetchDbdump();       // Fetch and output using DBdump format
    void TestMdigest();           // test of mdigest
    // test of mtrunc, then store large object: large-obj option disabled in mdbm_trunc

    // Utility methods
    void RunMashScript(FILE *fp, string outFileName);
    FILE *CreateScript(const string &firstLine);
    void AddToScript(FILE *fp, const string &anotherLine);
    string BuildCommand(const string &cmd, string &filename);
    void FinishScript(FILE *fp);

    MDBM *CreateMdbmWithNumbers(const string &prefix, string *filename, int flags,
                                int pageSize, int *count);
    void VerifyMdbmWithNumbers(MDBM *db);
    FILE *BasicLockingTest(const string &prefix, int flags);
    void ExportTest(const string &prefix, bool cdb);
    void ImportTest(const string &prefix, bool cdbFormat);
    bool FindInOutput(const string &item);
    void TestClose(const string &prefix, bool doUnlock);
    void TestFetchFormat(const string &prefix, const char *format);
    int insertSomeData(MDBM *db);
    string GetHexDatum(const datum &dt);

protected:
    static const int NUMBER_TEST_OFFSET = 1050;

private:
    string scriptFile;
    string outputFile;
};


int
MdbmUnitTestMash::insertSomeData(MDBM *db)
{
  int ret = 0;
  const char* keys[] = {"blort", "flarg"};
  int keycount = sizeof(keys)/sizeof(keys[0]);
  datum kv;
  for (int i=0; i<keycount; ++i) {
      kv.dptr = (char*)keys[i];
      kv.dsize = strlen(kv.dptr) +1;
      int sret = mdbm_store(db, kv, kv, 0);
      if (sret) {
        fprintf(stderr, "do_data() failed to store key [%s] ret:%d\n", kv.dptr, ret);
        ret = sret;
      }
    }
  return ret;
}

string
MdbmUnitTestMash::GetHexDatum(const datum &dt)
{
    string s;

    for (int i = 0; i < dt.dsize; ++i) {
        if (isprint(dt.dptr[i]) && (dt.dptr[i] != '\\')) {
            s.append(1, dt.dptr[i]);
        } else {
            // 4 chars of \x##
            s.append("\\\\x");
            char buf[4];
            snprintf(buf, 4, "%02x", dt.dptr[i]);
            s.append(1, buf[0]);
            s.append(1, buf[1]);
        }
    }
    return s;
}


MDBM *
MdbmUnitTestMash::CreateMdbmWithNumbers(const string &prefix, string *filename = NULL,
                                        int flags = 0, int pageSize = 8192, int *countPtr = NULL)
{
    MDBM *mdbm = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | flags, 0644,
                               pageSize, 0, filename);

    datum key, val;
    char bufk[16], bufv[16];
    const int TEST_MDBM_MAX_NUMBER = 100;
    for (int cnt = 0; cnt < TEST_MDBM_MAX_NUMBER; ++cnt)
    {
        snprintf(bufk, 16, "%d", cnt);
        snprintf(bufv, 16, "%d", cnt + NUMBER_TEST_OFFSET);
        key.dptr  = bufk;
        key.dsize = strlen(bufk);
        val.dptr  = bufv;
        val.dsize = strlen(bufv);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, key, val, MDBM_REPLACE));
    }
    if (countPtr != NULL) {
        *countPtr = TEST_MDBM_MAX_NUMBER;
    }
    return mdbm;
}

void
MdbmUnitTestMash::VerifyMdbmWithNumbers(MDBM *db)
{
    datum key, val;
    char bufk[16], bufv[16];
    for (int cnt = 0; cnt < 100; ++cnt)
    {
        snprintf(bufk, 16, "%d", cnt);
        snprintf(bufv, 16, "%d", cnt + NUMBER_TEST_OFFSET);
        key.dptr  = bufk;
        key.dsize = strlen(bufk);
        val = mdbm_fetch(db, key);
        CPPUNIT_ASSERT(NULL != val.dptr);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(val.dptr, bufv, val.dsize));
    }
}

// Open file containing a script and write first line
FILE *
MdbmUnitTestMash::CreateScript(const string &firstLine)
{
    FILE *fp = fopen(scriptFile.c_str(), "w");
    CPPUNIT_ASSERT(NULL != fp);
    AddToScript(fp, firstLine);
    return fp;
}

void
MdbmUnitTestMash::AddToScript(FILE *fp, const string &anotherLine)
{
    CPPUNIT_ASSERT(EOF != fputs(anotherLine.c_str(), fp));
    CPPUNIT_ASSERT(EOF != fputc('\n', fp));
}

void
MdbmUnitTestMash::FinishScript(FILE *fp)
{
    fflush(fp);
    fclose(fp);
}

// Run a Mash script and erase the script file when done
void
MdbmUnitTestMash::RunMashScript(FILE *fp, string outFileName = "")
{
    FinishScript(fp);
    if (outFileName.size() == 0) {
        outFileName = outputFile;
    }
    const char* args[] = { "mash", "-i", scriptFile.c_str(), "-o", outFileName.c_str(), NULL };
    reset_getopt();
    int ret = mash_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
    CPPUNIT_ASSERT_EQUAL(0, ret);

    unlink(scriptFile.c_str());
}

string
MdbmUnitTestMash::BuildCommand(const string &cmd, string &filename)
{
    string basedir, line(cmd);
    CPPUNIT_ASSERT_EQUAL(0, GetTmpDir(basedir));
    filename = basedir + "/" + filename;
    line += string(" ") + filename;
    return line;
}

bool
MdbmUnitTestMash::FindInOutput(const string &item)
{
    bool found = false;
    FILE *fp = fopen(outputFile.c_str(), "r");
    CPPUNIT_ASSERT(NULL != fp);
    const int MASH_OUTPUT_MAX = 4096;
    char buffer[MASH_OUTPUT_MAX];
    while(fgets(buffer, MASH_OUTPUT_MAX, fp) != NULL) {
        if (strstr(buffer, item.c_str()) != NULL) {
            found = true;
            break;
        }
    }
    fclose(fp);
    return found;
}

FILE *
MdbmUnitTestMash::BasicLockingTest(const string &prefix, int flags = 0)
{
    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname, flags);
    mdbm_close(mdbm);

    string line("mlock ");
    line += fname;
    FILE *fp = CreateScript(line);
    AddToScript(fp, string("mfetch %_ 44"));   // mfetch from current-working (%_) MDBM
    AddToScript(fp, "munlock %_");
    return fp;
}

// Export test: Use Mash to export the data, then import it back with the import API and check
// the resulting MDBM to make sure it has all the data
void
MdbmUnitTestMash::ExportTest(const string &prefix, bool cdb)
{
    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("export", MDBM_O_CREAT | MDBM_O_RDWR | MDBM_RW_LOCKS, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));

    string exportFile = GetTmpName(".out");
    string line("mexport -L shared ");
    if (cdb) {
       line += string("-F cdb");
    } else {
        line += string("-T");
    }
    line += string(" -o ") + exportFile + " " + fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);

    // Use import API to reimport into an MDBM, then read the data
    // Create second empty MDBM
    MdbmHolder db2(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | versionFlag, 0644, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db2);
    fp = fopen(exportFile.c_str(), "r");
    CPPUNIT_ASSERT(NULL != fp);
    uint32_t linenum = 1;
    if (cdb) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_cdbdump_import(db2, fp, exportFile.c_str(), MDBM_INSERT));
    } else {
        CPPUNIT_ASSERT_EQUAL(0,
            mdbm_dbdump_import(db2, fp, exportFile.c_str(), MDBM_INSERT, &linenum));
    }
    fclose(fp);
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(db2));
}

// Import test: Use export API to export the data to a file, then import it using Mash
void
MdbmUnitTestMash::ImportTest(const string &prefix, bool cdbFormat)
{
    MdbmHolder db(CreateMdbmWithNumbers(prefix));
    string exportFile = GetTmpName(".out");
    FILE *fp = fopen(exportFile.c_str(), "w");
    CPPUNIT_ASSERT(NULL != fp);

    kvpair kv;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    for (kv=mdbm_first_r(db, &iter); kv.key.dptr != NULL; kv=mdbm_next_r(db, &iter)) {
        if (cdbFormat) {
            CPPUNIT_ASSERT_EQUAL(0, mdbm_cdbdump_to_file(kv, fp));
        } else {
            CPPUNIT_ASSERT_EQUAL(0, mdbm_dbdump_to_file(kv, fp));
        }
    }

    int ret;
    if (cdbFormat) {
        ret = mdbm_cdbdump_trailer_and_close(fp);
    } else {
        ret = mdbm_dbdump_trailer_and_close(fp);
    }
    CPPUNIT_ASSERT_EQUAL(0, ret);

    string fname;
    MdbmHolder db2(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | versionFlag, 0644, 0 , 0,
                                 &fname));
    CPPUNIT_ASSERT(NULL != (MDBM *) db2);

    string line("mimport -L any ");
    if (cdbFormat) {
       line += string("-F cdb");
    } else {
        line += string("-T");
    }
    line += string(" -i ") + exportFile + " " + fname;
    fp = CreateScript(line);
    RunMashScript(fp);

    VerifyMdbmWithNumbers(db2);
}

// Initial setup for all Mash tests
void
MdbmUnitTestMash::initialSetup()
{
}

/// Tests

// Test mcreate
void
MdbmUnitTestMash::TestCreate()
{
    string prefix = "Test Create";
    TRACE_TEST_CASE(prefix);

    string line, file("create1.mdbm");
    line = BuildCommand("mcreate -p 36k -H 4 -L partition ", file);
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MdbmHolder db(mdbm_open(file.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db);
    CPPUNIT_ASSERT_EQUAL(4, mdbm_get_hash(db));
    CPPUNIT_ASSERT_EQUAL(36*1024, mdbm_get_page_size(db));
    CPPUNIT_ASSERT_EQUAL((uint32_t) MDBM_PARTITIONED_LOCKS, mdbm_get_lockmode(db));
}

// Test mcreate with different options
void
MdbmUnitTestMash::TestCreate2()
{
    string prefix = "Test Create2";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND() // can't use windowed mode (no remap_file_pages() in valgrind)

    string line, file("create2.mdbm");
    line = BuildCommand("mcreate -Z -C LFU -L shared -w 2m", file);
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MdbmHolder db(mdbm_open(file.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db);
    CPPUNIT_ASSERT_EQUAL(4*1024, mdbm_get_page_size(db));
    CPPUNIT_ASSERT_EQUAL((uint32_t) MDBM_RW_LOCKS, mdbm_get_lockmode(db));
    CPPUNIT_ASSERT_EQUAL(MDBM_CACHEMODE_LFU, mdbm_get_cachemode(db));
}

// Test mcreate with initial size and limit size
void
MdbmUnitTestMash::TestCreateInitial()
{
    string prefix = "CreateInitial";
    TRACE_TEST_CASE(prefix);

    string line, file("create_in.mdbm");
    // lockmode=none is valid when creating - should default to exclusive when opening later
    line = BuildCommand("mcreate -O -p 1024 -d 200k -D 1m -L none ", file);
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MdbmHolder db(mdbm_open(file.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db);
    int pagesize = 1024;
    CPPUNIT_ASSERT_EQUAL(pagesize, mdbm_get_page_size(db));
    uint64_t siz = 201 * pagesize;  // Extra header page
    CPPUNIT_ASSERT_EQUAL(siz, mdbm_get_size(db));
    CPPUNIT_ASSERT_EQUAL((uint32_t) 0U, mdbm_get_lockmode(db));
}

// Test mcreate with bad/invalid options
void
MdbmUnitTestMash::TestCreateFail()
{
    string prefix = "CreateBad";
    TRACE_TEST_CASE(prefix);

    string line, file("create_bad.mdbm");
    line = BuildCommand("mcreate -p 17m ", file);   // Page size too big
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MdbmHolder db(mdbm_open(file.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL == (MDBM *) db);
}

// Test mcreate with more invalid options
void
MdbmUnitTestMash::TestCreateFail2()
{
    string prefix = "CreateFail2";
    TRACE_TEST_CASE(prefix);

    string line, file("create_bad2.mdbm");
    line = BuildCommand("mcreate -H 15 ", file);   // Invalid hash function
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MdbmHolder db(mdbm_open(file.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL == (MDBM *) db);
}


// test mdelete_lockfiles
void
MdbmUnitTestMash::TestDeleteLockfiles()
{
    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("deleteLocks", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));
    db.Close();
    string line("mdelete_lockfiles ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    string lockfileName = "/tmp/.mlock-named/" + fname;
    struct stat statBuf;   // Delete lockfile, and make sure you can't find the lockfile
    CPPUNIT_ASSERT_EQUAL(-1, stat(lockfileName.c_str(), &statBuf));
}

// Test mexport in DBdump format
void
MdbmUnitTestMash::TestMashExport()
{
    string prefix = "Test Export";
    TRACE_TEST_CASE(prefix);

    ExportTest(prefix, false);
}

// Test mexport in CDB format
void
MdbmUnitTestMash::TestMashCdbExport()
{
    string prefix = "Test CDB Export";
    TRACE_TEST_CASE(prefix);

    ExportTest(prefix, true);
}

// Test mimport in DBdump format
void
MdbmUnitTestMash::TestMashImport()
{
    string prefix = "Test Import";
    TRACE_TEST_CASE(prefix);

    ImportTest(prefix, false);
}

// Test mimport in CDB format
void
MdbmUnitTestMash::TestMashCdbImport()
{
    string prefix = "Test CDB Import";
    TRACE_TEST_CASE(prefix);

    ImportTest(prefix, true);
}

// Simple locking test
void
MdbmUnitTestMash::TestFetch()
{
    string prefix = "Test Fetch";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND() // can't use windowed mode (no remap_file_pages() in valgrind)

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    // Exercise windowed mode and printing page number
    string line("mfetch --page-number -w 160k "), item("30");
    line += fname + " " + item;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(item));
}

// lock, fetch, and unlock
void
MdbmUnitTestMash::TestLockFetch()
{
    string prefix = "LockFetch";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mlock ");
    line += fname;
    FILE *fp = CreateScript(line);
    string key("20");
    line = "mfetch %_ " + key;
    AddToScript(fp, line);
    //line = "munlock %_ ";
    //AddToScript(fp, line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(key));
}


void
MdbmUnitTestMash::TestStore()
{
    string prefix = "Test Store";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    // exercise MDBM_MODIFY(3)
    string line("mstore --store-mode 3 ");
    string key("60");
    string val("YummyInMyTummy");
    line += fname + " " + key + " " + val;
    FILE *fp = CreateScript(line);
    line = "mfetch " + fname + " " + key;
    AddToScript(fp, line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(val));
}

void
MdbmUnitTestMash::TestStoreHex()
{
    string prefix = "StoreHex";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    // exercise MDBM_REPLACE(default=1)
    string line("mstore -L any -F hex ");
    string key("50");
    string val("Blue_Smurfs\\x01");   // Hex 1 (SOH)
    line += fname + " " + key + " " + val;
    FILE *fp = CreateScript(line);
    line = "mstore -F hex -S 1 ";
    string ky2("FlyHigh");
    string key2 = ky2 + string("\\x00");   // Store Null with mstore
    string val2("Jack\\x20Beanstalk\\x00");   // Space(hex 20) and Null
    line += fname + " " + key2 + " " + val2;
    AddToScript(fp, line);
    RunMashScript(fp);

    MdbmHolder db(mdbm_open(fname.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db);
    datum ky;
    ky.dsize = key.size();
    ky.dptr = (char *) key.c_str();
    datum fetched = mdbm_fetch(db, ky);
    CPPUNIT_ASSERT(NULL != fetched.dptr);
    const char *value1 = "Blue_Smurfs\x01";
    CPPUNIT_ASSERT_EQUAL(fetched.dsize, (int) strlen(value1));
    CPPUNIT_ASSERT_EQUAL(0, strncmp(value1, fetched.dptr, fetched.dsize));

    ky.dptr = (char *) ky2.c_str();
    ky.dsize = ky2.size() + 1;
    fetched = mdbm_fetch(db, ky);
    CPPUNIT_ASSERT(NULL != fetched.dptr);
    string value2("Jack Beanstalk");
    CPPUNIT_ASSERT_EQUAL(fetched.dsize, (int) (value2.size() + 1) );
    CPPUNIT_ASSERT_EQUAL(0, strncmp(value2.c_str(), fetched.dptr, fetched.dsize));
}

void
MdbmUnitTestMash::TestStoreLarge()
{
    string prefix = "StoreLarge";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname, 0, 768);  // pagesize=768
    mdbm_close(mdbm);

    // exercise MDBM_INSERT_DUP(2)
    string line("mstore -O --store-mode 2 ");
    string key("40");
    string val("Klingons");
    val.append(600, '@');   // 600+ chars is large object
    line += fname + " " + key + " " + val;
    FILE *fp = CreateScript(line);
    line = "mfetch " + fname + " " + key;  // mfetch fetches all dupes
    AddToScript(fp, line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(val));
}


void
MdbmUnitTestMash::TestFetchHex()
{
    string prefix = "Test FetchHex";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("fetchhex", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));

    // Store the null char of strings, then retrieve them using mfetch
    string ky("spamIam");
    const datum key = {(char *) ky.c_str(), (int)ky.size() + 1};
    string someval("some value to store");
    datum val = {(char *) someval.c_str(), (int)someval.size() + 1};

    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, key, val, MDBM_REPLACE));
    db.Close();

    string line("mfetch -F hex ");
    ky += string("\\x00");    // Append the null character in hex format
    line += fname + " " + ky;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(someval));   // Find value: only works if fetched
}

void
MdbmUnitTestMash::TestFetchCdb()
{
    string prefix = "Test FetchCdb";
    TRACE_TEST_CASE(prefix);

    TestFetchFormat(prefix, "cdb");
}

void
MdbmUnitTestMash::TestFetchDbdump()
{
    string prefix = "Test FetchDbdump";
    TRACE_TEST_CASE(prefix);

    TestFetchFormat(prefix, "dbdump");
}

void
MdbmUnitTestMash::TestFetchFormat(const string &prefix, const char *format)
{
    string fname;
    MDBM *db = CreateMdbmWithNumbers(prefix, &fname);

    string ky("slurp");
    const datum key = {(char *) ky.c_str(), (int)ky.size()};
    string someval("campbell soup");
    datum val = {(char *) someval.c_str(), (int)someval.size()};

    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, key, val, MDBM_REPLACE));
    mdbm_close(db);

    string line("mfetch -F ");
    line += format + string(" -o ");
    string outfile = GetTmpName(".fetch");
    line += outfile + " " + fname + " " + ky;
    FILE *fp = CreateScript(line);
    string newMdbm = GetTmpName(".mdbm");
    line = string("mimport -T -F ") + format + " -i " + outfile + " " + newMdbm;
    AddToScript(fp, line);
    RunMashScript(fp);
    MdbmHolder db2(mdbm_open(newMdbm.c_str(), MDBM_O_RDONLY, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db2);
    datum fetched = mdbm_fetch(db2, key);
    CPPUNIT_ASSERT(0 != fetched.dsize);
}


// Simple locking test
void
MdbmUnitTestMash::TestLock()
{
    string prefix = "Test Lock1";
    TRACE_TEST_CASE(prefix);

    FILE *fp = BasicLockingTest(prefix);
    RunMashScript(fp);
}

void
MdbmUnitTestMash::TestLockReadOnly()
{
    string prefix = "Test Lock Read Only";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(CreateMdbmWithNumbers(prefix, &fname, MDBM_RW_LOCKS));
    db.Close();

    string line("mlock --lock-mode shared --read-only ");
    line += fname;
    FILE *fp = CreateScript(line);
    line = string("mfetch %_ 75");
    AddToScript(fp, line);
    line = string("munlock %_");
    AddToScript(fp, line);
    RunMashScript(fp);
}

// Test mlock/mfetch/munlock, followed by mcopy, followed by another mlock/mfetch/munlock
void
MdbmUnitTestMash::TestLockAndCopy()
{
    string prefix = "Test LockAndCopy";
    TRACE_TEST_CASE(prefix);

    FILE *fp = BasicLockingTest(prefix);   // Open and lock, then fetch
    string targetMdbm = GetTmpName() + ".mdbm";
    string line("mcopy %_ ");              // Then use mcopy
    line += targetMdbm;
    AddToScript(fp, line);
    line = string("mlock ") + targetMdbm;
    AddToScript(fp, line);
    line = string("mfetch ") + targetMdbm + " 10";   // Then fetch from mcopied file
    AddToScript(fp, line);
    line = string("munlock ") + targetMdbm;
    AddToScript(fp, line);
    RunMashScript(fp);
}

// Lock, then fork and test that it is locked
void
MdbmUnitTestMash::TestLockFork()
{
    string prefix = "LockFork";
    TRACE_TEST_CASE(prefix);

    int ret=0;
    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("lfork", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));

    string line("mlock --lock-mode exclusive ");
    line += fname; line += "\nsleep 1";
    FILE *fp = CreateScript(line);

    pid_t pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) { // child
      RunMashScript(fp);
      exit(0);
    } else {
      // parent
      // NOTE: this is fragile, timing sensitive code.
      usleep(10000); // 10k usec is the minimum sleep to yield to other procs.
      usleep(50000); // Give the child fork a few chances...
      usleep(50000);
      ret = mdbm_islocked(db);
      CPPUNIT_ASSERT_EQUAL(1, ret);
      //CPPUNIT_ASSERT_EQUAL(0, kill(pid, SIGTERM));
      int status;
      CPPUNIT_ASSERT(pid == waitpid(pid, &status, 0));
      CPPUNIT_ASSERT_EQUAL(0, WEXITSTATUS(status));
    }

    db.Close();
}

// Unlock test
void
MdbmUnitTestMash::TestUnlock()
{
    string prefix = "Test Unlock";
    TRACE_TEST_CASE(prefix);

    FILE *fp = BasicLockingTest(prefix, MDBM_O_ASYNC);
    RunMashScript(fp);
}

// Unlock fails
void
MdbmUnitTestMash::TestUnlockFail()
{
    string prefix = "UnlockFail";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("unlockf", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));
    db.Close();

    string line("mlock ");
    line += fname;
    FILE *fp = CreateScript(line);
    line = "mclose -f %_";
    AddToScript(fp, line);
    line = "munlock %_";
    AddToScript(fp, line);

    RunMashScript(fp);
    string failureStr("since it is not locked");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(failureStr));   // should fail to unlock
}


void
MdbmUnitTestMash::TestMcheck()
{
    string prefix = "Test mcheck";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("check", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));
    db.Close();

    string line("mcheck -v 4 -I 3 ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    string failureStr("failed integrity check");
    CPPUNIT_ASSERT_EQUAL(false, FindInOutput(failureStr));   // should not find the failure string
}

void
MdbmUnitTestMash::TestMcompress()
{
    string prefix = "Test mcompress";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(EnsureTmpMdbm("compress", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_pre_split(db, 260));
    insertSomeData(db);
    CPPUNIT_ASSERT_EQUAL(0,InsertData(db, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    uint32_t size1 = mdbm_count_pages(db);
    db.Close();
    string line("mcompress ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);

    MdbmHolder db2(mdbm_open(fname.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db2);
    uint32_t size2 = mdbm_count_pages(db2);
    CPPUNIT_ASSERT(size1 > size2);   // Should have compressed the size
}

void
MdbmUnitTestMash::TestMcompare()
{
    string prefix = "Test mcompare";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mcopy ");
    line += fname + " ";
    string copyToFile = GetTmpName(".mdbm");
    line += copyToFile;
    FILE *fp = CreateScript(line);
    line = string("mcompare -v ") + fname + string(" ") + copyToFile;
    AddToScript(fp, line);
    RunMashScript(fp);
    string successStr("are the same");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(successStr));   // should compare successfully

    // Now add a difference and compare
    mdbm = mdbm_open(copyToFile.c_str(), MDBM_O_RDWR, 0644, 0, 0);
    CPPUNIT_ASSERT(NULL != mdbm);
    string ky("10"), val("RED_NOSE");
    const datum key = {(char *)ky.c_str(), (int)ky.size() };
    const datum value = {(char *)val.c_str(), (int)val.size() };
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, key, value, MDBM_REPLACE));
    mdbm_close(mdbm);
    line = "mcompare -k -V " + copyToFile + " " + fname;
    fp = CreateScript(line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(val));   // The difference should be "val"
}

// Purge test
void
MdbmUnitTestMash::TestMpurge()
{
    string prefix = "Purge";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("purge", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));
    db.Close();
    string line("mpurge ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MDBM *mdbm = mdbm_open(fname.c_str(), MDBM_O_RDONLY, 0444, 0, 0);
    CPPUNIT_ASSERT(NULL != mdbm);
    uint64_t zero = 0;
    CPPUNIT_ASSERT_EQUAL(zero, mdbm_count_records(mdbm));   // purged, so should be zero
    mdbm_close(mdbm);
}

// Sync test
void
MdbmUnitTestMash::TestMsync()
{
    string prefix = "Sync";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("sync", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE, 0, &fname));
    db.Close();
    string line("msync ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
}

// Simple mopen test
void
MdbmUnitTestMash::TestMopen()
{
    string prefix = "OpenSimple";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mopen ");
    line += fname;
    FILE *fp = CreateScript(line);
    string key("82"), value("DEADBEEF");
    line = "mstore %_ " + key + " " + value;
    AddToScript(fp, line);
    line = "mfetch %_ " + key;
    AddToScript(fp, line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(value));
}

void
MdbmUnitTestMash::TestMopenRO()
{
    string prefix = "OpenRO";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mopen -r ");
    line += fname;
    FILE *fp = CreateScript(line);
    string key("abc"), value("val");
    line = "mstore %_ " + key + " " + value;
    AddToScript(fp, line);
    RunMashScript(fp);
    string failureStr("Unable to store successfully");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(failureStr));
}

void
MdbmUnitTestMash::TestClose(const string &prefix, bool doUnlock)
{
    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mlock ");
    line += fname;
    FILE *fp = CreateScript(line);
    string key("20");
    line = "mfetch %_ " + key;
    AddToScript(fp, line);
    if (doUnlock) {
        AddToScript(fp, "munlock %_");
    }
    AddToScript(fp, "mclose -f %_");
    RunMashScript(fp);
}

//  mclose test - mclose should unlock
void
MdbmUnitTestMash::TestMclose()
{
    string prefix = "Mclose";
    TRACE_TEST_CASE(prefix);

    TestClose(prefix, true);
}

//  mclose test - mclose should unlock
void
MdbmUnitTestMash::TestMcloseNoUnlock()
{
    string prefix = "Mclose";
    TRACE_TEST_CASE(prefix);

    TestClose(prefix, false);
    string failureStr("Unlocked 1 lock(s) when");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(failureStr));
}

void
MdbmUnitTestMash::TestMcopy()
{
    string prefix = "copy";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("copy", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE*2, 0, &fname));
    db.Close();
    string copyToFile = GetTmpName(".mdbm");
    string line("mcopy ");
    line += fname + " " + copyToFile;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MDBM *db2 = mdbm_open(copyToFile.c_str(), MDBM_O_RDONLY, 0444, 0, 0);
    CPPUNIT_ASSERT(NULL != db2);
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(db2));
    mdbm_close(db2);
}

void
MdbmUnitTestMash::TestMlockPrint()
{
    string prefix = "lock print";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("lockpr", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE*4, 0, &fname));
    string line("mlock_print ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
}

void
MdbmUnitTestMash::TestMlockReset()
{
    string prefix = "lock reset";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm("lockreset", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                      DEFAULT_PAGE_SIZE*4, 0, &fname));
    string line("mlock_reset ");
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
}

void
MdbmUnitTestMash::TestMdelete()
{
    string prefix = "Test Delete";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mdelete -P "), key("82");
    line += fname + " " + key;
    FILE *fp = CreateScript(line);
    // Now fetch.  It should not be found
    line = "mfetch " + fname + " " + key;
    AddToScript(fp, line);
    RunMashScript(fp);
    string value("1132");   // Values are 1050+key
    CPPUNIT_ASSERT_EQUAL(false, FindInOutput(value));
}

void
MdbmUnitTestMash::TestMdump()
{
    string prefix = "Test Dump";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mdelete "), key("45");
    line += fname + " " + key;
    FILE *fp = CreateScript(line);
    // Now fetch.  It should not be found
    line = "mdump -k -V -x deleted " + fname + " " + key;
    AddToScript(fp, line);
    RunMashScript(fp);
    CPPUNIT_ASSERT_EQUAL(false, FindInOutput(string("1060")));  // Find something not deleted
    string deleted("1095");   // Values are 1050+key
    CPPUNIT_ASSERT_EQUAL(false, FindInOutput(deleted));
}

void
MdbmUnitTestMash::TestMtrunc()
{
    string prefix = "Test truncate";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string line("mtrunc -f -H 2 ");  // Set hash function=2
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    MdbmHolder db(mdbm_open(fname.c_str(), MDBM_O_RDONLY | MDBM_ANY_LOCKS, 0444, 0 , 0));
    uint64_t zero = 0;
    CPPUNIT_ASSERT_EQUAL(zero, mdbm_count_records(db));   // truncated: should be zero
    CPPUNIT_ASSERT_EQUAL(2, mdbm_get_hash(db));
}

// because large-object option is disabled in mdbm_trunc, we want to test
// mtrunc with large object option, then store large object and check that we can fetch it.
void
MdbmUnitTestMash::TestMtruncLarge()
{
    string prefix = "Truncate large";
    TRACE_TEST_CASE(prefix);

    string fname;
    MdbmHolder db(GetTmpPopulatedMdbm(
                      "tlarge",
                       MDBM_O_CREAT | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS | MDBM_LARGE_OBJECTS,
                       0644, DEFAULT_PAGE_SIZE, 0, &fname));

    string line("mtrunc -f -L partition -O ");   // match lock-mode, set large-obj
    line += fname;
    FILE *fp = CreateScript(line);
    RunMashScript(fp);
    uint64_t zero = 0;
    CPPUNIT_ASSERT_EQUAL(zero, mdbm_count_records(db));   // truncated: should be zero
    db.Close();
    string ky("stuff");
    string val(2000, '~');   // Store large object: DEFAULT_PAGE_SIZE=512
    const datum key = {(char *)ky.c_str(), (int)ky.size() };
    const datum value = {(char *)val.c_str(), (int)val.size() };
    MdbmHolder db2(mdbm_open(fname.c_str(), MDBM_O_RDWR | MDBM_ANY_LOCKS, 0644, 0 , 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db2, key, value, MDBM_INSERT));
    datum stuff = mdbm_fetch(db2, key);
    CPPUNIT_ASSERT_EQUAL((int) val.size(), stuff.dsize);
}

void
MdbmUnitTestMash::TestMdigest()
{
    string prefix = "Test digest";
    TRACE_TEST_CASE(prefix);

    string fname;
    MDBM *mdbm = CreateMdbmWithNumbers(prefix, &fname);
    mdbm_close(mdbm);

    string fname2;
    MdbmHolder db2(GetTmpPopulatedMdbm("lockreset", MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                       DEFAULT_PAGE_SIZE*4, 0, &fname2));

    string line("mdigest -H 0 ");
    line += fname + " " + fname2;
    FILE *fp = CreateScript(line);
    line = string("mdigest -v -H 1 ");
    line += fname;
    AddToScript(fp, line);
    RunMashScript(fp);
    string sha1("0996acb91e2f711475bdaacadfe1368a8a5d344c");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(sha1));
    string md5("9c0f21d431b97f2c24f401284a6b095e");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(md5));
    string countStr("Total items= 100");
    CPPUNIT_ASSERT_EQUAL(true, FindInOutput(countStr));
}

CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestMash);

