/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of General/Other features

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

// for usage
#include <sys/time.h>
#include <sys/resource.h>

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
#include "mdbm_internal.h"
#include "atomic.h"
#include "util.hh"
//#include "../lib/multi_lock_wrap.h"
#include "multi_lock.hh"
#include "mdbm_shmem.h"
extern "C" {
#include "stall_signals.h"
}

#include "TestBase.hh"
#include "../../tools/bench_data_utils.hh"
#include "../../tools/bench_existing.cc"   // Unit test the new mdbm_bench interface

#define DO_TEST_READS    0x1
#define DO_TEST_WRITES   0x2
#define DO_TEST_BOTH     (DO_TEST_READS | DO_TEST_WRITES)

extern "C" {
int open_tmp_test_file(char *file, uint32_t siz, char *buf);
}

static const char *LARGE_OBJ_KEY = "LargeObjKey";
static const int NUM_DATA_ITEMS = 456;
static const char * PREFIX  = "PRFX";
static const int STARTPOINT = 12345;

extern int ensure_lock_path(char* path, int skiplast);


using namespace std;

class MdbmUnitTestOther : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestOther(int tFlags) : TestBase(tFlags, "MdbmUnitTestOther") { testFlags = tFlags; }
    virtual ~MdbmUnitTestOther() {}

    void setUp() {}       // Per-test initialization - applies to every test
    void tearDown() {}    // Per-test cleanup - applies to every test

    void initialSetup();

    void testCompressTree();

    void test_OtherAF1();
    void test_OtherAF2();

    void test_OtherAH1();
    void test_OtherAH2();
    void test_OtherAH3();
    void test_OtherAH4();
    void test_OtherAH5();
    void test_OtherAH6();
    void test_OtherAH7();
    void test_OtherAH8();
    void test_OtherAH9();

    void test_OtherAI1();
    void test_OtherAI2();
    void test_OtherAI3();
    void test_OtherAI4();
    void test_OtherAI5();
    void test_OtherAI6();
    void test_OtherAI7();
    void test_OtherAI8();

    void test_OtherAJ1();
    void test_OtherAJ2();
    void test_OtherAJ3();
    void test_OtherAJ4();
    void test_OtherAJ5();
    void test_OtherAJ6();
    void test_OtherAJ7();

    void test_OtherAJ12();
    void test_OtherAJ13();
    void test_OtherAJ14();
    void test_OtherAJ15();
    void test_OtherAJ16();
    void test_OtherAJ17();
    void test_OtherAJ18();
    void test_OtherAJ20();
    void test_OtherAJ23();
    void test_OtherAJ24();
    void test_OtherAJ25();
    void test_OtherAJ36();
    void test_OtherAJ37();
    void test_OtherAJ28();
    void test_OtherAJ29();
    void test_OtherAJ30();
    void test_OtherAJ31();
    void test_OtherAJ32();
    void test_OtherAJ33();
    void test_OtherAJ34();
    void test_OtherAJ35();
    void test_GetDbInfoFails();

    void testRstatsChurn();
    void testSaveRestore();
    void testAtomic();
    void testFileLog();
    void testSysLog();
    void testRobustLocks();
    void testRobustLocksShared();
    void testRobustLocksPart();
    void testPLock();
    void testMLock();
    void testShmem();
    void test_OneFcopy();
    void test_BlockingFcopy();  // Block during the copy, and redo the fcopy
    void test_ThreeFcopys();
    void test_CorruptPadding();


    void finalCleanup();

    // Constants
    static const int VAL_OFFSET = 64;
    static const int MAX_LOOP = 60;

    // Helper methods
    int getmdbmFlags() { return testFlags; }
    virtual void setErrno(MDBM *db);
    string createEmptyMdbm(const string &prefix);
    void testProtectSegv(const string &prefix, int protect, int whatToPerform, bool readOnly = false);
    void testGetDbInfo(MDBM *mdbm, int extraFlags, bool isempty);
    virtual void corruptHeaderAndCheck(MDBM *db);
    double benchmarkFile(const string &filename, double writefrac, int lockmode);
    void test_BenchExisting();
    MDBM* createOversizedPage(const string &prefix);
    int StoreLargeObj(MDBM *mdbm_db, int objNum = 0);
    void oversizedFcopyTester(const string &prefix, bool useMdbmFcopy);
    void fcopyMultiTester(const string &prefix, int retries, bool success);
    off_t getFileSize(const string &fname);

  private:
    int testFlags;
};

void
MdbmUnitTestOther::initialSetup()
{
}

int do_data(MDBM* db, bool insert) {
  int ret = 0;
  const char* keys[] = {"blort", "flarg"};
  int keycount = sizeof(keys)/sizeof(keys[0]);
  datum kv, f;
  for (int i=0; i<keycount; ++i) {
    kv.dptr = (char*)keys[i];
    kv.dsize = strlen(kv.dptr) +1;
    if (insert) {
      int sret = mdbm_store(db, kv, kv, 0);
      int pg = mdbm_get_page(db, &kv);
      fprintf(stderr, "do_data() storing key [%s] page:%d\n", kv.dptr, pg);
      if (sret) {
        fprintf(stderr, "do_data() failed to store key [%s] ret:%d\n", kv.dptr, ret);
        ret = sret;
      }
    } else {
      f = mdbm_fetch(db, kv);
      int pg = mdbm_get_page(db, &kv);
      fprintf(stderr, "do_data() fetching key [%s] page:%d\n", kv.dptr, pg);
      if ((f.dsize != kv.dsize) || memcmp(f.dptr, kv.dptr, kv.dsize)) {
        fprintf(stderr, "do_data() failed to match key [%s] f:%s sz:%d\n", kv.dptr, f.dptr, f.dsize);
        ret = -1;
      }
    }
  }
  return ret;
}

void
MdbmUnitTestOther::testCompressTree()
{
    string prefix = string("testCompressTree") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    { // Trivial case... won't compress
      fprintf(stderr, "%s.Trivial\n", __func__);
      MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                          0644, DEFAULT_PAGE_SIZE, 0));
      mdbm_compress_tree(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
      //InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 10, true, DEFAULT_ENTRY_COUNT);  // Add 10 large objs
      CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));
    }

    { // simple case (no lob/oversize)
      fprintf(stderr, "%s.SimpleCompress\n", __func__);
      int flags = getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR; // | MDBM_LARGE_OBJECTS;
      MdbmHolder mdbm = EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE);
      mdbm_pre_split(mdbm, 260);
      do_data(mdbm, true);
      //mdbm_dump_all_page(mdbm);
      InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT);
      mdbm_compress_tree(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
      //mdbm_dump_all_page(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, do_data(mdbm, false));
      CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));

      //dump_chunk_headers(mdbm);
    }

    { // simple case (no lob/oversize)
      fprintf(stderr, "%s.CacheCompress\n", __func__);
      int flags = getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR; // | MDBM_LARGE_OBJECTS;
      MdbmHolder mdbm = EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE);
      mdbm_set_cachemode(mdbm, MDBM_CACHEMODE_GDSF);
      mdbm_pre_split(mdbm, 260);
      do_data(mdbm, true);
      //mdbm_dump_all_page(mdbm);
      InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT);
      mdbm_compress_tree(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
      //mdbm_dump_all_page(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, do_data(mdbm, false));
      CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));

      //dump_chunk_headers(mdbm);
    }


     { // test with LOBs
      fprintf(stderr, "\n\n%s.LobCompress\n", __func__);
      int lobSize = DEFAULT_PAGE_SIZE*2, lobCount = 1;
      int flags = getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_LARGE_OBJECTS;
      MdbmHolder mdbm = EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE);
      mdbm_pre_split(mdbm, 260);
      do_data(mdbm, true);
      //mdbm_dump_all_page(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, DEFAULT_KEY_SIZE, lobSize, lobCount));
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, lobSize, lobCount));

      {
        StdoutDiverter diverter("/dev/null");
        // just for coverage
        dump_chunk_headers(mdbm);
        dump_mdbm_header(mdbm);
      }

      mdbm_compress_tree(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, lobSize, lobCount));
      //mdbm_dump_all_page(mdbm);
      CPPUNIT_ASSERT_EQUAL(0, do_data(mdbm, false));
      CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));
    }

     { // test with Oversize
      fprintf(stderr, "\n\n%s.OversizeCompress\n", __func__);
      char kbuf[256];
      char vbuf[256];
      datum key, val, fval;
      MDBM_ITER iter;
      //int lobSize = DEFAULT_PAGE_SIZE*2, lobCount = 1;
      int flags = getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR; // | MDBM_LARGE_OBJECTS;
      MdbmHolder mdbm = EnsureTmpMdbm(prefix, flags|MDBM_LARGE_OBJECTS, 0644, DEFAULT_PAGE_SIZE);
      mdbm_pre_split(mdbm, 260);
      CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (mdbm, 260, NULL, NULL));
      do_data(mdbm, true);
      InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT);
      //mdbm_dump_all_page(mdbm);
      sprintf(kbuf, "over_key");
      key.dptr = kbuf;
      key.dsize = strlen(kbuf)+1;
      val.dptr = vbuf;
      for (int l=0; l<20; ++l) {
        memset(vbuf, 0, 256);
        memset(vbuf, '-', 120);
        sprintf(vbuf, "over_val%d", l);
        val.dsize = strlen(vbuf)+1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, key, val, MDBM_INSERT_DUP));
        //mdbm_dump_all_page(mdbm);
      }

      mdbm_compress_tree(mdbm);

      MDBM_ITER_INIT(&iter);
      for (int l=0; l<20; ++l) {
        memset(vbuf, 0, 256);
        memset(vbuf, '-', 120);
        sprintf(vbuf, "over_val%d", l);
        val.dsize = strlen(vbuf)+1;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_fetch_dup_r(mdbm, &key, &fval, &iter));
        CPPUNIT_ASSERT(0 == memcmp(val.dptr, fval.dptr, val.dsize));
      }
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
      CPPUNIT_ASSERT_EQUAL(0, do_data(mdbm, false));
      CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));
      //mdbm_dump_all_page(mdbm);
    }


    { // TODO churn 2 DBs
      fprintf(stderr, "\n\n%s.ChurnCompress\n", __func__);
      int keySize = DEFAULT_KEY_SIZE;
      int valSize = DEFAULT_VAL_SIZE;
      int lobSize = DEFAULT_PAGE_SIZE*2, lobCount = 3;
      int normCount = 100;
      int flags = getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_LARGE_OBJECTS;
      MdbmHolder mdbm = EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE);
      mdbm_pre_split(mdbm, 260);

      // insert initial data that we don't churn
      CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, keySize, lobSize, lobCount));
      CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, keySize, valSize, normCount, true, lobCount));
      // ensure that it inserted correctly
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, lobSize, lobCount));
      CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, valSize, normCount, true, lobCount));

      // churn
      for (int i=0; i<20; ++i) {
        //dump_chunk_headers(mdbm);
        //dump_mdbm_header(mdbm);
        //fprintf(stderr, "=================================================================\n");
        fprintf(stderr, "%s.ChurnCompress iteration %i\n", __func__, i);
        int offset = 1000+(i+1)*(normCount+lobCount);
        int loffset = offset + normCount;
        // Lob
        CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, keySize, lobSize, lobCount, true, loffset));
        CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, lobSize, lobCount, true, loffset));
        // Normal
        CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, keySize, valSize, normCount, true, offset));
        CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, valSize, normCount, true, offset));

        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));
        mdbm_compress_tree(mdbm);
        //fprintf(stderr, "\n##############################################################\n\n");
        //dump_mdbm_header(mdbm);
        //dump_chunk_headers(mdbm);
        //fprintf(stderr, "\n##############################################################\n\n");
        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));

        // Are they still there
        CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, lobSize, lobCount, true, loffset));
        CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, valSize, normCount, true, offset));
        // Delete them
        CPPUNIT_ASSERT_EQUAL(0, DeleteData(mdbm, keySize, lobSize, lobCount, true, loffset));
        CPPUNIT_ASSERT_EQUAL(0, DeleteData(mdbm, keySize, valSize, normCount, true, offset));
        // no longer there...
        CPPUNIT_ASSERT(0 != VerifyData(mdbm, keySize, lobSize, lobCount, true, loffset));
        CPPUNIT_ASSERT(0 != VerifyData(mdbm, keySize, valSize, normCount, true, offset));

        // Check that initial data is still there
        CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, lobSize, lobCount));
        CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, keySize, valSize, normCount, true, lobCount));

        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, 1));
        //dump_chunk_headers(mdbm);
        //dump_mdbm_header(mdbm);
        //fprintf(stderr, "\n\n==============================================================\n\n");
      }
    }


}




void
MdbmUnitTestOther::test_OtherAF1()
{
    string prefix = string("OtherAF1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    errno = 0;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_errno(mdbm));
}

void
MdbmUnitTestOther::test_OtherAF2()
{
    string prefix = string("OtherAF2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    setErrno(mdbm);
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_errno(mdbm));
}

void
MdbmUnitTestOther::test_OtherAH1()
{
    string prefix = string("OtherAH1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 10, true, DEFAULT_ENTRY_COUNT);  // Add 10 large objs
    for (int i = 0; i < 2; ++i) {  // iterate through verbosity levels 0,1
        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 0, i));   // 2nd param, level=0
    }
}

void
MdbmUnitTestOther::test_OtherAH2()
{
    string prefix = string("OtherAH2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 15, true, DEFAULT_ENTRY_COUNT);  // Add 15 large objs
    for (int i = 0; i < 2; ++i) {  // iterate through verbosity levels 0,1
        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 1, i));   // 2nd param, level=1
    }
}


void
MdbmUnitTestOther::test_OtherAH3()
{
    string prefix = string("OtherAH3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 8, true, DEFAULT_ENTRY_COUNT);  // Add 8 large objs
    for (int i = 0; i < 2; ++i) {  // iterate through verbosity levels 0,1
        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 2, i));   // 2nd param, level=2
    }
}

void
MdbmUnitTestOther::test_OtherAH4()
{
    string prefix = string("OtherAH4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 16, true, DEFAULT_ENTRY_COUNT);  // Add 16 large objs
    for (int i = 0; i < 2; ++i) {  // iterate through verbosity levels 0,1
        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(mdbm, 3, i));   // 2nd param, level=3
    }
}

void
MdbmUnitTestOther::test_OtherAH5()
{
    string prefix = string("OtherAH5") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    // Corruption tests do not use MdbmHolder - cannot close
    MDBM *mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0);
    CPPUNIT_ASSERT(mdbm != NULL);
    corruptHeaderAndCheck(mdbm);
}


void
MdbmUnitTestOther::test_OtherAH6()
{
    string prefix = string("OtherAH6") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    MDBM *mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0);
    // Corrupt the chunks

    struct mdbm *dbm = (struct mdbm *) mdbm;
    mdbm_hdr_t* hd = dbm->db_hdr;
    hd->h_first_free = 789;

    for (int verbosity=0; verbosity < 2; ++verbosity) {   // do the check
        CPPUNIT_ASSERT(mdbm_check(mdbm, 1, verbosity) != 0);
    }
}

void
MdbmUnitTestOther::test_OtherAH7()
{
    string prefix = string("OtherAH7") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    MDBM *mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0);

    // Corrupt the directory
    struct mdbm *dbm = (struct mdbm *) mdbm;
    mdbm_page_t *pagezero = (mdbm_page_t*)dbm->db_base;
    pagezero->p_type = 1;

    for (int verbosity=0; verbosity < 2; ++verbosity) {   // do the check
        CPPUNIT_ASSERT(mdbm_check(mdbm, 2, verbosity) != 0);
    }
}

void
MdbmUnitTestOther::test_OtherAH8()
{
    string prefix = string("OtherAH8") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    MDBM *mdbm = GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0);
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE/2, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);

    // Corrupt the directory
    struct mdbm *dbm = (struct mdbm *) mdbm;
    mdbm_page_t *pageone = (mdbm_page_t*) (dbm->db_base + dbm->db_pagesize);
    pageone->p.p_num_entries = 1234556;

    for (int verbosity=0; verbosity < 2; ++verbosity) {   // do the check
        CPPUNIT_ASSERT(mdbm_check(mdbm, 3, verbosity) != 0);
    }
}

void
MdbmUnitTestOther::test_OtherAH9()
{
    string prefix = string("OtherAH9") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    MDBM *mdbm = EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                               0644, DEFAULT_PAGE_SIZE);
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);

    // Corrupt the directory
    struct mdbm *dbm = (struct mdbm *) mdbm;
    mdbm_page_t *pageone = (mdbm_page_t*) (dbm->db_base + dbm->db_pagesize);
    pageone->p_num = 400;

    for (int verbosity=0; verbosity < 2; ++verbosity) {   // do the check
        CPPUNIT_ASSERT(mdbm_check(mdbm, 3, verbosity) != 0);
    }
}

void
MdbmUnitTestOther::test_OtherAI1()
{
    string prefix = string("OtherAI1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_protect(mdbm, MDBM_PROT_ACCESS));
    // Read and write - should succeed
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 10, true, DEFAULT_ENTRY_COUNT);  // Add large objs
}

void
MdbmUnitTestOther::test_OtherAI2()
{
    string prefix = string("OtherAI2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_protect(mdbm, MDBM_PROT_READ));
    // Read - should succeed
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
}

void
MdbmUnitTestOther::test_OtherAI3()
{
    string prefix = string("OtherAI3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_protect(mdbm, MDBM_PROT_WRITE));
    // Write - should succeed
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE/4, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);
}

void
MdbmUnitTestOther::test_OtherAI4()
{
    string prefix = string("OtherAI4") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    testProtectSegv(prefix, MDBM_PROT_NOACCESS, DO_TEST_READS);
}

void
MdbmUnitTestOther::test_OtherAI5()
{
    string prefix = string("OtherAI5") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    testProtectSegv(prefix, MDBM_PROT_NOACCESS, DO_TEST_WRITES);
}

void
MdbmUnitTestOther::test_OtherAI6()
{
    string prefix = string("OtherAI6") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    testProtectSegv(prefix, MDBM_PROT_READ, DO_TEST_WRITES);
}

void
MdbmUnitTestOther::test_OtherAI7()
{
    string prefix = string("OtherAI7") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    testProtectSegv(prefix, MDBM_PROT_WRITE, DO_TEST_READS);
}

void
MdbmUnitTestOther::test_OtherAI8()
{
    string prefix = string("OtherAI8") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_VALGRIND()

    testProtectSegv(prefix, MDBM_PROT_ACCESS, DO_TEST_WRITES, true);
}

void
MdbmUnitTestOther::test_OtherAJ1()
{
    string prefix = string("OtherAJ1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 10, true, DEFAULT_ENTRY_COUNT);  // Add large objs
    testGetDbInfo(mdbm, 0, false);
}

void
MdbmUnitTestOther::test_OtherAJ2()
{
    string prefix = string("OtherAJ2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR,
                    0644, DEFAULT_PAGE_SIZE, 0));
    testGetDbInfo(mdbm, 0, true);
}

void
MdbmUnitTestOther::test_OtherAJ3()
{
    string prefix = string("OtherAJ3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR | MDBM_LARGE_OBJECTS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 10, true, DEFAULT_ENTRY_COUNT);  // Add large objs
    testGetDbInfo(mdbm, MDBM_STAT_NOLOCK, false);
}

void
MdbmUnitTestOther::test_OtherAJ4()
{
    string prefix = string("OtherAJ4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR,
                    0644, DEFAULT_PAGE_SIZE, 0));
    testGetDbInfo(mdbm, MDBM_STAT_NOLOCK, true);
}

//getPartitionNumber

void
MdbmUnitTestOther::test_OtherAJ5()
{
    string prefix = string("OtherAJ5:exclusive lock") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    int partitionNumber = -1;
    datum key;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR,
                    0644, DEFAULT_PAGE_SIZE, 0));
    partitionNumber =  mdbm_get_partition_number(mdbm, key);
    CPPUNIT_ASSERT(partitionNumber == -1);
}

void
MdbmUnitTestOther::test_OtherAJ6()
{
    string prefix = string("OtherAJ6: Partition Lock") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    int partitionNumber = -1;
    datum key;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS,
                    0644, DEFAULT_PAGE_SIZE, 0));
    partitionNumber =  mdbm_get_partition_number(mdbm, key);
    CPPUNIT_ASSERT(partitionNumber != -1);
}

void
MdbmUnitTestOther::test_OtherAJ7()
{
    string prefix = string("OtherAJ7: NULL MDBM") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    int partitionNumber = -1;
    datum key;
    MDBM *mdbm = NULL;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    //MDBM is NULL
    partitionNumber =  mdbm_get_partition_number(mdbm, key);
    CPPUNIT_ASSERT(partitionNumber == -1);
}

//mdbm_get_filename
void
MdbmUnitTestOther::test_OtherAJ12()
{
    string prefix = string("OtherAJ12: get_file_name") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    datum key;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    const char* file_name_db =  fname.c_str();

    const char* file_name_api = mdbm_get_filename(mdbm);
    CPPUNIT_ASSERT(file_name_api != NULL);
    CPPUNIT_ASSERT_EQUAL(0, strcmp(file_name_api,file_name_db));
}

//mdbm_get_fd
void
MdbmUnitTestOther::test_OtherAJ13()
{
    string prefix = string("OtherAJ13: get_fd") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    datum key;
    int fd_id = -1;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    string fname;

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));
    fd_id = mdbm_get_fd(mdbm);
    CPPUNIT_ASSERT(fd_id != -1);
}

//mdbm_fcopy
void
MdbmUnitTestOther::test_OtherAJ14()
{
    string prefix = string("OtherAJ14: mdbm_fcopy") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    datum key, value;
    int ret = -1;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    string fname1 = GetTmpName("mdbm_fcopy");
    int fd = open("mdbm_fcopy", O_RDWR | O_CREAT | O_TRUNC, mode);
    CPPUNIT_ASSERT(fd != -1);

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);
    value.dptr = (char*)"value";
    value.dsize = strlen(value.dptr);

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, key, value, MDBM_INSERT)) == 0);
    CPPUNIT_ASSERT(ret = mdbm_fcopy(mdbm, fd, 0) !=  -1);
    close(fd);
}

void
MdbmUnitTestOther::test_OtherAJ15()
{
    string prefix = string("OtherAJ15: mdbm_fcopy, Large object") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    int ret = -1;
    int _largeObjSize = 512;
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    string fname1 = GetTmpName("mdbm_fcopy");
    int fd = open("mdbm_fcopy", O_RDWR | O_CREAT | O_TRUNC, mode);
    CPPUNIT_ASSERT(fd != -1);

     /* LO creation */
    string key = (char*)"lokey01", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB, 0644, DEFAULT_PAGE_SIZE, 0, &fname));
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, dk, dv, MDBM_INSERT)) == 0);
    CPPUNIT_ASSERT(ret = mdbm_fcopy(mdbm, fd, 0) !=  -1);
    close(fd);
}

//mdbm_init_rstats()
void
MdbmUnitTestOther::test_OtherAJ16()
{
    string prefix = string("OtherAJ16: mdbm_init_rstats") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    int ret = -1;
    datum dk, dv;

    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"value";
    dv.dsize = strlen(dv.dptr);

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, dk, dv, MDBM_INSERT)) == 0);
    CPPUNIT_ASSERT(ret = mdbm_init_rstats(mdbm, 0) !=  -1);
}

//mdbm_diff_rstats
void
MdbmUnitTestOther::test_OtherAJ17()
{
    string prefix = string("OtherAJ17: mdbm_diff_rstats") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    int ret = -1;
    datum dk, dv;
    struct mdbm_rstats_mem* m;
    mdbm_rstats_t* s;
    mdbm_rstats_t s0, d;

    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"value";
    dv.dsize = strlen(dv.dptr);

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    CPPUNIT_ASSERT((ret = mdbm_store(mdbm, dk, dv, MDBM_INSERT)) == 0);
    memset(&s0,0,sizeof(s0));
    CPPUNIT_ASSERT((ret = mdbm_open_rstats(fname.c_str(),O_RDWR,&m,&s)) == 0);
    mdbm_diff_rstats(&s0,s,&d,&s0);
    mdbm_close_rstats(m);
}

//mdbm_purge()
void
MdbmUnitTestOther::test_OtherAJ18()
{
    string prefix = string("OtherAJ18: mdbm_purge") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    int spillSize = 256;

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));
    mdbm_setspillsize(mdbm, spillSize);
    mdbm_purge(mdbm);
}


//mdbm_lock_reset()
void
MdbmUnitTestOther::test_OtherAJ20()
{
    string prefix = string("OtherAJ20: mdbm_lock_reset") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    int ret = -1;

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

#ifdef ALLOW_MLOCK_RESET
    CPPUNIT_ASSERT((ret = mdbm_lock_reset(fname.c_str(), 0)) == 0);
#else
    CPPUNIT_ASSERT((ret = mdbm_lock_reset(fname.c_str(), 0)) < 0);
#endif
}

void MdbmUnitTestOther::testSaveRestore() {
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm("testSaveRestore", getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));


  // not implemented
  errno = 0;
  CPPUNIT_ASSERT(0 > mdbm_save(mdbm, "/tmp/blort", 0, 0, 0));
  CPPUNIT_ASSERT(errno = ENOSYS);

  // not implemented
  errno = 0;
  CPPUNIT_ASSERT(0 > mdbm_restore(mdbm, "/tmp/blort"));
  CPPUNIT_ASSERT(errno = ENOSYS);

}
void MdbmUnitTestOther::testAtomic() {
  TRACE_TEST_CASE(__func__)
  int32_t  si32 = 1;
  uint32_t ui32 = 1;

  CPPUNIT_ASSERT( 1 == atomic_dec32s(&si32));
  CPPUNIT_ASSERT( 1 == atomic_dec32u(&ui32));

  CPPUNIT_ASSERT( 0 == atomic_inc32s(&si32));
  CPPUNIT_ASSERT( 0 == atomic_inc32u(&ui32));

  CPPUNIT_ASSERT( 1 == atomic_add32s(&si32, 2));
  CPPUNIT_ASSERT( 1 == atomic_add32u(&ui32, 2));

  CPPUNIT_ASSERT( 3 == atomic_cas32s(&si32, 3, 0));
  CPPUNIT_ASSERT( 3 == atomic_cas32u(&ui32, 3, 0));

  CPPUNIT_ASSERT( true == atomic_cmp_and_set_32_bool(&si32, 0, 5));
  CPPUNIT_ASSERT( si32 == 5);

  // just for coverage. These expose no testable state
  atomic_barrier();
  atomic_pause();
}
void MdbmUnitTestOther::testFileLog() {
  TRACE_TEST_CASE(__func__)
  string logname = GetTmpName();
  CPPUNIT_ASSERT(0 == mdbm_select_log_plugin("file"));
  mdbm_log(LOG_WARNING, "This is a logfile->stderr test message\n");
  CPPUNIT_ASSERT(0 == mdbm_set_log_filename(logname.c_str()));
  mdbm_log_minlevel(LOG_WARNING);
  mdbm_log(LOG_WARNING, "This is a logfile test message\n");

  logname += "b";
  CPPUNIT_ASSERT(0 == mdbm_set_log_filename(logname.c_str()));
  mdbm_log(LOG_WARNING, "This is a logfile-b test message\n");

  CPPUNIT_ASSERT(-1 == mdbm_set_log_filename(NULL));
  mdbm_log(LOG_WARNING, "This is a logfile->stderr test message\n");

  pid_t pid = fork();
  CPPUNIT_ASSERT(pid >= 0);
  if (!pid) {
    // child, ALERT causes exit(1);
    mdbm_log(LOG_ALERT, "This is a logfile->stderr test message\n");
    exit(0);
  } else {
    // parent
    int status;
    CPPUNIT_ASSERT(pid == waitpid(pid, &status, 0));
    CPPUNIT_ASSERT(WIFEXITED(status));
    CPPUNIT_ASSERT(0 != WEXITSTATUS(status));

  }

  pid = fork();
  CPPUNIT_ASSERT(pid >= 0);
  if (!pid) {
    // child, EMERG causes abort();
    mdbm_log(LOG_EMERG, "This is a logfile->stderr test message\n");
    exit(0);
  } else {
    // parent
    int status;
    CPPUNIT_ASSERT(pid == waitpid(pid, &status, 0));
    CPPUNIT_ASSERT(!WIFEXITED(status));
  }
}

void MdbmUnitTestOther::testSysLog() {
  TRACE_TEST_CASE(__func__)
  CPPUNIT_ASSERT(0 == mdbm_select_log_plugin("syslog"));
  mdbm_log_minlevel(LOG_WARNING);
  mdbm_log(LOG_WARNING, "This is a syslog test message\n");
}


void MdbmUnitTestOther::testRobustLocks() {
    TRACE_TEST_CASE(__func__)

    string fname;
    //MdbmHolder mdbm(GetTmpPopulatedMdbm("testRobustLocks", getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));
    MdbmHolder mdbm(GetTmpPopulatedMdbm("testRobustLocks", getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    pid_t pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) {
      // child, grab lock and exit
      mdbm_lock(mdbm);
      exit(1);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
      //CPPUNIT_ASSERT(1 == mdbm_trylock(mdbm)); // ylock has no robust try :(
      CPPUNIT_ASSERT(1 == mdbm_lock(mdbm));
      CPPUNIT_ASSERT(1 == mdbm_unlock(mdbm));
    }

    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) {
      // child, grab lock and exit
      mdbm_lock(mdbm);
      exit(1);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
      MdbmHolder mdbm2(fname);
      // open locks, so should clean the expired lock
      CPPUNIT_ASSERT(0 == mdbm2.Open(getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, 0, 0));
    }
    mdbm_lock_dump(mdbm);
}

void MdbmUnitTestOther::testRobustLocksShared() {
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm("testRobustLocksShared", getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_RW_LOCKS, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    pid_t pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) {
      // child, grab lock and exit
      mdbm_lock_shared(mdbm);
      exit(1);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
      CPPUNIT_ASSERT(1 == mdbm_trylock_shared(mdbm));
      CPPUNIT_ASSERT(1 == mdbm_unlock(mdbm));
    }

    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) {
      // child, grab lock and exit
      mdbm_lock_shared(mdbm);
      exit(1);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
      MdbmHolder mdbm2(fname);
      // open locks, so should clean the expired lock
      CPPUNIT_ASSERT(0 == mdbm2.Open(getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_RW_LOCKS, 0644, 0, 0));
    }
    mdbm_lock_dump(mdbm);
}

void MdbmUnitTestOther::testRobustLocksPart() {
    TRACE_TEST_CASE(__func__)

    string keyStr = "blort";
    datum key = { (char*)keyStr.c_str(), (int)keyStr.size() };

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm("testRobustLocksPart", getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    pid_t pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) {
      // child, grab lock and exit
      mdbm_plock(mdbm, &key, 0);
      exit(1);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
      CPPUNIT_ASSERT(1 == mdbm_tryplock(mdbm, &key, 0));
      CPPUNIT_ASSERT(1 == mdbm_punlock(mdbm, &key, 0));
    }

    pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) {
      // child, grab lock and exit
      mdbm_plock(mdbm, &key, 0);
      exit(1);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
      MdbmHolder mdbm2(fname);
      // open locks, so should clean the expired lock
      CPPUNIT_ASSERT(0 == mdbm2.Open(getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS, 0644, 0, 0));
    }
    mdbm_lock_dump(mdbm);
}


extern "C" {
  int db_part_owned(MDBM* db);
  int db_multi_part_locked(MDBM* db);
  //int get_lockfile_name(const char* dbname, char* lockname, int maxlen);
  //int ensure_lock_path(char* path, int skiplast);
}

extern const char* ErrToStr(int err);

void MdbmUnitTestOther::testPLock() {
    TRACE_TEST_CASE(__func__)

    PLockFile locks;
    bool created;
    string fname = GetTmpName("plock_test");

    locks.Open(fname.c_str(), 3, 3, created);
    // TODO CALL:
    CPPUNIT_ASSERT(0 == locks.RegisterGet(0));
    CPPUNIT_ASSERT(0 == locks.RegisterSet(0, 42));
    CPPUNIT_ASSERT(0 == locks.RegisterInc(0));
    CPPUNIT_ASSERT(0 == locks.RegisterDec(0));
    CPPUNIT_ASSERT(0 == locks.RegisterAdd(0, 20));
    CPPUNIT_ASSERT(0 == locks.RegisterCas(0, 62, 42));
    CPPUNIT_ASSERT(42 == locks.RegisterGet(0));
    CPPUNIT_ASSERT(locks.GetNumRegisters() > 0);
}

void MdbmUnitTestOther::testMLock() {
    TRACE_TEST_CASE(__func__)

    {
      string fname;
      MdbmHolder mdbm(GetTmpPopulatedMdbm("testMLock", getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS, 0644, DEFAULT_PAGE_SIZE, 0, &fname));
  //    MDBM* db = (MDBM*)mdbm;
  //    struct mdbm_locks *db_locks = db->db_locks;
  //    mlock_t* locks = db_locks->db_mlocks;
  //    uint32_t tid = gettid();

      CPPUNIT_ASSERT(0 == db_part_owned(mdbm));
      CPPUNIT_ASSERT(0 == db_multi_part_locked(mdbm));
      //CPPUNIT_ASSERT(0 > get_lockfile_name(fname.c_str(), NULL, 0));
      CPPUNIT_ASSERT(0 > ensure_lock_path(NULL, 0));
      CPPUNIT_ASSERT(0 > ensure_lock_path((char*)"", 0));
      if (0 != getuid()) { // expected fail doesn't happen for root
        CPPUNIT_ASSERT(0 > ensure_lock_path((char*)"/blort", 0));
      }
      mdbm_delete_lockfiles(fname.c_str());
    }

    MLock locks;
    string fname = GetTmpName("mlock_test");

    locks.Open(fname.c_str(), 1, MLOCK_INDEX, 128);

    // TODO CALL:
    CPPUNIT_ASSERT(locks.GetNumLocks() == 129); // partitions + core lock
    // invalid calls
    CPPUNIT_ASSERT(locks.LockBase(-1) !=0);
    CPPUNIT_ASSERT(locks.UnlockBase(0) !=0);
    CPPUNIT_ASSERT(locks.UnlockBase(-1) !=0);
    CPPUNIT_ASSERT(locks.Unlock(-100) !=0);
    CPPUNIT_ASSERT(locks.Unlock(0) !=0);

    CPPUNIT_ASSERT(locks.GetBaseOwnerId(-10) ==(owner_t)-1);
    CPPUNIT_ASSERT(locks.GetBaseOwnerId(0) ==0);
    CPPUNIT_ASSERT(locks.GetBaseLockCount(-1) ==-1);
    CPPUNIT_ASSERT(locks.GetBaseLockCount(0) ==0);
    CPPUNIT_ASSERT(locks.GetBaseLocalCount(-1) ==-1);
    CPPUNIT_ASSERT(locks.GetBaseLocalCount(0) ==0);

    CPPUNIT_ASSERT(locks.GetOwnerId(-10) ==(owner_t)-1);
    CPPUNIT_ASSERT(locks.GetOwnerId(0) ==0);
    CPPUNIT_ASSERT(locks.GetLockCount(-10) ==-1);
    CPPUNIT_ASSERT(locks.GetLockCount(0) ==0);
    CPPUNIT_ASSERT(locks.GetLocalCount(-10) ==-1);
    CPPUNIT_ASSERT(locks.GetLocalCount(0) ==0);

    CPPUNIT_ASSERT(locks.GetLockCountTotal() ==0);
    CPPUNIT_ASSERT(locks.GetLockedPartCount() ==0);
    CPPUNIT_ASSERT(locks.GetLocalPartCount() ==0);

    // force unlock-search
    CPPUNIT_ASSERT(locks.Lock(5) ==0);
    CPPUNIT_ASSERT(locks.Lock(7) ==0);
    CPPUNIT_ASSERT(locks.Unlock(MLOCK_ANY) ==0);
    CPPUNIT_ASSERT(locks.Unlock(MLOCK_ANY) ==0);
    // and a failed unlock-search
    CPPUNIT_ASSERT(locks.Unlock(MLOCK_ANY) !=0);

    // upgrade
    CPPUNIT_ASSERT(locks.Lock(5) ==0);
    CPPUNIT_ASSERT(locks.Upgrade(true) ==0); // TODO do for EXCL, SHARE, & PART
    CPPUNIT_ASSERT(locks.Downgrade(3, true) ==0); // TODO do for EXCL, SHARE, & PART

    // take out some locks to trigger dump code...
    CPPUNIT_ASSERT(locks.Lock(5) ==0);
    CPPUNIT_ASSERT(locks.LockBase(0) ==0);
    CPPUNIT_ASSERT(locks.Lock(MLOCK_EXCLUSIVE) ==0);
    locks.DumpLockState(stderr);


    CPPUNIT_ASSERT(NULL != ErrToStr(EBUSY));
    CPPUNIT_ASSERT(NULL != ErrToStr(EINPROGRESS));
    CPPUNIT_ASSERT(NULL != ErrToStr(EINVAL));
    CPPUNIT_ASSERT(NULL != ErrToStr(EAGAIN));
    CPPUNIT_ASSERT(NULL != ErrToStr(ENOENT));
    CPPUNIT_ASSERT(NULL != ErrToStr(ENOMEM));
    CPPUNIT_ASSERT(NULL != ErrToStr(EPERM));
    CPPUNIT_ASSERT(NULL != ErrToStr(EOPNOTSUPP));
    CPPUNIT_ASSERT(NULL != ErrToStr(-1));
}

void MdbmUnitTestOther::testShmem() {
  TRACE_TEST_CASE(__func__)

  mdbm_shmem_t* shm = NULL;
  int init;

  // normal open
  string shname = GetTmpName();
  int flags = MDBM_SHMEM_RDWR | MDBM_SHMEM_CREATE;
  shm = mdbm_shmem_open(shname.c_str(), flags, 4096, &init);
  CPPUNIT_ASSERT(shm);
  CPPUNIT_ASSERT(init == 1);
  mdbm_shmem_init_complete(shm);
  CPPUNIT_ASSERT(mdbm_shmem_fd(shm) >= 0);
  mdbm_shmem_close(shm, 0);

  // open, and auto-delete on close
  shm = mdbm_shmem_open(shname.c_str(), flags|MDBM_SHMEM_UNLINK, 4096, &init);
  CPPUNIT_ASSERT(shm);
  CPPUNIT_ASSERT(init != 1);
  mdbm_shmem_close(shm, MDBM_SHMEM_UNLINK);

  // no init, sync-on-close
  shm = mdbm_shmem_open(shname.c_str(), flags|MDBM_SHMEM_UNLINK, 4096, NULL);
  CPPUNIT_ASSERT(shm);
  mdbm_shmem_close(shm, MDBM_SHMEM_UNLINK|MDBM_SHMEM_SYNC);

  shname = GetTmpName();
  // open, with guard pages
  shm = mdbm_shmem_open(shname.c_str(), flags|MDBM_SHMEM_UNLINK|MDBM_SHMEM_GUARD, 4096, &init);
  CPPUNIT_ASSERT(shm);
  CPPUNIT_ASSERT(init == 1);
  mdbm_shmem_close(shm, MDBM_SHMEM_UNLINK);

  // delayed open for child (waiting on init)
  shname = GetTmpName();
  pid_t pid = fork();
  CPPUNIT_ASSERT(pid >= 0);
  if (!pid) {
    init = 0;
    sleep(1); // delay open, for parent
    shm = mdbm_shmem_open(shname.c_str(), flags, 4096, &init);
    if (!shm) {
      if (errno == EWOULDBLOCK) {
        exit(0); // OK, timeout waiting for parent to init
      }
      exit(-1); // other unexpected error
    }
    if (init == 1) {
      exit(-1); // parent should have init'd
    }
    mdbm_shmem_close(shm, 0);
    exit(0);
  } else {
    // parent
    int status;
    shm = mdbm_shmem_open(shname.c_str(), flags, 4096, &init);
    CPPUNIT_ASSERT(shm);
    CPPUNIT_ASSERT(init == 1);
    sleep(2); // delay flagging initialization for child
    mdbm_shmem_init_complete(shm);
    sleep(1);
    mdbm_shmem_close(shm, 0);

    CPPUNIT_ASSERT(pid == waitpid(pid, &status, 0));
    CPPUNIT_ASSERT(WIFEXITED(status));
    CPPUNIT_ASSERT(0 == WEXITSTATUS(status));

  }

  // TRUNC with invalid size
  shm = mdbm_shmem_open(shname.c_str(), flags|MDBM_SHMEM_TRUNC, 0, &init);
  CPPUNIT_ASSERT(NULL == shm);

  // // TRUNC with read-only
  // shname = GetTmpName();
  // shm = mdbm_shmem_open(shname.c_str(), MDBM_SHMEM_CREATE | MDBM_SHMEM_TRUNC, 4096, &init);
  // CPPUNIT_ASSERT(NULL == shm);

  // invalid name
  shm = mdbm_shmem_open(NULL, flags|MDBM_SHMEM_TRUNC, 0, &init);
  CPPUNIT_ASSERT(NULL == shm);
  shm = mdbm_shmem_open("", flags|MDBM_SHMEM_TRUNC, 0, &init);
  CPPUNIT_ASSERT(NULL == shm);
  shm = mdbm_shmem_open("/non-existent-dir/blort.shm", flags|MDBM_SHMEM_TRUNC, 0, &init);
  CPPUNIT_ASSERT(NULL == shm);
  shm = mdbm_shmem_open("/non-existent-dir/blort.shm", MDBM_SHMEM_RDWR, 0, &init);
  CPPUNIT_ASSERT(NULL == shm);


  // get-fd error case
  CPPUNIT_ASSERT(mdbm_shmem_fd(NULL) < 0);
}


void MdbmUnitTestOther::testRstatsChurn() {
    string prefix = "RstatsChurn";
    TRACE_TEST_CASE(__func__)
    string fname = GetTmpName();
    int i, ret = -1;
    char key[16];
    char val[4096];
    datum kdat = { key, 4 };
    datum vdat = { val, 4000 };
    struct mdbm_rstats_mem* m;
    mdbm_rstats_t* s;
    mdbm_rstats_t s0, d;
    int oflags = getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_LARGE_OBJECTS;
    MdbmHolder db(fname);
    db.Open(oflags, 0644, 4096, 0);

    CPPUNIT_ASSERT(ret = mdbm_init_rstats(db, MDBM_RSTATS_THIST) !=  -1);
    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 20, NULL, NULL));

    // churn large
    memset(key, 0, 16);
    memset(val, '*', 4096);
    for (i=0; i<17; ++i) {
      kdat.dsize = 1+ snprintf(key, 16, "%d", i);
      CPPUNIT_ASSERT(0 == mdbm_store(db, kdat, vdat, 0));
    }
    for (i=1; i<8; ++i) {
      kdat.dsize = 1+ snprintf(key, 16, "%d", i*2);
      CPPUNIT_ASSERT(0 == mdbm_delete(db, kdat));
    }
    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3 (db, 30, NULL, NULL));
    vdat.dsize = 4096;
    for (i=1; i<10; ++i) {
      vdat.dsize = 1+ snprintf(key, 16, "%d", i);
      CPPUNIT_ASSERT(0 == mdbm_store(db, kdat, vdat, 0));
    }
    for (i=1; i<10; ++i) {
      vdat.dsize = 1+ snprintf(key, 16, "%d", i);
      CPPUNIT_ASSERT(0 == mdbm_delete(db, kdat));
    }

    // churn oversize
    memset(val, '*', 256);
    kdat.dsize = 1+ snprintf(key, 16, "key");
    for (i=0; i<90; ++i) {
      //fprintf(stderr, "STORING ENTRY %d\n", i);
      CPPUNIT_ASSERT(0 == mdbm_store(db, kdat, vdat, MDBM_INSERT_DUP));
    }
    for (i=1; i<50; ++i) {
      CPPUNIT_ASSERT(0 == mdbm_delete(db, kdat));
    }
    for (i=1; i<50; ++i) {
      CPPUNIT_ASSERT(0 == mdbm_store(db, kdat, vdat, MDBM_INSERT_DUP));
    }

    memset(&s0,0,sizeof(s0));
    CPPUNIT_ASSERT((ret = mdbm_open_rstats(fname.c_str(),O_RDWR,&m,&s)) == 0);
    mdbm_diff_rstats(&s0,s,&d,&s0);
    mdbm_close_rstats(m);
}


//mdbm_set_stats_func error cases()
void
MdbmUnitTestOther::test_OtherAJ23()
{
    string prefix = string("OtherAJ23: mdbm_set_stats_func Error") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_set_stats_func(mdbm, 0, 0, 0));
}

//mdbm_dup_handle() Error
void
MdbmUnitTestOther::test_OtherAJ24()
{
    string prefix = string("OtherAJ24: mdbm_dup_handle Error") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    MDBM *bsdbh = mdbm_dup_handle(mdbm, MDBM_O_RDWR);
    // V2 does not support
    CPPUNIT_ASSERT(bsdbh == NULL);
}

//mdbm_get_partition_number() Error
void
MdbmUnitTestOther::test_OtherAJ25()
{
    string prefix = string("test_OtherAJ25: Error") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    int partitionNumber = -1;
    datum key;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS,
                    0644, DEFAULT_PAGE_SIZE, 0));
    partitionNumber =  mdbm_get_partition_number(mdbm, key);
    // V2 does not support
    CPPUNIT_ASSERT(partitionNumber == -1);
}

// test fcopy with oversized pages
// Somewhat tricky code that uses limit_size+dupe insertions to create an oversized page
void
MdbmUnitTestOther::test_OtherAJ36()
{
    string prefix = string("OtherAJ36: mdbm_fcopy") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    oversizedFcopyTester(prefix, true);
}

void
MdbmUnitTestOther::oversizedFcopyTester(const string &prefix, bool useMdbmFcopy)
{
    MDBM *db = createOversizedPage(prefix);

    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    string fname2 = GetTmpName("mdbm_fcopy");
    int fd = open(fname2.c_str(), O_RDWR | O_CREAT | O_TRUNC, mode);
    CPPUNIT_ASSERT(fd != -1);

    if (useMdbmFcopy) {
        CPPUNIT_ASSERT(mdbm_fcopy(db, fd, 0) != -1);
    } else {
        CPPUNIT_ASSERT(mdbm_internal_fcopy(db, fd, 0, 1) != -1);
    }

    close(fd);

    MDBM *db2;
    CPPUNIT_ASSERT((db2 = mdbm_open(fname2.c_str(), MDBM_O_RDWR|MDBM_O_FSYNC|MDBM_LARGE_OBJECTS,
                                    0666, 1024, 0)) != NULL);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_check(db2, 3, 1));

    datum k, value;
    int j = 1, temp;
    k.dptr = reinterpret_cast<char*>(&j);
    k.dsize = sizeof(j);

    for (int i = 0; i < 10; i++) {
        // fetch the record 10 times, just in case
        CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(db2));
        value = mdbm_fetch(db2, k);
        if (value.dptr) {
            CPPUNIT_ASSERT(value.dptr != NULL);
            temp = *((int *) value.dptr);   // Who knows which dupe was fetched
            CPPUNIT_ASSERT((temp >= VAL_OFFSET) || (temp < (VAL_OFFSET + MAX_LOOP)));
        }
        CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(db2));
    }

    mdbm_close(db);
    mdbm_close(db2);
}

void
MdbmUnitTestOther::test_OtherAJ37()
{
    string prefix = string("OtherAJ37: print_page_table/print_free_list") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MDBM *mdbm = createOversizedPage(prefix);

    print_page_table(mdbm);
    mdbm_internal_print_free_list(mdbm);

    mdbm_close(mdbm);
}

//mdbm_preload()
void
MdbmUnitTestOther::test_OtherAJ28()
{
    string prefix = string("test_OtherA28: Error Case:1") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    MDBM *mdbm_db = NULL;

    CPPUNIT_ASSERT(-1 == mdbm_preload(mdbm_db));
}

void
MdbmUnitTestOther::test_OtherAJ29()
{
    SKIP_IF_VALGRIND() // remap_file_pages not supported in valgrind
    RETURN_IF_WINMODE_LIMITED

    string prefix = string("test_OtherA29: Error Case:2, Windowed Mode") + versionString + ":";
    TRACE_TEST_CASE(__func__)


    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | MDBM_OPEN_WINDOWED | versionFlag;
    string fname(GetTmpName(string("OpenWindowed")));
    MdbmHolder mdbm_db(fname);
    int ret = mdbm_db.Open(openFlags, 0644, sysconf(_SC_PAGESIZE), 0);
    CPPUNIT_ASSERT(ret == 0);
    CPPUNIT_ASSERT(-1 == mdbm_preload(mdbm_db));
}

void
MdbmUnitTestOther::test_OtherAJ30()
{
    string prefix = string("test_OtherA30: Page size 512 bytes") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    string fname;
    MdbmHolder mdbm_db(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    CPPUNIT_ASSERT(0 == mdbm_preload(mdbm_db));
}

void
MdbmUnitTestOther::test_OtherAJ31()
{
    string prefix = string("test_OtherA31: Page size 65536 bytes") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    string fname;
    MdbmHolder mdbm_db(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, 65536, 0, &fname));

    CPPUNIT_ASSERT(0 == mdbm_preload(mdbm_db));
}

void
MdbmUnitTestOther::test_OtherAJ32()
{
    string prefix = string("test_OtherA32: MDBM Full with Page size 4096 bytes") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    datum dk, dv;
    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | versionFlag;
    string fname(GetTmpName(string("MDBMfull")));
    MdbmHolder mdbm_db(fname);
    int _limitSize = 220;
    int ret = -1;

    ret = mdbm_db.Open(openFlags, 0644, 4096, 0);
    CPPUNIT_ASSERT(ret == 0);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm_db, static_cast<mdbm_ubig_t>(_limitSize), NULL, NULL)) == 0);


    //store key-value pair until database is full
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"abc-defghi";
    dv.dsize = strlen(dv.dptr);

    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    CPPUNIT_ASSERT(0 == mdbm_preload(mdbm_db));
}

void
MdbmUnitTestOther::test_OtherAJ33()
{
    string prefix = string("test_OtherA33: MDBM Full with Page size 65536 bytes") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_FAST_VALGRIND()

    datum dk, dv;
    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | versionFlag;
    string fname(GetTmpName(string("MDBMfull2")));
    MdbmHolder mdbm_db(fname);
    int _limitSize = 120;
    int ret = -1;

    ret = mdbm_db.Open(openFlags, 0644, 65536, 0);
    CPPUNIT_ASSERT(ret == 0);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm_db, static_cast<mdbm_ubig_t>(_limitSize), NULL, NULL)) == 0);


    //store key-value pair until database is full
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"abc-defghi";
    dv.dsize = strlen(dv.dptr);

    for (int i = 0; i < 121708; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    CPPUNIT_ASSERT(0 == mdbm_preload(mdbm_db));
}

static size_t GetNumPages(const string& filename) {
    size_t syspg = sysconf(_SC_PAGESIZE);
    struct stat file_stat;
    int fd = open(filename.c_str(), 0);
    fstat(fd, &file_stat);
    close(fd);
    return ((file_stat.st_size+syspg-1) / syspg);
}

void
MdbmUnitTestOther::test_OtherAJ34()
{
    string prefix = string("test_OtherA34: page fault") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_FAST()

    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | versionFlag;
    string fname(GetTmpName(string("Page-fault")));
    int ret = -1;
    MDBM *mdbm_db = NULL;
    int who = RUSAGE_SELF;
    struct rusage rusage1, rusage2, rusage3, rusage4;
    CPPUNIT_ASSERT((mdbm_db = mdbm_open(fname.c_str(), openFlags, 0644, 65536, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));
    ret = mdbm_pre_split(mdbm_db, 10000);
    CPPUNIT_ASSERT(ret == 0);
    mdbm_ubig_t pg_in_pre=0, pg_out_pre=0;
    mdbm_ubig_t pg_in_post=0, pg_out_post=0;


    size_t total_pages = GetNumPages(fname.c_str());
    CPPUNIT_ASSERT(0 == mdbm_check_residency(mdbm_db, &pg_in_pre, &pg_out_pre));
    CPPUNIT_ASSERT_EQUAL(pg_in_pre+pg_out_pre, (mdbm_ubig_t)total_pages);

    getrusage(who,&rusage1);
    printf("\nPage faults:Before Store  = %ld\n",rusage1.ru_majflt);
    InsertData(mdbm_db, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, 1000000, true);
    getrusage(who,&rusage2);
    printf("Page faults:After Store   = %ld\n",rusage2.ru_majflt);

    // InsertData changed page-count ...
    total_pages = GetNumPages(fname.c_str());
    CPPUNIT_ASSERT(0 == mdbm_check_residency(mdbm_db, &pg_in_pre, &pg_out_pre));

    mdbm_preload(mdbm_db);
    getrusage(who,&rusage3);
    printf("Page faults:After preload = %ld\n",rusage3.ru_majflt);

    CPPUNIT_ASSERT(0 == mdbm_check_residency(mdbm_db, &pg_in_post, &pg_out_post));
    CPPUNIT_ASSERT_EQUAL(pg_in_pre+pg_out_pre, (mdbm_ubig_t)total_pages);
    CPPUNIT_ASSERT_EQUAL(pg_in_post+pg_out_post, (mdbm_ubig_t)total_pages);
    CPPUNIT_ASSERT(pg_in_pre <= pg_in_post);
    CPPUNIT_ASSERT(pg_out_pre >= pg_out_post);


    kvpair kv;
    datum dta;
    MDBM_ITER iter;
    string msg;

    MDBM_ITER_INIT(&iter);
    kv = mdbm_first_r(mdbm_db, &iter);
    while (kv.key.dptr != NULL) {
       dta = mdbm_fetch(mdbm_db, kv.key);
       msg = string("Could not find key ") + kv.key.dptr;
       CPPUNIT_ASSERT_MESSAGE(msg.c_str(), dta.dptr != NULL);
       msg = string("Value size of key ") + kv.key.dptr + " of " + ToStr(kv.val.dsize)
       + string(" is different than fetched: ") + ToStr(dta.dsize);
       CPPUNIT_ASSERT_MESSAGE(msg.c_str(), kv.val.dsize == dta.dsize);
       msg = string("Data for key ") + kv.key.dptr + string(" is not the same in both MDBMs");
       CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(kv.val.dptr, dta.dptr, kv.val.dsize) == 0);
       kv = mdbm_next_r(mdbm_db, &iter);
    }
    getrusage(who,&rusage4);
    printf("Page faults:After Fetch   = %ld\n",rusage4.ru_majflt);
    mdbm_close(mdbm_db);
}

void
MdbmUnitTestOther::test_OtherAJ35()
{
    string prefix = string("test_OtherA35: large object page fault") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    // TODO FIXME... this test is incredibly slow under valgrind
    SKIP_IF_VALGRIND()
    SKIP_IF_FAST()

    int openFlags = MDBM_O_CREAT | MDBM_O_RDWR | versionFlag | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;
    string fname(GetTmpName(string("lob Page-fault")));
    int ret = -1;
    MDBM *mdbm_db = NULL;
    int who = RUSAGE_SELF;
    struct rusage rusage1, rusage2, rusage3, rusage4;
    CPPUNIT_ASSERT((mdbm_db = mdbm_open(fname.c_str(), openFlags, 0644, 65536, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));
    ret = mdbm_pre_split(mdbm_db, 10000);
    CPPUNIT_ASSERT(ret == 0);

    getrusage(who,&rusage1);
    printf("\nPage faults:Before Store  = %ld\n",rusage1.ru_majflt);

    //store large objects
    for (int i = 0; i < 1000; ++i) {
       ret = StoreLargeObj(mdbm_db, i);
       CPPUNIT_ASSERT_EQUAL(0, ret);
    }
    getrusage(who,&rusage2);
    printf("Page faults:After Store   = %ld\n",rusage2.ru_majflt);
    mdbm_preload(mdbm_db);
    getrusage(who,&rusage3);
    printf("Page faults:After preload = %ld\n",rusage3.ru_majflt);

    kvpair kv;
    datum dta;
    MDBM_ITER iter;
    string msg;

    MDBM_ITER_INIT(&iter);
    kv = mdbm_first_r(mdbm_db, &iter);
    while (kv.key.dptr != NULL) {
       dta = mdbm_fetch(mdbm_db, kv.key);
       msg = string("Could not find key ") + kv.key.dptr;
       CPPUNIT_ASSERT_MESSAGE(msg.c_str(), dta.dptr != NULL);
       msg = string("Value size of key ") + kv.key.dptr + " of " + ToStr(kv.val.dsize)
       + string(" is different than fetched: ") + ToStr(dta.dsize);
       CPPUNIT_ASSERT_MESSAGE(msg.c_str(), kv.val.dsize == dta.dsize);
       msg = string("Data for key ") + kv.key.dptr + string(" is not the same in both MDBMs");
       CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(kv.val.dptr, dta.dptr, kv.val.dsize) == 0);
       kv = mdbm_next_r(mdbm_db, &iter);
    }
    getrusage(who,&rusage4);
    printf("Page faults:After Fetch   = %ld\n",rusage4.ru_majflt);
    mdbm_close(mdbm_db);
}

// This test cases is valid only for V2 - mdbm_get_db_info should fail on V2 MDBMs
// But it should not crash: bug 5964126
void
MdbmUnitTestOther::test_GetDbInfoFails()
{
    string prefix = string("GetDbInfoFails: mdbm_get_db_info fails") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    mdbm_db_info_t dbinfo;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_db_info(mdbm, &dbinfo));
}

void
MdbmUnitTestOther::finalCleanup()
{
    TRACE_TEST_CASE(__func__);
    CleanupTmpDir();
    GetLogStream() << "Completed " << versionString << " General/Other Tests." << endl<<flush;
}

/// ------------------------------------------------------------------------------
///
/// Helper methods
///
/// ------------------------------------------------------------------------------

int
MdbmUnitTestOther::StoreLargeObj(MDBM *mdbm_db, int objNum)
{
    char kbuf[32], buf[256];
    string largeObj;
    datum k, dta;

    snprintf(kbuf, sizeof(kbuf), "%s%d", LARGE_OBJ_KEY, objNum);

    k.dptr = kbuf;
    k.dsize = strlen(kbuf);

    for (int i = 0; i < NUM_DATA_ITEMS*10; ++i ) {
        snprintf(buf, sizeof(buf),
                 "%s%s%d", PREFIX, PREFIX, STARTPOINT + i);
        largeObj += buf;

    }

    dta.dptr  = (char *) largeObj.c_str();
    dta.dsize = largeObj.size();

    return mdbm_store(mdbm_db, k, dta, MDBM_REPLACE);
}

void
MdbmUnitTestOther::setErrno(MDBM *db)
{
    struct mdbm *dbm = (struct mdbm *) db;
    dbm->db_errno = -1;
}

string
MdbmUnitTestOther::createEmptyMdbm(const string &prefix)
{
    string fname;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                  DEFAULT_PAGE_SIZE, 0, &fname));
    return fname;
}

void
MdbmUnitTestOther::testProtectSegv(const string &prefix, int protect, int whatToPerform, bool readOnly)
{
    string fname;
    int flags = getmdbmFlags() | MDBM_LARGE_OBJECTS;
    int mode = 0644;
    if (readOnly) {
        flags |= MDBM_O_RDONLY;
        mode = 0444;
    } else {
        flags |= MDBM_O_RDWR;
    }

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, flags | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    int status, retsig = 0;
    pid_t mypid = fork();

    if (mypid == 0) { // Child
        MdbmHolder mdbm2(mdbm_open(fname.c_str(), flags, mode, 0, 0));
        if (NULL == (MDBM*)mdbm2) {
          fprintf(stderr, "ERROR: %s:%d child open failed!\n", __FILE__, __LINE__);
          exit(2);
        }
        int ret = mdbm_protect(mdbm2, protect);
        if (0 != ret) {
          fprintf(stderr, "ERROR: %s:%d child protect failed!\n", __FILE__, __LINE__);
          exit(3);
        }

        if (whatToPerform & DO_TEST_READS) {
            VerifyData(mdbm2, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT);
        }
        if (whatToPerform & DO_TEST_WRITES) {
            // Insert small and large objects
            InsertData(mdbm2, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE/4, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);
            InsertData(mdbm2, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE*2, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT*2);
        }

        exit(1);
    } else {  // Parent
        if (mypid < 0) {
            cerr << "Could not get child process PID" << endl << flush;
            exit(2);
        }
        waitpid(mypid, &status, 0);
        if (WIFSIGNALED(status)) {
            retsig = WTERMSIG(status);
        }
    }
    CPPUNIT_ASSERT((SIGSEGV == retsig) || (SIGBUS == retsig));
}

void
MdbmUnitTestOther::testGetDbInfo(MDBM *mdbm, int extraFlags, bool isempty)
{
    mdbm_db_info_t info;
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_db_info(mdbm, &info));

    CPPUNIT_ASSERT_EQUAL((u_int)DEFAULT_PAGE_SIZE,info.db_page_size);
    if (isempty) {
        CPPUNIT_ASSERT_EQUAL(2U,info.db_num_pages);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_max_pages);
        CPPUNIT_ASSERT_EQUAL(1U,info.db_num_dir_pages);
        CPPUNIT_ASSERT_EQUAL(1U,info.db_dir_width);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_max_dir_shift);
        CPPUNIT_ASSERT_EQUAL(1U,info.db_dir_min_level);
        CPPUNIT_ASSERT_EQUAL(1U,info.db_dir_max_level);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_dir_num_nodes);
        CPPUNIT_ASSERT_EQUAL(5U,info.db_hash_func);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_spill_size);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_cache_mode);
    } else {
        CPPUNIT_ASSERT(info.db_num_pages > 2);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_max_pages);
        CPPUNIT_ASSERT_EQUAL(1U,info.db_num_dir_pages);
        CPPUNIT_ASSERT(info.db_dir_width > 1);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_max_dir_shift);
        CPPUNIT_ASSERT(info.db_dir_min_level >= 1);
        CPPUNIT_ASSERT_EQUAL(2U,info.db_dir_max_level);
        CPPUNIT_ASSERT_EQUAL(3U,info.db_dir_num_nodes);
        CPPUNIT_ASSERT_EQUAL(5U,info.db_hash_func);
        CPPUNIT_ASSERT_EQUAL(384U,info.db_spill_size);
        CPPUNIT_ASSERT_EQUAL(0U,info.db_cache_mode);
    }
}

void
MdbmUnitTestOther::corruptHeaderAndCheck(MDBM *db)
{
    struct mdbm *dbm = (struct mdbm *) db;
    memset(dbm->db_hdr, 0, sizeof(uint32_t) * 10);

    for (int verbosity=0; verbosity < 2; ++verbosity) {
        CPPUNIT_ASSERT(mdbm_check(db, 0, verbosity) != 0);
    }
}


double
MdbmUnitTestOther::benchmarkFile(const string &filename, double writefrac, int lockmode)
{
    string benchoutput = filename + string(".benchout");
    benchmarkExisting(filename.c_str(), writefrac, lockmode, benchoutput.c_str(), 10000, 1);

    FILE *fp = fopen(benchoutput.c_str(), "r");

    if (fp == NULL) {
        cerr << "Unable to open benchmark output file " << benchoutput << endl;
        return -1.0;
    }

    const int LINESZ = 512;
    char line[LINESZ];
    uint32_t fetchespersec = 0, writespersec = 0;
    double ret = -1.0;
    while (fgets(line, LINESZ, fp) != NULL) {
        char *found = strstr(line, "nproc  fetch/s");
        if (found) {
            char *tmp = fgets(line, LINESZ, fp);   // Get next line
            if (tmp == NULL) {
                cerr << "Could not find last line with benchmark results" << endl;
                break;
            }
            stringstream st(tmp);
            st >> fetchespersec;   // throw out first number
            st >> fetchespersec;
            double skip;
            st >> skip;
            st >> writespersec;
            // Compute weighted average
            ret = ((double) fetchespersec * (1.0 - (writefrac / 100))) +
                  ((double) writespersec * writefrac / 100);
            break;
        }
    }
    CPPUNIT_ASSERT_EQUAL(0, fclose(fp));
    CPPUNIT_ASSERT_EQUAL(0, unlink(benchoutput.c_str()));
    return ret;
}

void
MdbmUnitTestOther::test_BenchExisting()
{
    string prefix = string("test_BenchExisting");
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR,
                    0644, sysconf(_SC_PAGESIZE), 0, &fname));
    InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, 100, true, DEFAULT_ENTRY_COUNT);
    mdbm.Close();
    double ret;

    stringstream msg;
    ret = benchmarkFile(fname, 0.0, 0);   // Benchmark with exclusive locks
    msg << "Benchmarking result of " << ret << " is invalid (using exclusive locks)";
    CPPUNIT_ASSERT_MESSAGE(msg.str(), ret > 0.0);

    stringstream msgp;
    ret = benchmarkFile(fname, 0.0, MDBM_PARTITIONED_LOCKS);   // Benchmark with partition locks
    msgp << "Benchmarking result of " << ret << " is invalid (using partition locks)";
    CPPUNIT_ASSERT_MESSAGE(msgp.str(), ret > 0.0);

    stringstream msgrw;
    ret = benchmarkFile(fname, 0.0, MDBM_RW_LOCKS);   // Benchmark with shared locks
    msgrw << "Benchmarking result of " << ret << " is invalid (using shared locks)";
    CPPUNIT_ASSERT_MESSAGE(msgrw.str(), ret > 0.0);

    stringstream msgw;
    ret = benchmarkFile(fname, 5.0, 0);   // Benchmark with 5% writes
    msgw << "Benchmarking result of " << ret << " is invalid (with 5% writes)";
    CPPUNIT_ASSERT_MESSAGE(msgw.str(), ret > 0.0);

}
MDBM*
MdbmUnitTestOther::createOversizedPage(const string &prefix)
{
    MDBM *db(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                  1024, 0));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(db, 10, NULL, NULL));

    datum k, v, value;
    int j = 1, temp;
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&j);
    k.dsize = sizeof(j);

    for (int i = 0; i < MAX_LOOP; i++) {
        temp = i + VAL_OFFSET;
        v.dptr = reinterpret_cast<char*>(&temp);  // Store different values
        v.dsize = sizeof(temp);
        // Store records
        CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(db));
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT_DUP));

        // fetch the record
        value = mdbm_fetch(db, k);
        CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(db));
    }

    // Store more data to create an oversized page, but keep key on same page w/ MDBM_REPLACE
    const int ARRAY_SIZE = 100;
    int intarray[ARRAY_SIZE];
    for (int i = 0; i < ARRAY_SIZE; i++) {
        intarray[i] = VAL_OFFSET + i;
    }
    v.dptr = reinterpret_cast<char*>(intarray);
    v.dsize = sizeof(intarray);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(db));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_REPLACE));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(db));

    return db;
}

off_t
MdbmUnitTestOther::getFileSize(const string &fname)
{
    struct stat statBuf;
    CPPUNIT_ASSERT_EQUAL(0, stat(fname.c_str(), &statBuf));
    return statBuf.st_size;
}

void
MdbmUnitTestOther::test_OneFcopy()
{
    string prefix = string("test_OneFcopy") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    oversizedFcopyTester(prefix, false);
}

// Simulate what happens if fcopy tries only once, copying blocks mid-way, and it
// therefore fails, because fcopy now checks that size did not increase during fcopy-ing
void
MdbmUnitTestOther::test_BlockingFcopy()
{
    string prefix = string("test_BlockingFcopy");
    TRACE_TEST_CASE(__func__)

    fcopyMultiTester(prefix, 1, false);  // 1 try, result is false, meaning it should fail
}

void
MdbmUnitTestOther::test_ThreeFcopys()
{
    string prefix = string("test_ThreeFcopys") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    fcopyMultiTester(prefix, 3, true);  // 3 tries, result is true, meaning it should succeed
}

void
MdbmUnitTestOther::fcopyMultiTester(const string &prefix, int retries, bool success)
{
    string fname;
    int flags = MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS;
    MDBM *mdbm = GetTmpPopulatedMdbm(prefix, MDBM_O_CREAT | MDBM_O_TRUNC | flags,
        0644, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE * 128, &fname);
    CPPUNIT_ASSERT(NULL != mdbm);
    // 300 page limit would allow MDBM to split once, and then create some oversized pages
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, 300, NULL, NULL));

    datum ky;
    bool found = false;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    for (ky=mdbm_firstkey_r(mdbm, &iter); ky.dptr != NULL; ky=mdbm_nextkey_r(mdbm, &iter)) {
       if (iter.m_pageno > 1) {   // Lock past first page, then expand it later to simulate failure
           found = true;
           break;
       }
    }
    CPPUNIT_ASSERT_EQUAL(true, found);
    // copy key, it will be potentially invalid after the unlock
    string keyStr(ky.dptr, ky.dsize);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm, &ky, 0));  // release iteration lock
    ky.dptr = (char*)keyStr.data();

    char buf[DEFAULT_PAGE_SIZE / 2 + 4];   // Use data a little over half a page
    memset(buf, '.', sizeof(buf));
    const datum newval = {buf, sizeof(buf)};

    char msgbuf[32];
    strcpy(msgbuf, "child");  // Message content is not important
    int pipefds[2];   // pipefds[0]=readfd, pipefds[1]=sendfd
    pipe(pipefds);
    string fcopyTo = GetTmpName("fcopy");
    const int MAX_TRIES = 500;   // Repeat this many times before deciding to fail

    int pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (0 == pid) { // child
        MDBM *childHandle = mdbm_open(fname.c_str(), flags, 0644, 0, 0);
        if (childHandle == NULL) {
            exit(1);
        }
        int fd = open(fcopyTo.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            mdbm_close(childHandle);
            close(fd);
            exit(2);
        }
        if (fcopy_header(childHandle, fd, 0) < 0) {
            mdbm_close(childHandle);
            exit(3);
        }
        write(pipefds[1], msgbuf, strlen(msgbuf)+1);  // Send message to parent
        int count = 0;
        do {   // Wait for the parent to plock
            sched_yield();
            usleep(20000);
            int lcnt = mdbm_tryplock(mdbm, &ky, 0);
            if (lcnt == 1) {
                mdbm_punlock(mdbm, &ky, 0);
            } else {
                break;
            }
        } while (++count < MAX_TRIES);
        if (count >= MAX_TRIES) {
            printf("Too many retries\n");
            mdbm_close(childHandle);
            close(fd);
            exit(5);
        }
        int ret = fcopy_body(childHandle, fd, 0);
        if (ret < 0) {
            ret = 6;
        }
        if (--retries > 0) {
            ret = 0;
            if (mdbm_internal_fcopy(childHandle, fd, 0, retries) < 0) {
                ret = 7;
            }
        }
        close(fd);
        mdbm_close(childHandle);
        mdbm_close(mdbm);
        exit(ret);
    } else { //parent
        int status = 0, readlen;
        char readbuf[32];
        readlen = read(pipefds[0], readbuf, 31);
        CPPUNIT_ASSERT(readlen != -1);
        off_t initialSize = getFileSize(fcopyTo);
        // Force a split by replacing and then inserting a dup that's bigger than a page
        CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbm, &ky, 0));  // Lock to stop fcopy() mid-track
        int count = 0;
        // Wait for the child a chance to start fcopy
        do {
            usleep(50000);
            sched_yield();
        } while ((getFileSize(fcopyTo) == initialSize) && (++count < MAX_TRIES));
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, ky, newval, MDBM_REPLACE));
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, ky, newval, MDBM_INSERT_DUP));
        int retlock = mdbm_lock(mdbm);
        CPPUNIT_ASSERT(FillDb(mdbm, -1) > 0);
        if (retlock  == 1) {
            mdbm_unlock(mdbm);
        }
        CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm, &ky, 0));  // release lock

        waitpid(pid, &status, 0);
        int exitStatus = WEXITSTATUS(status);
        bool result = (exitStatus == 0) ? true : false;
        CPPUNIT_ASSERT_EQUAL(success, result);
    }
    mdbm_close(mdbm);
    // Check new MDBM for corruption

    if (success == true) {   // Run check if test successful
        MDBM *db2 = mdbm_open(fcopyTo.c_str(), flags, 0666, DEFAULT_PAGE_SIZE, 0);
        CPPUNIT_ASSERT(db2 != NULL);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_check(db2, 3, 1));
        mdbm_close(db2);
    }
}

void
MdbmUnitTestOther::test_CorruptPadding()
{
    string prefix = string("test_CorruptPadding");
    TRACE_TEST_CASE(__func__)

    MDBM *mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                    DEFAULT_PAGE_SIZE, 0));
    struct mdbm *dbm = (struct mdbm *) mdbm;
    uint32_t tmp = dbm->guard_padding_1;
    dbm->guard_padding_1 = 0;
    const datum ky  = { (char*)"key",   static_cast<int>(strlen("key"))   };
    const datum val = { (char*)"stuff", static_cast<int>(strlen("stuff")) };
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_store(mdbm, ky, val, MDBM_REPLACE));
    // Test second guard padding
    dbm->guard_padding_1 = tmp;
    dbm->guard_padding_2 = 0;
    CPPUNIT_ASSERT_EQUAL(-1, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, 1));
    dbm->guard_padding_2 = tmp;
    dbm->guard_padding_3 = 0;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_delete(mdbm, ky));
    dbm->guard_padding_3 = tmp;
    dbm->guard_padding_4 = 0;
    CPPUNIT_ASSERT_EQUAL(1, mdbm_check(mdbm, 3, 1));

    mdbm_close(mdbm);  // close will complain but close
}

/// MDBM V3 class

class MdbmUnitTestOtherV3 : public MdbmUnitTestOther
{
  CPPUNIT_TEST_SUITE(MdbmUnitTestOtherV3);

    CPPUNIT_TEST(initialSetup);

    CPPUNIT_TEST(test_OneFcopy);
    CPPUNIT_TEST(test_BlockingFcopy);
    CPPUNIT_TEST(test_ThreeFcopys);
    CPPUNIT_TEST(test_CorruptPadding);

    CPPUNIT_TEST(test_OtherAF1);
    CPPUNIT_TEST(test_OtherAF2);

    CPPUNIT_TEST(test_OtherAH1);
    CPPUNIT_TEST(test_OtherAH2);
    CPPUNIT_TEST(test_OtherAH3);
    CPPUNIT_TEST(test_OtherAH4);
// No corruption tests for FreeBSD
#ifndef FREEBSD
    CPPUNIT_TEST(test_OtherAH5);
    CPPUNIT_TEST(test_OtherAH6);
    CPPUNIT_TEST(test_OtherAH7);
    CPPUNIT_TEST(test_OtherAH8);
    CPPUNIT_TEST(test_OtherAH9);
#endif
    CPPUNIT_TEST(test_OtherAI1);
    CPPUNIT_TEST(test_OtherAI2);
#ifndef FREEBSD
    CPPUNIT_TEST(test_OtherAI3);
#endif
    CPPUNIT_TEST(test_OtherAI4);
    CPPUNIT_TEST(test_OtherAI5);
    CPPUNIT_TEST(test_OtherAI6);
//  bug 5478361
//    CPPUNIT_TEST(test_OtherAI7);
//    CPPUNIT_TEST(test_OtherAI8);

    CPPUNIT_TEST(test_OtherAJ1);
    CPPUNIT_TEST(test_OtherAJ2);
    CPPUNIT_TEST(test_OtherAJ3);
    CPPUNIT_TEST(test_OtherAJ4);
    CPPUNIT_TEST(test_OtherAJ5);
    CPPUNIT_TEST(test_OtherAJ6);
    CPPUNIT_TEST(test_OtherAJ7);

    CPPUNIT_TEST(test_OtherAJ12);
    CPPUNIT_TEST(test_OtherAJ13);
    CPPUNIT_TEST(test_OtherAJ14);
    CPPUNIT_TEST(test_OtherAJ15);
    CPPUNIT_TEST(test_OtherAJ16);
    CPPUNIT_TEST(test_OtherAJ17);
    CPPUNIT_TEST(test_OtherAJ18);
    CPPUNIT_TEST(test_OtherAJ20);
    CPPUNIT_TEST(test_OtherAJ36);  // test fcopy with oversized pages
    CPPUNIT_TEST(test_OtherAJ37);
    CPPUNIT_TEST(test_OtherAJ28);
    CPPUNIT_TEST(test_OtherAJ29);
    CPPUNIT_TEST(test_OtherAJ30);
    CPPUNIT_TEST(test_OtherAJ31);
    CPPUNIT_TEST(test_OtherAJ32);
    CPPUNIT_TEST(test_OtherAJ33);
    CPPUNIT_TEST(test_OtherAJ34);
    CPPUNIT_TEST(test_OtherAJ35);

    CPPUNIT_TEST(testRstatsChurn);
    CPPUNIT_TEST(testSaveRestore);
    CPPUNIT_TEST(testAtomic);
#ifdef HAVE_ROBUST_PTHREADS
// robust locks are linux only
    CPPUNIT_TEST(testRobustLocks);
    CPPUNIT_TEST(testRobustLocksShared);
    CPPUNIT_TEST(testRobustLocksPart);
#endif
    CPPUNIT_TEST(testPLock);
    CPPUNIT_TEST(testMLock);
    CPPUNIT_TEST(testShmem);

    CPPUNIT_TEST(test_BenchExisting);
    //CPPUNIT_TEST(test_CountAllPage);

    CPPUNIT_TEST(testFileLog);
    CPPUNIT_TEST(testSysLog);
    CPPUNIT_TEST(testForkThenDupHandle);

    CPPUNIT_TEST(finalCleanup);

  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestOtherV3() : MdbmUnitTestOther(MDBM_CREATE_V3) { }
    virtual void setErrno(MDBM *db);
    virtual void corruptHeaderAndCheck(MDBM *db);
    virtual void testForkThenDupHandle();
    //void test_CountAllPage();
};

//void
//MdbmUnitTestOtherV3::test_CountAllPage()
//{
//    string prefix = string("test_CountAllPage") + versionString + ":";
//    TRACE_TEST_CASE(__func__)
//
//    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644,
//                    DEFAULT_PAGE_SIZE, 0));   // Creates 12 entries
//    CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE,
//                         DEFAULT_ENTRY_COUNT*2, true, DEFAULT_ENTRY_COUNT));   // Creates 24 more
//    CPPUNIT_ASSERT_EQUAL(0, DeleteData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE,
//                         DEFAULT_ENTRY_COUNT/2));   // Delete 6
//    //CPPUNIT_ASSERT_EQUAL(30U, count_all_page(mdbm));
//}

void
MdbmUnitTestOtherV3::setErrno(MDBM *db)
{
    struct mdbm *dbm = (struct mdbm *) db;
    dbm->db_errno = -1;
}

void
MdbmUnitTestOtherV3::corruptHeaderAndCheck(MDBM *db)
{
    struct mdbm *dbm = (struct mdbm *) db;
    memset(dbm->db_hdr, 0, sizeof(uint32_t) * 10);

    for (int verbosity=0; verbosity < 2; ++verbosity) {
        CPPUNIT_ASSERT(mdbm_check(db, 0, verbosity) != 0);
    }
}

void MdbmUnitTestOtherV3::testForkThenDupHandle()
{
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm("testForkThenDupHandle",
                  getmdbmFlags() | MDBM_O_CREAT | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0, &fname));

    pid_t pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (!pid) { // child
      MdbmHolder handle2(mdbm_dup_handle(mdbm, MDBM_O_RDWR));
      InsertData(handle2);
      exit(0);
    } else {
      // parent
      CPPUNIT_ASSERT(pid == waitpid(pid, NULL, 0));
    }
}

CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestOtherV3);


class MdbmUnitTestOtherV3PLock : public MdbmUnitTestOther
{
  CPPUNIT_TEST_SUITE(MdbmUnitTestOtherV3PLock);

    // must be first for signal handler setup...
    CPPUNIT_TEST(initialSetup);

    CPPUNIT_TEST(testCompressTree);

    CPPUNIT_TEST(test_OtherAF1);
    CPPUNIT_TEST(test_OtherAF2);

    CPPUNIT_TEST(test_OtherAH1);
    CPPUNIT_TEST(test_OtherAH2);
    CPPUNIT_TEST(test_OtherAH3);
    CPPUNIT_TEST(test_OtherAH4);
// No corruption tests for FreeBSD
#ifndef FREEBSD
    CPPUNIT_TEST(test_OtherAH5);
    CPPUNIT_TEST(test_OtherAH6);
    CPPUNIT_TEST(test_OtherAH7);
    CPPUNIT_TEST(test_OtherAH8);
    CPPUNIT_TEST(test_OtherAH9);
#endif
    CPPUNIT_TEST(test_OtherAI1);
    CPPUNIT_TEST(test_OtherAI2);
#ifndef FREEBSD
    CPPUNIT_TEST(test_OtherAI3);
#endif
    CPPUNIT_TEST(test_OtherAI4);
    CPPUNIT_TEST(test_OtherAI5);
    CPPUNIT_TEST(test_OtherAI6);
//  bug 5478361
//    CPPUNIT_TEST(test_OtherAI7);
//    CPPUNIT_TEST(test_OtherAI8);

    CPPUNIT_TEST(test_OtherAJ1);
    CPPUNIT_TEST(test_OtherAJ2);
    CPPUNIT_TEST(test_OtherAJ3);
    CPPUNIT_TEST(test_OtherAJ4);
    CPPUNIT_TEST(test_OtherAJ5);
    CPPUNIT_TEST(test_OtherAJ6);
    CPPUNIT_TEST(test_OtherAJ7);

    CPPUNIT_TEST(test_OtherAJ12);
    CPPUNIT_TEST(test_OtherAJ13);
    CPPUNIT_TEST(test_OtherAJ14);
    CPPUNIT_TEST(test_OtherAJ15);
    CPPUNIT_TEST(test_OtherAJ16);
    CPPUNIT_TEST(test_OtherAJ17);
    CPPUNIT_TEST(test_OtherAJ18);
    CPPUNIT_TEST(test_OtherAJ20);

    CPPUNIT_TEST(testRstatsChurn);
    CPPUNIT_TEST(testSaveRestore);
    CPPUNIT_TEST(testAtomic);
#ifdef HAVE_ROBUST_PTHREADS
    CPPUNIT_TEST(testRobustLocks);
    CPPUNIT_TEST(testRobustLocksShared);
    CPPUNIT_TEST(testRobustLocksPart);
#endif
    CPPUNIT_TEST(testMLock);
    CPPUNIT_TEST(testShmem);

    CPPUNIT_TEST(test_BenchExisting);

    CPPUNIT_TEST(testFileLog);
    CPPUNIT_TEST(testSysLog);

    CPPUNIT_TEST(finalCleanup);

  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestOtherV3PLock() : MdbmUnitTestOther(MDBM_CREATE_V3|MDBM_SINGLE_ARCH) { }
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestOtherV3PLock);

