/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_import and mdbm_export
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

// Include mdbm_import and redefine main() and usage() to perform unit tests w/o using fork+exec

#define main imp_main_wrapper
#define usage imp_usage_wrapper
#include "../../tools/mdbm_import.cc"
#undef main
#undef usage

// Include mdbm_export and redefine main() and usage() to perform unit tests w/o using fork+exec

#define main exp_main_wrapper
#define usage exp_usage_wrapper
#include "../../tools/mdbm_export.c"
#undef main
#undef usage



class ImportTestBase : public CppUnit::TestFixture, public TestBase
{

public:
    ImportTestBase(int vFlag) : TestBase(vFlag, "Import Test Suite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    void DoExport();
    void DoExportWithLocking(const string &lockmode);
    void TestImportWithDelete();
    void TestImportNoDelete();
    void TestSmallDbPageSize();

protected:
  static string file1;
  static string file2;
  static string exfile;
  void MakeTestMdbm(const string& fname);
  void ResetGetOpt();
  void TestValues(const string& fname, bool doDelete);
  string CreateCdbFile(int count = 100, int offst = 50);
  void VerifyMdbmFile(const string &fname, int count = 100, int offst = 50, int lflags=0);
  //void TestWithLockingMode(const string &infile, const string &lockmode, int lflags=0);
  void TestImportWithLockingMode(const string &infile, const string &lockmode, int count = 100);
};

string ImportTestBase::file1;
string ImportTestBase::file2;
string ImportTestBase::exfile;

void ImportTestBase::setUp()
{
    if (file1=="") {
      file1 = GetTmpName("input");
      file2 = GetTmpName("output");
      exfile = GetTmpName("export");
      MakeTestMdbm(file1);
    }
}

void ImportTestBase::MakeTestMdbm(const string& fname) {
    int i;
    char key[4096];
    char val[4096];
    datum kdat, vdat;
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder db = mdbm_open(fname.c_str(), flags, 0644, 1024, 4096);

    kdat.dptr = key;
    vdat.dptr = val;
    // first db only
    for (i=0; i<5; ++i) { // store regular values
      kdat.dsize = snprintf(key, sizeof(key), "key %04d -.val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value %07d", i);
      mdbm_store(db, kdat, vdat, MDBM_REPLACE);
    }
    for (i=5; i<10; ++i) { // store zero-length values
      kdat.dsize = snprintf(key, sizeof(key), "key %04d .-val", i);
      val[0] = 0;
      vdat.dsize = 0;
      mdbm_store(db, kdat, vdat, MDBM_REPLACE);
    }
}

void ImportTestBase::ResetGetOpt() {
    reset_getopt();
}

void ImportTestBase::TestValues(const string& fname, bool doDelete) {
    // test values
    int i;
    char key[4096];
    char val[4096];
    datum kdat, vdat, fvdat;

    int flags = MDBM_O_RDONLY;
    MdbmHolder db = mdbm_open(fname.c_str(), flags|MDBM_ANY_LOCKS, 0644, 1024, 4096);

    kdat.dptr = key;
    vdat.dptr = val;
    // first db only
    for (i=0; i<5; ++i) { // store regular values
      kdat.dsize = snprintf(key, sizeof(key), "key %04d -.val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value %07d", i);
      fvdat = mdbm_fetch(db, kdat);
      CPPUNIT_ASSERT(fvdat.dsize == vdat.dsize);
      CPPUNIT_ASSERT(fvdat.dptr != NULL);
      CPPUNIT_ASSERT(0 == memcmp(fvdat.dptr, vdat.dptr, vdat.dsize));
    }
    for (i=5; i<10; ++i) { // store zero-length values
      kdat.dsize = snprintf(key, sizeof(key), "key %04d .-val", i);
      val[0] = 0;
      vdat.dsize = 0;
      errno = 0;
      fvdat = mdbm_fetch(db, kdat);
      if (doDelete) {
        CPPUNIT_ASSERT(fvdat.dptr == NULL);
        CPPUNIT_ASSERT(errno == ENOENT);
      } else {
        CPPUNIT_ASSERT(fvdat.dptr != NULL);
        CPPUNIT_ASSERT(errno == 0);
      }
      CPPUNIT_ASSERT(fvdat.dsize == 0);
    }
}

string
ImportTestBase::CreateCdbFile(int count, int offst)
{
    string fname = GetTmpName("cdbinput");

    FILE *fptr = fopen(fname.c_str(), "w");

    CPPUNIT_ASSERT(fptr != NULL);

    string key, val;
    for (int i = 0; i < count; ++i) {
       key = string("key") + ToStr(i);
       val = string("value") + ToStr(i + offst);
       fprintf(fptr, "+%d,%d:%s->%s\n", (int)key.size(), (int)val.size(), key.c_str(), val.c_str());
    }
    fprintf(fptr, "\n");
    fclose(fptr);

    return fname;
}

void
ImportTestBase::VerifyMdbmFile(const string &fname, int count, int offst, int lflags)
{
    MdbmHolder db = mdbm_open(fname.c_str(), MDBM_O_RDONLY|lflags, 0444, 0, 0);
    CPPUNIT_ASSERT(NULL != (MDBM *)db);
    string key, val;
    errno = 0;
    for (int i = 0; i < count; ++i) {
        key = string("key") + ToStr(i);
        val = string("value") + ToStr(i + offst);
        const datum kdat = {(char *)key.c_str(), (int)key.size()};
        datum fetch = mdbm_fetch(db, kdat);
        CPPUNIT_ASSERT(fetch.dptr != NULL);
        int len = static_cast<int>(val.size());
        CPPUNIT_ASSERT_EQUAL(len, fetch.dsize);
        CPPUNIT_ASSERT_EQUAL(0, memcmp(fetch.dptr, val.c_str(), val.size()));
    }
}

void ImportTestBase::DoExport()
{
    TRACE_TEST_CASE("DoExport");
    { // export
      const char* args[] = { "foo", "-c", "-o", exfile.c_str(), file1.c_str(), NULL };
      ResetGetOpt();
      int ret = exp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      CPPUNIT_ASSERT(ret == 0);
      TestValues(file1, false);
    }
}

void ImportTestBase::DoExportWithLocking(const string &lockmode) {
    TRACE_TEST_CASE(lockmode + "-DoExportWithLocking");
    { // export
      mdbm_delete_lockfiles(file1.c_str());
      const char* args[] = { "foo", "-c", "-o", exfile.c_str(),
                             "-L", lockmode.c_str(), file1.c_str(), NULL };
      ResetGetOpt();
      int ret = exp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      CPPUNIT_ASSERT(ret == 0);
      TestValues(file1, false);
    }
}

void ImportTestBase::TestImportWithDelete()
{
    TRACE_TEST_CASE("TestImportWithDelete");
    string cmd = "rm -f " + file2;
    system(cmd.c_str());

    { // import, with delete zero-length values
      const char* args[] = { "foo", "-c", "-S", "1", "-D", "-i", exfile.c_str(), file2.c_str(), NULL };
      ResetGetOpt();
      int ret = imp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      fprintf(stderr, "import return code is %d\n", ret);
      CPPUNIT_ASSERT(ret == 0);
      TestValues(file2, true);
    }
}

void ImportTestBase::TestImportNoDelete()
{
    TRACE_TEST_CASE("TestImportNoDelete");
    string cmd = "rm -f " + file2;
    system(cmd.c_str());

    { // import, but *don't* delete zero-length values
      const char* args[] = { "foo", "-c", "-S", "1", "-i", exfile.c_str(), file2.c_str(), NULL };
      //const char* args[] = { "foo", "-c", "-i", exfile.c_str(), file2.c_str(), NULL };
      ResetGetOpt();
      int ret = imp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      fprintf(stderr, "import return code is %d\n", ret);
      CPPUNIT_ASSERT(ret == 0);
      TestValues(file2, false);
    }
}

void
ImportTestBase::TestSmallDbPageSize()
{
    string infile(CreateCdbFile());
    string outfile = GetTmpName("out1pg");
    const char* args[] = { "foo", "-c", "-p", "1024",
                           "-i", infile.c_str(), outfile.c_str(), NULL };
    ResetGetOpt();
    int ret = imp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
    CPPUNIT_ASSERT(ret == 0);
    VerifyMdbmFile(outfile);

}

// Will generate the following warning to stdout when using partition locking:
//   Partition locking requires a fixed size DB.
//   Consider using the -d or -y options to specify the size
void
ImportTestBase::TestImportWithLockingMode(const string &infile, const string &lockmode, int count)
{
    TRACE_TEST_CASE(lockmode + "-TestImportWithLockingMode");
    string outfile = GetTmpName("out" + lockmode);
    const char* args[] = { "foo", "-c", "-L", lockmode.c_str(),
                           "-i", infile.c_str(), outfile.c_str(), NULL };
    ResetGetOpt();
fprintf(stderr, "TESTWITHLOCKINGMODE %s\n", outfile.c_str());
    int ret = imp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
    CPPUNIT_ASSERT(ret == 0);
    //VerifyMdbmFile(outfile, 100, 50, lflags);
    VerifyMdbmFile(outfile, 100, 50, MDBM_ANY_LOCKS);
}

class ImportTestV3 : public ImportTestBase
{
    CPPUNIT_TEST_SUITE(ImportTestV3);
    CPPUNIT_TEST(DoExport);
    CPPUNIT_TEST(TestImportWithDelete);
    CPPUNIT_TEST(TestImportNoDelete);
    CPPUNIT_TEST(TestSmallDbPageSize);
    CPPUNIT_TEST(TestAllLocking);
    CPPUNIT_TEST_SUITE_END();

public:
    ImportTestV3() : ImportTestBase(MDBM_CREATE_V3) {}
    void TestAllLocking();
};

CPPUNIT_TEST_SUITE_REGISTRATION(ImportTestV3);

void
ImportTestV3::TestAllLocking()
{
    string infilename(CreateCdbFile());
    TestImportWithLockingMode(infilename, string("exclusive"));
    TestImportWithLockingMode(infilename, string("partition"));
    TestImportWithLockingMode(infilename, string("shared"));
    TestImportWithLockingMode(infilename, string("none"));

    DoExportWithLocking(string("exclusive"));
    DoExportWithLocking(string("partition"));
    DoExportWithLocking(string("shared"));
    DoExportWithLocking(string("none"));
}

