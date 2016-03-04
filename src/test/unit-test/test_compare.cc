/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_hash  mdbm_sethash
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

#include "mdbm_util.h"
#include "TestBase.hh"

#define main comp_main_wrapper
#include "../../tools/mdbm_compare.c"
#undef main

// include import so we can patch changes from cdb format
#define main imp_main_wrapper_c
#define usage imp_usage_wrapper_c
#include "../../tools/mdbm_import.cc"
#undef main
#undef usage


class CompareTestBase : public CppUnit::TestFixture, public TestBase
{

public:
    CompareTestBase(int vFlag) : TestBase(vFlag, "Compare Test Suite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    void CompareSame();
    void CompareDifferent();
    void CheckDiffCount();
    void PatchMissing();
    void PatchCommon();

protected:
  string file1, file2, difname;
  void MakeTestMdbms(const string& fname1, const string& fname2);
};

void CompareTestBase::setUp()
{
    file1 = GetTmpName();
    file2 = GetTmpName();
    difname = GetTmpName(".diff");
    MakeTestMdbms(file1, file2);
}

void CompareTestBase::MakeTestMdbms(const string& fname1, const string& fname2) {
    int i;
    char key[4096];
    char val[4096];
    datum kdat, vdat;
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder db1 = mdbm_open(fname1.c_str(), flags, 0644, 1024, 4096);
    MdbmHolder db2 = mdbm_open(fname2.c_str(), flags|MDBM_LARGE_OBJECTS, 0644, 1024, 8192);
    mdbm_setspillsize(db2, 50);

    kdat.dptr = key;
    vdat.dptr = val;
    // first db only
    for (i=0; i<5; ++i) {
      kdat.dsize = snprintf(key, sizeof(key), "key %04d -.val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value %07d", i);
      mdbm_store(db1, kdat, vdat, MDBM_REPLACE);
    }
    // second db only
    for (i=5; i<10; ++i) {
      kdat.dsize = snprintf(key, sizeof(key), "key %04d .-val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value %07d", i);
      mdbm_store(db2, kdat, vdat, MDBM_REPLACE);
    }
    // both dbs
    for (i=10; i<100; ++i) {
      kdat.dsize = snprintf(key, sizeof(key), "key %04d ==val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value %07d", i);
      mdbm_store(db1, kdat, vdat, MDBM_REPLACE);
      mdbm_store(db2, kdat, vdat, MDBM_REPLACE);
    }

    // different values
    for (i=100; i<105; ++i) {
      kdat.dsize = snprintf(key, sizeof(key), "key %04d !=val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value1 %07d", i);
      mdbm_store(db1, kdat, vdat, MDBM_REPLACE);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this is value2 %07d", i);
      mdbm_store(db2, kdat, vdat, MDBM_REPLACE);
    }

    // oversize in db1, same key/value
    for (i=105; i<110; ++i) {
      kdat.dsize = snprintf(key, sizeof(key), "large key %04d", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this oversized is value %07d", i);
      mdbm_store(db1, kdat, vdat, MDBM_REPLACE);
      mdbm_store(db2, kdat, vdat, MDBM_REPLACE);
    }

    // oversize in db1, different value
    for (i=110; i<115; ++i) {
      kdat.dsize = snprintf(key, sizeof(key), "key %04d !=val", i);
      vdat.dsize = snprintf(val, sizeof(val), "whereas this oversized is value1 %07d", i);
      mdbm_store(db1, kdat, vdat, MDBM_REPLACE);
      vdat.dsize = sprintf(val, "whereas this oversized is value2 %07d", i);
      mdbm_store(db2, kdat, vdat, MDBM_REPLACE);
    }
}

void CompareTestBase::CompareSame()
{ // compare MDBM against itself
    TRACE_TEST_CASE("CompareSame");
    const char* args[] = { "foo", file1.c_str(), file1.c_str(), NULL };
    reset_getopt();
    int ret = comp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
    CPPUNIT_ASSERT(ret == 0);
}

void CompareTestBase::CompareDifferent()
{ // compare different MDBMs
    TRACE_TEST_CASE("CompareDifferent");
    const char* args[] = { "foo", file1.c_str(), file2.c_str(), NULL };
    reset_getopt();
    int ret = comp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
    CPPUNIT_ASSERT(ret != 0);
}

void CompareTestBase::CheckDiffCount()
{ // compare different MDBMs
    TRACE_TEST_CASE("CheckDiffCount");
    MdbmHolder db1 = mdbm_open(file1.c_str(), MDBM_O_RDONLY, 0, 0, 0);
    MdbmHolder db2 = mdbm_open(file2.c_str(), MDBM_O_RDONLY, 0, 0, 0);

    uint64_t count1 = dump_different(db1, db2, 0, 0);
    uint64_t count2 = dump_different(db1, db2, 1, 0);
    uint64_t count3 = dump_different(db2, db1, 1, 0);

    CPPUNIT_ASSERT(count1 == 15);
    CPPUNIT_ASSERT(count2 ==  5);
    CPPUNIT_ASSERT(count3 ==  5);
}

void CompareTestBase::PatchMissing()
{ // patch missing entries from the first db to the second
    TRACE_TEST_CASE("PatchMissing");
    { // dump out keys only in the first db
      // NOTE: on FreeBSD, getopt works differently, so filenames *must* be last
      const char* args[] = { "foo", "-m", "-F", "cdb",
        "-f", difname.c_str(), file1.c_str(), file2.c_str(), NULL };
      reset_getopt();
      int ret = comp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      CPPUNIT_ASSERT(ret > 0);
    }

    { // import changes to the second db
      const char* args[] = { "foo", "-c", "-S", "1", "-i", difname.c_str(), file2.c_str(), NULL };
      reset_getopt();
      int ret = imp_main_wrapper_c(sizeof(args)/sizeof(args[0])-1, (char**)args);
      //fprintf(stderr, "import return code is %d\n", ret);
      CPPUNIT_ASSERT(ret == 0);
    }

    MdbmHolder db1 = mdbm_open(file1.c_str(), MDBM_O_RDONLY, 0, 0, 0);
    MdbmHolder db2 = mdbm_open(file2.c_str(), MDBM_O_RDONLY, 0, 0, 0);

    flags=0; // hacky, reset this so we don't trigger cdb output against null file
    uint64_t count1 = dump_different(db1, db2, 0, 0);
    uint64_t count2 = dump_different(db1, db2, 1, 0);
    uint64_t count3 = dump_different(db2, db1, 1, 0);

    CPPUNIT_ASSERT(count1 == 10);
    CPPUNIT_ASSERT(count2 ==  0);
    CPPUNIT_ASSERT(count3 ==  5);
}

void CompareTestBase::PatchCommon()
{ // patch common entries from the first db to the second
    TRACE_TEST_CASE("PatchCommon");
    { // dump out keys only in both dbs
      // NOTE: on FreeBSD, getopt works differently, so filenames *must* be last
      const char* args[] = { "foo", "-M", "-F", "cdb",
        "-f", difname.c_str(), file1.c_str(), file2.c_str(), NULL };
      reset_getopt();
      int ret = comp_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      CPPUNIT_ASSERT(ret > 0);
    }

    { // import changes to the second db
      const char* args[] = { "foo", "-c", "-S", "1", "-i", difname.c_str(), file2.c_str(), NULL };
      reset_getopt();
      int ret = imp_main_wrapper_c(sizeof(args)/sizeof(args[0])-1, (char**)args);
      //fprintf(stderr, "import return code is %d\n", ret);
      CPPUNIT_ASSERT(ret == 0);
    }

    MdbmHolder db1 = mdbm_open(file1.c_str(), MDBM_O_RDONLY, 0, 0, 0);
    MdbmHolder db2 = mdbm_open(file2.c_str(), MDBM_O_RDONLY, 0, 0, 0);

    flags=0; // hacky, reset this so we don't trigger cdb output against null file
    uint64_t count1 = dump_different(db1, db2, 0, 0);
    uint64_t count2 = dump_different(db1, db2, 1, 0);
    uint64_t count3 = dump_different(db2, db1, 1, 0);

    CPPUNIT_ASSERT(count1 == 5);
    CPPUNIT_ASSERT(count2 == 5);
    CPPUNIT_ASSERT(count3 == 5);
}



class CompareTestV3 : public CompareTestBase
{
    CPPUNIT_TEST_SUITE(CompareTestV3);
    CPPUNIT_TEST(CompareSame);
    CPPUNIT_TEST(CompareDifferent);
    CPPUNIT_TEST(CheckDiffCount);
    CPPUNIT_TEST(PatchMissing);
    CPPUNIT_TEST(PatchCommon);
    CPPUNIT_TEST_SUITE_END();

public:
    CompareTestV3() : CompareTestBase(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(CompareTestV3);



