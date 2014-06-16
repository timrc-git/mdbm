/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test_util.cc
// Purpose: Unit tests for mdbm utility api, mdbm_util_get_size

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/wait.h>

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
#include "mdbm_util.h"
#include "TestBase.hh"


using namespace std;

class MdbmUnitTestUtilSuite : public CppUnit::TestFixture, public TestBase
{
public:
    MdbmUnitTestUtilSuite(int vFlag) : TestBase(vFlag, "MdbmUnitTestUtilSuite") {}
    void setUp();
    void tearDown();
    void initialSetup();
    //util tests in this suite
    void unit_test_utility_01();      //        TC 01
    void unit_test_utility_02();      //        TC 02
    void unit_test_utility_03();      //        TC 03
    void unit_test_utility_04();      //        TC 04
    void unit_test_utility_05();      //        TC 05
    void unit_test_utility_06();      //        TC 06
    void unit_test_utility_07();      //        TC 07
    void unit_test_utility_08();      //        TC 08
    void unit_test_utility_09();      //        TC 09
    void unit_test_utility_10();      //        TC 10
    void unit_test_utility_11();      //        TC 11

    void create_mdbm_with_entry(const string& mdbm_name, uint64_t dbsize, bool forked = false);

private:
    static string _basePath;
    static int _pageSize;
};

string MdbmUnitTestUtilSuite::_basePath = "";
int MdbmUnitTestUtilSuite::_pageSize = 4096;

void MdbmUnitTestUtilSuite::setUp() {
}

void MdbmUnitTestUtilSuite::tearDown() {
}

// Initial setup for all mdbm util tests
void
MdbmUnitTestUtilSuite::initialSetup()
{
    GetTmpDir(_basePath);
}

void
MdbmUnitTestUtilSuite::unit_test_utility_01()
{
    string trprefix = "TC 01: Unit Test get size utility: option k  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_01.mdbm";
    uint64_t dbsize = 0;

    //get the db size
    dbsize = mdbm_util_get_size("k", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}


void
MdbmUnitTestUtilSuite::unit_test_utility_02()
{
    string trprefix = "TC 02: Unit Test get size utility: option g  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_02.mdbm";
    uint64_t dbsize = 0;

    //get the dbn size
    dbsize = mdbm_util_get_size("g", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}

void
MdbmUnitTestUtilSuite::unit_test_utility_03()
{
    string trprefix = "TC 03: Unit Test get size utility: option m  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_03.mdbm";
    uint64_t dbsize = 0;

    //get the db size
    dbsize = mdbm_util_get_size("m", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}

void
MdbmUnitTestUtilSuite::unit_test_utility_04()
{
    string trprefix = "TC 04: Unit Test get size utility: option num*k  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_04.mdbm";
    uint64_t dbsize = 0;

    //get the db size
    dbsize = mdbm_util_get_size("1024k", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}

void
MdbmUnitTestUtilSuite::unit_test_utility_05()
{
    string trprefix = "TC 05: Unit Test get size utility: option num*g  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_05.mdbm";
    uint64_t dbsize = 0;

    //get the db size
    dbsize = mdbm_util_get_size("1024g", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}

void
MdbmUnitTestUtilSuite::unit_test_utility_06()
{
    string trprefix = "TC 06: Unit Test get size utility: option num*m  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_06.mdbm";
    uint64_t dbsize = 0;

    //get the db size
    dbsize = mdbm_util_get_size("256m", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}


void
MdbmUnitTestUtilSuite::unit_test_utility_07()
{
    string trprefix = "TC 07: Unit Test get size utility: option long num*k  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_07.mdbm";
    uint64_t dbsize = 0;
    CPPUNIT_ASSERT(0 != mdbm_util_get_size_ref("18014398509481984k", 1024*1024, &dbsize));
}

void
MdbmUnitTestUtilSuite::unit_test_utility_08()
{
    string trprefix = "TC 08: Unit Test get size utility: option long num*g  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_08.mdbm";
    uint64_t dbsize = 0;
    CPPUNIT_ASSERT(0 != mdbm_util_get_size_ref("18014398509481984g", 1024*1024, &dbsize));
}

void
MdbmUnitTestUtilSuite::unit_test_utility_09()
{
    string trprefix = "TC 09: Unit Test get size utility: option long num*m  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_09.mdbm";
    uint64_t dbsize = 0;
    CPPUNIT_ASSERT(0 != mdbm_util_get_size_ref("18014398509481984m", 1024*1024, &dbsize));
}

void
MdbmUnitTestUtilSuite::unit_test_utility_10()
{
    string trprefix = "TC 10: Unit Test get size utility: Invalid option  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_10.mdbm";
    uint64_t dbsize = 0;
    CPPUNIT_ASSERT(0 != mdbm_util_get_size_ref("xxxxxx", 1024*1024, &dbsize));
}

void
MdbmUnitTestUtilSuite::unit_test_utility_11()
{
    string trprefix = "TC 11: Unit Test get size utility: option 0  ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/unit_test_utility_11.mdbm";
    uint64_t dbsize = 0;

    //get the db size
    dbsize = mdbm_util_get_size("0", 1024*1024);

    //create mdbm
    create_mdbm_with_entry(mdbm_name, dbsize);
}

//Function to create Mdbm
void
MdbmUnitTestUtilSuite::create_mdbm_with_entry(const string& mdbm_name, uint64_t dbsize, bool forked)
{
    MDBM *mdbm_db = NULL;
    int openflags;
    datum dk, dv;

    openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    mdbm_db = mdbm_open(mdbm_name.c_str(), openflags, 0644, _pageSize, dbsize);
    if (!forked) {
      CPPUNIT_ASSERT(mdbm_db);
    } else if (!mdbm_db) {
      fprintf(stderr, "ERROR: %s:%d Failed to open %s\n", __FILE__, __LINE__, mdbm_name.c_str());
      exit(2);
    }

    if (!mdbm_db) {
        stringstream msg;
        msg << mdbm_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(mdbm_db);
        return;
    }

    int set_hash_ret = mdbm_sethash(mdbm_db, MDBM_HASH_MD5);
    if (!forked) {
      CPPUNIT_ASSERT_EQUAL(1, set_hash_ret);
    } else if (1 != set_hash_ret) {
      fprintf(stderr, "ERROR: %s:%d Set hash failed %s\n", __FILE__, __LINE__, mdbm_name.c_str());
      exit(2);
    }

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"value";
    dv.dsize = strlen(dv.dptr);

    //Add data (key-value pair) to Mdbm
    int store_ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP);
    if (!forked) {
      CPPUNIT_ASSERT_EQUAL(0, store_ret);
    } else if (store_ret) {
      fprintf(stderr, "ERROR: %s:%d Store failed %s\n", __FILE__, __LINE__, mdbm_name.c_str());
      exit(2);
    }
    mdbm_close(mdbm_db);
}

class MdbmUnitTestUtil : public MdbmUnitTestUtilSuite
{
    CPPUNIT_TEST_SUITE(MdbmUnitTestUtil);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(unit_test_utility_01);   // TC 01
      CPPUNIT_TEST(unit_test_utility_02);   // TC 02
      CPPUNIT_TEST(unit_test_utility_03);   // TC 03
      CPPUNIT_TEST(unit_test_utility_04);   // TC 04
      CPPUNIT_TEST(unit_test_utility_05);   // TC 05
      CPPUNIT_TEST(unit_test_utility_06);   // TC 06
      CPPUNIT_TEST(unit_test_utility_07);   // TC 07
      CPPUNIT_TEST(unit_test_utility_08);   // TC 08
      CPPUNIT_TEST(unit_test_utility_09);   // TC 09
      CPPUNIT_TEST(unit_test_utility_10);   // TC 10
      CPPUNIT_TEST(unit_test_utility_11);   // TC 11
CPPUNIT_TEST_SUITE_END();

public:
    MdbmUnitTestUtil() : MdbmUnitTestUtilSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestUtil);
