/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-large-obj.cc
// Purpose: Smoke tests for mdbm large objects functionality

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

using namespace std;

class SmokeTestLargeObjSuite : public CppUnit::TestFixture, public TestBase {
public:
    SmokeTestLargeObjSuite(int vFlag) : TestBase(vFlag, "SmokeTestLargeObjSuite") {}
    void setUp();
    void tearDown();
    void initialSetup();
    // Smoke tests in this suite
    void smoke_test_largeobj_01();  //        TC 01
    void smoke_test_largeobj_02();  //        TC 02
    void smoke_test_largeobj_03();  //        TC 03
    void smoke_test_largeobj_04();  //        TC 04
    void smoke_test_largeobj_05();  //        TC 05
    void smoke_test_largeobj_06();  //        TC 06
    void smoke_test_largeobj_07();  //        TC 07
    void smoke_test_largeobj_08();  //        TC 08
    void smoke_test_largeobj_09();  //        TC 09
    void smoke_test_largeobj_10();  //        TC 10
    void smoke_test_largeobj_11();  //        TC 11

    MDBM* create_mdbm(const string& mdbm_cache_name);

private:
    static string _basePath;
    static int _pageSize;
    static int _limitSize;
    static int _largeObjSize;
};

string SmokeTestLargeObjSuite::_basePath = "";
int SmokeTestLargeObjSuite::_pageSize = 512;
int SmokeTestLargeObjSuite::_largeObjSize = 512; // for large object, size should be  > 3/4 pagesize

void SmokeTestLargeObjSuite::setUp() {
}

void SmokeTestLargeObjSuite::tearDown() {
}

// Initial setup for all mdbm smoke tests
void SmokeTestLargeObjSuite::initialSetup() {
    GetTmpDir(_basePath);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_01() {
    string trprefix = "TC 01: Smoke Test DB Large Object: Store record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_01.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    /* LO creation */
    string key = "lokey01", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store large object data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //fetch data and verify
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_02() {
    string trprefix = "TC 02: Smoke Test DB Large Object: Store and delete record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_02.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    /* LO creation */
    string key = "lokey02", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);

    //fetch data and verify
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    //delete the record
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);

    //Fetch the value for stored key
    value = mdbm_fetch(mdbm_db, dk);

    //Verify that fetched value is same as zero after deletion
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
    CPPUNIT_ASSERT(value.dptr == NULL);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_03() {
    string trprefix = "TC 03: Smoke Test DB Large Object: Modify record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_03.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    /* LO creation */
    string key = "lokey03", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store large object data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);

    //fetch data and verify
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    //Modify the record
    val.assign(_largeObjSize, 'm');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Add data (key-value pair) to Mdbm
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_REPLACE)) == 0);

    //fetch data and verify
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_04() {
    string trprefix = "TC 04: Smoke Test DB Large Object: Store, delete and Store record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_04.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    /* LO creation */
    string key = "lokey04", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //fetch data and verify
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    //delete the record
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);

    //Verify that after delete, data base is empty
    kvpair kv;
    kv = mdbm_first(mdbm_db);
    CPPUNIT_ASSERT(kv.key.dsize == 0);
    CPPUNIT_ASSERT(kv.key.dptr == NULL);
    CPPUNIT_ASSERT(kv.val.dsize == 0);
    CPPUNIT_ASSERT(kv.val.dptr == NULL);

    //Store data again
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //Fetch the value for stored key
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_05() {
    string trprefix = "TC 05: Smoke Test DB Large Object: Store record with empty string: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_05.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    string key = "lokey05";
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)"";
    dv.dsize = strlen(dv.dptr);
    //Store data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //Fetch and verify record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_06() {
    string trprefix = "TC 06: Smoke Test DB Large Object: Store record with empty string and update with non empty string: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_06.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    string key = "lokey06", val;
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)"";
    dv.dsize = strlen(dv.dptr);
    //Store data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);

    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    //Update record
    val.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val.c_str();;
    dv.dsize = _largeObjSize;

    //Store data again
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_REPLACE)) == 0);
    //Fetch and verify record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_07() {
    string trprefix = "TC 07: Smoke Test DB Large Object: Store record with empty key and empty string: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_07.mdbm";
    int ret = -1;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    string key = "";
    datum dk;
    dk.dptr  = (char*)"";
    dk.dsize = strlen(dk.dptr);
    datum dv;
    dv.dptr  = (char*)"";
    dv.dsize = strlen(dv.dptr);
    //Store data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) != 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_08() {
    string trprefix = "TC 08: Smoke Test DB Large Object: Retrieve non existing key: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_08.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    /* LO creation */
    string key = "lokey08", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store large object data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);

    //New key
    string key1 = "lonewkey08";
    dk.dptr  = (char*)key1.c_str();
    dk.dsize = key1.size();

    //Fetch non existing key. It should return NULL
    value = mdbm_fetch(mdbm_db, dk);
    //fetch data and verify
    CPPUNIT_ASSERT(value.dptr == NULL);
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_09() {
    string trprefix = "TC 09: Smoke Test DB Large Object: Store record with non-empty string and update with empty string: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_09.mdbm";
    int ret = -1;
    datum value;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    string key = "lokey09", val;
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    val.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store data
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //Fetch and verify record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    //Update record with empty string
    dv.dptr  = (char*)"";
    dv.dsize = strlen(dv.dptr);
    //Store data again
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_REPLACE)) == 0);
    //Fetch and verify record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_10() {
    string trprefix = "TC 10: Smoke Test DB Large Object: Store record with large and non-large objects: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_10.mdbm";
    int ret = -1;
    datum value;
    int _nonlargeObjSize = 256;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    //Large object creation
    string key = "lokey10", val;
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    val.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;


    //non-large object creation
    string key1 = "lokey10_1", val1;
    datum dk1;
    dk1.dptr  = (char*)key1.c_str();
    dk1.dsize = key1.size();
    datum dv1;
    val1.assign(_nonlargeObjSize, 'n');
    dv1.dptr  = (char*)val1.c_str();
    dv1.dsize = _nonlargeObjSize;

    //Store large object record
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //Store non-large object record
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk1, dv1, MDBM_INSERT)) == 0);

    //Fetch and verify large object record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    //Fetch and verify non-large object record
    value = mdbm_fetch(mdbm_db, dk1);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv1.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv1.dptr, value.dsize)) == 0);
}

void SmokeTestLargeObjSuite::smoke_test_largeobj_11() {
    string trprefix = "TC 10: Smoke Test DB Large Object: Store non-large object replace with large object: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_large_obj_11.mdbm";
    int ret = -1;
    datum value;
    int _nonlargeObjSize = 256;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name);

    //non-large object creation
    string key = "lokey11", val, val1;
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    val.assign(_nonlargeObjSize, 'n');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _nonlargeObjSize;

    //Store large object record
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT)) == 0);
    //Fetch and verify non-large object record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);

    val1.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val1.c_str();
    dv.dsize = _largeObjSize;

    //Replace the record with large object record
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_REPLACE)) == 0);
    //Fetch and verify large object record
    value = mdbm_fetch(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(value.dsize, dv.dsize);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT((ret = memcmp(value.dptr, dv.dptr, value.dsize)) == 0);
}

//Function to create Mdbm as cache
MDBM* SmokeTestLargeObjSuite::create_mdbm(const string& mdbm_name) {
    int openflags = MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC| 
                    versionFlag|MDBM_LARGE_OBJECTS|MDBM_DBSIZE_MB;

    MDBM *mdbm_db = mdbm_open(mdbm_name.c_str(), openflags, 0644, _pageSize, 0);
    CPPUNIT_ASSERT(mdbm_db != NULL);

    if (!mdbm_db) {
        stringstream msg;
        msg << mdbm_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(mdbm_db);
        return NULL;
    }

    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));

    return mdbm_db;
}


class MdbmSmokeTest : public SmokeTestLargeObjSuite {
    CPPUNIT_TEST_SUITE(MdbmSmokeTest);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(smoke_test_largeobj_01);   // TC 01
      CPPUNIT_TEST(smoke_test_largeobj_02);   // TC 02
      CPPUNIT_TEST(smoke_test_largeobj_03);   // TC 03
      CPPUNIT_TEST(smoke_test_largeobj_04);   // TC 04
      CPPUNIT_TEST(smoke_test_largeobj_05);   // TC 05
      CPPUNIT_TEST(smoke_test_largeobj_06);   // TC 06
      CPPUNIT_TEST(smoke_test_largeobj_07);   // TC 07
      CPPUNIT_TEST(smoke_test_largeobj_08);   // TC 08
      CPPUNIT_TEST(smoke_test_largeobj_09);   // TC 09
      CPPUNIT_TEST(smoke_test_largeobj_10);   // TC 10
      CPPUNIT_TEST(smoke_test_largeobj_11);   // TC 11
CPPUNIT_TEST_SUITE_END();

public:
    MdbmSmokeTest() : SmokeTestLargeObjSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmSmokeTest);


