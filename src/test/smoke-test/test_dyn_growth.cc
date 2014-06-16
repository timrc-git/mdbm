/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// File: test-dyn-growth.cc
// Purpose: Smoke tests for mdbm dynamic growth functionality

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

class SmokeTestDynGrowthSuite : public CppUnit::TestFixture, public TestBase {
public:
    SmokeTestDynGrowthSuite(int vFlag) : TestBase(vFlag, "SmokeTestDynGrowthSuite") {}
    void setUp();
    void tearDown();
    void initialSetup();
    // Smoke tests in this suite
    void smoke_test_dyn_growth_01();  //        TC 01
    void smoke_test_dyn_growth_02();  //        TC 02
    void smoke_test_dyn_growth_03();  //        TC 03
    void smoke_test_dyn_growth_04();  //        TC 04
    void smoke_test_dyn_growth_05();  //        TC 05
    void smoke_test_dyn_growth_06();  //        TC 06
    void smoke_test_dyn_growth_07();  //        TC 07
    void smoke_test_dyn_growth_08();  //        TC 08
    void smoke_test_dyn_growth_09();  //        TC 09
    void smoke_test_dyn_growth_10();  //        TC 10
    void smoke_test_dyn_growth_11();  //        TC 11

    MDBM* create_mdbm(const string& mdbm_name, bool losupport);
    int getNumberOfRecords(MDBM* mdbm, datum dk);
    int findRecord(MDBM* mdbm_db, datum dk, datum dv);

private:
    static string _basePath;
    static int _pageSize;
    static int _limitSize;
    static int _initialDbSize;
    static int _largeObjSize;
};

string SmokeTestDynGrowthSuite::_basePath = "";
int SmokeTestDynGrowthSuite::_pageSize = 4096;
int SmokeTestDynGrowthSuite::_initialDbSize = 4096*10;
int SmokeTestDynGrowthSuite::_largeObjSize = 4096; // for large object, size should be  > 3/4 pagesize
int SmokeTestDynGrowthSuite::_limitSize = 120;

void SmokeTestDynGrowthSuite::setUp() {
}

void SmokeTestDynGrowthSuite::tearDown() {
}

// Initial setup for all mdbm smoke tests
void SmokeTestDynGrowthSuite::initialSetup() {
    GetTmpDir(_basePath);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_01() {
    string trprefix = "TC 01: Smoke Test DB Dynamic Growth: Page full: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_01.mdbm";
    int ret = -1, numOfRecords = 0;
    int oldMdbmSize = 0, newMdbmSize = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"value";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 254 is derived by trial.
    for (int i = 0; i < 254; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 254 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 254);
    //Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm_db)) != 0);
    //Mdbm page is now full. This store will call split function. Existing Mdbm grows in size.
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
    //Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm_db)) > oldMdbmSize);
    //Verify that the Mdbm has exactly 255 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 255);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_02() {
    string trprefix = "TC 02: Smoke Test DB Dynamic Growth: Page full, delete all: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_02.mdbm";
    int ret = -1, numOfRecords = 0;
    int oldMdbmSize = 0, newMdbmSize = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"value";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 254 is derived by trial.
    for (int i = 0; i < 254; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 254 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 254);
    //Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm_db)) != 0);
    //Mdbm page is now full. This store will call split function. Existing Mdbm grows in size.
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
    //Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm_db)) > oldMdbmSize);
    //Verify that the Mdbm has exactly 255 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 255);

    //delete all the records
    for (int i = 0; i < 255; i++) {
       CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    }
    //verify number of records as zero
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_03() {
    string trprefix = "TC 03: Smoke Test DB Dynamic Growth: Page full, delete all, store again: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_03.mdbm";
    int ret = -1, numOfRecords = 0;
    int oldMdbmSize = 0, newMdbmSize = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 193 is derived by trial.
    for (int i = 0; i < 193; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 193 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 193);
    //Get the size of Mdbm
    CPPUNIT_ASSERT((oldMdbmSize = mdbm_get_size(mdbm_db)) != 0);
    //Mdbm page is now full. This store will call split function. Existing Mdbm grows in size.
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
    //Get the size of Mdbm
    CPPUNIT_ASSERT((newMdbmSize = mdbm_get_size(mdbm_db)) > oldMdbmSize);
    //Verify that the Mdbm has exactly 194 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 194);

    //delete all the records
    for (int i = 0; i < 194; i++) {
       CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    }
    //verify number of records as zero
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 0);

    //Store the records again
    for (int i = 0; i < 194; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }
    //Verify that the Mdbm has exactly 193 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 194);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_04() {
    string trprefix = "TC 04: Smoke Test DB Dynamic Growth:  MDBM size full, store fails: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_04.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 7605 is derived by trial.
    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 7605 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 7605);
    //Mdbm is now full. This store will fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_05() {
    string trprefix = "TC 05: Smoke Test DB Dynamic Growth:  MDBM size full, delete some record and store: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_05.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 7605 is derived by trial.
    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 7605 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 7605);
    //Mdbm is now full. This store will fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
    //delete some record and then store
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_06() {
    string trprefix = "TC 06: Smoke Test DB Dynamic Growth:  MDBM size full, delete some record and store record having size > deleted record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_06.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 7605 is derived by trial.
    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Make the mdbm size full with exact Bytes
    dv.dptr = (char*)"full";
    dv.dsize = strlen(dv.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);

    //verify that new record added
    ret = findRecord(mdbm_db, dk, dv);
    CPPUNIT_ASSERT_EQUAL(ret, 0);

    //Verify that the Mdbm has exactly 7606 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 7606);
    //Mdbm is now full. This store will fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
    //delete some record
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    //Store some record having size > deleted record
    dv.dptr = (char*)"dyn-growth1";
    dv.dsize = strlen(dv.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_07() {
    string trprefix = "TC 07: Smoke Test DB Dynamic Growth:  MDBM size full, delete some record and store record having size < deleted record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_07.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm page is full. This number 7605 is derived by trial.
    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Make the mdbm size full with exact Bytes
    dv.dptr = (char*)"full";
    dv.dsize = strlen(dv.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);

    //verify that new record added
    ret = findRecord(mdbm_db, dk, dv);
    CPPUNIT_ASSERT_EQUAL(ret, 0);

    //Verify that the Mdbm has exactly 7606 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 7606);
    //Mdbm is now full. This store will fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
    //delete some record
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    //Store some record having size < deleted record
    dv.dptr = (char*)"dyngrowth";
    dv.dsize = strlen(dv.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_08() {
    string trprefix = "TC 08: Smoke Test DB Dynamic Growth:  MDBM size full, delete some record and store record having size = deleted record: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_08.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, false);

    //Create key-value pair
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    //Store these records until Mdbm is full. This number 7605 is derived by trial.
    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Make the mdbm size full with exact Bytes
    dv.dptr = (char*)"full";
    dv.dsize = strlen(dv.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);

    //verify that new record added
    ret = findRecord(mdbm_db, dk, dv);
    CPPUNIT_ASSERT_EQUAL(ret, 0);

    //Verify that the Mdbm has exactly 7606 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 7606);
    //Mdbm is now full. This store will fail
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
    //delete some record
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    //Store some record having size = deleted record
    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_09() {
    string trprefix = "TC 09: Smoke Test DB Dynamic Growth:  Store Large object until MDBM full, delete some recod and add: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_09.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, true);

    string key = "key", val;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();

    val.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store these records until Mdbm is full. This number 56 is derived by trial.
    for (int i = 0; i < 56; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 56 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 56);
    //Store will fail since mdbm is already full
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
    //delete some record
    CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    //Add again
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_10() {
    string trprefix = "TC 10: Smoke Test DB Dynamic Growth:  Store Large object until MDBM full, delete all and add: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_10.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, true);

    string key = "key", val;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();

    val.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store these records until Mdbm is full. This number 56 is derived by trial.
    for (int i = 0; i < 56; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 56 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 56);
    //Store will fail since mdbm is already full
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);
    //delete all
    for (int i = 0; i< 56; i++) {
        CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    }
    //Verify that mdbm is empty
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 0);

    //Add again
    for (int i = 0; i < 56; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }
}

void SmokeTestDynGrowthSuite::smoke_test_dyn_growth_11() {
    string trprefix = "TC 11: Smoke Test DB Dynamic Growth: Store Large object until MDBM full, delete all and add non-large objects until mdbm full: ";
    TRACE_TEST_CASE(trprefix);
    const string mdbm_name = _basePath + "/smoke_test_dyn_growth_11.mdbm";
    int ret = -1, numOfRecords = 0;
    datum dk, dv;
    //Create mdbm with LO support
    MdbmHolder mdbm_db = create_mdbm(mdbm_name, true);

    string key = "key", val;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();

    val.assign(_largeObjSize, 'l');
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    //Store these records until Mdbm is full. This number 56 is derived by trial.
    for (int i = 0; i < 56; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }
    //Verify that the Mdbm has exactly 56 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 56);
    //Store will fail since mdbm is already full
    CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) != 0);

    //delete all records and store non-large objects
    for (int i = 0; i< 56; i++) {
        CPPUNIT_ASSERT((ret = mdbm_delete(mdbm_db, dk)) == 0);
    }
    //Verify that the Mdbm is empty after deletion
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 0);

    dv.dptr = (char*)"dyn-growth";
    dv.dsize = strlen(dv.dptr);

    for (int i = 0; i < 7605; i++) {
        //Add data (key-value pair) to Mdbm
        CPPUNIT_ASSERT((ret = mdbm_store(mdbm_db, dk, dv, MDBM_INSERT_DUP)) == 0);
        CPPUNIT_ASSERT((ret = mdbm_get_size(mdbm_db)) != 0);
    }

    //Verify that the Mdbm has exactly 7605 records
    numOfRecords = getNumberOfRecords(mdbm_db, dk);
    CPPUNIT_ASSERT_EQUAL(numOfRecords, 7605);
}

//Function to create Mdbm
MDBM* SmokeTestDynGrowthSuite::create_mdbm(const string& mdbm_name,  bool losupport) {
    MDBM *mdbm_db = NULL;
    int openflags, ret = -1;

    openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    if(losupport) {
       openflags = openflags | MDBM_LARGE_OBJECTS;
    }

    CPPUNIT_ASSERT((mdbm_db = mdbm_open(mdbm_name.c_str(), openflags, 0644, _pageSize, _initialDbSize)) != NULL);

    if (!mdbm_db) {
        stringstream msg;
        msg << mdbm_name << " Failed to mdbm_open DB(errno=" << errno;
        msg << endl;
        cerr << msg.str();
        assert(mdbm_db);
        return NULL;
    }

    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm_db, MDBM_HASH_MD5));
    CPPUNIT_ASSERT((ret = mdbm_limit_size_v3(mdbm_db, static_cast<mdbm_ubig_t>(_limitSize), NULL, NULL)) == 0);

    return mdbm_db;
}

// Function to fetch number of records in an Mdbm
int SmokeTestDynGrowthSuite::getNumberOfRecords(MDBM* mdbm, datum k) {
    datum v;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    int numOfRecords = -1, ret = 0;
    while (ret == 0) {
        ret = mdbm_fetch_dup_r(mdbm, &k, &v, &iter);
        numOfRecords++;
    }
    return numOfRecords;
}

// Function to check if a record exists in Mdbm or not
int SmokeTestDynGrowthSuite::findRecord(MDBM* mdbm, datum k, datum v) {
    datum v1;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    int ret = 0;
    while (ret == 0) {
        ret = mdbm_fetch_dup_r(mdbm, &k, &v1, &iter);
        if (!v1.dptr) {
            return -1;
        }
        if (memcmp(v1.dptr, v.dptr, v1.dsize) == 0) {
            return 0;
        }
    }
    return -1;
}


class MdbmSmokeTest : public SmokeTestDynGrowthSuite
{
    CPPUNIT_TEST_SUITE(MdbmSmokeTest);
      CPPUNIT_TEST(initialSetup);
      CPPUNIT_TEST(smoke_test_dyn_growth_01);   // TC 01
      CPPUNIT_TEST(smoke_test_dyn_growth_02);   // TC 02
      CPPUNIT_TEST(smoke_test_dyn_growth_03);   // TC 03
      CPPUNIT_TEST(smoke_test_dyn_growth_04);   // TC 04
      CPPUNIT_TEST(smoke_test_dyn_growth_05);   // TC 05
      CPPUNIT_TEST(smoke_test_dyn_growth_06);   // TC 06
      CPPUNIT_TEST(smoke_test_dyn_growth_07);   // TC 07
      CPPUNIT_TEST(smoke_test_dyn_growth_08);   // TC 08
      CPPUNIT_TEST(smoke_test_dyn_growth_09);   // TC 09
      CPPUNIT_TEST(smoke_test_dyn_growth_10);   // TC 10
      CPPUNIT_TEST(smoke_test_dyn_growth_11);   // TC 11
CPPUNIT_TEST_SUITE_END();

public:
    MdbmSmokeTest() : SmokeTestDynGrowthSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmSmokeTest);


