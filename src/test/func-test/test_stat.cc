/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-stat.cc
//    Purpose: Functional tests for stat functionality.

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

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

#include <mdbm.h>
#include "TestBase.hh"

using namespace std;

#define MODE 0777

class TestStatSuite : public CppUnit::TestFixture, public TestBase {
public:
  TestStatSuite(int vFlag) : TestBase(vFlag, "TestStatSuite") {}

  void setUp();
  void tearDown();
  void setEnv();

  void test_stat_01();
  void test_stat_02();
  void test_stat_03();
  void test_stat_04();
  void test_stat_05();

private:
  static int _pageSize;
  static int _initialDbSize;
  static mdbm_ubig_t _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static string _testDir;
  static int flags;
  static int flagsLO;
};

class TestStatFunc : public TestStatSuite {
  CPPUNIT_TEST_SUITE(TestStatFunc);

  CPPUNIT_TEST(setEnv);
  CPPUNIT_TEST(test_stat_01);
  CPPUNIT_TEST(test_stat_02);
  CPPUNIT_TEST(test_stat_03);
  CPPUNIT_TEST(test_stat_04);
  CPPUNIT_TEST(test_stat_05);

  CPPUNIT_TEST_SUITE_END();

public:
  TestStatFunc() : TestStatSuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestStatFunc);

int TestStatSuite::_pageSize = 4096;
int TestStatSuite::_initialDbSize = 4096*128;
mdbm_ubig_t TestStatSuite::_limitSize = 256;
int TestStatSuite::_largeObjSize = 4097;
int TestStatSuite::_spillSize = 3072;
string TestStatSuite::_testDir = "";
int TestStatSuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_CREATE_V3;
int TestStatSuite::flagsLO = TestStatSuite::flags|MDBM_LARGE_OBJECTS;

void TestStatSuite::setUp() {
}

void TestStatSuite::tearDown() {
}

void TestStatSuite::setEnv() {
  GetTmpDir(_testDir);
}

// Test case - Create a Mdbm. Store some key-value pairs and fetch them
// Use mdbm_get_stats() function to verify the statistics of Mdbm
void TestStatSuite::test_stat_01() {
  const string mdbm_name  = _testDir + "/stat01.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = NULL;
  CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize));
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

  for (int i = 0; i < 10; i++) {
    // Sore and fetch the record
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  // Call mdbm_get_stats()
  mdbm_stats t;
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &t, sizeof(t)));

  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_size), 528384);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_num_entries), 10);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_num_entries), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_num_free_entries), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_cache_mode), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_size), 4096);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_pages_used), 128);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_pages_used), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_count), 129);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_page_count), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_page_size), 4096);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_max_size), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_min_size), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_max_free), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_min_level), 7);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_max_level), 7);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_bytes_used), 80);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_bytes_used), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_threshold), 0);

  // Close Mdbm
  mdbm_close(mdbm);
}

// Test case - Create a Mdbm. Store some key-value pairs and fetch them
// Use mdbm_get_stats() function to verify the statistics of Mdbm
void TestStatSuite::test_stat_02() {
/*
  const string mdbm_name  = _testDir + "/stat02.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = NULL;
  CPPUNIT_ASSERT(mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize));
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

  for (int i = 0; i < 10; i++) {
    // Sore and fetch the record
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, 3075));
  }

  mdbm_stats t;
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &t, sizeof(t)));

  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_size), 569344);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_num_entries), 20);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_num_entries), 10);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_num_free_entries), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_cache_mode), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_size), 4096);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_pages_used), 128);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_pages_used), 10);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_count), 139);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_page_count), 10);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_page_size), 4096);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_max_size), 3075);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_min_size), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_max_free), 0);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_min_level), 7);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_max_level), 7);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_bytes_used), 30860);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_bytes_used), 30750);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_large_threshold), 3072);

  // Close Mdbm
  mdbm_close(mdbm);
*/
}

// Test case - Create a Mdbm. Store some key-value pair and fetch it.
// Verify that the fetched value is same as the stored value
void TestStatSuite::test_stat_03() {
    const string mdbm_name  = _testDir + "/stat03.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize));
    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(mdbm, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    datum k, v, value;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    mdbm_counter_t count_s, count_f, count_d;

    for (int i = 0; i < 10; i++) {
      // Store a record
      CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));
    }

    for (int i = 0; i < 10; i++) {
      // fetch the record
      value = mdbm_fetch(mdbm, k);
      CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
      CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    }

    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_counter(mdbm, MDBM_STAT_TYPE_STORE, &count_s));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_counter(mdbm, MDBM_STAT_TYPE_FETCH, &count_f));
    CPPUNIT_ASSERT_EQUAL(10, static_cast<int>(count_s));
    CPPUNIT_ASSERT_EQUAL(10, static_cast<int>(count_f));

    for (int i = 0; i < 10; i++) {
      // Delete the record
      CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));
    }

    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_counter(mdbm, MDBM_STAT_TYPE_DELETE, &count_d));
    CPPUNIT_ASSERT_EQUAL(10, static_cast<int>(count_d));

    // Reset all counters
    mdbm_reset_stat_operations(mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_counter(mdbm, MDBM_STAT_TYPE_STORE, &count_s));
    CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(count_s));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_counter(mdbm, MDBM_STAT_TYPE_FETCH, &count_f));
    CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(count_f));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_counter(mdbm, MDBM_STAT_TYPE_DELETE, &count_d));
    CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(count_d));

    // Close Mdbm
    mdbm_close(mdbm);
}


// Test case - Create a Mdbm. Store some key-value pair and fetch it.
// Verify that the fetched value is same as the stored value
void TestStatSuite::test_stat_04() {
    const string mdbm_name  = _testDir + "/stat04.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbm(db_name, flagsLO, _pageSize, _initialDbSize, _spillSize));
    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(mdbm, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    time_t time_s, time_f, time_d;
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_time(mdbm, MDBM_STAT_TYPE_STORE, &time_s));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_time(mdbm, MDBM_STAT_TYPE_FETCH, &time_f));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_time(mdbm, MDBM_STAT_TYPE_DELETE, &time_d));

    CPPUNIT_ASSERT(time_s == 0);
    CPPUNIT_ASSERT(time_f == 0);
    CPPUNIT_ASSERT(time_d == 0);

    datum k, v, value;
    for (int i = 0; i < 15; i++) {
      // Create key-value pair
      k.dptr = reinterpret_cast<char*>(&i);
      k.dsize = sizeof(i);
      v.dptr = reinterpret_cast<char*>(&i);
      v.dsize = sizeof(i);

      // Store a record
      CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

      // fetch the record
      value = mdbm_fetch(mdbm, k);
      CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
      CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);

      // //commenting this code. Need to fix this
      // // Delete a record
      // CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

      // // fetch the record
      // value = mdbm_fetch(mdbm, k);
      // CPPUNIT_ASSERT_EQUAL(0, value.dsize);

    }

    // Store and fetch the record
    //CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, 3075));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_time(mdbm, MDBM_STAT_TYPE_STORE, &time_s));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_time(mdbm, MDBM_STAT_TYPE_FETCH, &time_f));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stat_time(mdbm, MDBM_STAT_TYPE_DELETE, &time_d));

    CPPUNIT_ASSERT(time_s != 0);
    CPPUNIT_ASSERT(time_f != 0);
    //CPPUNIT_ASSERT(time_d != 0);

    // Close Mdbm
    mdbm_close(mdbm);
}


// Test case - To test mdbm_get_stat_name() function
void TestStatSuite::test_stat_05() {
    const string mdbm_name  = _testDir + "/stat05.mdbm";
    const char* db_name = mdbm_name.c_str();

    // Create a Mdbm
    MDBM *mdbm = NULL;
    CPPUNIT_ASSERT(mdbm = createMdbmNoLO(db_name, flagsLO, _pageSize, _initialDbSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

    const char* tagName;
    for (int tagCnt = 1; tagCnt <= 16; tagCnt++) {
      char tag[128];
      for (int flg = 0; flg < 4; flg++) {
        tagName = mdbm_get_stat_name(COMBINE_STAT_TAG(tagCnt, flg));
        switch(tagCnt) {
          case 1 :  strcpy(tag, "Fetch");           break;
          case 2 :  strcpy(tag, "Store");           break;
          case 3 :  strcpy(tag, "Delete");          break;
          case 4 :  strcpy(tag, "Lock");            break;
          case 5 :  strcpy(tag, "FetchUncached");   break;
          case 6 :  strcpy(tag, "GetPage");         break;
          case 7 :  strcpy(tag, "GetPageUncached"); break;
          case 8 :  strcpy(tag, "CacheEvict");      break;
          case 9 :  strcpy(tag, "CacheStore");      break;
          case 10 : strcpy(tag, "PageStore");       break;
          case 11 : strcpy(tag, "PageDelete");      break;
          case 12 : strcpy(tag, "MdbmSync");        break;
          case 13 : strcpy(tag, "FetchNotFound");   break;
          case 14 : strcpy(tag, "FetchError");      break;
          case 15 : strcpy(tag, "StoreError");      break;
          case 16 : strcpy(tag, "DeleteFailed");    break;
        }
        switch(flg) {
          case MDBM_STAT_CB_SET:     strcat(tag, "Val");     break;
          case MDBM_STAT_CB_TIME:    strcat(tag, "Time");    break;
          case MDBM_STAT_CB_ELAPSED: strcat(tag, "Latency"); break;
        }
        ostringstream oss;
        oss << "TagName= " << tagName << " Tag= " << tag << " TagCount= " << tagCnt
          << " Flag= " << flg;
        CPPUNIT_ASSERT_MESSAGE(oss.str(), 0 == strcmp(tagName, tag));
      }
    }

    tagName = mdbm_get_stat_name(0);
    CPPUNIT_ASSERT_EQUAL(0, strcmp(tagName, "unknown_tag"));

    // Close Mdbm
    mdbm_close(mdbm);
}

