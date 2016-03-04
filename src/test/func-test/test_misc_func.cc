/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-misc-func.cc
//    Purpose: Functional tests to test misc. functions
//      mdbm_get_magic_number
//      mdbm_get_partition_number
//      mdbm_set_alignment
//      mdbm_get_alignment
//      mdbm_set_cachemode
//      mdbm_get_cachemode
//      mdbm_get_errorno
//      mdbm_dump_page
//      mdbm_chk_page
//      mdbm_get_filename
//      mdbm_get_fd
//      mdbm_count_pages
//      mdbm_clean
//      get_version
//      mdbm_preload
//      mdbm_lock_pages
//      mdbm_unlock_pages

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>

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

class TestMiscFuncSuit : public CppUnit::TestFixture, public TestBase {
public:
  TestMiscFuncSuit(int vFlag) : TestBase(vFlag, "TestMiscFuncSuit") {}

  void setUp();
  void tearDown();
  void setEnv();

  void test_misc_func_01();
  void test_misc_func_02();
  void test_misc_func_03();
  void test_misc_func_04();
  void test_misc_func_05();
  void test_misc_func_06();
  void test_misc_func_07();
  void test_misc_func_08();
  void test_misc_func_09();
  void test_misc_func_10();
  void test_misc_func_11();
  void test_misc_func_12();
  void test_misc_func_13();
  void test_misc_func_14();
  void test_misc_func_15();
  void test_misc_func_16();
  void test_misc_func_17();
  void test_misc_func_18();
  void test_misc_func_19();
  void test_misc_func_20();
  void test_misc_func_21();
  void test_misc_func_22();
  void test_misc_func_23();
  void test_misc_func_24();
  void test_misc_func_25();
  void test_misc_func_26();
  void test_misc_func_27();
  void test_misc_func_28();
  void test_misc_func_29();
  void test_misc_func_30();
  void test_misc_func_31();
  void test_misc_func_32();
  void test_misc_func_33();
  void test_misc_func_34();
  void test_misc_func_35();
  void test_misc_func_36();

private:
  static int _pageSize;
  static int _initialDbSize;
  static int _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static int _windowSize;
  static string _mdbmDir;
  static string _testDir;
  static int flags;

};

class TestMiscFunc : public TestMiscFuncSuit {
  CPPUNIT_TEST_SUITE(TestMiscFunc);

  CPPUNIT_TEST(setEnv);

  CPPUNIT_TEST(test_misc_func_01);
  CPPUNIT_TEST(test_misc_func_02);
  CPPUNIT_TEST(test_misc_func_03);
  CPPUNIT_TEST(test_misc_func_04);
  CPPUNIT_TEST(test_misc_func_05);
  CPPUNIT_TEST(test_misc_func_06);
  CPPUNIT_TEST(test_misc_func_07);
  CPPUNIT_TEST(test_misc_func_08);
  CPPUNIT_TEST(test_misc_func_09);
  CPPUNIT_TEST(test_misc_func_10);
  CPPUNIT_TEST(test_misc_func_11);
  CPPUNIT_TEST(test_misc_func_12);
  CPPUNIT_TEST(test_misc_func_13);
  CPPUNIT_TEST(test_misc_func_14);
  CPPUNIT_TEST(test_misc_func_15);
  CPPUNIT_TEST(test_misc_func_16);
  CPPUNIT_TEST(test_misc_func_17);
  CPPUNIT_TEST(test_misc_func_18);
  CPPUNIT_TEST(test_misc_func_19);
  CPPUNIT_TEST(test_misc_func_20);
  CPPUNIT_TEST(test_misc_func_21);
  CPPUNIT_TEST(test_misc_func_22);
  CPPUNIT_TEST(test_misc_func_23);
  CPPUNIT_TEST(test_misc_func_24);
  CPPUNIT_TEST(test_misc_func_25);
  CPPUNIT_TEST(test_misc_func_26);
  CPPUNIT_TEST(test_misc_func_27);
  CPPUNIT_TEST(test_misc_func_28);
  CPPUNIT_TEST(test_misc_func_29);
  CPPUNIT_TEST(test_misc_func_30);
  CPPUNIT_TEST(test_misc_func_31);
  CPPUNIT_TEST(test_misc_func_32);
  CPPUNIT_TEST(test_misc_func_33);
  CPPUNIT_TEST(test_misc_func_35);
#ifdef HAVE_WINDOWED_MODE
  CPPUNIT_TEST(test_misc_func_34);
  CPPUNIT_TEST(test_misc_func_36);
#endif

  CPPUNIT_TEST_SUITE_END();

public:
  TestMiscFunc() : TestMiscFuncSuit(MDBM_CREATE_V3) {}
};


CPPUNIT_TEST_SUITE_REGISTRATION(TestMiscFunc);

int TestMiscFuncSuit::_pageSize = 4096;
// MDBM_DBSIZE_MB enabling this flag for specifying Db size in Megabytes
int TestMiscFuncSuit::_initialDbSize = 4;
int TestMiscFuncSuit::_limitSize = 8192;
int TestMiscFuncSuit::_largeObjSize = 3075;
int TestMiscFuncSuit::_spillSize = 3072;
int TestMiscFuncSuit::_windowSize = 2*64*4096;
string TestMiscFuncSuit::_mdbmDir = "";
string TestMiscFuncSuit::_testDir = "";
int TestMiscFuncSuit::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|
  MDBM_PARTITIONED_LOCKS|MDBM_CREATE_V3|MDBM_LARGE_OBJECTS|MDBM_DBSIZE_MB;

static int sleepTime = 0;

void TestMiscFuncSuit::setUp() {
}

void TestMiscFuncSuit::tearDown() {
}


// This function is called to set the environment
void TestMiscFuncSuit::setEnv() {
  GetTmpDir(_testDir);
}

// Test case - To test mdbm_get_magic_number(). It returns magic number
// of given Mdbm
void TestMiscFuncSuit::test_misc_func_01() {
  const string mdbm_name  = _testDir + "/misc01.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store and fetch LO
  //CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT, _largeObjSize));
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  uint32_t magic_no;
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_magic_number(mdbm, &magic_no));

  datum value,k;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  mdbm_truncate(mdbm);
  // Fetched the value for stored key
  value = mdbm_fetch(mdbm, k);

  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_magic_number(mdbm, &magic_no));

  MDBM *mdbm1 = NULL;
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_magic_number(mdbm1, &magic_no));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_set_alignment() and mdbm_get_alignment().
void TestMiscFuncSuit::test_misc_func_02() {
  const string mdbm_name  = _testDir + "/misc02.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Check for default alignment - MDBM_ALIGN_8_BITS
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_alignment(mdbm));

  // Set alignment = MDBM_ALIGN_16_BITS
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_alignment(mdbm, 1));
  CPPUNIT_ASSERT_EQUAL(1, mdbm_get_alignment(mdbm));

  // Set alignment = MDBM_ALIGN_32_BITS
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_alignment(mdbm, 3));
  CPPUNIT_ASSERT_EQUAL(3, mdbm_get_alignment(mdbm));

  // Set alignment = MDBM_ALIGN_64_BITS
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_alignment(mdbm, 7));
  CPPUNIT_ASSERT_EQUAL(7, mdbm_get_alignment(mdbm));

  // Store some data
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  // Set alignment = MDBM_ALIGN_32_BITS. This time it fails because
  // Mdbm is not empty
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_set_alignment(mdbm, 3));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_set_cachemode() and mdbm_get_cachemode().
void TestMiscFuncSuit::test_misc_func_03() {
  const string mdbm_name  = _testDir + "/misc03.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Check default cachemode : 0->MDBM_CACHEMODE_NONE
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_cachemode(mdbm));

  // Set cachemode : 1->MDBM_CACHEMODE_LFU
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, 1));
  CPPUNIT_ASSERT_EQUAL(1, mdbm_get_cachemode(mdbm));

  // Set cachemode : 1->MDBM_CACHEMODE_LRU
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, 2));
  CPPUNIT_ASSERT_EQUAL(2, mdbm_get_cachemode(mdbm));

  // Set cachemode : 1->MDBM_CACHEMODE_GDSF
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, 3));
  CPPUNIT_ASSERT_EQUAL(3, mdbm_get_cachemode(mdbm));

  // Store some data
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_get_hash_value(). It returns hash value
// of given key with a hash function
void TestMiscFuncSuit::test_misc_func_04() {
  const string mdbm_name  = _testDir + "/misc04.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store some data
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  datum k;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  uint32_t hash_val;
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_hash_value(k, MDBM_HASH_MD5, &hash_val));
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_hash_value(k, MDBM_HASH_SHA_1, &hash_val));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_get_partition_number(). It returns the partition no
// in which the given key lies. If the key is NULL or key is of zero size then
// it returns -1
void TestMiscFuncSuit::test_misc_func_05() {
  const string mdbm_name  = _testDir + "/misc05.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store some data
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  datum k;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  CPPUNIT_ASSERT(-1 != mdbm_get_partition_number(mdbm, k));

  k.dptr = (char*)"key1";
  k.dsize = 0;
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_partition_number(mdbm, k));

  k.dptr = NULL;
  k.dsize = 0;
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_partition_number(mdbm, k));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_get_errno()
void TestMiscFuncSuit::test_misc_func_06() {
  const string mdbm_name  = _testDir + "/misc06.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store some data
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  datum k, v;
  k.dptr = (char*)"key";
  k.dsize = 0;
  v.dptr = (char*)"val";
  v.dsize = strlen("val");
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_errno(mdbm));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_get_page(), mdbm_dump_page(), mdbm_chk_page()
void TestMiscFuncSuit::test_misc_func_07() {
  const string mdbm_name  = _testDir + "/misc07.mdbm";
  const char* db_name = mdbm_name.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(db_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store some data
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));

  datum k, v;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = (char*)"value";
  v.dsize = strlen("value");
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

  int pg_no = mdbm_get_page(mdbm, &k);
  mdbm_dump_page(mdbm, pg_no);
  mdbm_chk_page(mdbm, pg_no);

  mdbm_close(mdbm);
}


// Test case - To test mdbm_replace_file(). Db1 has some data and D2 is
// empty. Replace Db2 with Db1 and verify that the data is still present
void TestMiscFuncSuit::test_misc_func_08() {
  const string old_file  = _testDir + "/old08.mdbm";
  const char* old_name = old_file.c_str();
  const string new_file  = _testDir + "/new08.mdbm";
  const char* new_name = new_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(old_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  datum k,v,value;
  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Fetch previously stored records
    value = mdbm_fetch(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  MDBM *mdbm1 = createMdbm(new_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm1 != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(new_name, old_name));

  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Fetch previously stored records
    value = mdbm_fetch(mdbm1, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  mdbm_close(mdbm);
  mdbm_close(mdbm1);
}


// Test case - To test mdbm_replace_file(). Db1 has some data (with LO) and D2 is
// empty. Replace Db2 with Db1 and verify that the data is still present
void TestMiscFuncSuit::test_misc_func_09() {
  const string old_file  = _testDir + "/old09.mdbm";
  const char* old_name = old_file.c_str();
  const string new_file  = _testDir + "/new09.mdbm";
  const char* new_name = new_file.c_str();
  char* large_obj = new char[_largeObjSize];

  // Create a Mdbm
  MDBM *mdbm = createMdbm(old_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  datum k,v,value;
  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

    // Fetch previously stored records
    value = mdbm_fetch(mdbm, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  for (int i = 0; i < _largeObjSize; i++) {
    large_obj[i] = 'a';
  }
  large_obj[_largeObjSize-1] = '\0';

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = strlen(v.dptr);

  // Store a record
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

  MDBM *mdbm1 = createMdbm(new_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm1 != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(new_name, old_name));

  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Fetch previously stored records
    value = mdbm_fetch(mdbm1, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = strlen(v.dptr);

  // Fetch previously stored records
  value = mdbm_fetch(mdbm1, k);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));

  mdbm_close(mdbm);
  mdbm_close(mdbm1);
  delete[] large_obj;
}


// Test case - To test mdbm_replace_file(). Replace a Mdbm with some data
// with an empty Mdbm
void TestMiscFuncSuit::test_misc_func_10() {
  const string old_file  = _testDir + "/old10.mdbm";
  const char* old_name = old_file.c_str();
  const string new_file  = _testDir + "/new10.mdbm";
  const char* new_name = new_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(old_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Create one more Mdbm
  MDBM *mdbm1 = createMdbm(new_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm1 != NULL);

  datum k,v,value;
  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm1, k, v, MDBM_REPLACE));

    // Fetch previously stored records
    value = mdbm_fetch(mdbm1, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  // Replace mdbm1 (having some data) with mdbm (empty)
  CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(new_name, old_name));

  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Fetch previously stored records
    value = mdbm_fetch(mdbm1, k);
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
  }

  mdbm_close(mdbm);
  mdbm_close(mdbm1);
}


// Test case - To test mdbm_replace_file(). Replace an empty Mdbm
// with an empty Mdbm. Store some data in new Mdbm and verify that it is
// getting stored properly
void TestMiscFuncSuit::test_misc_func_11() {
  const string old_file  = _testDir + "/old11.mdbm";
  const char* old_name = old_file.c_str();
  const string new_file  = _testDir + "/new11.mdbm";
  const char* new_name = new_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(old_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Create one more Mdbm
  MDBM *mdbm1 = createMdbm(new_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm1 != NULL);

  // Replace mdbm1 (having some data) with mdbm (empty)
  CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(new_name, old_name));

  datum k,v,value;
  for (int i = 0; i  < 100; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm1, k, v, MDBM_REPLACE));

    // Fetch previously stored records
    value = mdbm_fetch(mdbm1, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  mdbm_close(mdbm);
  mdbm_close(mdbm1);
}

// Test case - To test mdbm_replace_file(). Replace a Mdbm with some data
// with another Mdbm (for oversized page)
//  Must use limit_size to make sure inserting the Dupes won't expand the DB too
//  much and cause 32bit mapping errors.  Then use store with MDBM_REPLACE to store
//  the key in the existing page but force the value into an oversized page.
void TestMiscFuncSuit::test_misc_func_12() {
  const string old_file  = _testDir + "/old12.mdbm";
  const char* old_name = old_file.c_str();
  const string new_file  = _testDir + "/new12.mdbm";
  const char* new_name = new_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(old_name, flags, 1024, 0, 768);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Create one more Mdbm
  MDBM *mdbm1 = createMdbm(new_name, flags, 1024, 0, 768);
  CPPUNIT_ASSERT(mdbm1 != NULL);

  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, 5, NULL, NULL));

  // Create key-value pair
  datum k,v,value;
  int i, j = 1;
  k.dptr = (char*)&j;
  k.dsize = sizeof(j);
  v.dptr = (char*)&j;
  v.dsize = sizeof(j);

  for (i = 0; i  < 60; i++) {
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Fetch previously stored records
    value = mdbm_fetch(mdbm, k);
    std::ostringstream oss;
    oss << "Failed on record " << i << " v.dsize= " << v.dsize;
    CPPUNIT_ASSERT_MESSAGE(oss.str(), 0 == memcmp(v.dptr, value.dptr, v.dsize));
  }

  int intarray[100];
  for (i = 0; i  < 100; i++) {
    intarray[i] = 1;
  }
  v.dptr = (char*)intarray;
  v.dsize = sizeof(intarray);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

  // Replace mdbm1 (having some data) with mdbm (empty)
  CPPUNIT_ASSERT_EQUAL(0, mdbm_replace_file(new_name, old_name));

  CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm1));

  for (i = 0; i  < 10; i++) {
    // Fetch previously stored records
    value = mdbm_fetch(mdbm1, k);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, sizeof(int)));
  }

  // Try a store - should work
  string s("key123");
  k.dptr = const_cast<char *>(s.c_str());
  k.dsize = s.size();
  string val("Value123");
  v.dptr = const_cast<char *>(val.c_str());
  v.dsize = val.size();
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm1, k, v, MDBM_REPLACE));

  CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm1));

  mdbm_close(mdbm);
  mdbm_close(mdbm1);
}


//  Create oversized page and delete some records from that.
void TestMiscFuncSuit::test_misc_func_13() {
  const string mdbm_file  = _testDir + "/test13.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, 1024, 0, 768);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, 5, NULL, NULL));

  // Create key-value pair
  datum k,v,value;
  int i, j = 1;
  k.dptr = (char*)(&j);
  k.dsize = sizeof(j);
  v.dptr = (char*)(&j);
  v.dsize = sizeof(j);

  for (i = 0; i  < 60; i++) {
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT_DUP));

    // Fetch previously stored records
    value = mdbm_fetch(mdbm, k);
    std::ostringstream oss;
    oss << "Failed on record " << i << " v.dsize= " << v.dsize;
    CPPUNIT_ASSERT_MESSAGE(oss.str(), 0 == memcmp(v.dptr, value.dptr, v.dsize));
  }

  int intarray[100];
  intarray[0] = 1;
  for (i = 0; i  < 100; i++) {
    intarray[i] = 1;
  }
  v.dptr = (char*)intarray;
  v.dsize = sizeof(intarray);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

  string s1("key1");
  k.dptr = const_cast<char *>(s1.c_str());
  k.dsize = s1.size();
  string val1("Value1");
  v.dptr = const_cast<char *>(val1.c_str());
  v.dsize = val1.size();
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

  // Lock Db
  CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

  for (i = 0; i  < 10; i++) {
    // Fetch previously stored records
    value = mdbm_fetch(mdbm, k);
    CPPUNIT_ASSERT(value.dptr != NULL);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, sizeof(int)));
  }

  CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, k));

  // Try a store - should work
  string s("key123");
  k.dptr = const_cast<char *>(s.c_str());
  k.dsize = s.size();
  string val("Value123");
  v.dptr = const_cast<char *>(val.c_str());
  v.dsize = val.size();
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

  // Unlock Db
  CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));

  mdbm_close(mdbm);
}

// Test case - Use cache mode = 1
void TestMiscFuncSuit::test_misc_func_14() {
  const string mdbm_file  = _testDir + "/test014.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, 1));

  for (int i = 0; i  < 4; i++) {
    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_close(mdbm);
}

// Test case - Use cache mode = 2
void TestMiscFuncSuit::test_misc_func_15() {
  const string mdbm_file  = _testDir + "/test015.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, 2));

  for (int i = 0; i  < 4; i++) {
    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_close(mdbm);
}

// Test case - Use cache mode = 3
void TestMiscFuncSuit::test_misc_func_16() {
  const string mdbm_file  = _testDir + "/test016.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, 3));

  for (int i = 0; i  < 4; i++) {
    // Store new records
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_close(mdbm);
}

// Test case - Mdbm is opened with an exclusive lock. Write using one process and try to write
// using other process. Second process should wait until process 1 finishes
// Test cases for write, update and fetch are covered in this scenario
void TestMiscFuncSuit::test_misc_func_17() {
  const string mdbm_name  = _testDir + "/test017.mdbm";
  const char* db_name = mdbm_name.c_str();
  datum k, v;
  int pid = -1, status = -1;

  // Create key-value pair
  k.dptr = (char*)"key";
  k.dsize = strlen(k.dptr);

  // Create a Mdbm
  MDBM *mdbm = NULL;
  CPPUNIT_ASSERT((mdbm = createMdbmNoLO(db_name, flags, _pageSize, _initialDbSize))
     != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, static_cast<mdbm_ubig_t>(_limitSize),
               NULL, NULL));

  // Fork main process to create a process
  pid = fork();
  CPPUNIT_ASSERT(pid >= 0);

  if (pid > 0) { // Parent process
    for (int i = 0; i < 1000; i++) {
      // Lock Mdbm
      CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));

      v.dptr = (char*)"value_p";
      v.dsize = strlen("value_p");

      // Store a record using parent process
      CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_REPLACE));

      // Sleep
      sleep(sleepTime);

      // Fetch the value for stored key
      datum value = mdbm_fetch(mdbm, k);

      // Verify that fetched value is same as stored value
      CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));

      // Unlock Mdbm
      CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));

      // Sleep
      sleep(sleepTime);
    }

    // Parent waits here for all children to exit
    while (true) {
      pid_t done = wait(&status);
      if (done == -1) {
        if (errno == ECHILD) break; // no more child processes
        } else {
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
          cerr << "pid " << done << " failed" << endl;
          exit(1);
        }
      }
    }
  } else { // (pid = 0)child process
    MDBM* mdbm1 = mdbm_dup_handle(mdbm, flags);
    for (int i = 0; i < 1000; i++) {
      // Lock Mdbm
      CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm1));

      v.dptr = (char*)"value_c";
      v.dsize = strlen("value_c");

      // Store a record using child process
      CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm1, k, v, MDBM_REPLACE));

      // Sleep
      sleep(sleepTime);

      // Fetch the value for stored key
      datum value = mdbm_fetch(mdbm1, k);

      // Verify that fetched value is same as stored value
      CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));

      // Unlock Mdbm
      CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm1));

      // Sleep
      sleep(sleepTime);
    }
    mdbm_close(mdbm);
    exit(EXIT_SUCCESS);
  }

  mdbm_close(mdbm);
}

// This function finds records which has substring (param) in their value
// and mark them for deletion
static int prunevals(MDBM *dbh, datum key, datum val, void *param) {
    const char *matcher = (const char*)param;
    string valtcH(val.dptr, val.dsize);
    if (valtcH.find(matcher) != string::npos) {
        return 1;
    }
    return 0;
}

// Test case - To test mdbm_prune()
void TestMiscFuncSuit::test_misc_func_18() {
  const string mdbm_file  = _testDir + "/test018.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  datum value,k;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_prune(mdbm, prunevals, const_cast<char*>("val"));

  // Fetched the value for stored key
  value = mdbm_fetch(mdbm, k);

  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  mdbm_close(mdbm);
}

// Test case - To test mdbm_prune()
void TestMiscFuncSuit::test_misc_func_19() {
  const string mdbm_file  = _testDir + "/test019.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store large object
  char* large_obj = new char[_largeObjSize];
  for (int i = 0; i < _largeObjSize; i++) {
    large_obj[i] = 'a';
  }
  large_obj[_largeObjSize-1] = '\0';

  datum value,k,v;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // Store a record using parent thread
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(mdbm, k, v, MDBM_INSERT));

  // Fetch the value for stored key
  value = mdbm_fetch(mdbm, k);

  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));

  mdbm_prune(mdbm, prunevals, const_cast<char*>("aa"));

  // Fetched the value for stored key
  value = mdbm_fetch(mdbm, k);

  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  mdbm_close(mdbm);
  delete[] large_obj;
}

// This function finds records which has substring (param) in their key
// and mark them for deletion
static int prunekeys(MDBM *dbh, datum key, datum val, void *param) {
    const char *matcher = (const char*)param;
    string k(key.dptr, key.dsize);
    if (k.find(matcher) != string::npos) {
        return 1;
    }
    return 0;
}

// Test case - To test mdbm_prune()
void TestMiscFuncSuit::test_misc_func_20() {
  const string mdbm_file  = _testDir + "/test020.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  datum value,k;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_prune(mdbm, prunekeys, const_cast<char*>("key"));

  // Fetched the value for stored key
  value = mdbm_fetch(mdbm, k);

  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  mdbm_close(mdbm);
}

// Test case - To test mdbm_purge()
void TestMiscFuncSuit::test_misc_func_21() {
  const string mdbm_file  = _testDir + "/test021.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = mdbm_open(mdbm_name, flags, 0600, 8192, 64);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, 2));

  datum value,k;
  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_stats t;
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &t, sizeof(t)));

  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_num_entries), 10);
  //CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_count), 2);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_size), 8192);

  CPPUNIT_ASSERT_EQUAL(2, mdbm_get_hash(mdbm));
  mdbm_purge(mdbm);
  // purge preserves the configuration of Mdbm - hash function
  CPPUNIT_ASSERT_EQUAL(2, mdbm_get_hash(mdbm));

  // Fetched the value for stored key
  value = mdbm_fetch(mdbm, k);
  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &t, sizeof(t)));

  // After purge, no of entries = 0
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_num_entries), 0);
  //CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_count), 2);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_size), 8192);

  mdbm_close(mdbm);
}

// Test case - To test mdbm_truncate()
void TestMiscFuncSuit::test_misc_func_22() {
  const string mdbm_file  = _testDir + "/test022.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = mdbm_open(mdbm_name, flags, 0600, 8192, 64);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_EJB));
  CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, 2));

  datum value,k,v;
  k.dptr = (char*)"key";
  k.dsize = strlen(k.dptr);
  v.dptr = (char*)"value";
  v.dsize = strlen(v.dptr);
  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_stats t;
  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &t, sizeof(t)));

  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_num_entries), 10);
  //CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_count), 2);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_size), 8192);

  CPPUNIT_ASSERT_EQUAL(2, mdbm_get_hash(mdbm));
  mdbm_truncate(mdbm);
  // truncate restores default hash function
  CPPUNIT_ASSERT_EQUAL(5, mdbm_get_hash(mdbm));

  // Fetched the value for stored key
  value = mdbm_fetch(mdbm, k);
  // Verify that fetched value is same as stored value
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &t, sizeof(t)));

  // After truncate, no of entries = 0
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_num_entries), 0);
  //CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_count), 2);
  CPPUNIT_ASSERT_EQUAL(static_cast<int>(t.s_page_size), 8192);

  mdbm_close(mdbm);
}

// Test case - Store and fetch some records from Memory only Mdbm
void TestMiscFuncSuit::test_misc_func_23() {
  //const string mdbm_file  = _testDir + "/test023.mdbm";
  //const string mdbm_file  = "";
  //const char* mdbm_name = mdbm_file.c_str();

  // mdbm_open fails if it is opned with MDBM_O_FSYNC flag
  int _flags = MDBM_CREATE_V3|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC;

  // Create a Mdbm
  MDBM *mdbm = mdbm_open(NULL, _flags, 0600, 4096, 4096*4+2000);

  // Pass NULL as Mdbm file name. This will create memory only Db.
  // Fails if initial Db size is power of 2
  CPPUNIT_ASSERT(mdbm != NULL);

  for (int i = 0; i < 10; i++) {
    //CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_close(mdbm);
}

// Test case - Store and fetch some records with LO from Memory only Mdbm
void TestMiscFuncSuit::test_misc_func_24() {
  // mdbm_open fails if it is opned with MDBM_O_FSYNC flag
  int flags = MDBM_CREATE_V3|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_LARGE_OBJECTS;

  // Pass NULL as Mdbm file name. This will create memory only Db.
  // Fails if initial Db size is power of 2
  MDBM *mdbm = mdbm_open(NULL, flags, 0600, 4096, 4096*4+2000);
  CPPUNIT_ASSERT(mdbm != NULL);

  for (int i = 0; i < 10; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  // Store and fetch LO
  CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));

  mdbm_close(mdbm);
}

// Test case - Create memory-only Mdbm. Store some records, delete then and verify that
// the records are getting deleted properly
void TestMiscFuncSuit::test_misc_func_25() {

  // mdbm_open fails if it is opned with MDBM_O_FSYNC flag
  int _flags = MDBM_CREATE_V3|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_LARGE_OBJECTS;

  // Pass NULL as Mdbm file name. This will create memory only Db.
  // Fails if initial Db size is power of 2
  MDBM *mdbm = mdbm_open(NULL, _flags, 0600, 4096, 4096*4+2000);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store some record and delete it. Verify that its getting deleted properly
  CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetch(mdbm, MDBM_INSERT));

  // Store some record with LO and delete it. Verify that its getting deleted properly
  CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));

  mdbm_close(mdbm);
}

// Test case - Create memory-only Mdbm. Store some records, update them and verify that
// the records are getting updated properly
void TestMiscFuncSuit::test_misc_func_26() {

  // mdbm_open fails if it is opned with MDBM_O_FSYNC flag
  int _flags = MDBM_CREATE_V3|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_LARGE_OBJECTS;

  // Pass NULL as Mdbm file name. This will create memory only Db.
  // Fails if initial Db size is power of 2
  MDBM *mdbm = mdbm_open(NULL, _flags, 0600, 4096, 4096*4+2000);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store some record and update it. Verify that its getting updated properly
  CPPUNIT_ASSERT_EQUAL(0, storeUpdateAndFetch(mdbm, MDBM_REPLACE));

  // Store some record with LO and update it. Verify that its getting updated properly
  CPPUNIT_ASSERT_EQUAL(0, storeDeleteAndFetchLO(mdbm, MDBM_REPLACE, _largeObjSize));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_get_filename() and mdbm_get_fd
void TestMiscFuncSuit::test_misc_func_27() {
  const string mdbm_file  = _testDir + "/test027.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbmNoLO(mdbm_name, flags, _pageSize, _initialDbSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Call mdbm_get_filename() and verify that it returns correct value
  const char* name =  mdbm_get_filename(mdbm);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(mdbm_name, name, strlen(name)));

  // Call mdbm_get_fd() and verify that the fd returned is not NULL
  CPPUNIT_ASSERT(mdbm_get_fd(mdbm));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_count_pages
void TestMiscFuncSuit::test_misc_func_28() {
  const string mdbm_file  = _testDir + "/test028.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbmNoLO(mdbm_name, flags, _pageSize, _initialDbSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Call mdbm_count_pages to count number of used pages
  CPPUNIT_ASSERT(mdbm_count_pages(mdbm) >= 0);
  //CPPUNIT_ASSERT_EQUAL(0, int(mdbm_count_pages(mdbm)));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_count_pages with some records in it
void TestMiscFuncSuit::test_misc_func_29() {
  const string mdbm_file  = _testDir + "/test029.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbmNoLO(mdbm_name, flags, 1024, _initialDbSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Call mdbm_count_pages to count number of used pages
  CPPUNIT_ASSERT(mdbm_count_pages(mdbm) >= 0);
  //CPPUNIT_ASSERT_EQUAL(0, int(mdbm_count_pages(mdbm)));

  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  CPPUNIT_ASSERT(mdbm_count_pages(mdbm) >= 0);
  //CPPUNIT_ASSERT_EQUAL(0, int(mdbm_count_pages(mdbm)));

  mdbm_close(mdbm);
}

// mdbm_clean_func()
static int myCleanFunc(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int* quit) {
  if (quit) {
    *quit = 0;
  }
  return 1;
}

// mdbm_clean_func()
static int myCleanFunc1(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int* quit) {
  if (quit) {
    *quit = 1;
  }
  return 1;
}

// Test case - To test mdbm_clean
void TestMiscFuncSuit::test_misc_func_30() {
  const string mdbm_file  = _testDir + "/test030.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbmNoLO(mdbm_name, flags, 1024, _initialDbSize);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

  mdbm_set_cachemode(mdbm, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

  for (int i = 0; i < 100; i++) {
     CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }


  mdbm_set_cleanfunc(mdbm, myCleanFunc, NULL);
  for (int j=0;j<=_limitSize;j++) {
     mdbm_clean(mdbm,j,0);
  }

  CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_clean
void TestMiscFuncSuit::test_misc_func_31() {
  const string mdbm_file  = _testDir + "/test031.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbmNoLO(mdbm_name, flags, 1024, _initialDbSize);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, _limitSize, NULL, NULL));

  mdbm_set_cachemode(mdbm, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

  for (int i = 0; i < 100; i++) {
     CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  mdbm_set_cleanfunc(mdbm, myCleanFunc1, NULL);
  for (int j=0;j<=_limitSize;j++) {
     mdbm_clean(mdbm,j,0);
  }

  CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_clean
void TestMiscFuncSuit::test_misc_func_32() {
  const string mdbm_file  = _testDir + "/test032.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = createMdbmNoLO(mdbm_name, flags, 1024, _initialDbSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  //uint32_t version;
  //CPPUNIT_ASSERT_EQUAL(0, get_version(mdbm_name, &version));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_preload
void TestMiscFuncSuit::test_misc_func_33() {
  const string mdbm_file  = _testDir + "/test033.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = NULL;
  // mdbm_preload() should return -1 since db is empty at this time
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_preload(mdbm));

  mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store few records into Mdbm
  for (int i = 0; i < 100; i++) {
     CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  // Store few large objects into Mdbm
  for (int i = 0; i < 5; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));
  }

  // Now, mdbm_preload() should return 0 upon success
  CPPUNIT_ASSERT_EQUAL(0, mdbm_preload(mdbm));

  mdbm_close(mdbm);
}

// Test case - To test mdbm_preload with Windowed mdbm
void TestMiscFuncSuit::test_misc_func_34() {
  SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
  const string bs_name  = _testDir + "/bs034.mdbm";
  const char* bs_mdbm = bs_name.c_str();
  const string cache_name  = _testDir + "/cache034.mdbm";
  const char* cache_mdbm = cache_name.c_str();

  // Create catch Mdbm
  MDBM *cache_db = createCacheMdbm(cache_mdbm, flags, _pageSize, _initialDbSize, _limitSize);
  CPPUNIT_ASSERT(cache_db != NULL);

  // Create another Mdbm as backing store
  int bsFlags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_O_RDWR|MDBM_PARTITIONED_LOCKS
        |MDBM_CREATE_V3|MDBM_LARGE_OBJECTS|MDBM_DBSIZE_MB|MDBM_OPEN_WINDOWED;
  MDBM *bs_db = createBSMdbmNoLO(bs_mdbm, bsFlags, _pageSize, _initialDbSize,
        _limitSize, _windowSize);
  CPPUNIT_ASSERT(bs_db != NULL);

  // mdbm_preload() should return -1 since db is Windowed
  // It's returning 0. Need to check with developers
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_preload(bs_db));

  // Link cache and backing store
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_backingstore(cache_db, MDBM_BSOPS_MDBM, bs_db, 0));

  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(cache_db, MDBM_INSERT_DUP));

  mdbm_close(cache_db);
}

// Test case - To test mdbm_lock_pages
void TestMiscFuncSuit::test_misc_func_35() {
  const string mdbm_file  = _testDir + "/test035.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = NULL;
  // mdbm_lock_pages() should return -1 since db is empty at this time
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_lock_pages(mdbm));

  mdbm = createMdbm(mdbm_name, flags, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);

  // Store few records into Mdbm
  for (int i = 0; i < 100; i++) {
     CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  // Lock pages of Mdbm in memory
  CPPUNIT_ASSERT_EQUAL(0, mdbm_lock_pages(mdbm));

  // Store few large objects into Mdbm
  for (int i = 0; i < 5; i++) {
    CPPUNIT_ASSERT_EQUAL(0, storeAndFetchLO(mdbm, MDBM_INSERT_DUP, _largeObjSize));
  }

  // Lock pages of Mdbm in memory
  CPPUNIT_ASSERT_EQUAL(0, mdbm_unlock_pages(mdbm));

  CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  mdbm_close(mdbm);
}

// Test case - To test mdbm_lock_pages
void TestMiscFuncSuit::test_misc_func_36() {
  SKIP_IF_VALGRIND() // Valgrind doesn't support remap_file_pages()
  const string mdbm_file  = _testDir + "/test036.mdbm";
  const char* mdbm_name = mdbm_file.c_str();

  // Create a Mdbm
  MDBM *mdbm = NULL;
  // mdbm_lock_pages() should return -1 since db is empty at this time
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_lock_pages(mdbm));

  mdbm = createMdbm(mdbm_name, flags|MDBM_OPEN_WINDOWED, _pageSize, _initialDbSize, _spillSize);
  CPPUNIT_ASSERT(mdbm != NULL);
  CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, static_cast<size_t>(_windowSize)));

  // Store few records into Mdbm
  for (int i = 0; i < 100; i++) {
     CPPUNIT_ASSERT_EQUAL(0, storeAndFetch(mdbm, MDBM_INSERT_DUP));
  }

  // Lock pages of Mdbm in memory. It returns -1 for windowed Db
  CPPUNIT_ASSERT_EQUAL(-1, mdbm_lock_pages(mdbm));

  mdbm_close(mdbm);
}

