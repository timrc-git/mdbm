/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//    File: test-copy-func.cc
//    Purpose: Functional tests for copy function

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
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

class TestCopySuite : public CppUnit::TestFixture, public TestBase {
public:
  TestCopySuite(int vFlag) : TestBase(vFlag,"TestCopySuite") {}

  void setUp();
  void tearDown();
  void setEnv();

  void test_copy_01();
  void test_copy_02();
  void test_copy_03();
  void test_copy_04();
  void test_copy_05();
  void test_copy_06();
  void test_copy_07();

private:
  static int _pageSize;
  static int _initialDbSize;
  static int _limitSize;
  static int _largeObjSize;
  static int _spillSize;
  static string _testDir;
  static int flags;
  static int flagsLO;
};

class TestCopyFunc : public TestCopySuite{
  CPPUNIT_TEST_SUITE(TestCopyFunc);

  CPPUNIT_TEST(setEnv);

  CPPUNIT_TEST(test_copy_01);
  CPPUNIT_TEST(test_copy_02);
  CPPUNIT_TEST(test_copy_03);
  CPPUNIT_TEST(test_copy_04);
  CPPUNIT_TEST(test_copy_05);
  CPPUNIT_TEST(test_copy_06);
  CPPUNIT_TEST(test_copy_07);

  CPPUNIT_TEST_SUITE_END();

public:
  TestCopyFunc() : TestCopySuite(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestCopyFunc);

int TestCopySuite::_pageSize = 4096;
int TestCopySuite::_initialDbSize = 4096*128;
int TestCopySuite::_limitSize = 256;
int TestCopySuite::_largeObjSize = 4097;
int TestCopySuite::_spillSize = 3072;
string TestCopySuite::_testDir = "";
int TestCopySuite::flags = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_CREATE_V3|MDBM_DBSIZE_MB;
int TestCopySuite::flagsLO = MDBM_O_CREAT|MDBM_O_FSYNC|MDBM_CREATE_V3
  |MDBM_LARGE_OBJECTS|MDBM_DBSIZE_MB;

void
TestCopySuite::setUp() {
}

void
TestCopySuite::tearDown() {
}

void
TestCopySuite::setEnv() {
  GetTmpDir(_testDir);
}

// Test case - Create a Mdbm (db1). Store some key-value pairs and fetch
// them. Create new Mdbm (db2). Copy data from db1 to db2
// and verify that all data exist after copy
void TestCopySuite::test_copy_01() {
  const string mdbm_name1  = _testDir + "/src01.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest01.mdbm";
  const char* db_name2 = mdbm_name2.c_str();

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = mdbm_open(db_name1,MDBM_O_RDWR|flags,0666,0,0)) != NULL);

  datum k, v, value;
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store a record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT_DUP));

    // fetch the record
    value = mdbm_fetch(db, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  int f;
  //fprintf(stderr, "@@@ opening %s @@@\n", db_name2);
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR|O_CREAT,0666)) >= 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  // Open newly copied Mdbm in RDONLY mode
  MDBM *db2 = NULL;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDONLY|flags,0,0,0)) != NULL);

  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // fetch the record
    value = mdbm_fetch(db2, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  mdbm_close(db);
  mdbm_close(db2);
}

// Test case - Create a Mdbm (db1) with LO support. Store some key-value
// pairs along with large obect and fetch them. Create a new Mdbm (db2)
// with LO support. Copy data from db1 to db2 and verify that all data
// exist after copy
void TestCopySuite::test_copy_02() {
  const string mdbm_name1  = _testDir + "/src02.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest02.mdbm";
  const char* db_name2 = mdbm_name2.c_str();
  char* large_obj = new char[_largeObjSize];

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = mdbm_open(db_name1,MDBM_O_RDWR|flagsLO,0666,0,0)) != NULL);

  datum k, v, value;
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store a record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT_DUP));

    // fetch the record
    value = mdbm_fetch(db, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  for (int i = 0; i < _largeObjSize; i++) {
    large_obj[i] = 'a';
  }
  large_obj[_largeObjSize-1] = '\0';

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // Store a record
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT));

  int f;
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR|O_CREAT,0666)) > 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  // Open newly copied Mdbm in RDONLY mode
  MDBM *db2 = NULL;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDONLY|flagsLO,0,0,0)) != NULL);

  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // fetch the record
    value = mdbm_fetch(db2, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // fetch the record
  value = mdbm_fetch(db2, k);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));

  mdbm_close(db);
  mdbm_close(db2);
  delete[] large_obj;
}


// Test case - Create a Mdbm (db1) with LO support. Store some key-value
// pairs and fetch them. Create a new Mdbm (db2) with LO support and store some
// data with large obects in it. Copy data from db1 to db2 and verify that only
// new data (data in db1) exist after copy
void TestCopySuite::test_copy_03() {
  const string mdbm_name1  = _testDir + "/src03.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest03.mdbm";
  const char* db_name2 = mdbm_name2.c_str();
  char* large_obj = new char[_largeObjSize];

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = mdbm_open(db_name1,MDBM_O_RDWR|flags,0666,0,0)) != NULL);

  datum k, v, value;
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store a record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT_DUP));

    // fetch the record
    value = mdbm_fetch(db, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  // Open newly copied Mdbm in RDONLY mode
  MDBM *db2 = NULL;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDWR|flagsLO,0666,0,0)) != NULL);

  for (int i = 0; i < _largeObjSize; i++) {
    large_obj[i] = 'a';
  }
  large_obj[_largeObjSize-1] = '\0';

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // Store a record
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db2, k, v, MDBM_INSERT));

  int f;
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR|O_CREAT,0666)) > 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  mdbm_close(db2);
  db2=NULL;
  // Open newly copied Mdbm in RDONLY mode
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDONLY|flagsLO,0,0,0)) != NULL);

  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // fetch the record
    value = mdbm_fetch(db2, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // fetch the record
  value = mdbm_fetch(db2, k);
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  mdbm_close(db);
  mdbm_close(db2);
  delete[] large_obj;
}

// Test case - Create a Mdbm (db1). Store some key-value pairs and fetch
// them. Create new Mdbm (db2). Copy the data from db1 to db2 and verify
// that all data exist after copy. Use MDBM_COPY_LOCK_ALL flag while copy
void TestCopySuite::test_copy_04() {
  const string mdbm_name1  = _testDir + "/src04.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest04.mdbm";
  const char* db_name2 = mdbm_name2.c_str();
  char* large_obj = new char[_largeObjSize];

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = mdbm_open(db_name1,MDBM_O_RDWR|flagsLO,0666,0,0)) != NULL);

  datum k, v, value;
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store a record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT_DUP));

    // fetch the record
    value = mdbm_fetch(db, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  for (int i = 0; i < _largeObjSize; i++) {
    large_obj[i] = 'a';
  }
  large_obj[_largeObjSize-1] = '\0';

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // Store a record
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_INSERT));

  int f;
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR|O_CREAT,0666)) > 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  MDBM* db2;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDONLY|flagsLO,0,0,0)) != NULL);

  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // fetch the record
    value = mdbm_fetch(db2, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // fetch the record
  value = mdbm_fetch(db2, k);
  CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, value.dsize));

  mdbm_close(db);
  mdbm_close(db2);
  delete[] large_obj;
}


// Test case - Create a Mdbm (db1). Create new Mdbm (db2) and store some
// data into this Mdbm. Copy the data from db1 (empty Mdbm) to
// db2. Verify that the old data exist in db2
void TestCopySuite::test_copy_05() {
  const string mdbm_name1  = _testDir + "/src05.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest05.mdbm";
  const char* db_name2 = mdbm_name2.c_str();
  char* large_obj = new char[_largeObjSize];

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = mdbm_open(db_name1,MDBM_O_RDWR|flagsLO,0666,0,0)) != NULL);

  // Open newly copied Mdbm in RDONLY mode
  MDBM *db2 = NULL;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDWR|flagsLO,0666,0,0)) != NULL);

  datum k, v, value;
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store a record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db2, k, v, MDBM_INSERT_DUP));

    // fetch the record
    value = mdbm_fetch(db2, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  for (int i = 0; i < _largeObjSize; i++) {
    large_obj[i] = 'a';
  }
  large_obj[_largeObjSize-1] = '\0';

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // Store a record
  CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db2, k, v, MDBM_INSERT));

  mdbm_close(db2);

  int f = 0;
  errno = 0;
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR,0666)) > 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  // Open newly copied Mdbm in RDONLY mode
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDONLY|flagsLO,0666,0,0)) != NULL);

  // Verify that no data exist in this Mdbm
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // fetch the record
    value = mdbm_fetch(db2, k);
    // Value size = 0
    CPPUNIT_ASSERT_EQUAL(0, value.dsize);
  }

  k.dptr = (char*)"key";
  k.dsize = strlen("key");
  v.dptr = large_obj;
  v.dsize = _largeObjSize;

  // fetch the record
  value = mdbm_fetch(db2, k);
  // Value size = 0
  CPPUNIT_ASSERT_EQUAL(0, value.dsize);

  mdbm_close(db);
  mdbm_close(db2);
  delete[] large_obj;
}

// Test case - Create a Mdbm (db1). Create new Mdbm (db2). Copy the data
// from db1 (empty Mdbm) to db2 (empty Mdbm). Store some data in Db2 after
// copy Verify that the data is getting stored in db2
void TestCopySuite::test_copy_06() {
  const string mdbm_name1  = _testDir + "/src06.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest06.mdbm";
  const char* db_name2 = mdbm_name2.c_str();

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = mdbm_open(db_name1,MDBM_O_RDWR|flagsLO,0666,0,0)) != NULL);

  int f;
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR|O_CREAT,0666)) > 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  MDBM *db2;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDWR|flagsLO,0,0,0)) != NULL);

  datum k, v, value;
  for (int i = 0; i < 15; i++) {
    // Create key-value pair
    k.dptr = reinterpret_cast<char*>(&i);
    k.dsize = sizeof(i);
    v.dptr = reinterpret_cast<char*>(&i);
    v.dsize = sizeof(i);

    // Store records
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db2, k, v, MDBM_INSERT_DUP));

    // fetch the record
    value = mdbm_fetch(db2, k);
    CPPUNIT_ASSERT_EQUAL(0, memcmp(v.dptr, value.dptr, v.dsize));
  }

  mdbm_close(db);
  mdbm_close(db2);
}


// Test case - Create a Mdbm (db1) with oversized pages. Create new fd. Copy the data
// from db1 (empty Mdbm) to fd (empty Mdbm). mdbm_open the "fd" and verify data is there.
void TestCopySuite::test_copy_07() {
  const string mdbm_name1  = _testDir + "/src07.mdbm";
  const char* db_name1 = mdbm_name1.c_str();
  const string mdbm_name2  = _testDir + "/dest07.mdbm";
  const char* db_name2 = mdbm_name2.c_str();

  // Create a Mdbm
  MDBM *db = NULL;
  CPPUNIT_ASSERT((db = createMdbm(db_name1,MDBM_O_RDWR|flagsLO,1024,0, 768)) != NULL);

  CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(db, 10, NULL, NULL));

  datum k, v, value;
  int j = 1, temp;
  // Create key-value pair
  k.dptr = reinterpret_cast<char*>(&j);
  k.dsize = sizeof(j);

  const int VAL_OFFSET = 64;
  const int MAX_LOOP = 60;
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

  int f;
  CPPUNIT_ASSERT((f = open(db_name2,O_RDWR|O_CREAT,0666)) > 0);
  CPPUNIT_ASSERT(mdbm_fcopy(db, f, 0) == 0);
  close(f);

  MDBM *db2;
  CPPUNIT_ASSERT((db2 = mdbm_open(db_name2,MDBM_O_RDWR|flagsLO,0666,1024,0)) != NULL);

  CPPUNIT_ASSERT_EQUAL(0, mdbm_check(db2, 3, 1));

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
  mdbm_close(db2);

  mdbm_close(db);
}

