/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>

#include <stdlib.h>
#include <string.h>
#include <string>

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mdbm_handle_pool.h"

/**
 * Tests Basic functionality of the ycsify library
 */
class YccmpTest: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(YccmpTest);
   
#ifdef HAVE_PROC_FILESYS
  CPPUNIT_TEST(test_mdbm_pool_parse);
  CPPUNIT_TEST(test_mdbm_pool_verify);
  CPPUNIT_TEST(test_mdbm_pool_pool_valid);
  CPPUNIT_TEST(test_mdbm_pool_pool_invalid);
#endif

  CPPUNIT_TEST_SUITE_END();

public:                                                                         
  void setUp (void) ;
  void tearDown (void) ;

protected:

  void test_mdbm_pool_parse();
  void test_mdbm_pool_verify();
  void test_mdbm_pool_pool_valid();
  void test_mdbm_pool_pool_invalid();
};

const char *mdbm_name= "/tmp/test_handle_pool.mdbm";

/** Per-test setup.  Invoked before each test*() method below. */
void YccmpTest::setUp() {
  MDBM *db = mdbm_open(mdbm_name, MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC, 0644, 4096, 4096*10);
  CPPUNIT_ASSERT(db != NULL);
  //Add data (key-value pair) to Mdbm
  datum dk, dv;
  dk.dptr = (char*)"key";
  dk.dsize = strlen(dk.dptr);
  dv.dptr = (char*)"value";
  dv.dsize = strlen(dv.dptr);
  int ret = mdbm_store(db, dk, dv, MDBM_INSERT);
  CPPUNIT_ASSERT(ret == 0);
  mdbm_close(db);
}

/** Per-test teardown.  Invoked after each test*() method below. */
void YccmpTest::tearDown() {
}

void YccmpTest::test_mdbm_pool_parse() {
  int size;
  const char *size_str;

  size_str = "14";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 14);

  size_str = "14,25";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 25);

  size_str = "13,yjava=8";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 13);

  size_str = "yjava=16,23";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 23);
  
  //size_str = "CppUnitTestRunner=15,25";
  size_str = "test_ccmp=15,25";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 15);
  
  size_str = "14,test_ccmp=22";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 22);

  size_str = "yjava_daemon=22,test_ccmp=15,25";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 15);

  size_str = "yjava_daemon=22,test_ccmp=17";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 17);

  size_str = "yjava_daemon=22,yjava=17";
  size = mdbm_pool_parse_pool_size(size_str);
  CPPUNIT_ASSERT(size == 0);
}

void YccmpTest::test_mdbm_pool_verify() {

  int min_val;
  int values[4];

  struct rlimit open_files_limit = {0,0};
  struct rlimit processes_or_threads_limit = {0,0};

  if (getrlimit(RLIMIT_NOFILE, &open_files_limit)) {
    fprintf(stderr, "getrlimit(RLIMIT_NOFILE) failed, skipping test!\n");
    return;
  }
  if (getrlimit(RLIMIT_NPROC, &processes_or_threads_limit)) {
    fprintf(stderr, "getrlimit(RLIMIT_NPROC) failed, skipping test!\n");
    return;
  }

  values[0] = 2;
  values[1] = -1;
  values[2] = open_files_limit.rlim_cur * 2;
  values[3] = processes_or_threads_limit.rlim_cur * 2;

  mdbm_pool_verify_pool_size(values, 4);
  
  CPPUNIT_ASSERT(values[0] == 2);
  CPPUNIT_ASSERT(values[1] == 0);

  min_val = open_files_limit.rlim_cur / 2;
  if ((int) (processes_or_threads_limit.rlim_cur * 3 / 4) < min_val) {
    min_val = processes_or_threads_limit.rlim_cur * 3 / 4;
  }

  fprintf(stderr, "values [%d, %d, %d, %d] min_val:%d\n", 
      values[0], values[1], values[2], values[3], min_val);

  CPPUNIT_ASSERT(values[2] > 0);
  CPPUNIT_ASSERT(values[2] <= min_val);
  CPPUNIT_ASSERT(values[3] > 0);  
  CPPUNIT_ASSERT(values[3] <= min_val);  
}

void YccmpTest::test_mdbm_pool_pool_valid() {

  MDBM *main_handle = mdbm_open(mdbm_name, MDBM_O_FSYNC, O_RDONLY, 4096, 0);
  
  mdbm_pool_t *pool = mdbm_pool_create_pool(main_handle, 8);
  CPPUNIT_ASSERT(pool != NULL);

  MDBM *mdbm_handle = mdbm_pool_acquire_handle(pool);
  CPPUNIT_ASSERT(mdbm_handle != NULL);

  int ret = mdbm_pool_release_handle(pool, mdbm_handle);
  CPPUNIT_ASSERT(ret == 1);

  mdbm_handle = mdbm_pool_acquire_excl_handle(pool);
  CPPUNIT_ASSERT(mdbm_handle != NULL);
  CPPUNIT_ASSERT(mdbm_handle == main_handle);

  ret = mdbm_pool_release_excl_handle(pool, mdbm_handle);
  CPPUNIT_ASSERT(ret == 1);

  ret = mdbm_pool_destroy_pool(pool);
  CPPUNIT_ASSERT(ret == 1);
  
  mdbm_close(main_handle);
}

void YccmpTest::test_mdbm_pool_pool_invalid() {

  MDBM *main_handle = mdbm_open(mdbm_name, MDBM_O_FSYNC, O_RDONLY, 4096, 0);
  
  mdbm_pool_t *pool = mdbm_pool_create_pool(main_handle, 0);
  CPPUNIT_ASSERT(pool == NULL);

  pool = mdbm_pool_create_pool(main_handle, -1);
  CPPUNIT_ASSERT(pool == NULL);

  pool = mdbm_pool_create_pool(NULL, 8);
  CPPUNIT_ASSERT(pool == NULL);

  MDBM *mdbm_handle = mdbm_pool_acquire_handle(NULL);
  CPPUNIT_ASSERT(mdbm_handle == NULL);

  int ret = mdbm_pool_release_handle(NULL, NULL);
  CPPUNIT_ASSERT(ret == 0);

  mdbm_handle = mdbm_pool_acquire_excl_handle(NULL);
  CPPUNIT_ASSERT(mdbm_handle == NULL);

  ret = mdbm_pool_release_excl_handle(NULL, NULL);
  CPPUNIT_ASSERT(ret == 0);

  ret = mdbm_pool_destroy_pool(NULL);
  CPPUNIT_ASSERT(ret == 0);

  mdbm_close(main_handle);
}

CPPUNIT_TEST_SUITE_REGISTRATION(YccmpTest);

