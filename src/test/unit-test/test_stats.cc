/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of Statistics features

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <functional>
#include <numeric>
#include <set>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"
#include "mdbm_stats.h"
#include "TestBase.hh"

#define main mdbm_stat_main_wrapper
#define usage mdbm_stat_usage_wrapper
#include "../../tools/mdbm_stat.c"
#undef main
#undef usage


using namespace std;

#define KEY_PREFIX          "KPrefix"
#define STARTPOINT          30
#define KEY_COUNT_DEFAULT   40

typedef enum {
    DO_NULL_CALLBACK = 1,
    DO_COUNT_CALLBACK = 2,
    DO_TIMED_CALLBACK = 3
} CallbackType;

class MdbmUnitTestStats : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestStats(int vFlags, int tFlags) : TestBase(vFlags, "MdbmUnitTestStats")
        {
            testFlags = vFlags | tFlags;
        }
    virtual ~MdbmUnitTestStats() {}

    void setUp() {}       // Per-test initialization - applies to every test
    void tearDown() {}    // Per-test cleanup - applies to every test

    void initialSetup();

    void test_StatsAB1();
    void test_StatsAB2();
    void test_StatsAB3();
    void test_StatsAB4();
    void test_StatsAB6();
    void test_StatsAB7();

    void test_StatsAC1();
    void test_StatsAC2();
    void test_StatsAC3();
    void test_StatsAC4();
    void test_StatsAC6();

    void test_StatsAD1();
    void test_StatsAD2();

    void test_StatsAE1();
    void test_StatsAE2();
    void test_StatsAE3();

    void test_StatsCallback();

    void test_StatsMdbmCountRecords();
    void test_StatsMdbmCountPages();

    void test_StatsEnable();

    void test_UsedPages();
    void test_FreePages();

    void finalCleanup();

    // Helper methods

    int getmdbmFlags() { return (testFlags | MDBM_O_RDWR); }

  protected:

    // Helper methods
    MDBM *createTestFile(const string &prefix, bool addLargeObjs, int keyCount = KEY_COUNT_DEFAULT);
    string createEmptyMdbm(const string &prefix);
    int getMixedSize();
    MDBM *createMixedObjFile(const string &prefix,
                             int keyCount = KEY_COUNT_DEFAULT,
                             string *filename = NULL,
                             int statsFlags = (MDBM_STATS_BASIC | MDBM_STATS_TIMED) );
    void fetchMixedObjFile(MDBM *mdbm, int keyCount = KEY_COUNT_DEFAULT);
    void deleteEveryOther(MDBM *mdbm, const string &prefix, int keyCount = KEY_COUNT_DEFAULT);
    void checkCount(MDBM *mdbm, mdbm_stat_type type, int shouldEqual);
    void checkTime(MDBM *mdbm, mdbm_stat_type type, time_t shouldEqual);
    void checkReset(const char *filename);
    string getStatType(mdbm_stat_type type);
    virtual void checkMdbmStatInfo(MDBM *mdbm, mdbm_stats_t *refInfo, bool empty = false);
    static int freePageCounter(void *user, const mdbm_chunk_info_t *info);
    uint32_t getUsedPages(MDBM *db);
    void storeSequential(MDBM *db, int startCnt, int endCnt, uint32_t valSize);
    void fetchSequential(MDBM *db, int startCnt, int endCnt, uint32_t valSize);
    void storeIncremental(MDBM *db, int startCnt, int endCnt, uint32_t maxValSize);
    void redirectStdout(const string &toFilename);

  private:

    int testFlags;
};

void
MdbmUnitTestStats::initialSetup()
{
    srandom(751);  // arbitrary prime
}

void
MdbmUnitTestStats::test_StatsAB1()
{
    string prefix = string("StatAB1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT));

    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, KEY_COUNT_DEFAULT);  // Check before fetches
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 0);

    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // Fetch
    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, KEY_COUNT_DEFAULT);
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, KEY_COUNT_DEFAULT);  // Check after fetches
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 0);
}

void
MdbmUnitTestStats::test_StatsAB2()
{
    string prefix = string("StatAB2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, NULL, MDBM_STATS_BASIC));

    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, KEY_COUNT_DEFAULT);
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 0);
}

void
MdbmUnitTestStats::test_StatsAB3()
{
    string prefix = string("StatAB3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, NULL, MDBM_STATS_BASIC));
    deleteEveryOther(mdbm, prefix);

    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, KEY_COUNT_DEFAULT);
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, KEY_COUNT_DEFAULT / 2 );
}

void
MdbmUnitTestStats::test_StatsAB4()
{
    string prefix = string("StatAB4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags()|MDBM_O_RDWR,
                                         0644, DEFAULT_PAGE_SIZE, 0));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_enable_stat_operations(mdbm, MDBM_STATS_BASIC));

    mdbm_counter_t count;
    CPPUNIT_ASSERT(mdbm_get_stat_counter(mdbm, (mdbm_stat_type) 80000, &count) != 0);
}

void
MdbmUnitTestStats::test_StatsAB6()
{
    string prefix = string("StatAB6") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, &fname));
    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, KEY_COUNT_DEFAULT);
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 0);
    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // Fetch
    mdbm.Close();   // Closing to test the persistence of stats data
    MdbmHolder db2(mdbm_open(fname.c_str(), MDBM_O_RDWR, 0444, 0, 0));
    CPPUNIT_ASSERT(NULL != (MDBM *)db2 );

    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(db2, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
    checkCount(db2, MDBM_STAT_TYPE_FETCH, KEY_COUNT_DEFAULT);
    checkCount(db2, MDBM_STAT_TYPE_STORE, KEY_COUNT_DEFAULT);
    checkCount(db2, MDBM_STAT_TYPE_DELETE, 0);
}

// Test for bug 5429189
void
MdbmUnitTestStats::test_StatsAB7()
{
    string prefix = string("StatAB7") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags(),
          0644, DEFAULT_PAGE_SIZE, 0, &fname));
    mdbm.Close();
    MdbmHolder db2(mdbm_open(fname.c_str(), MDBM_O_RDONLY, 0444, 0, 0));
    // Supposed to fail
    CPPUNIT_ASSERT_EQUAL(-1,mdbm_enable_stat_operations(db2, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
}

void
MdbmUnitTestStats::test_StatsAC1()
{
    string prefix = string("StatAC1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT));

    checkTime(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_STORE, time(0));  // Check before fetches
    checkTime(mdbm, MDBM_STAT_TYPE_DELETE, 0);

    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // Fetch
    checkTime(mdbm, MDBM_STAT_TYPE_FETCH, time(0));
    checkTime(mdbm, MDBM_STAT_TYPE_STORE, time(0));  // Check after fetches
    checkTime(mdbm, MDBM_STAT_TYPE_DELETE, 0);
}

void
MdbmUnitTestStats::test_StatsAC2()
{
    string prefix = string("StatAC2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, NULL, MDBM_STATS_TIMED));

    checkTime(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_STORE, time(0));
    checkTime(mdbm, MDBM_STAT_TYPE_DELETE, 0);
}

void
MdbmUnitTestStats::test_StatsAC3()
{
    string prefix = string("StatAC3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, NULL, MDBM_STATS_TIMED));
    deleteEveryOther(mdbm, prefix);

    checkTime(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_STORE, time(0));
    checkTime(mdbm, MDBM_STAT_TYPE_DELETE, time(0));
}

void
MdbmUnitTestStats::test_StatsAC4()
{
    string prefix = string("StatAC4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, getmdbmFlags() | MDBM_O_RDWR,
                                     0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(mdbm, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
    time_t timeStat;
    CPPUNIT_ASSERT(mdbm_get_stat_time(mdbm, (mdbm_stat_type) 70000, &timeStat) != 0);
}

void
MdbmUnitTestStats::test_StatsAC6()
{
    string prefix = string("StatAC6") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, &fname));
    checkTime(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_STORE, time(0));
    checkTime(mdbm, MDBM_STAT_TYPE_DELETE, 0);
    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // Fetch
    mdbm.Close();   // Closing to test the persistence of stats data
    MdbmHolder db2(mdbm_open(fname.c_str(), MDBM_O_RDWR, 0444, 0, 0));
    CPPUNIT_ASSERT(NULL != (MDBM *)db2 );
    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(db2, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
    checkTime(db2, MDBM_STAT_TYPE_FETCH, time(0));
    checkTime(db2, MDBM_STAT_TYPE_STORE, time(0));
    checkTime(db2, MDBM_STAT_TYPE_DELETE, 0);
}

void
MdbmUnitTestStats::test_StatsAD1()
{
    string prefix = string("StatAD1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fname;
    MdbmHolder mdbm( createMixedObjFile(prefix, KEY_COUNT_DEFAULT, &fname));  // perform stores
    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);             // perform fetches
    deleteEveryOther(mdbm, prefix);  // deletes
    mdbm.Close();    // Close and check reset
    checkReset(fname.c_str());   // check all counters are reset
}

void
MdbmUnitTestStats::test_StatsAD2()
{
    string prefix = string("StatAD2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string emptyFile( createEmptyMdbm(prefix) );
    checkReset(emptyFile.c_str());
}

void
MdbmUnitTestStats::test_StatsAE1()
{
    string prefix = string("StatAE1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    // File already created, just get stats
    MdbmHolder mdbm(createTestFile(prefix, true));  // keyCount=KEY_COUNT_DEFAULT
    mdbm_stats_t refInfo;
    memset(&refInfo, 0, sizeof(refInfo));
    refInfo.s_num_entries = KEY_COUNT_DEFAULT ;
    refInfo.s_large_num_entries = KEY_COUNT_DEFAULT / 2;
    refInfo.s_page_count = 5;
    refInfo.s_page_count = 128;
    refInfo.s_pages_used = 16;
    refInfo.s_bytes_used = 13840;
    refInfo.s_min_level = 2;
    refInfo.s_max_level = 7;
    refInfo.s_large_threshold = 384;
    refInfo.s_large_pages_used = refInfo.s_large_page_count = 40;
    refInfo.s_large_bytes_used = 10640;
    refInfo.s_large_min_size = refInfo.s_large_max_size = 532;
    checkMdbmStatInfo(mdbm, &refInfo);
}

void
MdbmUnitTestStats::test_StatsAE2()
{
    string prefix = string("StatAE2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix) );
    MdbmHolder mdbm(mdbm_open(fileNameEmpty.c_str(), MDBM_O_RDWR | getmdbmFlags(),
                              0644, DEFAULT_PAGE_SIZE, 0));
    mdbm_stats_t refInfo;
    memset(&refInfo, 0, sizeof(refInfo));
    refInfo.s_pages_used = refInfo.s_page_count = 1;
    checkMdbmStatInfo(mdbm, &refInfo, true);
}

void
MdbmUnitTestStats::test_StatsAE3()
{
    string prefix = string("StatAE3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string fileNameEmpty(createEmptyMdbm(prefix));
    MdbmHolder mdbm(mdbm_open(fileNameEmpty.c_str(), MDBM_O_RDWR | getmdbmFlags(),
                               0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT(NULL != (MDBM *)mdbm);
    mdbm_stats_t dbInfo;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_stats(mdbm, &dbInfo, (sizeof(dbInfo) / 2)));
}

class StatsCallbackMap {
public:
  StatsCallbackMap(MDBM* db, int flags) : expectedDb(db), cflags(flags) {
    mdbm_set_stats_func(db, flags, StatsCallbackMap::StatCB, this);
    deletes = 0;
    fetches = 0;
    stores = 0;
    err_deletes = 0;
    err_fetches = 0;
    err_stores = 0;
    cache_stores = 0;
    cache_evict = 0;
    fetch_uncached = 0;
    getpage = 0;
    getpage_uncached = 0;
  }
  static void StatCB(MDBM* db, int tag, int flags, uint64_t value, void* user) {
    CPPUNIT_ASSERT(user != NULL);
    StatsCallbackMap* scb = (StatsCallbackMap*)user;
    scb->StatsCb(db, tag, flags, value);
  }
  void StatsCb(MDBM* db, int tag, int flags, uint64_t value) {
    string label = mdbm_get_stat_name(tag);
    CPPUNIT_ASSERT(label != "unknown_tag");
    if (flags == MDBM_STAT_CB_ELAPSED) {
        value += 1; // ensure it's not zero for testing
    }
    //fprintf(stderr, "  stat[%s]=%llu flag:%d\n", label.c_str(), (unsigned long long)value, flags);
    if ((flags == MDBM_STAT_CB_SET) || (flags == MDBM_STAT_CB_TIME) || theMap.find(label) == theMap.end()) {
      theMap[label] = value;
    } else { // it's a delta, and already exists in the map
      theMap[label] += value;
    }
  }

  int GetStat(const char* key) {
    if (theMap.find(key) == theMap.end()) { // not present
      return 0;
    }
    return theMap[key];
  }
  void Validate(const char* step) {
    //fprintf(stderr, "StatsCallback test Validating step %s\n", step);
    CPPUNIT_ASSERT_EQUAL(GetStat("Delete")          , deletes);
    CPPUNIT_ASSERT_EQUAL(GetStat("Fetch")           , fetches);
    CPPUNIT_ASSERT_EQUAL(GetStat("Store")           , stores);
    CPPUNIT_ASSERT_EQUAL(GetStat("DeleteFailed")    , err_deletes);
    CPPUNIT_ASSERT_EQUAL(GetStat("FetchNotFound")   , err_fetches);
    CPPUNIT_ASSERT_EQUAL(GetStat("StoreError")      , err_stores);

    CPPUNIT_ASSERT_EQUAL(GetStat("CacheStore")      , cache_stores);
    CPPUNIT_ASSERT_EQUAL(GetStat("CacheEvict")      , cache_evict);
    CPPUNIT_ASSERT_EQUAL(GetStat("FetchUncached")   , fetch_uncached);
    CPPUNIT_ASSERT_EQUAL(GetStat("GetPage")         , getpage);
    CPPUNIT_ASSERT_EQUAL(GetStat("GetpageUncached"), getpage_uncached);

    if ((deletes) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("DeleteLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("DeleteLatency") == 0);
    }
    if ((fetches) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("FetchLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("FetchLatency") == 0);
    }
    if ((stores) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("StoreLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("StoreLatency") == 0);
    }
    if ((cache_stores) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("CacheStoreLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("CacheStoreLatency") == 0);
    }
    if ((cache_evict) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("CacheEvictLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("CacheEvictLatency") == 0);
    }
    if ((fetch_uncached) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("FetchUncachedLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("FetchUncachedLatency") == 0);
    }
    if ((getpage) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("GetPageLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("GetPageLatency") == 0);
    }
    if ((getpage_uncached) && (cflags == MDBM_STATS_TIMED)) {
        CPPUNIT_ASSERT(GetStat("GetPageUncachedLatency") > 0);
    } else {
        CPPUNIT_ASSERT(GetStat("GetPageUncachedLatency") == 0);
    }
  }
  void Update(const char* step) {
    deletes          = GetStat("Delete");
    fetches          = GetStat("Fetch");
    stores           = GetStat("Store") ;
    err_deletes      = GetStat("DeleteFailed");
    err_fetches      = GetStat("FetchNotFound");
    err_stores       = GetStat("StoreError") ;
    cache_stores     = GetStat("CacheStore");
    cache_evict      = GetStat("CacheEvict");
    fetch_uncached   = GetStat("FetchUncached");
    getpage          = GetStat("GetPage");
    getpage_uncached = GetStat("GetPageUncached");
  }


public:
  MDBM* expectedDb;
  int   cflags; // MDBM_STATS_BASIC, MDBM_STATS_TIMED
  map<string, int> theMap;

  int deletes;
  int fetches;
  int stores;
  int err_deletes;
  int err_fetches;
  int err_stores;
  int cache_stores;
  int cache_evict;
  int fetch_uncached;
  int getpage;
  int getpage_uncached;
};

// TODO TODO TODO
//  getpage
//    count
//    error
//    time
//  getpage_uncached
//    count
//    error
//    time


void
MdbmUnitTestStats::test_StatsCallback()
{
    int i, j, ret;
    datum key, val;

    for (i=0; i<2; ++i) {
        //
        // normal tests
        //
        MdbmHolder mdbm(EnsureTmpMdbm("stats_cb", MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|versionFlag, 0644, 128));
        CPPUNIT_ASSERT(NULL != (MDBM *)mdbm);
        StatsCallbackMap scb(mdbm, (i==0) ? MDBM_STATS_BASIC : MDBM_STATS_TIMED); // auto-registers
        scb.Validate("initial");
        CPPUNIT_ASSERT(ret = mdbm_init_rstats(mdbm, MDBM_RSTATS_THIST) !=  -1);
        string skey="key", sval="val";
        key.dsize = skey.size()+1;
        key.dptr = (char*)skey.c_str();
        val.dsize = sval.size()+1;
        val.dptr = (char*)sval.c_str();

        // normal store
        ret = mdbm_store(mdbm, key, val, MDBM_REPLACE);
        CPPUNIT_ASSERT(0 == ret);
        ++scb.stores;
        scb.Validate("normal_store");

        // normal fetch
        val = mdbm_fetch(mdbm, key);
        CPPUNIT_ASSERT(0 != val.dsize);
        CPPUNIT_ASSERT(0 != val.dptr);
        ++scb.fetches;
        scb.Validate("normal_fetch");

        // normal delete
        ret = mdbm_delete(mdbm, key);
        CPPUNIT_ASSERT(0 == ret);
        ++scb.deletes;
        scb.Validate("normal_delete");

        // invalid store
        skey = "key2";
        key.dsize = skey.size()+1;
        key.dptr = (char*)skey.c_str();
        ret = mdbm_store(mdbm, key, val, MDBM_MODIFY);
        CPPUNIT_ASSERT(0 != ret);
        ++scb.err_stores;
        scb.Validate("store_fail");

        // failing fetch
        val = mdbm_fetch(mdbm, key);
        CPPUNIT_ASSERT(0 == val.dsize);
        CPPUNIT_ASSERT(0 == val.dptr);
        ++scb.err_fetches;
        scb.Validate("fetch_fail");

        // failing delete
        ret = mdbm_delete(mdbm, key);
        CPPUNIT_ASSERT(0 != ret);
        ++scb.err_deletes;
        scb.Validate("delete_fail");

        //
        // cache tests
        //
        MdbmHolder cache(EnsureTmpMdbm("stats_cb_cache", MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|versionFlag, 0644, 128, 256));
        ret = mdbm_limit_size_v3(cache, 2, 0, 0); //limit to 2 pages
        CPPUNIT_ASSERT(0 == ret);
        ret = mdbm_set_cachemode(cache, MDBM_CACHEMODE_LRU);
        CPPUNIT_ASSERT(0 == ret);
        ret = mdbm_set_backingstore(cache, MDBM_BSOPS_MDBM, mdbm, 0);
        CPPUNIT_ASSERT(0 == ret);
        mdbm.Release(); // it's now owned/managed by the cache mdbm
        StatsCallbackMap scbCache(cache, (i==0) ? MDBM_STATS_BASIC : MDBM_STATS_TIMED);
        scbCache.Validate("initial");

        for (j=0; j<7; ++j) {
          // normal store cache (but fill page)
          //val.dsize = 100;
          skey = string("key")+(char)('0'+j);
          key.dsize = skey.size()+1;
          key.dptr = (char*)skey.c_str();
          sval = "................................................................"; // 64 bytes
          val.dsize = sval.size()+1;
          val.dptr = (char*)sval.c_str();
          ret = mdbm_store(cache, key, val, MDBM_INSERT);
          CPPUNIT_ASSERT(0 == ret);
          ++scb.stores;
          scb.Validate("cache_store (bs)");
          if (j<7) {
            ++scbCache.stores;
            ++scbCache.cache_stores;
          } else {
            ++scbCache.err_stores;
          }
          if (j>=2) {
            ++scbCache.cache_evict;
          }
          scbCache.Validate("cache_store (cache)");
        }
        // fetch the first key, which should have been evicted
        skey = string("key")+(char)('0');
        key.dsize = skey.size()+1;
        key.dptr = (char*)skey.c_str();
        sval = "................................................................"; // 64 bytes
        val.dsize = sval.size()+1;
        val.dptr = (char*)sval.c_str();
        val = mdbm_fetch(cache, key);
        CPPUNIT_ASSERT(val.dsize != 0);
        CPPUNIT_ASSERT(val.dptr != 0);
        ++scb.fetches;
        ++scbCache.fetches;
        ++scbCache.fetch_uncached;
        ++scbCache.cache_stores; // pulling back into the cache counts as a store...
        ++scbCache.cache_evict; // and evicts some other entry...
        scb.Validate("uncached_fetch (bs)");
        scbCache.Validate("uncached_fetch (cache)");

        //
        // getpage and getpage_uncached
        //
        if (TestBase::IsInValgrind()) {
          fprintf(stderr, "RUNNING UNDER VALGRIND. SKIPPING WINDOWED MODE TESTS...\n");
        } else if (!remap_is_limited(sysconf(_SC_PAGESIZE))) {
          MdbmHolder windowed(EnsureTmpMdbm("stats_cb_cache", MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|versionFlag|MDBM_OPEN_WINDOWED, 0644, 4096));
          ret = mdbm_set_window_size(windowed, 4096*10);
          CPPUNIT_ASSERT(ret == 0);
          StatsCallbackMap scbWind(windowed, (i==0) ? MDBM_STATS_BASIC : MDBM_STATS_TIMED);
          for (j=0; j<50; ++j) {
            // normal store windowed
            skey = string("key")+(char)('0'+j);
            key.dsize = skey.size()+1;
            key.dptr = (char*)skey.c_str();
            sval.assign(2048+64, '.'); // make a large-ish value
            val.dsize = sval.size()+1;
            val.dptr = (char*)sval.c_str();
            ret = mdbm_store(windowed, key, val, MDBM_INSERT);
            CPPUNIT_ASSERT(0 == ret);
          }
          for (j=0; j<7; ++j) {
            // normal fetch windowed
            skey = string("key")+(char)('0'+j);
            key.dsize = skey.size()+1;
            key.dptr = (char*)skey.c_str();
            val = mdbm_fetch(windowed, key);
            CPPUNIT_ASSERT(val.dsize != 0);
            CPPUNIT_ASSERT(val.dptr != 0);
          }
          CPPUNIT_ASSERT(scbWind.GetStat("GetPage") != 0);
          CPPUNIT_ASSERT(scbWind.GetStat("GetPageUncached") != 0);
          if (i==1) {
            CPPUNIT_ASSERT(scbWind.GetStat("GetPageLatency") > 0);
            CPPUNIT_ASSERT(scbWind.GetStat("GetPageUncachedLatency") > 0);
          } else {
            CPPUNIT_ASSERT(scbWind.GetStat("GetPageLatency") == 0);
            CPPUNIT_ASSERT(scbWind.GetStat("GetPageUncachedLatency") == 0);
          }
      }
    }

    //
    // page tests
    //
    MdbmHolder db2(EnsureTmpMdbm("stats_cb", MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|versionFlag, 0644, 128, 512));
    CPPUNIT_ASSERT(NULL != (MDBM *)db2);
    StatsCallbackMap scb2(db2, MDBM_STATS_BASIC); // auto-registers

    mdbm_fsync(db2);
    CPPUNIT_ASSERT(scb2.GetStat("MdbmSync") == 1);
    mdbm_sync(db2);
    CPPUNIT_ASSERT(scb2.GetStat("MdbmSync") == 2);

    string skey="key23", sval="val"; // key chosen to not hit page 0
    key.dsize = skey.size()+1;
    key.dptr = (char*)skey.c_str();
    val.dsize = sval.size()+1;
    val.dptr = (char*)sval.c_str();

    // normal store
    ret = mdbm_store(db2, key, val, MDBM_REPLACE);
    CPPUNIT_ASSERT(0 == ret);
    CPPUNIT_ASSERT(scb2.GetStat("PageStoreVal") != 0);

    // normal delete
    ret = mdbm_delete(db2, key);
    CPPUNIT_ASSERT(0 == ret);
    CPPUNIT_ASSERT(scb2.GetStat("PageDeleteVal") != 0);

}


void
MdbmUnitTestStats::test_StatsEnable()
{
    string prefix = string("StatsEnable") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    // Start with stats collection disabled
    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, NULL, 0));

    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // Fetch
    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, 0);    // Stats turned off, should be zero
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, 0);    // Stats turned off, should be zero
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 0);   // Stats turned off, should be zero

    CPPUNIT_ASSERT_EQUAL(0, mdbm_enable_stat_operations(mdbm, MDBM_STATS_BASIC));
    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT / 2 );
    // Stats turned on - fetches count
    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, KEY_COUNT_DEFAULT / 2);

    string key;
    datum ky;
    key = KEY_PREFIX + ToStr(STARTPOINT);
    ky.dptr = const_cast<char *> (key.c_str());
    ky.dsize = key.size() + 1;
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm, ky));  // Delete first key
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 1);   // One delete

    // Start with stats collection enabled
    MdbmHolder mdbm2 (createMixedObjFile(prefix, KEY_COUNT_DEFAULT*2, NULL, MDBM_STATS_TIMED));
    // Timing Stats collection enabled, time should be set for stores, but no deletes
    checkTime(mdbm2, MDBM_STAT_TYPE_STORE, time(0));
    checkTime(mdbm2, MDBM_STAT_TYPE_DELETE, 0);

    // Turn off stats, then delete.  Timestamp should be zero
    CPPUNIT_ASSERT_EQUAL(0, mdbm_enable_stat_operations(mdbm2, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(mdbm2, ky));
    checkTime(mdbm2, MDBM_STAT_TYPE_DELETE, 0);
}

void
MdbmUnitTestStats::test_UsedPages()
{
    string prefix = string("StatsUsedPages") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT*4, NULL, 0));
    /* NOTE: this is dependent on the random seed and test ordering */
    CPPUNIT_ASSERT_EQUAL(129U, getUsedPages(mdbm));
}

void
MdbmUnitTestStats::test_FreePages()
{
    string prefix = string("StatsFreePages") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT*3, NULL, 0));
    mdbm_db_info_t dbinfo;

    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_db_info(mdbm, &dbinfo));
    int freePages = dbinfo.db_num_pages - getUsedPages(mdbm);
    /* NOTE: this is dependent on the random seed and test ordering */
    CPPUNIT_ASSERT_EQUAL(936, freePages);
}

void
MdbmUnitTestStats::finalCleanup()
{
    TRACE_TEST_CASE(__func__);
    CleanupTmpDir();
    GetLogStream() << "Completed " << versionString << " Statistics Tests." << endl<<flush;
}

/// ------------------------------------------------------------------------------
///
/// Helper methods
///
/// ------------------------------------------------------------------------------

MDBM *
MdbmUnitTestStats::createTestFile(const string &prefix, bool addLargeObjs, int keyCount)
{
    errno = 0;
    MDBM *mdbm = EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_LARGE_OBJECTS | MDBM_O_CREAT,
                               0644, DEFAULT_PAGE_SIZE, 0);

    int ret = InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE/4, keyCount/2, true, 0);
    string msg = string("Failed to insert data, errno = ") + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(msg, 0 == ret);

    if (addLargeObjs) {  // Add large objects
        CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_PAGE_SIZE+20,
                                           keyCount/2, true, keyCount/2 ));
    }
    return mdbm;
}

string
MdbmUnitTestStats::createEmptyMdbm(const string &prefix)
{
    string fname;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix, getmdbmFlags() | MDBM_O_CREAT,
                                  0644, DEFAULT_PAGE_SIZE, 0, &fname));
    return fname;
}

int
MdbmUnitTestStats::getMixedSize()
{
    int value = (random() % (DEFAULT_PAGE_SIZE * 2)) - (DEFAULT_PAGE_SIZE * 3 / 4);

    if (value <= 0)
        return 1;
    return value;
}

MDBM *
MdbmUnitTestStats::createMixedObjFile(const string &prefix, int keyCount, string *filename,
                                      int statsFlags)
{
    int flags = getmdbmFlags() | MDBM_LARGE_OBJECTS | MDBM_O_CREAT;
    MDBM *mdbm = EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE, 0, filename);
    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(mdbm, statsFlags));
    string key;
    for (int i = 0; i < keyCount; ++i) {
        datum val;
        datum ky = CreateTestValue("",  getMixedSize(), val);  // return value "ky" unused
        ++val.dsize;  // store the null
        key = KEY_PREFIX + ToStr(i + STARTPOINT);
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        int ret = mdbm_store( mdbm, ky, val, MDBM_REPLACE);
        std::ostringstream oss;
        oss << "createMixedObjFile: Cannot add key"
            << ", key=" << key
            << ", flags="
#if __GNUC__ > 2
            << std::showbase
#endif
            << std::hex << flags << std::dec
            << ", errno=" + ToStr(errno);
        CPPUNIT_ASSERT_MESSAGE(oss.str(), ret == 0);
    }
    return mdbm;
}

void
MdbmUnitTestStats::fetchMixedObjFile(MDBM *mdbm, int keyCount)
{
    errno = 0;
    string key, msg;
    datum val, ky;
    for (int i = 0; i < keyCount; ++i) {
        key = KEY_PREFIX + ToStr(i + STARTPOINT);
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        val = mdbm_fetch(mdbm, ky);
        msg = string("fetchMixedObjFile: cannot find key ") + key;
        CPPUNIT_ASSERT_MESSAGE(msg, (val.dsize != 0) && (val.dptr != NULL));
    }
}

void
MdbmUnitTestStats::deleteEveryOther(MDBM *mdbm, const string &prefix, int keyCount)
{
    errno = 0;
    string key, msg;
    datum ky;
    for (int i = 0; i < keyCount; i += 2) {
        key = KEY_PREFIX + ToStr(i + STARTPOINT);
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        int ret = mdbm_delete(mdbm, ky);
        msg = string("deleteEveryOther: cannot find key ") + key;
        CPPUNIT_ASSERT_MESSAGE(msg, ret == 0);
    }
}

void
MdbmUnitTestStats::checkCount(MDBM *mdbm, mdbm_stat_type type, int shouldEqual)
{
    errno = 0;
    string typeStr(getStatType(type));
    mdbm_counter_t count;
    int ret = mdbm_get_stat_counter(mdbm, type, &count);
    string msg = string("Unable to collect count of") + typeStr;
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), ret == 0);
    msg +=  string(", errno= ") + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), errno == 0);
    int value = count;
    msg = string("Expecting ") + ToStr(shouldEqual) + typeStr + string(" but got ") + ToStr(value);
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), value == shouldEqual);
}

void
MdbmUnitTestStats::checkTime(MDBM *mdbm, mdbm_stat_type type, time_t shouldEqual)
{
    errno = 0;
    string typeStr(getStatType(type));
    time_t statTime;
    int ret = mdbm_get_stat_time(mdbm, type, &statTime);
    string msg = string("Unable to collect time stamp of") + typeStr;
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), ret == 0);
    msg +=  string(", errno= ") + ToStr(errno);
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), errno == 0);
    // Should be equal to this second or the previous second
//fprintf(stderr, "COMPARING STAT TIME %ld vs %ld\n", (long)statTime, (long)shouldEqual);
    CPPUNIT_ASSERT((statTime == shouldEqual) || ((statTime+1) == shouldEqual));
}

void
MdbmUnitTestStats::checkReset(const char *filename)
{
    MdbmHolder mdbm(mdbm_open(filename, MDBM_O_RDWR, 0444, 0, 0) );
    CPPUNIT_ASSERT_EQUAL(0,mdbm_enable_stat_operations(mdbm, MDBM_STATS_BASIC|MDBM_STATS_TIMED));
    mdbm_reset_stat_operations(mdbm);
    checkCount(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkCount(mdbm, MDBM_STAT_TYPE_STORE, 0);
    checkCount(mdbm, MDBM_STAT_TYPE_DELETE, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_FETCH, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_STORE, 0);
    checkTime(mdbm, MDBM_STAT_TYPE_DELETE, 0);
}

string
MdbmUnitTestStats::getStatType(mdbm_stat_type type)
{
    if (type == MDBM_STAT_TYPE_FETCH) {
        return std::string(" Fetches");
    } else if (type == MDBM_STAT_TYPE_DELETE) {
        return std::string(" Deletes");
    } else if (type == MDBM_STAT_TYPE_STORE) {
        return std::string( " Stores");
    }
    return std::string("");
}

int
MdbmUnitTestStats::freePageCounter(void *user, const mdbm_chunk_info_t *info)
{
    uint32_t *counter = (uint32_t *) user;

    switch (info->page_type) {
        case MDBM_PTYPE_DATA:
        case MDBM_PTYPE_DIR:
        case MDBM_PTYPE_LOB:
            *counter += info->num_pages;
            break;
    }
    return 0;
}


uint32_t
MdbmUnitTestStats::getUsedPages(MDBM *db)
{
    uint32_t usedPages = 0;

    CPPUNIT_ASSERT(0 >= mdbm_chunk_iterate(db, freePageCounter, 0, &usedPages));

    return usedPages;
}


void
MdbmUnitTestStats::checkMdbmStatInfo(MDBM *mdbm, mdbm_stats_t *refInfo, bool empty)
{
    cerr << "CheckMdbmStatInfo() produces version dependent results" << endl << flush;
}

void
MdbmUnitTestStats::test_StatsMdbmCountRecords()
{
    string prefix = string("test_StatsMdbmCountRecords");
    TRACE_TEST_CASE(__func__)

    uint64_t count = KEY_COUNT_DEFAULT * 3;
    MdbmHolder mdbm(createMixedObjFile(prefix, count));
    /* NOTE: this is dependent on the random seed and test ordering */
    CPPUNIT_ASSERT_EQUAL(count, mdbm_count_records(mdbm));
}

void
MdbmUnitTestStats::test_StatsMdbmCountPages()
{
    string prefix = string("test_StatsMdbmCountPages");
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT*2));
    /* NOTE: this is dependent on the random seed and test ordering */
    CPPUNIT_ASSERT_EQUAL(57U, mdbm_count_pages(mdbm));
}

// Convenient method for performance-tests MDBM that store data values from startCnt to endCnt
void
MdbmUnitTestStats::storeSequential(MDBM *db, int startCnt, int endCnt, uint32_t valSize)
{
    datum key, val;
    void *tmp = malloc(valSize);
    CPPUNIT_ASSERT(NULL != tmp);
    memset(tmp, '.', valSize);
    for (int cnt = startCnt; cnt < endCnt; ++cnt)
    {
        key.dptr  = (char*)(&cnt);
        key.dsize = sizeof(cnt);
        val.dptr  = (char *) tmp;
        val.dsize = valSize;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, key, val, MDBM_REPLACE));
    }
    free(tmp);
}

// redirect stdout to a file: useful only inside a forked child,
// // since it doesn't reopen stdout
void
MdbmUnitTestStats::redirectStdout(const string &toFilename)
{
    int out;
    int mode = S_IRUSR|S_IRGRP|S_IWGRP|S_IWUSR;
    int oflags = O_WRONLY | O_TRUNC | O_CREAT | O_APPEND;
    const char *filename = toFilename.c_str();

    // replace standard output with output file
    out = open(filename, oflags, mode);
    dup2(out, 1);
    close(out);
}

void
MdbmUnitTestStats::storeIncremental(MDBM *db, int startCnt, int endCnt, uint32_t maxValSize)
{
    datum key, val;
    void *tmp = malloc(maxValSize);
    CPPUNIT_ASSERT(NULL != tmp);
    memset(tmp, '.', maxValSize);
    for (int cnt = startCnt; cnt < endCnt; ++cnt) {
        key.dptr  = (char*)(&cnt);
        key.dsize = sizeof(cnt);
        val.dptr  = (char *) tmp;
        val.dsize = cnt % maxValSize;
        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, key, val, MDBM_REPLACE));
    }
    free(tmp);
}

// Convenient method for performance-tests MDBM that fetch data values from startCnt to endCnt
void
MdbmUnitTestStats::fetchSequential(MDBM *db, int startCnt, int endCnt, uint32_t valSize)
{
    char buf[65536];
    datum key, val, bufd;
    bufd.dsize = valSize;
    bufd.dptr = buf;
    for (int cnt = startCnt; cnt < endCnt; ++cnt)
    {
        key.dptr  = (char*)(&cnt);
        key.dsize = sizeof(cnt);
        CPPUNIT_ASSERT_EQUAL(0, mdbm_fetch_buf(db, &key, &val, &bufd, 0));
    }
}


/// MDBM V3 class

class MdbmUnitTestStatsV3 : public MdbmUnitTestStats
{
    CPPUNIT_TEST_SUITE(MdbmUnitTestStatsV3);
    CPPUNIT_TEST(initialSetup);

    CPPUNIT_TEST(test_StatsAB1);
    CPPUNIT_TEST(test_StatsAB2);
    CPPUNIT_TEST(test_StatsAB3);
    CPPUNIT_TEST(test_StatsAB4);
    CPPUNIT_TEST(test_StatsAB6);
    CPPUNIT_TEST(test_StatsAB7);

    CPPUNIT_TEST(test_StatsAC1);
    CPPUNIT_TEST(test_StatsAC2);
    CPPUNIT_TEST(test_StatsAC3);
    CPPUNIT_TEST(test_StatsAC4);
    CPPUNIT_TEST(test_StatsAC6);

    CPPUNIT_TEST(test_StatsAD1);
    CPPUNIT_TEST(test_StatsAD2);

    CPPUNIT_TEST(test_StatsAE1);
    CPPUNIT_TEST(test_StatsAE2);
    CPPUNIT_TEST(test_StatsAE3);
    CPPUNIT_TEST(test_StatsCallback);
    CPPUNIT_TEST(test_StatsEnable);
    CPPUNIT_TEST(test_StatsMdbmCountRecords);
    CPPUNIT_TEST(test_StatsMdbmCountPages);
    CPPUNIT_TEST(test_UsedPages);
    CPPUNIT_TEST(test_FreePages);
    CPPUNIT_TEST(test_new_mdbm_stat_HeaderOption);
    CPPUNIT_TEST(test_new_mdbm_stat_ResidentOption);
    CPPUNIT_TEST(test_SetTimeFunc);
    //CPPUNIT_TEST(test_GetTscUsec);
    CPPUNIT_TEST(new_mdbm_stat_OversizedNoLargeOneSize);
    CPPUNIT_TEST(new_mdbm_stat_OversizedLargeOneSize);
    CPPUNIT_TEST(new_mdbm_stat_OversizedNoLargeIncremental);
    CPPUNIT_TEST(new_mdbm_stat_OversizedLargeIncremental);

// Used for performance testing stats gathering using mdbm_enable_stat_operations() and stats
// gathering using the callback functionality mdbm_set_stats_func().  Ifdef-ed out until needed.
#ifdef STATS_PERF_TESTS
    CPPUNIT_TEST(test_StatPerformance);
    CPPUNIT_TEST(test_StatNullCallbackPerformance);
    CPPUNIT_TEST(test_StatCountingCallbackPerformance);
    CPPUNIT_TEST(test_StatTimedCallbackPerformance);
#endif

    CPPUNIT_TEST(test_PreloadPerf);

    CPPUNIT_TEST(finalCleanup);
    CPPUNIT_TEST_SUITE_END();

  public:
    //MdbmUnitTestStatsV3() : MdbmUnitTestStats(MDBM_CREATE_V3, MDBM_STAT_OPERATIONS) { }
    MdbmUnitTestStatsV3() : MdbmUnitTestStats(MDBM_CREATE_V3, 0) { }
    virtual void checkMdbmStatInfo(MDBM *mdbm, mdbm_stats_t *refInfo, bool empty = false);
    void test_new_mdbm_stat_HeaderOption();
    void test_new_mdbm_stat_ResidentOption();
    void test_StatPerformance();
    void test_StatNullCallbackPerformance();
    void test_StatCountingCallbackPerformance();
    void test_StatTimedCallbackPerformance();
    void test_PreloadPerf();

    void measureCallbackOverhead(CallbackType callbackType);
    pair<double,double> getStatPerformance(int id);
    pair<double,double> getStatCallbackPerformance(CallbackType callbackType, int id);
    void test_SetTimeFunc();
    //void test_GetTscUsec();
    void new_mdbm_stat_OversizedNoLargeOneSize();
    void new_mdbm_stat_OversizedLargeOneSize();
    void new_mdbm_stat_OversizedNoLargeIncremental();
    void new_mdbm_stat_OversizedLargeIncremental();
    vector<int> testMdbmStatOversized(const string &prefix, bool incremental, bool addLargeReplace);
    vector<int> readStatOutputFile(const string &filename);
    string createPreloadMdbm(const string &prefix, int pagesize, uint32_t pages);
    double getIterateTime(const string &filename, bool preload);
    double getPreloadSpeedup(const string &prefix, int pagesize, uint32_t pages);

  private:
    double getInsertTime(MDBM *mdbm, uint32_t valSize);
    double geFetchTime(MDBM *mdbm, uint32_t valSize);
    double getStdDev(const vector<double> &values, double mean);
    double getOverhead(const vector<double> &values, double mean, double stdDev);
    static void nullCallback(MDBM *db, int tag, int statFlags, uint64_t value, void *userDefined);
    static void cntCallback(MDBM *db, int tag, int statFlags, uint64_t value, void *userDefined);
    static void timedCallback(MDBM *db, int tag, int statFlags, uint64_t value, void *userDefined);
    uint64_t storeCounter;
    uint64_t fetchCounter;
    uint64_t storeLatency;
    uint64_t fetchLatency;
};

CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestStatsV3);

void
MdbmUnitTestStatsV3::checkMdbmStatInfo(MDBM *mdbm, mdbm_stats_t *refInfo, bool empty)
{
    if (empty) {
        ++refInfo->s_page_count;   // V3 uses extra page in an empty MDBM
        refInfo->s_max_level = refInfo->s_min_level = 1;
    }
    refInfo->s_size = refInfo->s_page_count * DEFAULT_PAGE_SIZE;

    CPPUNIT_ASSERT(mdbm != NULL);
    mdbm_stats_t dbInfo;
    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_stats(mdbm, &dbInfo, sizeof(dbInfo)));
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_size, refInfo->s_size);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_page_size, (u_int) DEFAULT_PAGE_SIZE);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_page_count, refInfo->s_page_count);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_pages_used, refInfo->s_pages_used);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_bytes_used, refInfo->s_bytes_used);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_num_entries, refInfo->s_num_entries);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_min_level, refInfo->s_min_level);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_max_level, refInfo->s_max_level);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_page_size, (u_int) DEFAULT_PAGE_SIZE);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_page_count, refInfo->s_large_page_count);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_threshold, refInfo->s_large_threshold);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_pages_used, refInfo->s_large_pages_used);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_num_free_entries, 0U);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_max_free, 0U);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_num_entries, refInfo->s_large_num_entries);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_bytes_used, refInfo->s_large_bytes_used);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_min_size, refInfo->s_large_min_size);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_large_max_size, refInfo->s_large_max_size);
    CPPUNIT_ASSERT_EQUAL(dbInfo.s_cache_mode, refInfo->s_cache_mode);
}

// Test for bug 5823240 - should not crash
void
MdbmUnitTestStatsV3::test_new_mdbm_stat_HeaderOption()
{
    string prefix = string("new_mdbm_stat_HeaderOption") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string filename;
    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, &filename)); // store counters
    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // set Fetch counters
    deleteEveryOther(mdbm, prefix);  // Set deletes counters

    const char *args[] = { "mdbm_stat", "-H", filename.c_str(), NULL };
    reset_getopt();
    CPPUNIT_ASSERT_EQUAL(0, mdbm_stat_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args));
}

void MdbmUnitTestStatsV3::test_new_mdbm_stat_ResidentOption()
{
    string prefix = string("new_mdbm_stat_ResidentOption") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string filename;
    MdbmHolder mdbm(createMixedObjFile(prefix, KEY_COUNT_DEFAULT, &filename)); // store counters
    fetchMixedObjFile(mdbm, KEY_COUNT_DEFAULT);   // set Fetch counters
    deleteEveryOther(mdbm, prefix);  // Set deletes counters

    const char *args[] = { "mdbm_stat", "-i", "resident", filename.c_str(), NULL };
    reset_getopt();
    CPPUNIT_ASSERT_EQUAL(0, mdbm_stat_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args));
}

// Compare MDBM store+fetch w/o any stats, with a data of size DataSiz, against:
// 1. enable_stat_operations(STATS_BASIC) and get a count: test_StatPerformance()
// 2. NULL stats-callback: test_StatNullCallbackPerformance()
// 3. stats-callback that keeps counts: test_StatCountingCallbackPerformance()
// 4. stats-callback that keeps counts and times: test_StatTimedCallbackPerformance()
//
// All of the above tests run 10 tests and drop any negative result or if it is
// outside of +/- 2 std-deviations, since these performance tests are pretty jittery.
// The general logic is to benchmark sequential stores with MDBM_REPLACE against a previously
// populated MDBM, then benchmark the fetches on the same data.

#define NUM_STORE_TESTS  300000U
#define NUM_FETCH_TESTS  299000U
#define REPEAT_TESTS    10

void
MdbmUnitTestStatsV3::test_StatPerformance()
{
    TRACE_TEST_CASE(__func__)

    vector<double> storeResults;   // Store Overheads
    vector<double> fetchResults;   // Fetch Overheads
    pair<double,double> storeAndFetch;

    for (int i = 0; i < REPEAT_TESTS; ++i) {
        storeAndFetch = getStatPerformance(i);
        storeResults.push_back((storeAndFetch.first < 0.0) ? 0.0 : storeAndFetch.first);
        fetchResults.push_back((storeAndFetch.second < 0.0) ? 0.0 : storeAndFetch.second);
    }
    double meanStore =
        (accumulate(storeResults.begin(), storeResults.end(), 0.0) / storeResults.size());
    double storeStdDev = getStdDev(storeResults, meanStore);
    double storeOverhead = getOverhead(storeResults, meanStore, storeStdDev);
    cout << fixed << setprecision(2)
         << "Mean of STATS_BASIC store overhead is " << storeOverhead << "%"  << endl;
    CPPUNIT_ASSERT(storeOverhead < 4.0);

    double meanFetch =
        (accumulate(fetchResults.begin(), fetchResults.end(), 0.0) / fetchResults.size());
    double fetchStdDev = getStdDev(fetchResults, meanFetch);
    double fetchOverhead = getOverhead(fetchResults, meanFetch, fetchStdDev);
    cout << fixed << setprecision(2)
         << "Mean of STATS_BASIC fetch overhead is " << fetchOverhead << "%"  << endl;
    CPPUNIT_ASSERT(fetchOverhead < 4.0);
}

const uint32_t DataSiz =  1024U;
const uint64_t LimitSiz = ((1024*1024*1024)*3ULL);

pair <double,double>
MdbmUnitTestStatsV3::getStatPerformance(int id)
{
    string prefix = string("getStatPerformance_") + ToStr(id) + versionString + ":";

    MdbmHolder mdbmstats(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC,
                                       0644, 8192, 0));
    storeSequential(mdbmstats, 0, NUM_STORE_TESTS, DataSiz);  // Initial store, don't time
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(mdbmstats));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_preload(mdbmstats));

    CPPUNIT_ASSERT_EQUAL(0, mdbm_enable_stat_operations(mdbmstats, MDBM_STATS_BASIC));
    double storeWithStats = getInsertTime(mdbmstats, DataSiz);
    checkCount(mdbmstats, MDBM_STAT_TYPE_STORE, NUM_STORE_TESTS);
    double fetchWithStats = geFetchTime(mdbmstats, DataSiz);
    checkCount(mdbmstats, MDBM_STAT_TYPE_FETCH, NUM_FETCH_TESTS);
    mdbmstats.Close();

    MdbmHolder basemdbm(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC,
                                  0644, 8192, 0));
    storeSequential(basemdbm, 0, NUM_STORE_TESTS, DataSiz);  // Initial store, don't time
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(basemdbm));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_preload(basemdbm));

    double baselineStore = getInsertTime(basemdbm, DataSiz);
    double baselineFetch = geFetchTime(basemdbm, DataSiz);

    double storeOverhead = ((storeWithStats / baselineStore) - 1.0) * 100;
    double fetchOverhead = ((fetchWithStats / baselineFetch) - 1.0) * 100;

    pair<double,double> storeAndFetch;
    storeAndFetch.first = storeOverhead;
    storeAndFetch.second = fetchOverhead;
    return storeAndFetch;
}

double
MdbmUnitTestStatsV3::getStdDev(const vector<double> &values, double mean)
{
    double varianceTot = 0.0;
    int siz = values.size();
    for (int i = 0; i < siz; ++i) {
        varianceTot += (values[i] - mean) * (values[i] - mean);
    }
    return sqrt(varianceTot / siz);
}

double
MdbmUnitTestStatsV3::getOverhead(const vector<double> &values, double mean, double stdDev)
{
    int count = 0;
    double goodMean = 0.0;
    for (int i = 0; i < REPEAT_TESTS; ++i) {
        if (fabs(values[i] - mean) < (stdDev * 2.0)) {
             ++count;
             goodMean += values[i];
        }
    }
    return ((count) ? goodMean / count : 0.0);
}


void
MdbmUnitTestStatsV3::test_StatNullCallbackPerformance()
{
    TRACE_TEST_CASE(__func__)

    measureCallbackOverhead(DO_NULL_CALLBACK);
}

void
MdbmUnitTestStatsV3::measureCallbackOverhead(CallbackType callbackType)
{
    string printedCallbackType;
    double maxStoreOverhead = 4.0;
    double maxFetchOverhead = 4.0;
    if (callbackType == DO_NULL_CALLBACK) {
        printedCallbackType = "Null";
    } else if (callbackType == DO_COUNT_CALLBACK) {
        printedCallbackType = "Counting";
    } else if (callbackType == DO_TIMED_CALLBACK) {
        printedCallbackType = "Timed";
        maxStoreOverhead = 15.0;
        maxFetchOverhead = 16.0;
    }

    vector<double> storeResults;   // Store Overheads
    vector<double> fetchResults;   // Fetch Overheads
    pair<double,double> storeAndFetch;

    for (int i = 0; i < REPEAT_TESTS; ++i) {
        storeAndFetch = getStatCallbackPerformance(callbackType, i);
        storeResults.push_back((storeAndFetch.first < 0.0) ? 0.0 : storeAndFetch.first);
        fetchResults.push_back((storeAndFetch.second < 0.0) ? 0.0 : storeAndFetch.second);
    }
    double meanStore =
        (accumulate(storeResults.begin(), storeResults.end(), 0.0) / storeResults.size());
    double storeStdDev = getStdDev(storeResults, meanStore);
    double storeOverhead = getOverhead(storeResults, meanStore, storeStdDev);
    cout << fixed << setprecision(2) << "Mean of " << printedCallbackType
         <<  " callback store overhead is " << storeOverhead << "%"  << endl;
    CPPUNIT_ASSERT(maxStoreOverhead > storeOverhead);

    double meanFetch =
        (accumulate(fetchResults.begin(), fetchResults.end(), 0.0) / fetchResults.size());
    double fetchStdDev = getStdDev(fetchResults, meanFetch);
    double fetchOverhead = getOverhead(fetchResults, meanFetch, fetchStdDev);
    cout << fixed << setprecision(2) << "Mean of " << printedCallbackType
         << " callback fetch overhead is " << fetchOverhead << "%"  << endl;
    CPPUNIT_ASSERT(maxFetchOverhead > fetchOverhead);
}

void
MdbmUnitTestStatsV3::test_StatCountingCallbackPerformance()
{
    TRACE_TEST_CASE(__func__)

    storeCounter = 0;
    fetchCounter = 0;
    measureCallbackOverhead(DO_COUNT_CALLBACK);
    CPPUNIT_ASSERT((NUM_STORE_TESTS * REPEAT_TESTS) == storeCounter);
    CPPUNIT_ASSERT((NUM_FETCH_TESTS * REPEAT_TESTS) == fetchCounter);
}

void
MdbmUnitTestStatsV3::test_StatTimedCallbackPerformance()
{
    TRACE_TEST_CASE(__func__)

    storeCounter = 0;
    fetchCounter = 0;
    storeLatency = 0;
    fetchLatency = 0;
    measureCallbackOverhead(DO_TIMED_CALLBACK);
    uint64_t totalStores = NUM_STORE_TESTS * REPEAT_TESTS;
    uint64_t totalFetches = NUM_FETCH_TESTS * REPEAT_TESTS;
    CPPUNIT_ASSERT_EQUAL(totalStores, storeCounter);
    CPPUNIT_ASSERT_EQUAL(totalFetches, fetchCounter);
    cout << "For " << totalStores << " stores, latency was " << storeLatency << endl;
    cout << "For " << totalFetches << " fetches, latency was " << fetchLatency << endl;
}


pair<double,double>
MdbmUnitTestStatsV3::getStatCallbackPerformance(CallbackType callbackType, int id)
{
    string prefix = string("getStatCallbackPerformance_") + ToStr(id) + versionString + ":";

    MdbmHolder basemdbm(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC,
                                  0644, 8192, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(basemdbm, LimitSiz / 8192, NULL, NULL));
    storeSequential(basemdbm, 0, NUM_STORE_TESTS, DataSiz);  // Initial store, don't time
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(basemdbm));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_preload(basemdbm));
    double baselineStore = getInsertTime(basemdbm, DataSiz);
    double baselineFetch = geFetchTime(basemdbm, DataSiz);
    basemdbm.Close();
    sleep(2);

    MdbmHolder mdbmstats(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC,
                                       0644, 8192, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbmstats, LimitSiz / 8192, NULL, NULL));
    storeSequential(mdbmstats, 0, NUM_STORE_TESTS, DataSiz);  // Store but don't time - init DB
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(mdbmstats));            // Then sync not to affect times
    CPPUNIT_ASSERT_EQUAL(0, mdbm_preload(mdbmstats));
    if (callbackType == DO_NULL_CALLBACK) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stats_func(mdbmstats, MDBM_STATS_BASIC,
                             nullCallback, this));
    } else if (callbackType == DO_COUNT_CALLBACK) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stats_func(mdbmstats, MDBM_STATS_BASIC,
                             cntCallback, this));
    } else if (callbackType == DO_TIMED_CALLBACK) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stats_func(mdbmstats, MDBM_STATS_BASIC | MDBM_STATS_TIMED,
                             timedCallback, this));
        CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stat_time_func(mdbmstats, 1));
    }
    double storeOverhead = ((getInsertTime(mdbmstats, DataSiz) / baselineStore) - 1.0) * 100;
    double fetchOverhead = ((geFetchTime(mdbmstats, DataSiz) / baselineFetch) - 1.0) * 100;
    mdbmstats.Close();
    sleep(2);

    pair<double,double> storeAndFetch;
    storeAndFetch.first = storeOverhead;
    storeAndFetch.second = fetchOverhead;
    return storeAndFetch;
}

double
MdbmUnitTestStatsV3::getInsertTime(MDBM *mdbm, uint32_t valSize)
{
    double start = getnow();
    storeSequential(mdbm, 0, NUM_STORE_TESTS, valSize);
    return getnow() - start;
}

double
MdbmUnitTestStatsV3::geFetchTime(MDBM *mdbm, uint32_t valSize)
{
    double start = getnow();
    fetchSequential(mdbm, 0, NUM_FETCH_TESTS, valSize);
    return getnow() - start;
}

void
MdbmUnitTestStatsV3::nullCallback(MDBM *db, int tag, int statFlags, uint64_t value,
                                  void *userDefined)
{
}

void
MdbmUnitTestStatsV3::cntCallback(MDBM *db, int tag, int statFlags, uint64_t value,
                                 void *userDefined)
{
    MdbmUnitTestStatsV3 *statsClass = static_cast<MdbmUnitTestStatsV3 *>(userDefined);

    if (tag == MDBM_STAT_TAG_STORE) {
        statsClass->storeCounter += value;
    } else if (tag == MDBM_STAT_TAG_FETCH) {
        statsClass->fetchCounter += value;
    }
}

void
MdbmUnitTestStatsV3::timedCallback(MDBM *db, int tag, int statFlags, uint64_t value,
                                   void *userDefined)
{
    MdbmUnitTestStatsV3 *statsClass = static_cast<MdbmUnitTestStatsV3 *>(userDefined);

    if (tag == MDBM_STAT_TAG_STORE) {
        statsClass->storeCounter += value;
    } else if (tag == MDBM_STAT_TAG_FETCH) {
        statsClass->fetchCounter += value;
    }
    if (tag == MDBM_STAT_TAG_STORE_LATENCY) {
        statsClass->storeLatency += value;
    } else if (tag == MDBM_STAT_TAG_FETCH_LATENCY) {
        statsClass->fetchLatency += value;
    }
}

void
MdbmUnitTestStatsV3::test_SetTimeFunc()
{
    string prefix = string("test_SetTimeFunc");

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC, 0644, 8192, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stats_func(mdbm, MDBM_STATS_BASIC | MDBM_STATS_TIMED,
                         timedCallback, this));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stat_time_func(mdbm, MDBM_CLOCK_TSC));
    uint64_t count = 200;
    storeCounter = fetchCounter = storeLatency = fetchLatency = 0;
    storeSequential(mdbm, 0, count, 64U);
    fetchSequential(mdbm, 0, count, 64U);
    CPPUNIT_ASSERT_EQUAL(count, storeCounter);
    CPPUNIT_ASSERT_EQUAL(count, fetchCounter);
    CPPUNIT_ASSERT(storeLatency > 0);
    CPPUNIT_ASSERT(fetchLatency > 0);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_stat_time_func(mdbm, MDBM_CLOCK_STANDARD));
    count = 300;
    storeCounter = fetchCounter = storeLatency = fetchLatency = 0;
    storeSequential(mdbm, 0, count, 64U);
    fetchSequential(mdbm, 0, count, 64U);
    CPPUNIT_ASSERT_EQUAL(count, storeCounter);
    CPPUNIT_ASSERT_EQUAL(count, fetchCounter);
    CPPUNIT_ASSERT(storeLatency > 0);
    CPPUNIT_ASSERT(fetchLatency > 0);
}

//extern "C" { uint64_t tsc_get_usec(void); }
//
//void
//MdbmUnitTestStatsV3::test_GetTscUsec()
//{
//    uint64_t t0, delta;
//    // give the timer a couple tries to stabilize...
//    for (int i=0; i<3; ++i) {
//      t0 = tsc_get_usec();
//      sleep(1);
//      delta = tsc_get_usec() - t0;
//      fprintf(stderr, "---- delta = %llu (expected ~1000000)----\n", (unsigned long long)delta);
//      if (delta > 900000ULL && delta < 1200000ULL) {
//        break;
//      }
//    }
//
//    CPPUNIT_ASSERT(delta > 900000ULL && delta < 1200000ULL);
//}

void
MdbmUnitTestStatsV3::new_mdbm_stat_OversizedNoLargeOneSize()
{
    string prefix = string("new_mdbm_stat_OversizedNoLargeOneSize:");
    TRACE_TEST_CASE(__func__)

    int sizesAndCounts[] = {2,1,3,2,6,1};
    vector<int> expected(sizesAndCounts, sizesAndCounts+6);
    vector<int> ret = testMdbmStatOversized(prefix, false, false);
    CPPUNIT_ASSERT(expected == ret);
}

void
MdbmUnitTestStatsV3::new_mdbm_stat_OversizedLargeOneSize()
{
    string prefix = string("new_mdbm_stat_OversizedLargeOneSize:");
    TRACE_TEST_CASE(__func__)

    int sizesAndCounts[] = {2,6,3,3,4,2,5,2,6,1};
    vector<int> expected(sizesAndCounts, sizesAndCounts+10);
    vector<int> ret = testMdbmStatOversized(prefix, false, true);
    CPPUNIT_ASSERT(expected == ret);
}

void
MdbmUnitTestStatsV3::new_mdbm_stat_OversizedNoLargeIncremental()
{
    string prefix = string("new_mdbm_stat_OversizedNoLargeIncremental:");
    TRACE_TEST_CASE(__func__)

    int sizesAndCounts[] = {2,1,3,2,6,1};
    vector<int> expected(sizesAndCounts, sizesAndCounts+6);
    vector<int> ret = testMdbmStatOversized(prefix, true, false);
    CPPUNIT_ASSERT(expected == ret);
}

void
MdbmUnitTestStatsV3::new_mdbm_stat_OversizedLargeIncremental()
{
    string prefix = string("new_mdbm_stat_OversizedLargeIncremental:");
    TRACE_TEST_CASE(__func__)

    int sizesAndCounts[] = {2,8,3,2,4,4,5,1,6,1};
    vector<int> expected(sizesAndCounts, sizesAndCounts+10);
    vector<int> ret = testMdbmStatOversized(prefix, true, true);
    CPPUNIT_ASSERT(expected == ret);
}

vector<int>
MdbmUnitTestStatsV3::testMdbmStatOversized(const string &prefix,
                                           bool incremental, bool addLargeReplace)
{
    string filename;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix,
                    MDBM_O_RDWR | MDBM_O_CREAT | MDBM_LARGE_OBJECTS | MDBM_O_FSYNC,
                    0644, DEFAULT_PAGE_SIZE, 8192, &filename));
    int maxpages = 31;
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, maxpages, 0, 0));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_MD5));

    int keysize = DEFAULT_KEY_SIZE, start = 100, maxcount = 22;
    if (incremental) {
        storeIncremental(mdbm, start, start + maxcount, maxcount);
    } else {
        CPPUNIT_ASSERT_EQUAL(0, InsertData(mdbm, keysize, DEFAULT_VAL_SIZE, maxcount, false));
    }

    // Get keys, then insert dupes until it fails
    //set<mdbm_ubig_t> pages;
    char bigbuf[DEFAULT_PAGE_SIZE + DEFAULT_PAGE_SIZE];
    char *curkey = new char[keysize];
    char *curvalue = new char[DEFAULT_VAL_SIZE];
    datum key, bigval, value = {curvalue, DEFAULT_VAL_SIZE};
    int count;
    memset(curkey, 0, keysize);
    memset(curvalue, 0, DEFAULT_VAL_SIZE);
    memset(bigbuf, 0, DEFAULT_PAGE_SIZE+DEFAULT_PAGE_SIZE);
    for (int i = 0, keycounter = start; i < maxcount; ++i, ++keycounter) {
        int ret = 0;
        count = 0;

        while (ret == 0 && count < 10000) {   // Ensure no infinite loop
            if (incremental) {
                key.dptr = (char*)&keycounter;
                key.dsize = sizeof(keycounter);
            } else {
                GenerateData(keysize, curkey, false, i);
                key.dptr = curkey;
                key.dsize = keysize;
            }
            value.dptr=curvalue; value.dsize=DEFAULT_VAL_SIZE;
            ret = mdbm_store(mdbm, key, value, MDBM_INSERT_DUP);
            ++count;
        }
        if (addLargeReplace) {
            if (incremental) {
                key.dptr = (char*)&keycounter;
                key.dsize = sizeof(keycounter);
            } else {
                GenerateData(keysize, curkey, false, i);
                key.dptr = curkey;
                key.dsize = keysize;
            }
            maxpages += 3;
            CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, maxpages, 0, 0));
            bigval.dptr = bigbuf;
            bigval.dsize = DEFAULT_PAGE_SIZE * 3 / 4;
            ret = mdbm_store(mdbm, key, bigval, MDBM_REPLACE);
        }
    }
    delete[] curvalue;

    string outfile("/tmp/test_stat_out"), cmd("rm -f ");
    outfile += ToStr(incremental) + ToStr(addLargeReplace);
    cmd += outfile;
    system(cmd.c_str());

    int pid = fork();
    CPPUNIT_ASSERT(pid >= 0);
    if (0 == pid) { // child
        redirectStdout(outfile);
        const char *args[] = { "mdbm_stat", "-o", filename.c_str(), NULL };
        reset_getopt();
        int retmain =
            mdbm_stat_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
        mdbm.Close();
        exit(retmain);
    } else { //parent
        int status = 0;
        waitpid(pid, &status, 0);
        CPPUNIT_ASSERT_EQUAL(0, WEXITSTATUS(status));
    }

    mdbm.Close();
    delete[] curkey;
    return readStatOutputFile(outfile);
}

vector<int>
MdbmUnitTestStatsV3::readStatOutputFile(const string &filename)
{
    FILE *fp = fopen(filename.c_str(), "r");
    CPPUNIT_ASSERT(fp != NULL);

    const int LINESZ = 512;
    char *found = NULL, line[LINESZ];
    while (fgets(line, LINESZ, fp) != NULL) {
        found = strstr(line, "Oversized Page(s), with distribution");
        if (found) {
            fgets(line, LINESZ, fp);   // Skip next line
            break;
        }
    }

    CPPUNIT_ASSERT(NULL != found);
    vector<int> ret;   // Vector of sizes and counts
    int size, count;
    while (fgets(line, LINESZ, fp) != NULL) {
        if (sscanf(line, "%d %d", &size, &count) == 2) {
            ret.push_back(size);
            ret.push_back(count);
        }
    }
    fclose(fp);
    CPPUNIT_ASSERT_EQUAL(0, unlink(filename.c_str()));
    return ret;
}

const int PRELOAD_TEST_DATA_SIZE = 100;  // Run preload test with 100 byte data size

// Create a large MDBM for preload test
string
MdbmUnitTestStatsV3::createPreloadMdbm(const string &prefix, int pagesize, uint32_t pages)
{
    string fname;
    MDBM *mdbm = EnsureTmpMdbm(prefix, MDBM_O_CREAT | MDBM_O_RDWR | MDBM_O_FSYNC,
                               0644, pagesize, 0, &fname);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, pages, NULL, NULL));
    uint64_t minData = ((uint64_t) pagesize) * pages / 2;  // Fill MDBM to at least 50% of capacity
    uint64_t data = 0;
    uint32_t failures = 0;    // Count failures to stop infinite loops
    uint32_t maxFailures = (pagesize / 2 / PRELOAD_TEST_DATA_SIZE) * pages;
    char buffer[PRELOAD_TEST_DATA_SIZE];

    memset(buffer, '.', PRELOAD_TEST_DATA_SIZE);
    srandom(1709);   // arbitrary prime

    do {
        uint64_t key = ((uint64_t) random()) * random();
        const datum ky = {(char *) &key, sizeof(key)};
        const datum value = {buffer, sizeof(buffer)};
        if (mdbm_store(mdbm, ky, value, MDBM_INSERT) == 0) {
            data += PRELOAD_TEST_DATA_SIZE;
        } else if (failures < maxFailures) {
            ++failures;
        } else {
            cout << endl << "Too many insert failures, total="
                  << data << endl;
            break;
        }
    } while (data < minData);
    mdbm_close(mdbm);
    return fname;
}

double
MdbmUnitTestStatsV3::getIterateTime(const string &filename, bool preload)
{
    MDBM *db = mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0444, 0, 0);
    CPPUNIT_ASSERT(NULL != db);
    if (preload) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_preload(db));
    }
    kvpair kv;
    double start = getnow();
    for (kv=mdbm_first(db); kv.key.dptr != NULL; kv=mdbm_next(db)) {
        if (kv.val.dptr == NULL) {                  // Shouldn't happen, but...
            cout << "NULL value pointer" << endl;   // examine value to avoid overoptimization
        }
    }
    double end = getnow();
    mdbm_close(db);
    return (end - start);
}

double
MdbmUnitTestStatsV3::getPreloadSpeedup(const string &prefix, int pagesize, uint32_t pages)
{
    string filename = createPreloadMdbm(prefix, pagesize, pages);
    double noPreloadTime = getIterateTime(filename, false);
    double preloadTime = getIterateTime(filename, true);
    return ((noPreloadTime / preloadTime) -1.0) * 100;
}

const int UNITTEST_LEVEL_LONG = 4;

void
MdbmUnitTestStatsV3::test_PreloadPerf()
{
    string prefix = string("test_PreloadPerf") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    SKIP_IF_FAST_VALGRIND()

    double pctBetter;
    if (testLevel < UNITTEST_LEVEL_LONG) {
        pctBetter = getPreloadSpeedup(prefix, 8192, 32768);  // 256MB preload test
        cout << endl << fixed << setprecision(1) << "256MB preload advantage is: "
             << pctBetter << "%" << endl;
        // this test is arbitrary and prone to intermittent failures
        //CPPUNIT_ASSERT(0.01 < pctBetter);
    } else {
        // Drop caches before running:
        // sudo bash -c "echo 3 > /proc/sys/vm/drop_caches"

        //uint32_t pages = 128 * 1024;   // 128K * 8K = 1GB
        uint32_t pages = 256 * 1024;   // 256K * 8K = 2GB
        //uint32_t pages = 512 * 1024;   // 512K * 8K = 4GB
        //uint32_t pages = 1024 * 1024;   // 1MB * 8K = 8GB
        cout << (pages/131072) << "GB preload advantage is: ";
        pctBetter = getPreloadSpeedup(prefix, 8192, pages);
        cout << fixed << setprecision(1) << pctBetter << "%" << endl;
    }
}

