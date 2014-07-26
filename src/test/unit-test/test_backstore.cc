/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// FIX BZ 5512918: v3: mdbm_set_backingstore: doesnt check MDBM version
// FIX BZ 5512957: v3: mdbm_set_backingstore: doesnt check for null bsops param = SEGV
// FIX BZ 5455314 - mdbm_open generates core with MDBM_OPEN_WINDOWED flag
// FIX BZ 5518388: v3: mdbm_set_window_size: returns success if window size=0; but SEGV in mdbm_store_r
// FIX BZ 5530655: v3: mdbm_get_window_stats: SEGV with null mdbm_window_stats_t parameter
// FIX BZ 5536317: v3: mdbm_set_backingstore: MDBM_BSOPS_FILE reuse old BS file with new cache - cannot fetch any data
#include <unistd.h>
#include <string.h>

#include <iostream>
#include <vector>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"
#include "test_backstore.hh"

int BackStoreTestSuite::_setupCnt = 0;

void BackStoreTestSuite::setUp()
{
    if (_setupCnt++ == 0)
    {
        cout << endl << suiteName << " Beginning..." << endl << flush;
        if (logLevel > LOG_NOTICE)
        {
            mdbm_log_minlevel(LOG_DEBUG);
        }
    }
}

void BackStoreTestSuite::TestWindowedMode()
{
    string prefix = "TestWindowedMode: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    for (int i=0; i<5; ++i) {
      string baseName = "tcwindowed";
      string dbName = GetTmpName(baseName);
      MdbmHolder dbh(dbName);

      int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
      int pageSize = 4096*(i+1);
      fprintf(stderr, "Testing PAGE_SIZE %d\n", pageSize);
      int ret = dbh.Open(openflags, 0644, pageSize, 0);
      prefix = SUITE_PREFIX() + prefix;
      CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

      // set limit and window size
      ret = mdbm_limit_size_v3(dbh, 12, 0, 0); // 8, 16 good
      size_t wsize = pageSize * 10;
      ret = mdbm_set_window_size(dbh, wsize);
      //fprintf(stderr, "Testing PAGE_SIZE %d check=%d \n", pageSize, mdbm_check(dbh,3,1));
      CPPUNIT_ASSERT_MESSAGE("post-create windowed check", 0 == mdbm_check(dbh,3,1));

      // fill DB
      int recCnt = FillDb(dbh);

      CPPUNIT_ASSERT_MESSAGE("post-fill windowed check", 0 == mdbm_check(dbh,3,1));

      // lets iterate
      stringstream ssss;
      ssss << prefix << "DB filled with " << recCnt
           << " records ";

      MDBM_ITER iter;
      datum key = mdbm_firstkey_r(dbh, &iter);
      if (key.dsize == 0) {
          ssss << " Failed to iterate data in DB!" << endl;
          CPPUNIT_ASSERT_MESSAGE(ssss.str(), (key.dsize > 0));
      }

      for (int cnt = 0; key.dsize > 0 && cnt < recCnt; ++cnt) {
          datum dval = mdbm_fetch(dbh, key);
          (void)dval; // unused, make compiler happy
          key = mdbm_nextkey_r(dbh, &iter);
      }
      CPPUNIT_ASSERT_MESSAGE("post-iterate windowed check", 0 == mdbm_check(dbh,3,1));
    }
}

void BackStoreTestSuite::BsSetWithNullParamA2()
{
#if 0
// FIX BZ 5512957: v3: mdbm_set_backingstore: doesnt check for null bsops param = SEGV
    // call mdbm_set_backingstore with null mdbm_bsops_t parameter
    string prefix = "TC A2: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA2";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_set_backingstore(dbh, 0, 0, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore should return -1 since no Backing Store provided, it returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == -1));
#endif
}
void BackStoreTestSuite::BsSetMdbmMultipleTimesThenCloseA3()
{
    // cleanup issues: set backing store MDBM_BSOPS_MDBM n times; then close the cache
    // NOTE: this requires something like valgrind to see the lost memory problem
    // if we had access to memcheck API could do it that way?
    string prefix = "TC A3: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA3";
    string dbName = GetTmpName(baseName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    prefix = SUITE_PREFIX() + prefix;

    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    string bsprefix = prefix;
    bsprefix += "Opening the Backing Store DB Failed: ";

    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore should return 0 until it runs out of memory, it returned=";
    for (int cnt = 0; cnt < 3; ++cnt)
    {
        MdbmHolder bsdbh(bsdbName); // clean up the file
        MdbmHolder dbh(dbName);
        int dbret = dbh.Open(openflags, 0644, 512, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

        // have to open without MdbmHolder since only dbh should clean it up
        MDBM *bsdb = mdbm_open(bsdbName.c_str(), openflags, 0644, 512, 0);
        CPPUNIT_ASSERT_MESSAGE(bsprefix, (bsdb != 0));

        int ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdb, 0);
        if (ret == -1)
        {
            bsss << ret << endl;
            CPPUNIT_ASSERT_MESSAGE(bsss.str(), (ret == 0));
        }
    }
}
void BackStoreTestSuite::BsSetUsingSameDbAsCacheAndBsA4()
{
    // create a DB; call mdbm_set_backingstore specifying the same DB handle
    //  for cache and Backing Store
    // perform insert of new key into DB; expect failure
    // perform modify of the key in DB; expect infinite recursion
    string prefix = "TC A4: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA4";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // dont use same handle for both else get a SEGV when storing later
    MDBM *bsdbh = mdbm_dup_handle(dbh, MDBM_O_RDWR);
    stringstream dhss;
    dhss << prefix << "mdbm_dup_handle FAILED getting duplicate handle" << endl;
    CPPUNIT_ASSERT_MESSAGE(dhss.str(), (bsdbh != 0));

    int ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore Should not have been able to "
                   << "set duplicate handle, it returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(), (ret == -1));
}
void BackStoreTestSuite::BsSetBsWithoutCacheModeA5()
{
    // call mdbm_set_backingstore(MDBM_BSOPS_MDBM) without calling mdbm_set_cachemode
    string prefix = "TC A5: DB Back Store: ";
    TRACE_TEST_CASE(prefix);

    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize  = 512;

    string bsbaseName = "tcbackstoreA5bs";
    string bsdbName = GetTmpName(bsbaseName);
    MdbmHolder bsdbholder(bsdbName);

    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openFlags,  0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));

    string baseName = "tcbackstoreA5";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    int dbret = dbh.Open(openFlags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore FAILED setting empty MDBM, it returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(), (ret == 0));

    string key = "tcbsA5key";
    string val = "tcbsA5value";
    ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using MDBM_BSOPS_MDBM as Backing Store, "
         << " mdbm_store_str FAILed returning=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));
}

class UDefinedBsops
{
public:
    UDefinedBsops(map<string, string> &udefmap) : _udefmap(udefmap) {}
    ~UDefinedBsops() {}

    int lock(const datum* key, int flags);
    int unlock(const datum* key);
    int fetch(const string &key, string &val, datum* buf, int flags);
    int store(const string &key, const string &val, int flags);
    int deleteval(const datum* key, int flags);
    void* dup(void* data) { return new UDefinedBsops(_udefmap); }

    map<string, string> &_udefmap;
};
int UDefinedBsops::store(const string &key, const string &val, int flags)
{
    // we cannot suport MDBM_INSERT_DUP with this test example
    if (flags & MDBM_MODIFY)
    {
        map<string, string>::iterator mit = _udefmap.find(key);
        if (mit == _udefmap.end())
        {
            return -1; // only allowed to update if exists
        }
        // else we delete it so we can add it below
        _udefmap.erase(key);
    }
    pair<string, string> hkpair(key, val);
    pair<map<string, string>::iterator, bool> iret = _udefmap.insert(hkpair);
    if (iret.second == false)
    {
        if (flags & MDBM_INSERT)
        {
            return MDBM_STORE_ENTRY_EXISTS;
        }
        else if (flags & MDBM_REPLACE || flags & MDBM_CACHE_MODIFY)
        {
            // OK, delete the key then add the new value
            _udefmap.erase(key);
            iret = _udefmap.insert(hkpair);
            if (iret.second == true)
            {
                return MDBM_STORE_SUCCESS;
            }
        }
        return -1;
    }

    return MDBM_STORE_SUCCESS;
}
int UDefinedBsops::fetch(const string &key, string &val, datum* buf, int flags)
{
    map<string, string>::iterator mit = _udefmap.find(key);
    if (mit == _udefmap.end())
    {
        return -1;
    }
    val = (*mit).second;
    return 0;
}
static void* udef_init(MDBM* db, const char* filename, void* opt, int flags)
{
    UDefinedBsops *udefbsops = new UDefinedBsops(*((map<string, string>*)opt));
    return udefbsops;
}
static int udef_term(void* data, int flags)
{
    if (!data)
    {
        return 0;
    }
    UDefinedBsops *udefbsops = static_cast<UDefinedBsops*>(data);
    delete udefbsops;
    return 0;
}
static int udef_lock(void* data, const datum* key, int flags)
{
    return 0; // TODO: NO-OP for this test
}
static int udef_unlock(void* data, const datum* key)
{
    return 0; // TODO: NO-OP for this test
}
static int udef_fetch(void* data, const datum* key, datum* val, datum* buf, int flags)
{
    return 0; // TODO: NO-OP for this test
}
static int udef_store(void* data, const datum* dkey, const datum* dval, int flags)
{
    if (!data)
    {
        return -1;
    }
    UDefinedBsops *udefbsops = static_cast<UDefinedBsops*>(data);
    string key(dkey->dptr, dkey->dsize);
    string val(dval->dptr, dval->dsize);
    int ret = udefbsops->store(key, val, flags);
    return ret;
}
static int udef_delete(void* data, const datum* key, int flags)
{
    return 0; // TODO: NO-OP for this test
}
static void* udef_dup(MDBM* db, MDBM* newdb, void* data)
{
    return 0; // TODO: NO-OP for this test
}
static void initBSOPS(mdbm_bsops_t &bsops)
{
    bsops.bs_init = udef_init;
    bsops.bs_term = udef_term;
    bsops.bs_lock = udef_lock;
    bsops.bs_unlock = udef_unlock;
    bsops.bs_fetch = udef_fetch;
    bsops.bs_store = udef_store;
    bsops.bs_delete = udef_delete;
    bsops.bs_dup = udef_dup;
}
void BackStoreTestSuite::BsUserDefinedBsFunctionsA6()
{
    // user defined BS functions set in mdbm_bsops structure
    string prefix = "TC A6: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA6";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    mdbm_bsops_t bsops;
    initBSOPS(bsops);
    map<string, string> udefBackStore;
    int ret = mdbm_set_backingstore(dbh, &bsops, &udefBackStore, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore FAILED setting empty MDBM, it returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(), (ret == 0));

    string key = "tcbsA6key";
    string val = "tcbsA6value";
    ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using MDBM_BSOPS_MDBM as Backing Store, "
         << " mdbm_store_str FAILed returning=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    // fetch the value
    char *fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data to the User Defined Backing Store"
         << " mdbm_fetch_str with key=" << key << "FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0));

    string fval(fstr);
    fsss << "The value fetched=" << fstr << " doesnt match the one stored=" << val << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fval == val));
}
void BackStoreTestSuite::BsStoreFetchLargeObjectA7()
{
    // store and fetch a large object from backing store
    string prefix = "TC A7: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA7";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_LARGE_OBJECTS | versionFlag;
    int pageSize = 512;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MdbmHolder bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (NULL != (MDBM*)bsdbh));

    string key = "tcbsA7key";
    int losize = pageSize;
    char *val = new char[losize];
    memset(val, 'b', losize);
    datum dkey;
    dkey.dptr  = (char*)key.c_str();
    dkey.dsize = key.size();
    datum dval;
    dval.dptr  = val;
    dval.dsize = losize;
    int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    delete [] val;
    stringstream ssss;
    ssss << prefix << "Using page size=" << pageSize
         << " mdbm_store FAILed to store large object size=" << losize
         << " and returned=" << ret;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    datum fdata = mdbm_fetch(dbh, dkey);
    stringstream fdss;
    fdss << prefix
         << "Fetching the large object stored with key=" << key
         << " from the DB=" << dbName
         << " FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize == losize));
}

void BackStoreTestSuite::BsUseFileForBsA8()
{
    string prefix = "TC A8: DB Back Store: ";
    string bsName(GetTmpName("bsfile"));
    UseFileForBs(prefix, bsName.c_str());
}

void BackStoreTestSuite::BsUseNullForBsA8()
{
    string prefix = "TC A8: DB Back Store is NULL: ";
    UseFileForBs(prefix, NULL);
}

void BackStoreTestSuite::BsUseInvalidForBsA8()
{
    string prefix = "TC A8: DB Back Store is invalid: ";
    TRACE_TEST_CASE(prefix);
    string dbName = GetTmpName("A8InvalidCache");
    MdbmHolder dbh(dbName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    CPPUNIT_ASSERT_EQUAL(0, dbh.Open(openflags, 0644, 512, 0));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_set_backingstore(dbh, MDBM_BSOPS_FILE, (void *) "invalid",
                                                    O_DIRECT | O_RDWR | O_CREAT | O_TRUNC));
}

void BackStoreTestSuite::BsUseInvalid2ForBsA8()
{
    string prefix = "TC A8: DB Back Store is invalid: ";
    TRACE_TEST_CASE(prefix);
    string dbName = GetTmpName("A8Invalid2Cache");
    MdbmHolder dbh(dbName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    CPPUNIT_ASSERT_EQUAL(0, dbh.Open(openflags, 0644, 512, 0));
    CPPUNIT_ASSERT_EQUAL(-1,  mdbm_set_backingstore(dbh, MDBM_BSOPS_FILE, (void *) "bad/invalid",
                                                    O_DIRECT | O_RDWR | O_CREAT | O_TRUNC));
}

void BackStoreTestSuite::UseFileForBs(const string &prfx, const char *bsFileName)
{
    // perform backing store using file; call mdbm_set_backingstore(MDBM_BSOPS_FILE)
    TRACE_TEST_CASE(prfx);
    string baseName = "tcbackstoreA8";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX() + prfx;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_FILE, (void*)bsFileName,
                                    O_DIRECT | O_RDWR | O_CREAT | O_TRUNC);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore with MDBM_BSOPS_FILE FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // store some data
    string key = "tcbsA8key";
    string val = "tcbsA8value";
    datum k, v;
    k.dptr = const_cast<char *>(key.c_str());
    k.dsize = key.size();
    v.dptr = const_cast<char *>(val.c_str());
    v.dsize = val.size();
    ret = mdbm_store(dbh, k, v, MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using MDBM_BSOPS_FILE as Backing Store, "
         << " mdbm_store FAILed returning=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    string key2 = "tcbsA8key2";
    string val2 = "tcbsA8value2";
    ret = mdbm_store_str(dbh, key2.c_str(), val2.c_str(), MDBM_INSERT);
    stringstream ssss2;
    ssss2 << prefix << "Using MDBM_BSOPS_FILE as Backing Store, "
         << " mdbm_store_str FAILed on 2nd time, returning=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssss2.str(), (ret == 0));

    // fetch key2 data
    char *fstr = mdbm_fetch_str(dbh, key2.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data to the MDBM_BSOPS_FILE Backing Store"
         << " mdbm_fetch_str with key=" << key2 << " FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0));

    fsss << "The value fetched=" << fstr << " doesnt match the one stored=" << val2 << endl;
    string fval(fstr);
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fval == val2));

    // delete key using mdbm_delete
    k.dptr = const_cast<char *>(key.c_str());
    k.dsize = key.size();
    ret = mdbm_delete(dbh, k);
    stringstream dsss;
    dsss << prefix << "Deleting data with MDBM_BSOPS_FILE Backing Store"
         << " mdbm_delete with key=" << key << " FAILed, errno=" << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(dsss.str(), (ret == 0));

    // delete key2 using mdbm_delete_str
    ret = mdbm_delete_str(dbh, key2.c_str());
    stringstream d2sss;
    d2sss << prefix << "Deleting data with MDBM_BSOPS_FILE Backing Store"
          << " mdbm_delete_str with key=" << key2 << " FAILed, errno=" << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(d2sss.str(), (ret == 0));
}
void BackStoreTestSuite::bsCacheModeStore(string &prefix, string &baseName,
    int cacheMode, int storeFlag, bool expectSuccess)
{
    // set backing store(MDBM_BSOPS_MDBM) and cacheMode;
    // store using storeFlag - MDBM_CACHE_ONLY - acts like INSERT for the cache;
    // close the cache; create a new DB cache;
    // set the same backing store and fetch the data;
    // expect it to fail since data should not have been stored in the
    // Backing Store and using a new cache
    TRACE_TEST_CASE(prefix);

    // create a Backing Store DB
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    prefix = SUITE_PREFIX() + prefix;
    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName); // clean up the file

    string key = "key";
    key += baseName;
    string val = "value";
    val += baseName;
    {
        // create a cache
        string dbName = GetTmpName(baseName);
        MdbmHolder dbh(dbName);
        int ret = dbh.Open(openflags, 0644, pageSize, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

        ret = mdbm_set_cachemode(dbh, cacheMode);
        stringstream scss;
        scss << prefix << "mdbm_set_cachemode with mode=" << cacheMode << " FAILed" << endl;
        CPPUNIT_ASSERT_MESSAGE(scss.str(), (ret == 0));

        // set BS
        ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
        stringstream bsss;
        bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
             << " Using DB cache file=" << dbName << endl;
        CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

        // store the data
        ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), storeFlag);
        stringstream ssss;
        ssss << prefix << "Using MDBM_BSOPS_MDBM as Backing Store, "
             << " mdbm_store_str FAILed returning=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

        // close the cache - upon dbh destruction
    }

    // create a new cache
    baseName += "_2";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    ret = mdbm_set_cachemode(dbh, cacheMode);
    stringstream scss;
    scss << prefix << "mdbm_set_cachemode with mode=" << cacheMode << " FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(scss.str(), (ret == 0));

    // re-open the same BS as used before and set it as the BS
    bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // HAVE: a new cache with the old Backing Store
    // fetch the old data
    char *fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data=" << val
         << " to the Cache, then using a new Cache,"
         << " mdbm_fetch_str with key=" << key;
    if (expectSuccess)
    {
        fsss << " FAILed" << endl;
        CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0));
    }
    else
    {
        fsss << " Succeeded but should NOT have" << endl;
        CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr == 0));
    }
}
void BackStoreTestSuite::BsCacheModeLruStoreCacheOnlyA9()
{
    string prefix = "TC A9: DB Back Store: ";
    string baseName = "tcbackstoreA9";
    bsCacheModeStore(prefix, baseName, MDBM_CACHEMODE_LRU, MDBM_CACHE_ONLY, false);
}
void BackStoreTestSuite::BsCacheModeNoneStoreCacheOnlyA10()
{
    string prefix = "TC A10: DB Back Store: ";
    string baseName = "tcbackstoreA10";
    bsCacheModeStore(prefix, baseName, MDBM_CACHEMODE_NONE, MDBM_CACHE_ONLY, false);
}
void BackStoreTestSuite::BsCacheModeLruStoreModifyNewKeyA11()
{
    string prefix = "TC A11: DB Back Store: ";
    string baseName = "tcbackstoreA11";
    bsCacheModeStore(prefix, baseName, MDBM_CACHEMODE_LRU, MDBM_CACHE_MODIFY, false);
}
void BackStoreTestSuite::BsCacheModeLruStoreModifyOldKeyA12()
{
    string prefix = "TC A12: DB Back Store: ";
    TRACE_TEST_CASE(prefix);

    // create a Backing Store DB
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = 512;
    prefix = SUITE_PREFIX() + prefix;
    string baseName = "tcbackstoreA12";
    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName); // clean up the file

    string key = "tcbsA12key";
    string val = "tcbsA12value";
    // create a cache
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_LRU);
    stringstream scss;
    scss << prefix << "mdbm_set_cachemode with mode=MDBM_CACHEMODE_LRU FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(scss.str(), (ret == 0));

    // set BS
    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // store data
    ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using MDBM_BSOPS_MDBM as Backing Store, "
         << " mdbm_store_str FAILed to INSERT returning=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    char *fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data=" << val
         << " to the Cache, then using a new Cache,"
         << " mdbm_fetch_str with key=" << key
         << " FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0));

    string val2 = val + "_2";
    ret = mdbm_store_str(dbh, key.c_str(), val2.c_str(), MDBM_CACHE_MODIFY);
    stringstream ssss2;
    ssss2 << prefix << "Using MDBM_BSOPS_MDBM as Backing Store, "
         << " mdbm_store_str FAILed to CACHE_MODIFY returning=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ssss2.str(), (ret == 0));

    fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss2;
    fsss2 << prefix << "After storing data=" << val2
          << " to the Cache, then using a new Cache,"
          << " mdbm_fetch_str with key=" << key
          << " FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss2.str(), (fstr != 0));

    string sval2(fstr);
    fsss2 << " Value should be MODIFIED but was NOT. Value fetched=" << sval2 << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss2.str(), (sval2 == val2));
}

void BackStoreTestSuite::bsWindowsAndFill(string &prefix, string &baseName,
    size_t cachewsize, size_t bswsize, mdbm_ubig_t cacheNumPages,
    mdbm_ubig_t bsNumPages, int pageSize)
{
    TRACE_TEST_CASE(prefix);

    // create a Backing Store DB
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    prefix = SUITE_PREFIX() + prefix;
    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName); // clean up the file

    if (bswsize > 0)
    {
        int ret = mdbm_set_window_size(bsdbh, bswsize);
        stringstream wsss;
        wsss << prefix << "Set Valid Window size=" << bswsize
             << " for BS DB and expected Success. mdbm_set_window_size returned=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
    }

    int ret = mdbm_limit_size_v3(bsdbh, bsNumPages, 0, 0);
    stringstream lsss;
    lsss << prefix << "Set limit size pages=" << bsNumPages
         << " for BS DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    // create the cache
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    ret = dbh.Open(openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    if (cachewsize > 0)
    {
        int ret = mdbm_set_window_size(bsdbh, cachewsize);
        stringstream wsss2;
        wsss2 << prefix << "Set Valid Window size=" << cachewsize
              << " for Cache DB and expected Success. mdbm_set_window_size returned=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(wsss2.str(), (ret == 0));
    }

    ret = mdbm_limit_size_v3(dbh, cacheNumPages, 0, 0);
    stringstream lsss2;
    lsss2 << prefix << "Set limit size pages=" << cacheNumPages
          << " for Cache DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss2.str(), (ret == 0));

    // set BS
    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // fill the DB to capacity
    int recCnt = FillDb(dbh);
    stringstream fdss;
    fdss << prefix << "FAILed to fill DB through the cache using MDBM_BSOPS_MDBM record count returned=" << recCnt << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(),(recCnt > 0));
}
void BackStoreTestSuite::BsSameWindowSizeCacheAndBsA13()
{
    string prefix = "TC A13: DB Back Store: ";
    size_t wsize = getpagesize() * 2;
    string baseName = "tcbackstoreA13";
    mdbm_ubig_t limitNumPages = 8;
    int pageSize = 512;
    bsWindowsAndFill(prefix, baseName, wsize, wsize, limitNumPages, limitNumPages, pageSize);
}
void BackStoreTestSuite::BsSmallWindSizeCacheAndBigWindSizeBsA14()
{
    string prefix = "TC A14: DB Back Store: ";
    size_t wsize = getpagesize() * 2;
    string baseName = "tcbackstoreA14";
    mdbm_ubig_t limitNumPages = 8;
    int pageSize = 512;
    bsWindowsAndFill(prefix, baseName, wsize, (wsize*2), limitNumPages, limitNumPages, pageSize);
}
void BackStoreTestSuite::BsBigWindSizeCacheAndSmallWindSizeBsA15()
{
    string prefix = "TC A15: DB Back Store: ";
    size_t wsize = getpagesize() * 2;
    string baseName = "tcbackstoreA15";
    mdbm_ubig_t limitNumPages = 8;
    int pageSize = 512;
    bsWindowsAndFill(prefix, baseName, (wsize*2), wsize, limitNumPages, limitNumPages, pageSize);
}
void BackStoreTestSuite::BsLimitSizeSameCacheAndBsA16()
{
    // mdbm_limit_size of cache to (n) and backing store to (2*n); fill DB
    string prefix = "TC A16: DB Back Store: ";
    size_t wsize = 0;
    string baseName = "tcbackstoreA16";
    mdbm_ubig_t cacheLimitNumPages = 4;
    mdbm_ubig_t bsLimitNumPages    = 8;
    int pageSize = 512;
    bsWindowsAndFill(prefix, baseName, wsize, wsize, cacheLimitNumPages, bsLimitNumPages, pageSize);
}
void BackStoreTestSuite::BsLimitSizeBigCacheAndSmallBsA17()
{
    // mdbm_limit_size of cache to (2*n) and backing store size to (n); fill DB
    string prefix = "TC A17: DB Back Store: ";
    size_t wsize = 0;
    string baseName = "tcbackstoreA17";
    mdbm_ubig_t cacheLimitNumPages = 8;
    mdbm_ubig_t bsLimitNumPages    = 4;
    int pageSize = 512;
    bsWindowsAndFill(prefix, baseName, wsize, wsize, cacheLimitNumPages, bsLimitNumPages, pageSize);
}
void BackStoreTestSuite::BsForEachPSizeOpenCacheForEachPSizeOpenBsLimitAndFillA18()
{
    // for each page size n: open cache with page size=n;
    //   then for each page size n: open backing store with page size = n;
    //     limit size of each DB to same size and fill DB
    string prefix = "TC A18: DB Back Store: ";
    size_t wsize = 0;
    string baseName = "tcbackstoreA18";
    mdbm_ubig_t cacheLimitNumPages = 4;
    mdbm_ubig_t bsLimitNumPages    = 4;

    int pageSizes[] = { 128, 256, 512, 1024, 4096, 8192, 16384, 32768, 65536 };
    for (size_t cnt = 0; cnt < sizeof(pageSizes) / sizeof(int); ++cnt)
    {
        bsWindowsAndFill(prefix, baseName, wsize, wsize, cacheLimitNumPages, bsLimitNumPages, pageSizes[cnt]);
    }
}
void BackStoreTestSuite::BsUseFileBsFillDbCloseDbNewDbUseFileBsA19()
{
#if 0
    // populate FILE-based backingstore; close the DB; open a new DB using same backing store FILE
    string prefix = "TC A19: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA19";
    string dbName = GetTmpName(baseName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pgsize = 512;
    MDBM *dbh = mdbm_open(dbName.c_str(), openflags, 0644, pgsize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh != 0));

    int ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_LRU);
    string msg = prefix + string("mdbm_set_cachemode failed");
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), (ret == 0));

    ret = mdbm_limit_size_v3(dbh, 4, 0, 0);
    stringstream lsss2;
    lsss2 << prefix << "Set limit size pages=4"
          << " for Cache DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss2.str(), (ret == 0));

    string bsfile = GetTmpName("TestA19_bsfile");

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_FILE, (void*)bsfile.c_str(),
                                O_DIRECT | O_RDWR | O_CREAT | O_TRUNC);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_FILE FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    simpleFileBsTest(dbh, true, "First Store");   // store BS data using write-through cache
    simpleFileBsTest(dbh, false, "First Fetch");  // read back BS data using write-through cache

    mdbm_close(dbh);

    MdbmHolder cachedbh(EnsureTmpMdbm("BStestA19_2", openflags, 0644, pgsize));  // reopen cache

    ret = mdbm_set_cachemode(cachedbh, MDBM_CACHEMODE_LRU);
    msg = prefix + string("mdbm_set_cachemode #2 failed");
    CPPUNIT_ASSERT_MESSAGE(msg.c_str(), (ret == 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_FILE, (void*)bsfile.c_str(), O_DIRECT | O_RDWR);
    stringstream bsss2;
    bsss2 << prefix
          << "2nd time calling mdbm_set_backingstore for MDBM_BSOPS_FILE failed " << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss2.str(),(ret == 0));

    simpleFileBsTest(cachedbh, false, "Second Fetch");  // re-read the data using new cache
#endif
}
void BackStoreTestSuite::BsUseMdbmBsFillDbCloseDbNewDbUseMdbmBsA20()
{
    // create DB with backing store as MDBM; fill DB;
    // close the DB; open a new DB using same backing store MDBM
    string prefix = "TC A20: DB Back Store: ";
    string baseName = "tcbackstoreA20";
    int openBSflag = MDBM_O_RDWR;
    int fillDBrecCnt = -1;
    bsStoreDataSetBsWithNewDbStoreNew(prefix, baseName, openBSflag, fillDBrecCnt);
}
void BackStoreTestSuite::BsUseMdbmBsFillDbCloseDbSameDbUseMdbmBsA21()
{
    // create DB with backing store as MDBM; fill DB; close the DB;
    // open same DB using same backing store MDBM; fetch all values
    string prefix = "TC A21: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA21";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbholder(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MDBM *dbh = mdbm_open(dbName.c_str(), openflags, 0644, 512, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh != 0));

    int ret = mdbm_limit_size_v3(dbh, 2, 0, 0);
    stringstream lsss;
    lsss << prefix << "Set limit size pages=2"
          << " for Cache DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName); // clean up the file

    ret = mdbm_limit_size_v3(bsdbh, 8, 0, 0);
    stringstream lsss2;
    lsss2 << prefix << "Set limit size pages=8"
          << " for BS DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss2.str(), (ret == 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // fill the DB
    int recCnt = FillDb(dbh);
    stringstream fdss;
    fdss << prefix << "FAILed to fill DB through the cache using MDBM_BSOPS_MDBM. Record count returned=" << recCnt << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(),(recCnt > 0));

    // close the DB cache
    mdbm_close(dbh);

    // reopen the MDBM BS
    bsdbh = mdbm_open(bsdbName.c_str(), MDBM_O_RDWR | versionFlag, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));

    // lets iterate the BS and count the entries
    MDBM_ITER iter;
    kvpair pair = mdbm_first_r(bsdbh, &iter);
    stringstream bsitss;
    bsitss << prefix << "FAILed to iterate BS DB=" << bsdbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsitss.str(), (pair.key.dsize > 0 && pair.val.dsize > 0));

    vector<kvpair> kvvec;
    kvvec.push_back(pair);

    int bsRecCnt;
    for (bsRecCnt = 0; pair.key.dsize > 0 && pair.val.dsize > 0; ++bsRecCnt)
    {
        pair = mdbm_next_r(bsdbh, &iter);
        if (pair.key.dsize > 0 && pair.val.dsize > 0)
        {
            kvvec.push_back(pair);
        }
    }

    stringstream icss;
    icss << prefix << "FAILed to iterate BS DB without the cache. Record count returned=" << bsRecCnt
         << " and number of records originally stored=" << recCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(icss.str(), (recCnt == bsRecCnt));

    // HAVE: the BS MDBM has all the original stored data,
    // So now lets set the BS with the old cache again

    dbh = mdbm_open(dbName.c_str(), MDBM_O_RDWR | versionFlag, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh != 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss2;
    bsss2 << prefix << "2nd time calling mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss2.str(), (ret == 0));

    // iterate the vector of keys to ensure we can get them all from the BS
    stringstream mmss;
    size_t afterRecCnt;
    for (afterRecCnt = 0; afterRecCnt < kvvec.size(); ++afterRecCnt)
    {
        pair = kvvec[afterRecCnt];
        datum dval = mdbm_fetch(dbh, pair.key);
        string keystr(pair.key.dptr, pair.key.dsize);
        if (dval.dptr == 0 && dval.dsize == 0)
        {
            mmss << prefix << "The retrieved key/value pair fetched for key=" << keystr
                 << " is missing!" << endl;
            CPPUNIT_ASSERT_MESSAGE(mmss.str(), (dval.dptr != 0 && dval.dsize > 0));
        }
        else if (dval.dsize != pair.val.dsize)
        {
            mmss << prefix << "The retrieved key/value pair fetched for key=" << keystr
                 << " do NOT match!" << endl;
            CPPUNIT_ASSERT_MESSAGE(mmss.str(), (dval.dsize == pair.val.dsize));
        }
        else if (memcmp(dval.dptr, pair.val.dptr, dval.dsize) != 0)
        {
            mmss << prefix << "The retrieved value fetched for key=" << keystr
                 << " does NOT match what was stored" << endl;
            CPPUNIT_ASSERT_MESSAGE(mmss.str(), (false));
        }
    }
    stringstream icss2;
    icss2 << prefix << "FAILed to fetch all gathered keys through the cache. Record count returned=" << afterRecCnt
         << " and number of records originally stored=" << recCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(icss2.str(), (recCnt == (int)afterRecCnt));
    mdbm_close(dbh);
}

void BackStoreTestSuite::bsStoreDataSetBsWithNewDbStoreNew(string &prefix,
    string &baseName, int openBSflag, int fillDBrecCnt)
{
    TRACE_TEST_CASE(prefix);
    string dbName = GetTmpName(baseName);
    MdbmHolder dbholder(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MDBM *dbh = mdbm_open(dbName.c_str(), openflags, 0644, 512, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh != 0));

    int ret = mdbm_limit_size_v3(dbh, 4, 0, 0);
    stringstream lsss;
    lsss << prefix << "Set limit size pages=4"
          << " for Cache DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName); // clean up the file

    ret = mdbm_limit_size_v3(bsdbh, 4, 0, 0);
    stringstream lsss2;
    lsss2 << prefix << "Set limit size pages=4"
          << " for BS DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss2.str(), (ret == 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // fill the DB
    int recCnt = FillDb(dbh, fillDBrecCnt);
    stringstream fdss;
    fdss << prefix << "FAILed to partially fill DB through the cache using MDBM_BSOPS_MDBM. Record count returned=" << recCnt << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(),(recCnt > 0));

    // close the DB cache
    mdbm_close(dbh);

    // reopen the MDBM
    bsdbh = mdbm_open(bsdbName.c_str(), openBSflag | versionFlag, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));

    // lets iterate the BS and count the entries
    MDBM_ITER iter;
    kvpair pair = mdbm_first_r(bsdbh, &iter);
    stringstream bsitss;
    bsitss << prefix << "FAILed to iterate BS DB=" << bsdbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsitss.str(), (pair.key.dsize > 0 && pair.val.dsize > 0));

    vector<kvpair> kvvec;
    kvvec.push_back(pair);

    int bsRecCnt;
    for (bsRecCnt = 0; pair.key.dsize > 0 && pair.val.dsize > 0; ++bsRecCnt)
    {
        pair = mdbm_next_r(bsdbh, &iter);
        if (pair.key.dsize > 0 && pair.val.dsize > 0)
        {
            kvvec.push_back(pair);
        }
    }

    stringstream icss;
    icss << prefix << "FAILed to iterate BS DB without the cache. Record count returned=" << bsRecCnt
         << " and number of records originally stored=" << recCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(icss.str(), (recCnt == bsRecCnt));

    // HAVE: the BS MDBM has all the original stored data,
    // So now lets set the BS with the new cache
    string newcachedbName = dbName + "NEW";
    MdbmHolder newcachedbholder(newcachedbName);

    ret = newcachedbholder.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    ret = mdbm_set_backingstore(newcachedbholder, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss2;
    bsss2 << prefix << "2nd time calling mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << newcachedbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss2.str(), (ret == 0));

    // add some new data - should FAIL if BS is read-only
    string newKey_a = baseName + "key_a";
    string newVal_a = baseName + "val_a";
    ret = mdbm_store_str(newcachedbholder, newKey_a.c_str(), newVal_a.c_str(), MDBM_INSERT);
    // assert(openBSflag == RDONLY && ret == -1)
    // assert(openBSflag == RDWR && ret == 0)
    if (ret == 0)
    {
        pair.key.dsize = newKey_a.size() + 1;
        pair.key.dptr  = (char*)newKey_a.c_str();
        pair.val.dsize = newVal_a.size() + 1;
        pair.val.dptr  = (char*)newVal_a.c_str();
        kvvec.push_back(pair);
        recCnt++;
    }

    string newKey_b = baseName + "key_b";
    string newVal_b = baseName + "val_b";
    ret = mdbm_store_str(newcachedbholder, newKey_b.c_str(), newVal_b.c_str(), MDBM_INSERT);
    if (ret == 0)
    {
        pair.key.dsize = newKey_b.size() + 1;
        pair.key.dptr  = (char*)newKey_b.c_str();
        pair.val.dsize = newVal_b.size() + 1;
        pair.val.dptr  = (char*)newVal_b.c_str();
        kvvec.push_back(pair);
        recCnt++;
    }

    // iterate the vector of keys to ensure we can get them all from the BS
    stringstream mmss;
    size_t afterRecCnt;
    for (afterRecCnt = 0; afterRecCnt < kvvec.size(); ++afterRecCnt)
    {
        pair = kvvec[afterRecCnt];
        datum dval = mdbm_fetch(newcachedbholder, pair.key);
        string keystr(pair.key.dptr, pair.key.dsize);
        if (dval.dptr == 0 && dval.dsize == 0)
        {
            mmss << prefix << "The retrieved key/value pair fetched for key=" << keystr
                 << " is missing!" << endl;
            CPPUNIT_ASSERT_MESSAGE(mmss.str(), (dval.dptr != 0 && dval.dsize > 0));
        }
        else if (dval.dsize != pair.val.dsize)
        {
            mmss << prefix << "The retrieved key/value pair fetched for key=" << keystr
                 << " do NOT match!" << endl;
            CPPUNIT_ASSERT_MESSAGE(mmss.str(), (dval.dsize == pair.val.dsize));
        }
        else if (memcmp(dval.dptr, pair.val.dptr, dval.dsize) != 0)
        {
            mmss << prefix << "The retrieved value fetched for key=" << keystr
                 << " does NOT match what was stored" << endl;
            CPPUNIT_ASSERT_MESSAGE(mmss.str(), (false));
        }
    }
    stringstream icss2;
    icss2 << prefix << "FAILed to fetch all gathered keys through the cache. Record count returned=" << afterRecCnt
         << " and number of records originally stored=" << recCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(icss2.str(), (recCnt == (int)afterRecCnt));
}
void BackStoreTestSuite::BsStoreDataSetBsWithNewDbStoreNewA22()
{
    // create DB with backing store as MDBM; partial fill DB; close DB;
    // create a new DB using same backing store MDBM; store new values;
    // fetch all values old and new
    string prefix = "TC A22: DB Back Store: ";
    string baseName = "tcbackstoreA22";
    int openBSflag = MDBM_O_RDWR;
    int fillDBrecCnt = 10;
    bsStoreDataSetBsWithNewDbStoreNew(prefix, baseName, openBSflag, fillDBrecCnt);
}
void BackStoreTestSuite::BsStoreDataSetReadOnlyBsWithNewDbStoreNewA23()
{
    // create DB with backing store as MDBM; partial fill DB; close DB;
    // open Backing Store DB read-only and set with new Cache;
    // fetch all values; then store new data
    string prefix = "TC A23: DB Back Store: ";
    string baseName = "tcbackstoreA23";
    int openBSflag = MDBM_O_RDONLY;
    int fillDBrecCnt = 10;
    bsStoreDataSetBsWithNewDbStoreNew(prefix, baseName, openBSflag, fillDBrecCnt);
}

void BackStoreTestSuite::BsCreateDbNoWindowFlagSetWindowSizeZeroB1()
{
    // create DB without MDBM_OPEN_WINDOWED flag; set window size=0; expect failure
    string prefix = "TC B1: Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreB1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = 0;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set invalid Window size=" << wsize
         << " and expected Failure. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret != 0));

    //int limitNumPages = 2;
    //ret = mdbm_limit_size_v3(dbh, limitNumPages, 0, 0);

    //// fill DB then iterate
    //int recCnt = FillDb(dbh);

    //stringstream itss;
    //itss << prefix << "FAILed to fill DB=" << dbName << endl;
    //MDBM_ITER iter;
    //datum key = mdbm_firstkey_r(dbh, &iter);
    //CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize > 0));

    //int icnt;
    //for (icnt = 0; key.dsize > 0; ++icnt) {
    //    key = mdbm_nextkey_r(dbh, &iter);
    //}
    //if (icnt != recCnt) {
    //    stringstream icss;
    //    icss << prefix
    //         << "FAILed to fetch all keys. Current cnt="
    //         << icnt << " but number of records in DB=" << recCnt << endl;
    //    CPPUNIT_ASSERT_MESSAGE(icss.str(), (icnt == recCnt));
    //}
}
void BackStoreTestSuite::BsCreateDbNoWindowFlagSetWindowSizeB2()
{
    // create DB without MDBM_OPEN_WINDOWED flag; set window size=z; store, fetch
    string prefix = "TC B2: Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreB2";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = pageSize * 4;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    int limitNumPages = 6;
    ret = mdbm_limit_size_v3(dbh, limitNumPages, 0, 0);

    // fill DB then iterate
    int recCnt = FillDb(dbh);

    stringstream itss;
    itss << prefix << "FAILed to fill DB=" << dbName << endl;
    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize > 0));

    int icnt;
    for (icnt = 0; key.dsize > 0; ++icnt)
    {
        key = mdbm_nextkey_r(dbh, &iter);
    }
    if (icnt != recCnt)
    {
        stringstream icss;
        icss << prefix
             << "FAILed to fetch all keys. Current cnt="
             << icnt << " but number of records in DB=" << recCnt << endl;
        CPPUNIT_ASSERT_MESSAGE(icss.str(), (icnt == recCnt));
    }
}
void BackStoreTestSuite::BsSetWindowSizeMaxIntB3()
{
    //RETURN_IF_WINMODE_LIMITED

    // set enormous window size (MAX INT)
    string prefix = "TC B3: Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB3";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 4096, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    //size_t wsize = UINT_MAX / 2; // 2 G
    size_t wsize = 1UL << 31; // 2 G
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid(system dependent) Large Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
}
void BackStoreTestSuite::BsSetWindowSizeMinusOneB4()
{
#if 0
    // set invalid window size = -1=ULLONG_MAX=18446744073709551615
    string prefix = "TC B4: Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreB4";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 4096, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // NOTE: set win size=18446744073709551615
    size_t wsize = (size_t)-1;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Invalid Window size=" << wsize
         << " and expected FAILure. mdbm_set_window_size returned=" << ret << endl;
    //CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == -1));
#endif
}

void BackStoreTestSuite::BsSetWindowSizeZeroB5()
{
    // create DB; set window size=0; expect failure
    string prefix = "TC B5: Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB5";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = 0;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set invalid Window size=" << wsize
         << " and expected failure. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == -1));
}

void BackStoreTestSuite::BsCheckSeveralDiffPageSizesSetWinSizeB6()
{
#if 0
// FIX BZ 5455314 - mdbm_open generates core with MDBM_OPEN_WINDOWED flag
// FIX TODO what are valid page sizes, what to expect for invalid page size?
    string prefix = "TC B6: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    // for each pagesize p in range(128 to 64M):
    //  for each number of pages range(1 to 8):
    //   limit size to number of pages; set window size to pagesize * 3;
    //   fill DB then iterate
    string baseName = "tcbackstoreB6";

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    prefix = SUITE_PREFIX() + prefix;

    stringstream itss;
    itss << prefix
         << "FAILed to fill DB=";
    stringstream icss;
    icss << prefix
         << "FAILed to fetch all keys. Current cnt=";

    int pageSizes [] = { 128, 512, 1024, 4096, 8192, MDBM_MAXPAGE };
    for (size_t cnt = 0; cnt < sizeof(pageSizes) / sizeof(int); ++cnt)
    {
        for (int lcnt = 1; lcnt < 8; ++lcnt) // various page limits for DB size
        {
            // create DB
            baseName += "B";
            string dbName = GetTmpName(baseName);
            MdbmHolder dbh(dbName);
            int dbret = dbh.Open(openflags, 0644, pageSizes[cnt], 0);
            CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

            // limit size of DB to lcnt pages
            int ret = mdbm_limit_size_v3(dbh, lcnt, 0, 0);

            // set window size=pagesize*3
            size_t wsize = pageSizes[cnt] * 3;
            ret = mdbm_set_window_size(dbh, wsize);

            // fill DB then iterate
            int recCnt = FillDb(dbh);

            MDBM_ITER iter;
            datum key = mdbm_firstkey_r(dbh, &iter);
            if (key.dsize <= 0)
            {
                itss << dbName << endl;
                CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize > 0));
            }

            int icnt;
            for (icnt = 0; key.dsize > 0; ++icnt)
            {
                key = mdbm_nextkey_r(dbh, &iter);
            }
            if (icnt != recCnt)
            {
                icss << icnt << " but number of records in DB=" << recCnt << endl;
                CPPUNIT_ASSERT_MESSAGE(icss.str(), (icnt == recCnt));
            }
        }
    }
#endif
}

void BackStoreTestSuite::BsCheckDiffPageSizesAgainstDiffWinSizesB7()
{
#if 0
// FIX BZ 5455314 - mdbm_open generates core with MDBM_OPEN_WINDOWED flag
// FIX TODO what are valid page sizes, what to expect for invalid page size?
    string prefix = "TC B7: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    // for each pagesize p in range(128 to 64M):
    //  for each window size in range(2*pagesize to 3*pagesize):
    //   set window size; limit size to bigger than window size; fill DB then iterate
    string baseName = "tcbackstoreB7";
    string dbName = GetTmpName(baseName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    prefix = SUITE_PREFIX() + prefix;

    stringstream itss;
    itss << prefix
         << "FAILed to fill DB=" << dbName
         << endl;
    stringstream icss;
    icss << prefix
         << "FAILed to fetch all keys. Current cnt=";

    int pageSizes [] = { 128, 512, 1024, 4096, 8192, MDBM_MAXPAGE };
    for (size_t cnt = 0; cnt < sizeof(pageSizes) / sizeof(int); ++cnt)
    {
        for (int wcnt = 2; wcnt < 4; ++wcnt)
        {
            // create DB
            MdbmHolder dbh(dbName);
            int dbret = dbh.Open(openflags, 0644, pageSizes[cnt], 0);
            CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

            // set window size=pagesize*wcnt
            size_t wsize = pageSizes[cnt] * wcnt;
            int ret = mdbm_set_window_size(dbh, wsize);

            // limit size of DB to bigger than window size
            int limitNumPages = wcnt + 2;
            ret = mdbm_limit_size_v3(dbh, limitNumPages, 0, 0);

            // fill DB then iterate
            int recCnt = FillDb(dbh);

            MDBM_ITER iter;
            datum key = mdbm_firstkey_r(dbh, &iter);
            CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize > 0));

            int icnt;
            for (icnt = 0; key.dsize > 0; ++icnt)
            {
                key = mdbm_nextkey_r(dbh, &iter);
            }
            if (icnt != recCnt)
            {
                icss << icnt << " but number of records in DB=" << recCnt << endl;
                CPPUNIT_ASSERT_MESSAGE(icss.str(), (icnt == recCnt));
            }
        }
    }
#endif
}
void BackStoreTestSuite::BsNPageDbSetWinSizeStoreDataSetWinSize0B8()
{
#if 0
// FIX BZ 5518388: v3: mdbm_set_window_size: returns success if window size=0; but SEGV in mdbm_store_r
    // create n page DB(limit size); set window size=z;
    // store data; reset window size=0; fetch same data
    string prefix = "TC B8: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreB8";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = pageSize * 2;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    string key = "tcbsB8key";
    string val = "tcbsB8value";
    ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using Window size=" << wsize
         << " mdbm_store_str FAILed with ret=" << ret;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    ret = mdbm_set_window_size(dbh, 0);
    stringstream swss;
    swss << prefix << "After storing data, reset Window size=0."
         << " mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(swss.str(), (ret == 0));

    char *fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data, and reset Window size=0."
         << " mdbm_fetch_str with key=" << key << "FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0));
#endif
}
void BackStoreTestSuite::BsNPageDbSetWinSize3timesPsizeStoreDataSetWinSize2timesPsizeB9()
{
    //RETURN_IF_WINMODE_LIMITED

    // create n page DB(limit size); set window size=(3*pagesize);
    // store data; reset window size=(2*pagesize); fetch same data
    string prefix = "TC B9: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB9";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    //int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |  versionFlag;
    openflags |= MDBM_OPEN_WINDOWED;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = pageSize * 3;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    string key = "tcbsB9key";
    string val = "tcbsB9value";
    ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using Window size=" << wsize
         << " mdbm_store_str FAILed with ret=" << ret;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    wsize = pageSize * 2;
    ret = mdbm_set_window_size(dbh, wsize);
    stringstream swss;
    swss << prefix << "After storing data, lower Window size=" << wsize
         << " mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(swss.str(), (ret == 0));

    char *fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data, and reset Window size=" << wsize
         << " mdbm_fetch_str with key=" << key << " FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0 && strcmp(fstr, val.c_str()) == 0));
}
void BackStoreTestSuite::BsNPageDbSetWinSize3timesPsizeStoreDataSetWinSize4timesPsizeB10()
{
    //RETURN_IF_WINMODE_LIMITED

    // create n page DB(limit size); set window size=(3*pagesize);
    // store data; reset window size=(4*pagesize); fetch same data
    string prefix = "TC B10: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB10";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = pageSize * 3;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    string key = "tcbsB10key";
    string val = "tcbsB10value";
    ret = mdbm_store_str(dbh, key.c_str(), val.c_str(), MDBM_INSERT);
    stringstream ssss;
    ssss << prefix << "Using Window size=" << wsize
         << " mdbm_store_str FAILed with ret=" << ret;
    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));

    wsize = pageSize * 4;
    ret = mdbm_set_window_size(dbh, wsize);
    stringstream swss;
    swss << prefix << "After storing data, raise Window size=" << wsize
         << " mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(swss.str(), (ret == 0));

    char *fstr = mdbm_fetch_str(dbh, key.c_str());
    stringstream fsss;
    fsss << prefix << "After storing data, and reset Window size=" << wsize
         << " mdbm_fetch_str with key=" << key << "FAILed" << endl;
    CPPUNIT_ASSERT_MESSAGE(fsss.str(), (fstr != 0 && strcmp(fstr, val.c_str()) == 0));
}
void BackStoreTestSuite::BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeLessNB11()
{
    //RETURN_IF_WINMODE_LIMITED

    // create DB with large object support; set window size=w; insert object size < w
    string prefix = "TC B11: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB11";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |
                    MDBM_OPEN_WINDOWED | MDBM_LARGE_OBJECTS | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // the window size MUST be at least 4 * page size; 4 is the MIN that works
    size_t wsize = pageSize * 4; // FAILS to store the large object using: 2 or 3
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    string key = "tcbsB11key";
    int losize = pageSize;
    char *val = new char[losize];
    memset(val, 'b', losize);
    datum dkey;
    dkey.dptr  = (char*)key.c_str();
    dkey.dsize = key.size();
    datum dval;
    dval.dptr  = val;
    dval.dsize = losize;
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    delete [] val;
    stringstream ssss;
    ssss << prefix << "Using Window size=" << wsize
         << " mdbm_store FAILed to store large object size=" << losize
         << " and returned=" << ret;
    if (ret != 0)
    {
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));
    }
}
void BackStoreTestSuite::BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeEqualNB12()
{
    //RETURN_IF_WINMODE_LIMITED

    // create DB with large object support; set window size=w; insert object size = w
    string prefix = "TC B12: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB12";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |
                    MDBM_OPEN_WINDOWED | MDBM_LARGE_OBJECTS | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = pageSize * 4;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    string key = "tcbsB12key";
    int losize = wsize;
    char *val = new char[losize];
    memset(val, 'b', losize);
    datum dkey;
    dkey.dptr  = (char*)key.c_str();
    dkey.dsize = key.size();
    datum dval;
    dval.dptr  = val;
    dval.dsize = losize;
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    delete [] val;
    stringstream ssss;
    ssss << prefix << "Using Window size=" << wsize
         << " mdbm_store UNexpectedly Succeeded to store large object size=" << losize
         << " and returned=" << ret;
    if (ret == 0)
    {
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret != 0));
    }
}
void BackStoreTestSuite::BsCreateDbLargeObjFlagSetWinSizeNinsertObjSizeGreaterNB13()
{
    //RETURN_IF_WINMODE_LIMITED

    // create DB with large object support; set window size=w; insert object size > w
    string prefix = "TC B13: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreB13";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |
                    MDBM_OPEN_WINDOWED | MDBM_LARGE_OBJECTS | versionFlag;
    int pageSize = getpagesize();
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    size_t wsize = pageSize * 4;
    int ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Set Valid Window size=" << wsize
         << " and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    string key = "tcbsB13key";
    int losize = wsize + pageSize;
    char *val = new char[losize];
    memset(val, 'b', losize);
    datum dkey;
    dkey.dptr  = (char*)key.c_str();
    dkey.dsize = key.size();
    datum dval;
    dval.dptr  = val;
    dval.dsize = losize;
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    delete [] val;
    stringstream ssss;
    ssss << prefix << "Using Window size=" << wsize
         << " mdbm_store UNexpectedly Succeeded to store large object size=" << losize
         << " and returned=" << ret;
    if (ret == 0)
    {
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret != 0));
    }
}
void BackStoreTestSuite::BsSetWindowIncreaseNumPagesFillDbB14()
{
#if 0
// FIX BZ 5455314 - mdbm_open generates core with MDBM_OPEN_WINDOWED flag
// NOTE: pageSize * 2 causes abort
// using limit size of 8 pages,  and wsize of 2 * pageSize causes the following:
// using limit size of 16 pages, and wsize of 2 * pageSize causes the following:
// E 18-092655.021638  2460 /tmp/mdbm/mlakes-2460/mdbm-00010tcbackstoreC4: Unable to allocate 1 window page(s): need a larger window size
// E 18-092655.021649  2460 : window.num_pages=2 window.first_free=2
// E 18-092655.021657  2460 : [   0] pagenum=1      num_pages=1
// E 18-092655.021669  2460 : [   1] pagenum=2      num_pages=1
// ....Aborted

// using limit num pages = 7...18 and window size = pagesize * 4
// E 18-105308.939246  8010 /tmp/mdbm/mlakes-8010/mdbm-00010tcbackstoreC4: Unable to allocate 3 window page(s): need a larger window size
// E 18-105308.939256  8010 : window.num_pages=4 window.first_free=2
// E 18-105308.939262  8010 : [   0] pagenum=3      num_pages=2
// E 18-105308.939266  8010 : [   1] pagenum=3      num_pages=0

// FIX limit size=18
// E 18-112717.990246 11171 /tmp/mdbm/mlakes-11171/mdbm-00010tcbackstoreC4: Unable to allocate 3 window page(s): need a larger window size
// E 18-112717.990267 11171 : window.num_pages=4 window.first_free=2
// E 18-112717.990280 11171 : [   0] pagenum=3      num_pages=2
// E 18-112717.990291 11171 : [   1] pagenum=3      num_pages=0
// Aborted

    string prefix = "TC B14: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreB14";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    size_t wsize = pageSize * 4; // 2 always causes abort
    ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "Using Window size=" << wsize
         << " and system page size=" << pageSize << endl;
    if (ret != 0)
    {
        wsss << "mdbm_set_window_size FAILed, returned=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
    }

    for (int cnt = 1; cnt < 256; ++cnt)
    {
        ret = mdbm_limit_size_v3(dbh, cnt, 0, 0);
        if (ret != 0)
        {
            wsss << "mdbm_limit_size_v3 FAILed setting number of pages=" << cnt << endl;
            CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
        }

        int recCnt = FillDb(dbh);
        if (recCnt <= 0)
        {
            cout << wsss.str()
                 << "WARNING: TestBase::FillDb FAILed to fill the DB...continuing..."
                 << endl << flush;
        }
    }
#endif
}

void BackStoreTestSuite::BsUseNullStatsPointerC1()
{
    // BZ 5530655: v3: mdbm_get_window_stats: SEGV with null mdbm_window_stats_t parameter
    // call using a null mdbm_window_stats_t pointer
    string prefix = "TC C1: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()
    string baseName = "tcbackstoreC1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int ret = dbh.Open(openflags, 0644, 4096, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // call mdbm_get_window_stats and expect failure
    ret = mdbm_get_window_stats(dbh, 0, sizeof(mdbm_window_stats_t));

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have FAILed upon null stats parameter. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == -1));
}
void BackStoreTestSuite::BsUseTooSmallSizeC2()
{
    //RETURN_IF_WINMODE_LIMITED

    // call using less than offsetof(mdbm_window_stats_t,w_window_size)
    string prefix = "TC C2: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreC2";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int ret = dbh.Open(openflags, 0644, 4096, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // call mdbm_get_window_stats and expect failure
    mdbm_window_stats_t wstats;
    size_t offset = offsetof(mdbm_window_stats_t,w_window_size) - 1;
    ret = mdbm_get_window_stats(dbh, &wstats, offset);

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have FAILed upon offset=" << offset
         << " being too small. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == -1));
}
void BackStoreTestSuite::BsUseBigSizeC3()
{
    //RETURN_IF_WINMODE_LIMITED

    // call using >= sizeof(mdbm_window_stats_t) gives all windowing stats
    string prefix = "TC C3: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreC3";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int ret = dbh.Open(openflags, 0644, 4096, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // call mdbm_get_window_stats and expect success
    mdbm_window_stats_t wstats;
    size_t offset = sizeof(mdbm_window_stats_t) + 1;
    ret = mdbm_get_window_stats(dbh, &wstats, offset);

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have Succeeded upon offset=" << offset
         << " being bigger than sizeof mdbm_window_stats_t. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == 0));
}
void BackStoreTestSuite::BsUseInBetweenSizeC4()
{
    //RETURN_IF_WINMODE_LIMITED

    // call using > offsetof(mdbm_window_stats_t,w_window_size)
    // and < sizeof(mdbm_window_stats_t) gives number reused and number remapped stats
    string prefix = "TC C4: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreC4";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // fill DB
    ret = mdbm_limit_size_v3(dbh, 8, 0, 0); // 8, 16 good
    size_t wsize = pageSize * 6; // 4 good: 2 always causes abort
    ret = mdbm_set_window_size(dbh, wsize);
    int recCnt = FillDb(dbh);

    // lets iterate
    stringstream ssss;
    ssss << prefix << "DB filled with " << recCnt
         << " records ";

    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    if (key.dsize == 0)
    {
        ssss << " Failed to iterate data in DB!" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (key.dsize > 0));
    }

    for (int cnt = 0; key.dsize > 0 && cnt < recCnt; ++cnt)
    {
        datum dval = mdbm_fetch(dbh, key);
        (void)dval; // unused, make compiler happy
        key = mdbm_nextkey_r(dbh, &iter);
    }

    // call mdbm_get_window_stats and expect success
    mdbm_window_stats_t wstats;
    memset(&wstats, 0, sizeof(mdbm_window_stats_t));
    size_t offset = sizeof(mdbm_window_stats_t) - 1;
    ret = mdbm_get_window_stats(dbh, &wstats, offset);

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have Succeeded upon offset=" << offset
         << " being bigger than sizeof mdbm_window_stats_t. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == 0));

    ssss << " and mdbm_get_window_stats size=" << offset
         << endl;
    // check w_num_reused and w_num_remapped
    if (wstats.w_num_reused == 0 || wstats.w_num_remapped == 0)
    {
        ssss << "Window pages should have been remapped and re-used" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_num_reused > 0 && wstats.w_num_remapped > 0));
    }

    // verify w_window_size and w_max_window_used are 0
    if (wstats.w_window_size != 0 || wstats.w_max_window_used != 0)
    {
        ssss << "Window page size=" << wstats.w_window_size
             << " and max window used=" << wstats.w_max_window_used
             << " These parameters should NOT be set" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_window_size == 0 && wstats.w_max_window_used == 0));
    }
}
void BackStoreTestSuite::BsCheckVariousWinSizesInLoopC5()
{
    //RETURN_IF_WINMODE_LIMITED

    // loop through various window sizes: set window size; call mdbm_get_window_stats
    string prefix = "TC C5: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    prefix = SUITE_PREFIX() + prefix;

    string baseName = "tcbackstoreC5";
    string dbName = GetTmpName(baseName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    stringstream wsss;
    wsss << prefix << "Using system-page-size=" << pageSize << " Set Window size=";
    mdbm_window_stats_t wstats;
    size_t offset = sizeof(mdbm_window_stats_t);

    for (size_t cnt = 2; cnt < 11; ++cnt)
    {
        // create DB
        MdbmHolder dbh(dbName);
        int dbret = dbh.Open(openflags, 0644, pageSize, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

        // set window size=pagesize*wcnt
        size_t wsize = pageSize * cnt;
        int ret = mdbm_set_window_size(dbh, wsize);
        if (ret == -1)
        {
            wsss << wsize << endl
                 << "mdbm_set_window_size FAILed, returned=" << ret << endl;
            CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
        }

        // confirm window size is expected size
        memset(&wstats, 0, sizeof(mdbm_window_stats_t));
        ret = mdbm_get_window_stats(dbh, &wstats, offset);
        if (ret != 0)
        {
            wsss << wsize << endl
                 << "mdbm_get_window_stats FAILed, returned=" << ret << endl;
            CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
        }

        if (wstats.w_window_size < wsize)
        {
            wsss << wsize << endl
                 << "Window size returned from mdbm_get_window_stats is too small=" << wstats.w_window_size << endl;
            CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));
        }
    }
}
void BackStoreTestSuite::BsNoWinModeFillDbGetStatsC6()
{
    // create DB without windowing mode; fill DB with n pages; call mdbm_get_window_stats
    string prefix = "TC C6: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreC6";
    SKIP_IF_VALGRIND()

    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    // no window mode this time
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize = getpagesize();
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // fill DB
    ret = mdbm_limit_size_v3(dbh, 8, 0, 0);
    size_t wsize = pageSize * 4;

    // this should pass
    ret = mdbm_set_window_size(dbh, wsize);
    stringstream wsss;
    wsss << prefix << "mdbm_set_window_size Should pass but returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    int recCnt = FillDb(dbh);

    // lets iterate
    stringstream ssss;
    ssss << prefix << "DB filled with " << recCnt
         << " records ";

    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    if (key.dsize == 0) {
        ssss << " Failed to iterate data in DB!" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (key.dsize > 0));
    }

    for (int cnt = 0; key.dsize > 0 && cnt < recCnt; ++cnt) {
        mdbm_fetch(dbh, key);
        key = mdbm_nextkey_r(dbh, &iter);
    }

    // call mdbm_get_window_stats and expect success
    mdbm_window_stats_t wstats;
    memset(&wstats, 0, sizeof(mdbm_window_stats_t));
    size_t offset = sizeof(mdbm_window_stats_t);
    ret = mdbm_get_window_stats(dbh, &wstats, offset);

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have Succeeded upon offset=" << offset
         << " being bigger than sizeof mdbm_window_stats_t. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == 0));

    ssss << " and mdbm_get_window_stats size=" << offset
         << endl;
    // check w_num_reused and w_num_remapped
    if (wstats.w_num_reused != 0 || wstats.w_num_remapped != 0) {
        ssss << "Window pages should have been remapped and re-used" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_num_reused == 0 && wstats.w_num_remapped == 0));
    }

    //// verify w_window_size and w_max_window_used are 0
    //if (wstats.w_window_size == 0 || wstats.w_max_window_used == 0) {
    //    ssss << "Window page size=" << wstats.w_window_size
    //         << " and max window used=" << wstats.w_max_window_used
    //         << " These parameters should not be 0" << endl;
    //    CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_window_size != 0 && wstats.w_max_window_used != 0));
    //}
}
void BackStoreTestSuite::BsWinModeNpagesFillDbNplusNpagesGetStatsC7()
{
    //RETURN_IF_WINMODE_LIMITED

    // create DB; set window size of n pages; fill DB with n+n pages; call mdbm_get_window_stats
    string prefix = "TC C7: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    SKIP_IF_VALGRIND()

    string baseName = "tcbackstoreC7";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // fill DB
    ret = mdbm_limit_size_v3(dbh, 8, 0, 0);
    size_t wsize = pageSize * 6;

    ret = mdbm_set_window_size(dbh, wsize);
    int recCnt = FillDb(dbh);

    // lets iterate
    stringstream ssss;
    ssss << prefix << "DB filled with " << recCnt
         << " records ";

    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    if (key.dsize == 0)
    {
        ssss << " Failed to iterate data in DB!" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (key.dsize > 0));
    }

    for (int cnt = 0; key.dsize > 0 && cnt < recCnt; ++cnt)
    {
        datum dval = mdbm_fetch(dbh, key);
        (void)dval; // unused, make compiler happy
        key = mdbm_nextkey_r(dbh, &iter);
    }

    // call mdbm_get_window_stats and expect success
    mdbm_window_stats_t wstats;
    memset(&wstats, 0, sizeof(mdbm_window_stats_t));
    size_t offset = sizeof(mdbm_window_stats_t);
    ret = mdbm_get_window_stats(dbh, &wstats, offset);

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have Succeeded upon offset=" << offset
         << " being bigger than sizeof mdbm_window_stats_t. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == 0));

    ssss << " and mdbm_get_window_stats size=" << offset
         << endl;
    // check w_num_reused and w_num_remapped are > 0
    if (wstats.w_num_reused == 0 || wstats.w_num_remapped == 0)
    {
        ssss << "Window pages should have been remapped and re-used" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_num_reused > 0 && wstats.w_num_remapped > 0));
    }

    // verify w_window_size and w_max_window_used are > 0
    if (wstats.w_window_size == 0 || wstats.w_max_window_used == 0)
    {
        ssss << "Window page size=" << wstats.w_window_size
             << " and max window used=" << wstats.w_max_window_used
             << " These parameters should NOT be 0" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_window_size != 0 && wstats.w_max_window_used != 0));
    }
}
void BackStoreTestSuite::BsWinModeNpagesFillDbNpagesSetWinSize0GetStatsC8()
{
#if 0
// FIX BZ 5518388: v3: mdbm_set_window_size: returns success if window size=0; but SEGV in mdbm_store_r
// FIX get an SIGABRT error due to mdbm_close calling mdbm_set_window_size
// *** glibc detected *** double free or corruption (!prev):
// Program received signal SIGABRT, Aborted.
// [Switching to Thread 46968801354080 (LWP 27164)]
// 0x00002ab7c5be521d in raise () from /lib64/tls/libc.so.6
// (gdb) where
// #0  0x00002ab7c5be521d in raise () from /lib64/tls/libc.so.6
// #1  0x00002ab7c5be6a1e in abort () from /lib64/tls/libc.so.6
// #2  0x00002ab7c5c1a291 in __libc_message () from /lib64/tls/libc.so.6
// #3  0x00002ab7c5c1feae in _int_free () from /lib64/tls/libc.so.6
// #4  0x00002ab7c5c201f6 in free () from /lib64/tls/libc.so.6
// #5  0x00002ab7c529c55a in mdbm_set_window_size (db=0x10d46c20, wsize=0) at new_mdbm.c:8054
// #6  0x00002ab7c529d5e2 in mdbm_close (db=0x10d46c20) at new_mdbm.c:5271
// #7  0x0000000000407935 in BackStoreTestSuite::BsWinModeNpagesFillDbNpagesSetWinSize0GetStatsC8 (this=Variable "this" is not available.

    // create DB; set window size of n pages; fill DB with n pages;
    // reset window size to 0 pages; call mdbm_get_window_stats
    string prefix = "TC C8: DB Window Stats: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreC8";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_WINDOWED | versionFlag;
    int pageSize = getpagesize();
    int ret = dbh.Open(openflags, 0644, pageSize, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // fill DB
    ret = mdbm_limit_size_v3(dbh, 4, 0, 0);
    size_t wsize = pageSize * 4;

    ret = mdbm_set_window_size(dbh, wsize);
    int recCnt = FillDb(dbh);

    // lets iterate
    stringstream ssss;
    ssss << prefix << "DB filled with " << recCnt
         << " records ";

    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    if (key.dsize == 0)
    {
        ssss << " Failed to iterate data in DB!" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (key.dsize > 0));
    }

    for (int cnt = 0; key.dsize > 0 && cnt < recCnt; ++cnt)
    {
        datum dval = mdbm_fetch(dbh, key);
        key = mdbm_nextkey_r(dbh, &iter);
    }

    // reset window size to 0
    ret = mdbm_set_window_size(dbh, 0);

    // call mdbm_get_window_stats and expect success
    mdbm_window_stats_t wstats;
    memset(&wstats, 0, sizeof(mdbm_window_stats_t));
    size_t offset = sizeof(mdbm_window_stats_t);
    ret = mdbm_get_window_stats(dbh, &wstats, offset);

    stringstream gsss;
    gsss << prefix << "mdbm_get_window_stats should have Succeeded upon offset=" << offset
         << " being bigger than sizeof mdbm_window_stats_t. It returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(gsss.str(), (ret == 0));

    ssss << " and mdbm_get_window_stats size=" << offset
         << endl;
    // check w_num_reused and w_num_remapped
    if (wstats.w_num_reused == 0 || wstats.w_num_remapped == 0)
    {
        ssss << "Window pages should have been remapped and re-used" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_num_reused > 0 && wstats.w_num_remapped > 0));
    }

    // verify w_window_size and w_max_window_used are 0
    if (wstats.w_window_size == 0 || wstats.w_max_window_used == 0)
    {
        ssss << "Window page size=" << wstats.w_window_size
             << " and max window used=" << wstats.w_max_window_used
             << " These parameters should NOT be 0" << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (wstats.w_window_size != 0 && wstats.w_max_window_used != 0));
    }
#endif
}

void BackStoreTestSuite::BsTestReplaceA24()
{
    string prefix = "Back Store Test Replace A24 - ";
    TRACE_TEST_CASE(prefix);

    size_t bswsize = 65536;
    string baseName("bstorereplace");
    mdbm_ubig_t cacheNumPages = 24;
    mdbm_ubig_t bsNumPages    = 320;
    int pageSize = sysconf(_SC_PAGESIZE);

    // create a Backing Store DB
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    prefix = SUITE_PREFIX() + prefix;
    string bsbaseName = baseName + string("BS");
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName);

    int ret = mdbm_set_window_size(bsdbh, bswsize);
    stringstream wsss;
    wsss << prefix << "Set Window size=" << bswsize
         << " for BS DB and expected Success. mdbm_set_window_size returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(wsss.str(), (ret == 0));

    ret = mdbm_limit_size_v3(bsdbh, bsNumPages, 0, 0);
    stringstream lsss;
    lsss << prefix << "Set limit size pages=" << bsNumPages
         << " for BS expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    // create the cache
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    ret = dbh.Open(openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    ret = mdbm_limit_size_v3(dbh, cacheNumPages, 0, 0);
    stringstream lsss2;
    lsss2 << prefix << "Set limit size pages=" << cacheNumPages
          << " for Cache DB expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss2.str(), (ret == 0));

    // set BS
    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore failed, return= " << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    // fill the DB to capacity
    int recCnt = FillDb(dbh);
    stringstream fdss;
    fdss << prefix << "FillDb failed, record count returned=" << recCnt << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(),(recCnt > 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(dbh));

    string newbsbase = baseName + string("BSRep");
    string newBsDbName = GetTmpName(newbsbase);
    string cmd("cp ");
    cmd += bsdbName + string(" ") + newBsDbName;   // Copy data to the replace-to
    CPPUNIT_ASSERT_EQUAL(0, system(cmd.c_str()));
    ret = mdbm_replace_file(bsdbName.c_str(), newBsDbName.c_str());
    stringstream rsss;
    rsss << prefix << "MDBM replace failed: " << strerror(errno) << endl;
    CPPUNIT_ASSERT_MESSAGE(rsss.str(), ret == 0);

    CPPUNIT_ASSERT_EQUAL(0, VerifyData(dbh));  // Verify again post-replace
}

void BackStoreTestSuite::BsTestReplaceWithCache()
{
    string prefix = "Back Store Test Replace With Cache Purge ";
    TRACE_TEST_CASE(prefix);

    int ret;
    string baseName("bs_replace_cache");
    int pageSize = sysconf(_SC_PAGESIZE);

    // create a Backing Store DB
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    prefix = SUITE_PREFIX() + prefix;
    string bsbaseName = baseName + string("BS");

    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName);

    string bsdb2Name = GetTmpName(bsbaseName);
    MDBM *bsdbh2 = mdbm_open(bsdb2Name.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh2 != 0));
    MdbmHolder bsdb2holder(bsdb2Name);

    const char* oldStr = "old-entry";
    const char* newStr = "new-entry";
    datum oldKv = {(char*)oldStr, 10};
    datum newKv = {(char*)newStr, 10};
    datum found = {NULL, 0};

    // add entry to backstore
    mdbm_store(bsdbh,  oldKv, oldKv, MDBM_REPLACE);
    // add different entry to replacement backstore
    mdbm_store(bsdbh2, newKv, newKv, MDBM_REPLACE);

    // create the cache
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    ret = dbh.Open(openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret != -1));

    // try to mdbm_replace_backing_store(), expect FAIL
    ret = mdbm_replace_backing_store(dbh, bsdb2Name.c_str());
    {
      stringstream msg;
      msg << prefix << "mdbm_set_backingstore expected fail, return= " << ret << endl
           << " Using DB cache file=" << dbName << endl;
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(ret != 0));
    }
    

    // set BS
    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    {
      stringstream msg;
      msg << prefix << "mdbm_set_backingstore failed, return= " << ret << endl
           << " Using DB cache file=" << dbName << endl;
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(ret == 0));
    }

    // check for old key, expect success
    found = mdbm_fetch(dbh, oldKv);
    {
      stringstream msg;
      msg << prefix << "fetch old-key failed" << endl
           << " Using DB cache file=" << dbName << endl;
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(found.dptr != NULL));
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(found.dsize != 0));
    }

    ret = mdbm_replace_backing_store(dbh, bsdb2Name.c_str());
    {
      stringstream msg;
      msg << prefix << "MDBM mdbm_replace_backing_store failed: " << strerror(errno) << endl;
      CPPUNIT_ASSERT_MESSAGE(msg.str(), ret == 0);
    }

    // TODO verify that old entry is gone, and new entry is present
    found = mdbm_fetch(dbh, oldKv);
    {
      stringstream msg;
      msg << prefix << "fetch old-key succeeded" << endl
           << " Using DB cache file=" << dbName << endl;
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(found.dptr == NULL));
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(found.dsize == 0));
    }
    found = mdbm_fetch(dbh, newKv);
    {
      stringstream msg;
      msg << prefix << "fetch new-key failed" << endl
           << " Using DB cache file=" << dbName << endl;
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(found.dptr != NULL));
      CPPUNIT_ASSERT_MESSAGE(msg.str(),(found.dsize != 0));
    }
}

void BackStoreTestSuite::BsUseMdbmBsInsertDataDeleteDataA25()
{
    // create DB with backing store as MDBM; fill DB; delete DB close the DB;
    string prefix = "TC A25: DB Back Store: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "tcbackstoreA26";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbholder(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MDBM *dbh = mdbm_open(dbName.c_str(), openflags, 0644, 512, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh != 0));

    int ret = mdbm_limit_size_v3(dbh, 2, 0, 0);
    stringstream lsss;
    lsss << prefix << "Set limit size pages=2"
          << " for Cache DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    datum dk, dv;
    dk.dptr = (char*)"key";
    dk.dsize = strlen(dk.dptr);
    dv.dptr = (char*)"parent";
    dv.dsize = sizeof(dv.dptr);

    string bsbaseName = baseName;
    bsbaseName += "BS";
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (bsdbh != 0));
    MdbmHolder bsdbholder(bsdbName); // clean up the file

    ret = mdbm_limit_size_v3(bsdbh, 8, 0, 0);
    stringstream lsss2;
    lsss2 << prefix << "Set limit size pages=8"
          << " for BS DB and expected Success. mdbm_limit_size_v3 returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss2.str(), (ret == 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore for MDBM_BSOPS_MDBM FAILed returning=" << ret << endl
         << " Using DB cache file=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    //store data
    CPPUNIT_ASSERT(0 == InsertData(dbh));
    //delete data
    CPPUNIT_ASSERT(0 == DeleteData(dbh));

    MDBM *bsdup = mdbm_dup_handle(dbh, MDBM_O_RDWR);
    CPPUNIT_ASSERT(bsdup != NULL);
    mdbm_close(bsdup);
    mdbm_close(dbh);
}

void BackStoreTestSuite::OpenTooSmallReopenWindowed()
{
    // create DB with small page size (<4k), re-open with windowed mode (fail)
    string prefix = "OpenTooSmallReopenWindowed ";
    TRACE_TEST_CASE(prefix);
    string baseName = "reopen-win-too-small";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbholder(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | versionFlag;
    MDBM *dbh = mdbm_open(dbName.c_str(), openflags, 0644, 512, 0);
    prefix = SUITE_PREFIX() + prefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh != 0));
    mdbm_close(dbh);

    openflags |= MDBM_OPEN_WINDOWED;

    dbh = mdbm_open(dbName.c_str(), openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh == 0));

    dbh = mdbm_open(dbName.c_str(), openflags, 0644, 0, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbh == 0));
}

void BackStoreTestSuite::BsTestMisc()
{
    TRACE_TEST_CASE(__func__);

    string baseName("bsmisc");
    mdbm_ubig_t cacheNumPages = 24;
    mdbm_ubig_t bsNumPages    = 320;
    int pageSize = sysconf(_SC_PAGESIZE);

    // create a Backing Store DB
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    string bsbaseName = baseName + string("BS");
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, pageSize, 0);
    CPPUNIT_ASSERT(bsdbh);
    MdbmHolder bsdbholder(bsdbName);

    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3(bsdbh, bsNumPages, 0, 0));

    // create the cache
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    CPPUNIT_ASSERT(-1 != dbh.Open(openflags, 0644, pageSize, 0));
    CPPUNIT_ASSERT(0 == mdbm_limit_size_v3(dbh, cacheNumPages, 0, 0));

    // set BS
    CPPUNIT_ASSERT(0 == mdbm_set_backingstore(dbh, MDBM_BSOPS_MDBM, bsdbh, 0));

    MDBM* dup = mdbm_dup_handle(dbh, 0);

    const char* str = "blort";
    datum keyval = { (char*)str, 6 };

    // insert
    CPPUNIT_ASSERT(mdbm_store(dbh, keyval, keyval, 0) >= 0);
    // check in dup
    errno = 0;
    (void)mdbm_fetch(dup, keyval);
    CPPUNIT_ASSERT(0 == errno);
    // delete
    CPPUNIT_ASSERT(mdbm_delete(dbh, keyval) == 0);
    // check not in dup

    mdbm_close(dup);
}


void BackStoreTestSuite::BsTestInvalid()
{
    TRACE_TEST_CASE(__func__);

    string baseName("bsinvalid");
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    string bsbaseName = baseName + string("BS");
    string bsdbName = GetTmpName(bsbaseName);
    MDBM *bsdbh = mdbm_open(bsdbName.c_str(), openflags, 0644, 0, 0);
    CPPUNIT_ASSERT(bsdbh);

    CPPUNIT_ASSERT(-1 == mdbm_set_backingstore(bsdbh, 0, 0, 0));
    mdbm_close(bsdbh);
}

void
BackStoreTestSuite::simpleFileBsTest(MDBM *dbh, bool store, const char *location)
{
    string key, kbase("key");
    string val, valbase("Avalue1234567890......................................................");
    datum k, v;
    int i = 0;
    for (; i < 100; ++i) {   // make sure some kvpairs are evicted from the cache
        key = kbase + ToStr(i);
        k.dptr = const_cast<char *>(key.c_str());
        k.dsize = key.size();
        val = valbase + ToStr(i);
        v.dptr = const_cast<char *>(val.c_str());
        v.dsize = val.size();
        if (store) {
            int ret = mdbm_store(dbh, k, v, MDBM_INSERT);
            stringstream ssss;
            ssss << "Using MDBM_BSOPS_FILE as Backing Store at " << location
                 << ", mdbm_store return= " << ret << endl;
            CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));
        } else {   // fetch
            datum retv = mdbm_fetch(dbh, k);
            stringstream ssss;
            ssss << "Using MDBM_BSOPS_FILE as Backing Store at " << location
                 << ", mdbm_fetch failed with key: " << key << endl;
            CPPUNIT_ASSERT_MESSAGE(ssss.str(), (retv.dsize == v.dsize));
        }
    }
}

