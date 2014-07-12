/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// FIX BZ 5509909: v3: mdbm_get_cachemode_name doesnt handle cachemode = MDBM_CACHEMODE_EVICT_CLEAN_FIRST
#include <unistd.h>
#include <string.h>
#include <sys/stat.h>

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
#include "test_cache.hh"

int CacheBaseTestSuite::_ValidCacheModes[] = {
    MDBM_CACHEMODE_NONE, MDBM_CACHEMODE_LFU, MDBM_CACHEMODE_LRU,
    MDBM_CACHEMODE_GDSF, MDBM_CACHEMODE_EVICT_CLEAN_FIRST
};
int CacheBaseTestSuite::_ValidCacheModeLen = sizeof(_ValidCacheModes) / sizeof(int);

int CacheBaseTestSuite::_InvalidCacheModes[] = {
    4, 5, 6, 7, 15
};
int CacheBaseTestSuite::_InvalidCacheModeLen = sizeof(_InvalidCacheModes) / sizeof(int);

string CacheBaseTestSuite::_ModeNone  = "none";
string CacheBaseTestSuite::_ModeLFU   = "LFU";
string CacheBaseTestSuite::_ModeLRU   = "LRU";
string CacheBaseTestSuite::_ModeGDSF  = "GDSF";
string CacheBaseTestSuite::_ModeCLEAN = "CLEAN";

int CacheBaseTestSuite::_setupCnt = 0;

void CacheBaseTestSuite::setUp()
{
    if (_setupCnt++ == 0)
    {
        cout << endl << suiteName << " Beginning..." << endl << flush;
    }
}

void CacheBaseTestSuite::CacheAddDataThenSetValidModeA1()
{
    string trprefix = "TC A1: Partly filled DB Cache: ";
    TRACE_TEST_CASE(trprefix);
    // create DB and store some data
    string baseName = "tcCacheA1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_store_str(dbh, baseName.c_str(), "cachetcA3val", MDBM_INSERT);

    // set a valid cache mode and expect an error since it contains data
    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_LFU);

    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode should NOT have succeeded setting mode=" << MDBM_CACHEMODE_LFU
         << " because the DB already has data in it" << endl;
    CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == -1));
}
void CacheBaseTestSuite::CacheStoreLargeObjA2()
{
    string trprefix = "TC A2: Large Object DB Cache: ";
    TRACE_TEST_CASE(trprefix);
    // create DB and store large object
    string baseName = "tcCacheA2";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_LARGE_OBJECTS | versionFlag;
    int pageSize = 512;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // set a valid cache mode
    int ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_LFU);

    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode FAILed setting mode=" << MDBM_CACHEMODE_LFU << endl;
    CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == 0));

    string largeobj(512, '4');
    datum dkey;
    dkey.dptr  = (char*)baseName.c_str();
    dkey.dsize = baseName.size();

    datum dval;
    dval.dsize = largeobj.size();
    dval.dptr  = (char*)largeobj.c_str();
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);

    stringstream stss;
    stss << prefix << "mdbm_store FAILed storing large object size=" << largeobj.size()
         << " and cache mode=" << MDBM_CACHEMODE_LFU << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
}

void CacheBaseTestSuite::cacheAllModesModifyNewKey(string &prefix, int storeFlags, int expectedStoreRet)
{
    TRACE_TEST_CASE(prefix);
    string baseName = "tcCacheAllModes";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    // for each cache mode: store using storeFlags with a new key
    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode FAILed setting mode=";

    stringstream stss;
    stss << prefix << "mdbm_store_str with key=" << baseName.c_str()
         << " using store flags=" << storeFlags;
    // lets skip over MDBM_CACHEMODE_NONE
    for (int cnt = 1; cnt < _ValidCacheModeLen; ++cnt)
    {
        MdbmHolder dbh(dbName);

        int dbret = dbh.Open(openflags, 0644, 512, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

        int ret = mdbm_set_cachemode(dbh, _ValidCacheModes[cnt]);
        if (ret == -1)
        {
            cmss << _ValidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == 0));
        }

        // store a new key in the cache
        ret = mdbm_store_str(dbh, baseName.c_str(), "cachetcAllval", storeFlags);
        if (ret != expectedStoreRet) {
            stss << " returned FAILure=" << ret
                 << " with cache mode(" << cnt << ")=" << _ValidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == expectedStoreRet));
        }

        char *val = mdbm_fetch_str(dbh, baseName.c_str());
        if (expectedStoreRet != MDBM_STORE_SUCCESS) {
          // since this is a cache and it was a new key, it should NOT have
          // store the new entry, lets verify it returns null
          if (val) {
              stss << " cache mode=" <<  _ValidCacheModes[cnt]
                   << " Value should NOT exist in the DB, yet we have fetched val=" << val << endl;
              CPPUNIT_ASSERT_MESSAGE(stss.str(), (val == NULL));
          }
        } else if (!val) {
            stss << " cache mode=" <<  _ValidCacheModes[cnt]
                 << " Value should exist in the DB, yet we have fetched val=NULL" << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (val == NULL));
        }
    }
}
void CacheBaseTestSuite::CacheAllModesModifyNewKeyA3()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC A3 Cache(MDBM_CACHE_MODIFY new): ";
    cacheAllModesModifyNewKey(prefix, MDBM_CACHE_MODIFY, MDBM_STORE_SUCCESS);
}

void CacheBaseTestSuite::cacheAllModesModifyOldKey(string &prefix, int storeFlags, int expectedStoreRet)
{
    TRACE_TEST_CASE(prefix);
    string baseName = "tcCacheAllOldkey";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    // for each cache mode: store using storeFlags with a new value
    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode FAILed setting mode=";
    stringstream stss;
    stss << prefix << "mdbm_store_str with key=" << baseName.c_str();
    // lets skip over MDBM_CACHEMODE_NONE
    for (int cnt = 1; cnt < _ValidCacheModeLen; ++cnt) {
        MdbmHolder dbh(dbName);

        int dbret = dbh.Open(openflags, 0644, 512, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

        int ret = mdbm_set_cachemode(dbh, _ValidCacheModes[cnt]);
        if (ret == -1)
        {
            cmss << _ValidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == 0));
        }

        // store a new key
        ret = mdbm_store_str(dbh, baseName.c_str(), "cachetcAllval", MDBM_CACHE_REPLACE);
        if (ret == -1) {
            stss << " should have succeeded to store using the CACHE REPLACE flag"
                 << " and cache mode=" << _ValidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
        }
        // replace the value now
        string replacementVal = "cachetcAllval_replaced";
        ret = mdbm_store_str(dbh, baseName.c_str(), replacementVal.c_str(), storeFlags);
        if (ret != expectedStoreRet) {
            stss << " store using the flags=" << storeFlags << " FAILed=" << ret
                 << " with cache mode(" << cnt << ")=" << _ValidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == expectedStoreRet));
        }

        // since this is a cache and it was a existing key, it should have
        // stored the new value, lets verify it replaced the old value
        char *val = mdbm_fetch_str(dbh, baseName.c_str());
        if (!val) {
            stss << " after storing using flags=" << storeFlags
                 << " cache mode=" <<  _ValidCacheModes[cnt]
                 << " Value should exist in the DB but does NOT" << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (val));
        }
        string fval = val;
        bool replaced = fval.compare(replacementVal) == 0;
        if (expectedStoreRet == MDBM_STORE_SUCCESS) {
          stss << " after storing using the flags=" << storeFlags
               << " cache mode=" <<  _ValidCacheModes[cnt]
               << " Value should have been replaced, but we have fetched val=" << val << endl;
          CPPUNIT_ASSERT_MESSAGE(stss.str(), (replaced));
        } else {
          stss << " after storing using the flags=" << storeFlags
               << " cache mode=" <<  _ValidCacheModes[cnt]
               << " Value should NOT have been replaced, but we have fetched val=" << val << endl;
          CPPUNIT_ASSERT_MESSAGE(stss.str(), (!replaced));
        }
    }
}
void CacheBaseTestSuite::CacheAllModesModifyOldKeyA4()
{
    // NOTE: MDBM_CACHE_MODIFY is ignored (default MDBM_INSERT) unless there is a backing store
    string prefix = SUITE_PREFIX();
    prefix += "TC A4 Cache(MDBM_CACHE_MODIFY existing): ";
    cacheAllModesModifyOldKey(prefix, MDBM_CACHE_MODIFY, MDBM_STORE_ENTRY_EXISTS);
}
void CacheBaseTestSuite::setCacheModifyNewKey(string &prefix, int cacheFlags, int storeFlags, int expectedStoreRet, bool expectStored)
{
    TRACE_TEST_CASE(prefix);
    // do not set cache mode but store using MDBM_CACHE_MODIFY
    string baseName = "tcSetCacheModNew";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    MdbmHolder dbh(dbName);
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_set_cachemode(dbh, cacheFlags);

    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode=" << cacheFlags
         << " and attempt store a NEW key=" << baseName.c_str()
         << " with flag=" << storeFlags << endl;

    // store a new key in the cache
    ret = mdbm_store_str(dbh, baseName.c_str(), "cachetcval", storeFlags);
    if (ret != expectedStoreRet)
    {
        cmss << endl << "mdbm_store_str returned FAILure=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == expectedStoreRet));
    }

    char *val = mdbm_fetch_str(dbh, baseName.c_str());
    if (val && expectStored == false)
    {
        cmss << endl << "After storing value should NOT exist in the DB, yet we have fetched val=" << val << endl;
        CPPUNIT_ASSERT_MESSAGE(cmss.str(), (val == (char*)0));
    }
}
void CacheBaseTestSuite::CacheNoModeModifyNewKeyA5()
{
    // NOTE: MDBM_CACHE_MODIFY is ignored (default MDBM_INSERT) unless there is a backing store
    string prefix = SUITE_PREFIX();
    prefix += "TC A5 Cache(MDBM_CACHEMODE_NONE, MDBM_CACHE_MODIFY): ";
    // no cachemode set, so MDBM_CACHE_MODIFY should be ignored
    setCacheModifyNewKey(prefix, MDBM_CACHEMODE_NONE, MDBM_CACHE_MODIFY, MDBM_STORE_SUCCESS, true);
}
void CacheBaseTestSuite::setCacheModifyOldKey(string &prefix, int cacheFlags, int storeFlags, int expectedStoreRet)
{
    TRACE_TEST_CASE(prefix);
    string baseName = "tcSetCacheModOldKey";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;

    MdbmHolder dbh(dbName);
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_set_cachemode(dbh, cacheFlags);

    ret = mdbm_store_str(dbh, baseName.c_str(), "cachetcval", MDBM_CACHE_REPLACE);
    if (ret == -1)
    {
        stringstream stss;
        stss << prefix << "mdbm_store_str with key=" << baseName.c_str();
        stss << endl << "Should have succeeded to store using the CACHE REPLACE flag" << endl;
        CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
    }

    // replace the value now
    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode=" << cacheFlags
         << " and attempt store an OLD key=" << baseName.c_str()
         << " with flag=" << storeFlags << endl;

    string replacementVal = "cachetcval_replaced";
    ret = mdbm_store_str(dbh, baseName.c_str(), replacementVal.c_str(), storeFlags);
    if (ret != expectedStoreRet)
    {
        cmss << endl << "mdbm_store_str returned FAILure=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == expectedStoreRet));
    }

    // since this is a cache and it was a existing key, it should have
    // stored the new value, lets verify it replaced the old value
    char *val = mdbm_fetch_str(dbh, baseName.c_str());
    if (!val)
    {
        cmss << endl << "After storing value should NOT exist in the DB, yet we have fetched val=" << val << endl;
        CPPUNIT_ASSERT_MESSAGE(cmss.str(), (val));
    }
    string fval = val;
    bool replaced = fval.compare(replacementVal) != 0;
    if (!replaced)
    {
        cmss << endl << "After storing value should NOT exist in the DB, yet we have fetched val=" << val << endl;
        CPPUNIT_ASSERT_MESSAGE(cmss.str(), (replaced));
    }
}
void CacheBaseTestSuite::CacheNoModeModifyOldKeyA6()
{
    // NOTE: MDBM_CACHE_MODIFY is ignored (default MDBM_INSERT) unless there is a backing store
    string prefix = SUITE_PREFIX();
    prefix += "TC A6 Cache(MDBM_CACHEMODE_NONE, MDBM_CACHE_MODIFY): ";
    // no cachemode set, so MDBM_CACHE_MODIFY will be ignored
    setCacheModifyOldKey(prefix, MDBM_CACHEMODE_NONE, MDBM_CACHE_MODIFY, MDBM_STORE_ENTRY_EXISTS);
}
void CacheBaseTestSuite::CacheModifyInsertNewKeyA7()
{
    // mdbm_store using MDBM_CACHE_MODIFY, MDBM_INSERT flags for a new key
    string prefix = SUITE_PREFIX();
    prefix += "TC A7 Cache(MDBM_CACHEMODE_LRU, MDBM_CACHE_MODIFY|MDBM_INSERT): ";
    // expect return=0 and since MDBM_INSERT takes predecence the entry should
    // be inserted
    setCacheModifyNewKey(prefix, MDBM_CACHEMODE_LRU, MDBM_CACHE_MODIFY|MDBM_INSERT, MDBM_STORE_SUCCESS, true);
}
void CacheBaseTestSuite::CacheModifyInsertOldKeyA8()
{
    // mdbm_store using MDBM_CACHE_MODIFY, MDBM_INSERT flags for an existing key
    string prefix = SUITE_PREFIX();
    prefix += "TC A8 Cache(MDBM_CACHEMODE_LRU, MDBM_INSERT|MDBM_CACHE_MODIFY): ";
    // expect return=1 (implies INSERT took precedence over CACHE_MODIFY)
    setCacheModifyOldKey(prefix, MDBM_CACHEMODE_LRU, MDBM_INSERT|MDBM_CACHE_MODIFY, MDBM_STORE_ENTRY_EXISTS);
}
void CacheBaseTestSuite::CacheAllModesCacheOnlyNewKeyA9()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC Cache A9: ";
    cacheAllModesModifyNewKey(prefix, MDBM_CACHE_ONLY | MDBM_MODIFY, -1);
}
void CacheBaseTestSuite::CacheAllModesCacheOnlyOldKeyA10()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC Cache A10: ";
    cacheAllModesModifyOldKey(prefix, MDBM_CACHE_ONLY | MDBM_MODIFY, MDBM_STORE_SUCCESS);
}
void CacheBaseTestSuite::CacheNoModeCacheOnlyNewKeyA11()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC A11 Cache(MDBM_CACHEMODE_NONE, MDBM_CACHE_ONLY|MDBM_MODIFY): ";
    setCacheModifyNewKey(prefix, MDBM_CACHEMODE_NONE, MDBM_CACHE_ONLY|MDBM_MODIFY, -1, false);
}
void CacheBaseTestSuite::CacheNoModeCacheOnlyOldKeyA12()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC A12 Cache(MDBM_CACHEMODE_NONE, MDBM_INSERT|MDBM_CACHE_ONLY): ";
    setCacheModifyOldKey(prefix, MDBM_CACHEMODE_NONE, MDBM_INSERT|MDBM_CACHE_ONLY, MDBM_STORE_ENTRY_EXISTS);
}
void CacheBaseTestSuite::CacheOnlyInsertNewKeyA13()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC A13 Cache(MDBM_CACHEMODE_LFU, MDBM_CACHE_ONLY|MDBM_INSERT): ";
    setCacheModifyNewKey(prefix, MDBM_CACHEMODE_LFU, MDBM_CACHE_ONLY|MDBM_INSERT, MDBM_STORE_SUCCESS, true);
}
void CacheBaseTestSuite::CacheOnlyInsertOldKeyA14()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC A14 Cache(MDBM_CACHEMODE_LFU, MDBM_INSERT|MDBM_CACHE_ONLY): ";
    setCacheModifyOldKey(prefix, MDBM_CACHEMODE_LFU, MDBM_INSERT|MDBM_CACHE_ONLY, MDBM_STORE_ENTRY_EXISTS);
}

void CacheBaseTestSuite::CacheNoSetModeThenGetModeB1()
{
    string trprefix = "TC B1: DB Cache: ";
    TRACE_TEST_CASE(trprefix);
    // specify NO cache mode; call mdbm_get_cachemode
    string baseName = "tcCacheB1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int ret = mdbm_get_cachemode(dbh);

    stringstream cmss;
    cmss << prefix << "mdbm_get_cachemode returned=" << ret
         << " which should have matched MDBM_CACHEMODE_NONE=" << MDBM_CACHEMODE_NONE << endl;

    CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == MDBM_CACHEMODE_NONE));
}
void CacheBaseTestSuite::CacheAllValidModesSetAndGetModeB2()
{
    string trprefix = "TC B2: DB Cache: ";
    TRACE_TEST_CASE(trprefix);
    // for each valid cache mode; call mdbm_set_cachemode, mdbm_get_cachemode
    string baseName = "tcCacheB2";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // for each valid cache mode
    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode FAILed setting mode=";
    stringstream gmss;
    gmss << prefix << "mdbm_get_cachemode FAILed matching mode set=";
    for (int cnt = 0; cnt < _ValidCacheModeLen; ++cnt)
    {
        int ret = mdbm_set_cachemode(dbh, _ValidCacheModes[cnt]);
        if (ret == -1)
        {
            cmss << _ValidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == 0));
        }

        ret = mdbm_get_cachemode(dbh);
        if (ret != _ValidCacheModes[cnt])
        {
            gmss << prefix << "mdbm_get_cachemode returned=" << ret
                 << " which should have matched what was previously set=" << _ValidCacheModes[cnt] << endl;

            CPPUNIT_ASSERT_MESSAGE(gmss.str(), (ret == _ValidCacheModes[cnt]));
        }
    }
}
void CacheBaseTestSuite::CacheAllInvalidModesSetAndGetModeB3()
{
    string trprefix = "TC B3: Cache DB: ";
    TRACE_TEST_CASE(trprefix);
    // for each invalid cache mode; call mdbm_set_cachemode, mdbm_get_cachemode
    string baseName = "tcCacheB3";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += trprefix;
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int validFlags = MDBM_CACHEMODE_LFU | MDBM_CACHEMODE_LRU | MDBM_CACHEMODE_GDSF | MDBM_CACHEMODE_EVICT_CLEAN_FIRST;

    // for each invalid cache mode
    stringstream cmss;
    cmss << prefix << "mdbm_set_cachemode should NOT have succeeded setting invalid mode=";
    stringstream gmss;
    gmss << prefix << "mdbm_get_cachemode should return MDBM_CACHEMODE_NONE but matched a cache mode setting=";
    for (int cnt = 0; cnt < _InvalidCacheModeLen; ++cnt)
    {
        int ret = mdbm_set_cachemode(dbh, _InvalidCacheModes[cnt]);
        if (ret == 0)
        {
            cmss << _InvalidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(cmss.str(), (ret == -1));
        }

        ret = mdbm_get_cachemode(dbh);
        if (ret & validFlags)
        {
            gmss << ret << endl
                 << "The last invalid cache mode setting was=" << _InvalidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(gmss.str(), (ret == MDBM_CACHEMODE_NONE));
        }
    }
}

// return true if the mode matches correctly to the associated cacheMode
bool CacheBaseTestSuite::matchModeString(const char *mode, int cacheMode)
{
    if (!mode)
    {
        return false;
    }
    bool match = false;
    switch (cacheMode)
    {
        case MDBM_CACHEMODE_NONE:
            match = _ModeNone.compare(mode) == 0;
            break;
        case MDBM_CACHEMODE_LFU:
            match = _ModeLFU.compare(mode) == 0;
            break;
        case MDBM_CACHEMODE_LRU:
            match = _ModeLRU.compare(mode) == 0;
            break;
        case MDBM_CACHEMODE_GDSF:
            match = _ModeGDSF.compare(mode) == 0;
            break;
        case MDBM_CACHEMODE_EVICT_CLEAN_FIRST:
            match = _ModeCLEAN.compare(mode) == 0;
            break;
        default: ;
    }
    return match;
}
void CacheBaseTestSuite::CacheAllValidModesSetAndGetModeNameC1()
{
    string trprefix = "TC C1: Cache DB: ";
    TRACE_TEST_CASE(trprefix);
#if 0
// FIX BZ 5509909 - v3: mdbm_get_cachemode_name doesnt handle cachemode = MDBM_CACHEMODE_EVICT_CLEAN_FIRST
    // for each valid cache mode; mdbm_get_cachemode_name
    string prefix = SUITE_PREFIX();
    prefix += trprefix;

    // for each valid cache mode
    stringstream gmss;
    gmss << prefix << "mdbm_get_cachemode_name should return appropriate string for valid cache mode=";
    for (int cnt = 0; cnt < _ValidCacheModeLen; ++cnt)
    {
        const char *mode  = mdbm_get_cachemode_name(_ValidCacheModes[cnt]);
        bool matchmode = matchModeString(mode, _ValidCacheModes[cnt]);
        if (!matchmode)
        {
            mode = !mode ? "<nothing returned>" : mode;
            gmss << _ValidCacheModes[cnt] << endl
                 << "The cache mode string returned was=" << mode << endl;
            CPPUNIT_ASSERT_MESSAGE(gmss.str(), (matchmode));
        }
    }
#endif
}
void CacheBaseTestSuite::CacheAllInvalidModesSetAndGetModeNameC2()
{
    string trprefix = "TC C2: Cache DB: ";
    TRACE_TEST_CASE(trprefix);
    // for each invalid cache mode; mdbm_get_cachemode_name
    string prefix = SUITE_PREFIX();
    prefix += trprefix;

    // for each invalid cache mode
    string unknown = "unknown";
    stringstream gmss;
    gmss << prefix << "mdbm_get_cachemode_name should return 'unknown' but matched a cache mode setting=";
    for (int cnt = 0; cnt < _InvalidCacheModeLen; ++cnt)
    {
        const char *mode  = mdbm_get_cachemode_name(_InvalidCacheModes[cnt]);
        bool matchmode = matchModeString(mode, _InvalidCacheModes[cnt]);
        if (matchmode)
        {
            mode = !mode ? "<nothing returned>" : mode;
            gmss << mode << endl
                 << "The invalid cache mode setting was=" << _InvalidCacheModes[cnt] << endl;
            CPPUNIT_ASSERT_MESSAGE(gmss.str(), (!matchmode));
        }
    }
}

void CacheBaseTestSuite::CacheChurn()
{
    TRACE_TEST_CASE(__func__);
    int i, cnt;
    char keybuf[128];
    char largebuf[1024];
    memset(largebuf, '*', 1023);
    largebuf[1023] = 0; // trailing null
    // for each valid cache mode, except NONE
    for (cnt=1; cnt < _ValidCacheModeLen; ++cnt) {
        string baseName = "tcCacheChurn";
        string dbName = GetTmpName(baseName);
        MdbmHolder dbh(dbName);

        int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
        CPPUNIT_ASSERT(dbh.Open(openflags, 0644, 512, 0) >= 0);
        CPPUNIT_ASSERT(mdbm_limit_size_v3(dbh, 3, NULL, NULL) >= 0);
        CPPUNIT_ASSERT(mdbm_set_cachemode(dbh, _ValidCacheModes[cnt]) >= 0);

        for (i=0; i<500; ++i) {
          snprintf(keybuf, sizeof(keybuf), "key_number_%d", i);
          mdbm_store_str(dbh, keybuf, "cachetcval", 0);
        }
        snprintf(keybuf, sizeof(keybuf), "key_number_%d", ++i);
        mdbm_store_str(dbh, keybuf, largebuf, 0);
        snprintf(keybuf, sizeof(keybuf), "key_number_%d", ++i);
        mdbm_store_str(dbh, keybuf, largebuf, 0);
        snprintf(keybuf, sizeof(keybuf), "key_number_%d", ++i);
        mdbm_store_str(dbh, keybuf, largebuf, 0);
    }
}

void
CacheBaseTestSuite::CacheLimitSize()
{
    string prefix("CacheLimitSize");
    TRACE_TEST_CASE(prefix);
    string baseName(prefix);
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int pgsize = 512;
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, pgsize, 0);
    CPPUNIT_ASSERT(dbret != -1);

    int ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_GDSF);
    stringstream scss;
    scss << prefix << "mdbm_set_cachemode failed" << endl;
    CPPUNIT_ASSERT_MESSAGE(scss.str(), (ret == 0));

    ret = mdbm_limit_size_v3(dbh, 4, 0, 0);  // 2K limit
    stringstream lsss;
    lsss << prefix << "mdbm_limit_size_v3 with limit of 4 pages failed" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    ret = mdbm_set_backingstore(dbh, MDBM_BSOPS_FILE, NULL, O_DIRECT | O_RDWR | O_CREAT | O_TRUNC);
    stringstream bsss;
    bsss << prefix << "mdbm_set_backingstore using MDBM_BSOPS_FILE failed" << endl
         << " Using DB cache file=" << dbName << " errno is " << strerror(errno) << endl;
    CPPUNIT_ASSERT_MESSAGE(bsss.str(),(ret == 0));

    string key, kbase("key");
    string val, valbase("Avalue1234567890......................................................");
    datum k, v;
    for (int i = 0; i < 100; ++i) {   // 100 * 72 chars > 2K limit_size
        key = kbase + ToStr(i);
        k.dptr = const_cast<char *>(key.c_str());
        k.dsize = key.size();
        val = valbase + ToStr(i);
        v.dptr = const_cast<char *>(val.c_str());
        v.dsize = val.size();
        ret = mdbm_store(dbh, k, v, MDBM_INSERT);
        stringstream ssss;
        ssss << prefix << "Using MDBM_BSOPS_FILE as Backing Store, mdbm_store failed return="
             << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(ssss.str(), (ret == 0));
    }
    CPPUNIT_ASSERT_EQUAL(0, mdbm_sync(dbh));
    dbh.Close();
    struct stat statbuf;
    CPPUNIT_ASSERT(0 == stat(dbName.c_str(), &statbuf));
    CPPUNIT_ASSERT(5 * pgsize <= statbuf.st_size);
}

