/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_get_hash  mdbm_sethash
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include <iostream>
#include <vector>
#include <algorithm>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

//#include "configstoryutil.hh"
#include "TestBase.hh"



class HashTestBase : public CppUnit::TestFixture, public TestBase
{

public:
    HashTestBase(int vFlag) : TestBase(vFlag, "Hash Test Suite") {}
    void setUp();
    void tearDown() { }

    // Asserts if file hash-ID does not match the specified hashID
    static int  assertGetHash(MDBM *dbh, const string &prefix, int hashID);

    // will try to set the specified hashID and assert upon failure
    static void assertSetHash(MDBM *dbh, const string &prefix, int hashID, bool shouldSucceed=true);

    // unit tests in this suite
    void CreateDefaultDB();         // Test Case F-1
    void OpenPreMadeDB();           // Test Case F-2
    void CreateValidSeries();       // Test Case F-3
    void CreateInvalidSeries();     // Test Case F-4
    void SetValidResetInvalid();    // Test Case F-5
    void StoreDataSetDefHash();     // Test Case F-6
    void StoreDataSetNonDefHash();  // Test Case F-7
    void PreMadeSetDefHash();       // Test Case F-8
    void PreMadeSetNonDefHash();    // Test Case F-9
    void GetKeyhashValue();         // Test Case F-10
    void GetKeyhashValInvalidHashFunc();  //Test Case F-11
    void ErrorCaseSetHash();        // Test Case F12
    void Checkhashvalue(int mode);
    int getDefaultHashID()  {
        return (versionFlag == MDBM_CREATE_V3) ? MDBM_CONFIG_DEFAULT_HASH : MDBM_HASH_FNV;
    }
    void ExerciseHashFuncs();

protected:
    void createDefaultDB(const string &prefix);
    void openPreMade(const string &prefix);
    void getKeyhashValue(const string &tcprefix);
    void getKeyhashValInvalidHashFunc(const string &tcprefix);
    void CheckHash(int hashId, const string& prefix, bool expectFail);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST

    static int _ValidHashSeries[];

    // key value pair used in unit tests
    static const char *_Key;
    static const char *_Val;
};

const char * HashTestBase::_Key = "tcF6";
const char * HashTestBase::_Val = "find me";

// NOTE: v2 doesnt support MDBM_HASH_HSIEH which is the last element below
int HashTestBase::_ValidHashSeries[] = {
    MDBM_HASH_CRC32, MDBM_HASH_EJB, MDBM_HASH_PHONG, MDBM_HASH_OZ,
    MDBM_HASH_TOREK, MDBM_HASH_FNV, MDBM_HASH_STL, MDBM_HASH_MD5,
    MDBM_HASH_SHA_1, MDBM_HASH_JENKINS, MDBM_HASH_HSIEH };

int HashTestBase::_setupCnt = 0;

void HashTestBase::setUp()
{

    if (_setupCnt++ == 0) {
        cout << endl << "Hash Test Suite Beginning..." << endl << flush;
    }
}

void HashTestBase::CreateDefaultDB()  // Test Case F-1
{
    createDefaultDB("TC F-1: CreateDefaultDB: ");
}

int HashTestBase::assertGetHash(MDBM *dbh, const string &prefix, int hashID)
{
    // 4. Call mdbm_get_hash: h
    int getHashID = mdbm_get_hash(dbh);

    stringstream ssmsg;
    ssmsg << prefix << "Expected hash ID " << hashID << " but DB hash ID=" << getHashID << endl;

    // 5. Expected results: h == MDBM_HASH_SHA_1
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (getHashID == hashID));

    return getHashID;
}
void HashTestBase::assertSetHash(MDBM *dbh, const string &prefix, int hashID, bool shouldSucceed)
{
    // mdbm_sethash: Upon success, 1 is returned. A return of 0 indicates an invalid hashid
    errno = 0;
    int ret = mdbm_sethash(dbh, hashID);
    int errnum = errno;

    // NOTE: diff in behavior between v2 and v3, if there is data, v3 returns error but v2 doesnt check
    stringstream sethmsg;
    sethmsg << prefix;
    if (shouldSucceed) {
        sethmsg  << "Failed to mdbm_sethash=" << hashID << " (errno=" << errnum << ")" << endl;
        CPPUNIT_ASSERT_MESSAGE(sethmsg.str(), (ret == 1));
    } else {
        sethmsg  << "Should have Failed to mdbm_sethash=" << hashID
                 << " Returned value should be -1, got=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(sethmsg.str(), (ret == -1));
    }
}

void HashTestBase::createDefaultDB(const string &tcprefix)
{
    TRACE_TEST_CASE(tcprefix);
    //  New DB should have default hash ID.
    string prefix = tcprefix + "DB version=" + (versionFlag == MDBM_CREATE_V3 ? "3: " : "2: ");

    // 1. Setup DB: Open and create a DB with typical defaults
    MdbmHolder dbh = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |versionFlag);

    // 2. Call mdbm_get_hash: h
    int default_hash_ID = getDefaultHashID();
    // 3. Expected result: h == default_hash_ID;
    assertGetHash(dbh, prefix, default_hash_ID);
}

void HashTestBase::OpenPreMadeDB()      // Test Case F-2
{
    openPreMade("TC F-2: OpenPreMadeDB: ");
}

void HashTestBase::openPreMade(const string &tcprefix)
{
    TRACE_TEST_CASE(tcprefix);
    string prefix = tcprefix;
    prefix += "DB version=" + versionString + ": ";

    // Pre-made DB should have default hash ID.
    // 1. Setup DB: Copy the premade DB
    MdbmHolder dbh = GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|versionFlag);

    // 3. Expected result: h == default hash ID
    int default_hash_ID = getDefaultHashID();
    assertGetHash(dbh, prefix, default_hash_ID);
}

void HashTestBase::CheckHash(int hashID, const string& prefix, bool expectFail)
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << prefix << "CheckHash: hash ID=" << hashID
           << " DB version=" << versionString;
    string prefix2 = premsg.str();

    // 1. Setup DB: Open and create a DB with typical defaults
    MdbmHolder dbh = EnsureTmpMdbm(prefix2,
                              MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_LARGE_OBJECTS | versionFlag);

    // 2. Call mdbm_sethash: cnt
    if (expectFail) {
        mdbm_sethash(dbh, hashID);
        hashID = getDefaultHashID();
    } else {
        assertSetHash(dbh, prefix2, hashID);
    }

    // 3. Call mdbm_get_hash: h
    // 4. Expected results: h == cnt
    assertGetHash(dbh, prefix2, hashID);

    if (!expectFail) {
        const int KEYLEN = 100;
        InsertData(dbh, KEYLEN, DEFAULT_PAGE_SIZE/4, DEFAULT_ENTRY_COUNT);
        VerifyData(dbh, KEYLEN, DEFAULT_PAGE_SIZE/4, DEFAULT_ENTRY_COUNT);
        InsertData(dbh, KEYLEN, DEFAULT_PAGE_SIZE*2, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);
        VerifyData(dbh, KEYLEN, DEFAULT_PAGE_SIZE*2, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);
        DeleteData(dbh, KEYLEN, DEFAULT_PAGE_SIZE/4, DEFAULT_ENTRY_COUNT);
        DeleteData(dbh, KEYLEN, DEFAULT_PAGE_SIZE*2, DEFAULT_ENTRY_COUNT, true, DEFAULT_ENTRY_COUNT);
    }
}



void HashTestBase::CreateValidSeries() // Test Case F-3
{
    string prefix = "TC F-3: CreateValidSeries";
    TRACE_TEST_CASE(prefix)

    // Set all hash id's from set of valid ID's
    // Valid test range: loop from cnt = 0 to MDBM_MAX_HASH(for v2, MDBM_MAX_HASH-1)
    int len = (sizeof(_ValidHashSeries)/sizeof(int));
    for (int i=0; i<len; ++i) {
        CheckHash(_ValidHashSeries[i], prefix, false);
    }
}

void HashTestBase::CreateInvalidSeries() // Test Case F-4
{
    string prefix = "TC F-4: CreateInvalidSeries: ";
    TRACE_TEST_CASE(prefix);

    // Set invalid hash id's for new DB. v2 Invalid values: -1, MDBM_HASH_JENKINS+1
    // v3 Invalid values: -1, MDBM_MAX_HASH + 1
    int invalidHash[] = { -1, MDBM_MAX_HASH+1 };
    int len = (sizeof(invalidHash)/sizeof(int));
    for (int i=0; i<len; ++i) {
        CheckHash(invalidHash[i], prefix, true);
    }
}

void HashTestBase::SetValidResetInvalid() // Test Case F-5
{
    string tcprefix = "TC F-5: SetValidResetInvalid: ";
    // Set invalid hash id's after setting valid non-default hash ID. Invalid values: -1, MDBM_MAX_HASH + 1
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Setup DB: Open and create a DB with typical defaults
    MDBM *dbh = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |versionFlag);

    // 2. Call mdbm_sethash: MDBM_HASH_SHA_1 = 8
    assertSetHash(dbh, prefix, MDBM_HASH_SHA_1);

    // 3. Call mdbm_sethash: -1
    int ret = mdbm_sethash(dbh, -1);

    stringstream setbad1;
    setbad1 << prefix
            << "mdbm_sethash called with invalid hash ID=-1"
            << " should have returned an error(-1) but returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(setbad1.str(), (ret == -1));

    // 4. Call mdbm_get_hash: h
    int getHashID = assertGetHash(dbh, prefix, MDBM_HASH_SHA_1);
    (void)getHashID; // unused, make compiler happy

    // 6. Call mdbm_sethash: MDBM_HASH_OZ = 3
    assertSetHash(dbh, prefix, MDBM_HASH_OZ);

    // 7. Call mdbm_sethash: MDBM_MAX_HASH + 1
    ret = mdbm_sethash(dbh, MDBM_MAX_HASH + 1);

    stringstream setbad2;
    setbad2 << prefix << "mdbm_sethash called with invalid hash ID=" << (MDBM_MAX_HASH + 1)
            << " should have returned an error but it did not!" << endl;
    CPPUNIT_ASSERT_MESSAGE(setbad2.str(), (ret == -1));

    // 8. Call mdbm_get_hash: h
    // 9. Expected results: h == MDBM_HASH_OZ
    getHashID = assertGetHash(dbh, prefix, MDBM_HASH_OZ);
    mdbm_close(dbh);
}


void HashTestBase::StoreDataSetDefHash()  // Test Case F-6
{
    // Create DB with defaults; store data; set default hash ID; then fetch the data.
    string prefix = SUITE_PREFIX() + "TC F-6: StoreDataSetDefHash: ";
    TRACE_TEST_CASE(prefix);

    // 1. Setup DB: Open and create a DB with typical defaults
    MDBM *dbh = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |versionFlag);

    // 2. Call mdbm_store_str: INSERT key="tcF6", data="find me"
    mdbm_store_str(dbh, _Key, _Val, MDBM_INSERT);

    // verify we can fetch before set hash ID
    int expected_def_hash_ID = getDefaultHashID();
    int cur_def_hash_ID = mdbm_get_hash(dbh);
    char *val = mdbm_fetch_str(dbh, _Key);
    stringstream fetchmsg;
    fetchmsg << prefix << "FAILed to find string(" << _Val << ") for key("
             << _Key << ") in DB(current default hash ID="
             << cur_def_hash_ID << ") BEFORE setting Default hash ID("
             << expected_def_hash_ID << ")" << endl;
    CPPUNIT_ASSERT_MESSAGE(fetchmsg.str(), (val != 0 && strcmp(_Val, val) == 0));

    // 3. Call mdbm_sethash: MDBM_DEFAULT_HASH
    bool expectSuccess = false;
    assertSetHash(dbh, prefix, expected_def_hash_ID, expectSuccess);

    // 4. Call mdbm_fetch_str
    val = mdbm_fetch_str(dbh, _Key);

    // 5. Expected results: successfully fetch the stored data
    stringstream foundmsg;
    foundmsg << prefix << "FAILed to find string for key(" << _Key
             << ") in DB(current  default hash ID=" << cur_def_hash_ID
             << ") AFTER setting Default hash ID(" << expected_def_hash_ID
             << ")" << endl;
    CPPUNIT_ASSERT_MESSAGE(foundmsg.str(), (val != 0 && strcmp(_Val, val) == 0));
    mdbm_close(dbh);
}


void HashTestBase::StoreDataSetNonDefHash()  // Test Case F-7
{
    // Create DB with defaults; store data; set non-default hash ID; then fetch the data.
    string tcprefix = "TC F-7: StoreDataSetNonDefHash: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create a DB with typical defaults
    MDBM *dbh = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC |versionFlag);

    // 2. Call mdbm_store_str: INSERT key="tcF6", data="find me"
    mdbm_store_str(dbh, _Key, _Val, MDBM_INSERT);

    // verify we can fetch before set hash ID
    int cur_def_hash_ID = mdbm_get_hash(dbh);
    char *val = mdbm_fetch_str(dbh, _Key);
    stringstream fetchmsg;
    fetchmsg << prefix << "FAILed to find string(" << _Val << ") for key("
             << _Key << " in DB(current default hashID=" << cur_def_hash_ID
             << ") BEFORE setting new hash ID(" << MDBM_HASH_PHONG << ")" << endl;
    CPPUNIT_ASSERT_MESSAGE(fetchmsg.str(), (val != 0 && strcmp(_Val, val) == 0));

    // 3. Call mdbm_sethash: MDBM_HASH_PHONG
    bool expectSuccess = false;
    assertSetHash(dbh, prefix, MDBM_HASH_PHONG, expectSuccess);

    // 4. Call mdbm_fetch_str: key="tcF6"
    // 5. Expected results: succed to fetch the stored data (sethash fails)
    val = mdbm_fetch_str(dbh, _Key);

    stringstream refetchmsg;
    refetchmsg << prefix << "Didn't find the string(" << (val ? val : "ignore") << ") for key("
             << _Key << " in DB(current hashID=" << MDBM_HASH_PHONG
             << ") but should have (hash ID change attempted between store and fetch)" << endl;

    CPPUNIT_ASSERT_MESSAGE(refetchmsg.str(), (val != NULL));
    mdbm_close(dbh);
}

void HashTestBase::PreMadeSetDefHash()       // Test Case F-8
{
    // Pre-made DB that had known limits set (page size and number of pages).
    string tcprefix = "TC F-8: PreMadeSetDefHash: ";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Setup DB: Copy the premade DB
    MDBM *dbh = GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|versionFlag);

    // 2. Call mdbm_sethash: MDBM_DEFAULT_HASH
    int cur_def_hash_ID      = mdbm_get_hash(dbh);
    int expected_def_hash_ID = getDefaultHashID();
    bool expectSuccess = false;
    assertSetHash(dbh, prefix, expected_def_hash_ID, expectSuccess);

    // 3. Call mdbm_fetch: known key="pass1"
    int ret = VerifyData(dbh, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, 1);

    // 4. Expected results: successfully fetch the stored data
    stringstream foundmsg;
    foundmsg << prefix << "FAILed to find string for key(" << 0
             << ") in DB(current  default hash ID=" << cur_def_hash_ID
             << ") AFTER setting Default hash ID(" << expected_def_hash_ID
             << ")" << endl;
    CPPUNIT_ASSERT_MESSAGE(foundmsg.str(), ret == 0);
    mdbm_close(dbh);
}

void HashTestBase::PreMadeSetNonDefHash()    // Test Case F-9
{
    // Open pre-made DB with data; set non-default hash ID; fetch data
    string tcprefix = "TC F-9: PreMadeSetNonDefHash: ";
    string prefix = SUITE_PREFIX() +tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Copy the premade DB
    MDBM *dbh = GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR|versionFlag);

    // 2. Call mdbm_sethash: MDBM_HASH_MD5
    // NOTE: if there is data, sethash returns error
    int cur_def_hash_ID      = mdbm_get_hash(dbh);
    bool expectSuccess = false;
    assertSetHash(dbh, prefix, MDBM_HASH_MD5, expectSuccess);

    // 3. Call mdbm_fetch: known key="pass1"
    int ret = VerifyData(dbh, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, 1);

    // 4. Expected results: succeed to fetch the stored data
    stringstream foundmsg;
    foundmsg << prefix << "Didn't find value "
             << "for key(" << 0 << ") in DB(current  default hash ID="
             << cur_def_hash_ID << ") AFTER attempt to change hash ID(" << MDBM_HASH_MD5
             << ") but should have (sethash should have failed)" << endl;
    CPPUNIT_ASSERT_MESSAGE(foundmsg.str(), ret == 0);
    mdbm_close(dbh);
}


void HashTestBase::GetKeyhashValue()    // Test Case F-10
{
    // Open pre-made DB with data; set non-default hash ID; fetch data
    getKeyhashValue("TC F-10: getKeyhashValue: ");
}

void HashTestBase::getKeyhashValue(const string &tcprefix)
{
    string prefix = SUITE_PREFIX() +tcprefix;
    TRACE_TEST_CASE(tcprefix);
    int i;

    for (i=0; i<=MDBM_MAX_HASH; i++)
       Checkhashvalue(i);
}

void HashTestBase::Checkhashvalue(int hashMode)
{
    datum key;
    int i;
    //Create key
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);
    uint32_t hashValue, hashValueTemp = 0;

    CPPUNIT_ASSERT_EQUAL(0, mdbm_get_hash_value(key, hashMode, &hashValue));
    CPPUNIT_ASSERT(hashValue != 0);

    //The hash value should be same on each run with same key and same hash function
    for (i=0; i<100; i++)
    {
       CPPUNIT_ASSERT_EQUAL(0, mdbm_get_hash_value(key, hashMode, &hashValueTemp));
       CPPUNIT_ASSERT_EQUAL(hashValue, hashValueTemp);
    }
}

void HashTestBase::GetKeyhashValInvalidHashFunc()    // Test Case F-11
{
    // Open pre-made DB with data; set non-default hash ID; fetch data
    getKeyhashValInvalidHashFunc("TC F-11: getKeyhashValInvalidHashFunc: ");
}

void HashTestBase::getKeyhashValInvalidHashFunc(const string &tcprefix)
{
    string prefix = SUITE_PREFIX() +tcprefix;
    TRACE_TEST_CASE(tcprefix);
    datum key;

    //Create key
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);
    uint32_t hashValue = 0;

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_get_hash_value(key, 100, &hashValue));
}

#define bucketCount 16
void HashTestBase::ExerciseHashFuncs()
{
  int bucket[bucketCount];
  int i, h, k;
  uint32_t hash;
  datum d;
  char buf[128];
  for (h = 0; h < MDBM_MAX_HASH; ++h) {
    for (i=0; i<bucketCount; ++i) {
      bucket[i] = 0;
    }
    for (k=0; k<1024; ++k) {
      d.dptr = buf;
      d.dsize = snprintf(buf, 128, "hashing %d", k);
      mdbm_get_hash_value(d, h, &hash);
      ++bucket[hash%bucketCount];
    }
    fprintf(stderr, "HashFunction %d distribution: [ ", h);
    for (i=0; i<bucketCount; ++i) {
      float pct = ((float)bucket[i]*bucketCount)/1024.0;
      CPPUNIT_ASSERT(pct > 0.5f);
      CPPUNIT_ASSERT(pct < 2.0f);
      fprintf(stderr, "%3.3f, ", pct);
    }
    fprintf(stderr, " ]\n");
  }

}

void HashTestBase::ErrorCaseSetHash()    // Test Case F-12
{
    string tcprefix = "TC F-12: ErrorcaseSetHash: ";
    TRACE_TEST_CASE(tcprefix);
    int hashID=0;
    MDBM *dbh=NULL;

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_sethash(dbh, hashID));
}

class HashTestV3 : public HashTestBase
{
    CPPUNIT_TEST_SUITE(HashTestV3);
    CPPUNIT_TEST(CreateDefaultDB);
    CPPUNIT_TEST(OpenPreMadeDB);
    CPPUNIT_TEST(CreateValidSeries);
    CPPUNIT_TEST(CreateInvalidSeries);
    CPPUNIT_TEST(SetValidResetInvalid);
    CPPUNIT_TEST(StoreDataSetDefHash);
    CPPUNIT_TEST(StoreDataSetNonDefHash);
    CPPUNIT_TEST(PreMadeSetDefHash);
    CPPUNIT_TEST(PreMadeSetNonDefHash);
    CPPUNIT_TEST(GetKeyhashValue);
    CPPUNIT_TEST(GetKeyhashValInvalidHashFunc);
    CPPUNIT_TEST(ErrorCaseSetHash);
    CPPUNIT_TEST(ExerciseHashFuncs);
    CPPUNIT_TEST_SUITE_END();

public:
    HashTestV3() : HashTestBase(MDBM_CREATE_V3) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(HashTestV3);


