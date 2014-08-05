/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef DONT_MULTI_INCLUDE_TEST_BASE
#define DONT_MULTI_INCLUDE_TEST_BASE

#include <errno.h>
#include <pwd.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

#include <string>
#include <map>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>


#include "mdbm.h"
#include "mdbm_internal.h"

using namespace std;

enum RunLevel_t {
    DontRunTestCases      = 0, // dont run test cases just invoke test suite
    RunMinNumberTestCases = 1, // run minimum number of test cases
    RunModNumberTestCases = 2, // run moderate number of test cases
    RunMostTestCases      = 3, // run most of the test cases
    RunAllTestCases       = 4  // run all the test cases
};

struct TestStats {
  int  psize;   // page size
  uint64_t fileSize;
  int  hashFunc;
  int  alignment;
  bool largeObj;
};

#define DEFAULT_PAGE_SIZE   512
#define DEFAULT_DB_SIZE     512
#define DEFAULT_KEY_SIZE    16
#define DEFAULT_VAL_SIZE    16
#define DEFAULT_ENTRY_COUNT 12
#define DEFAULT_INSERT_ROUND 0

extern int GetUnitTestLevelEnv();
extern int GetUnitTestLogLevelEnv();

class StdoutDiverter {
public:
  int outbak;
  int outtmp;

public:
  StdoutDiverter() : outbak(-1), outtmp(-1) {
  }
  StdoutDiverter(const char* dest="/dev/null", bool silent=false) : outbak(-1), outtmp(-1) {
    Divert(dest, silent);
  }
  ~StdoutDiverter() {
    if (outbak>=0) {
      Restore();
    }
  }
  void Divert(const char* dest="/dev/null", bool silent=false) {
    // temporarily divert stdout (i.e. to /dev/null)
    fprintf(stderr, "NOTE: diverting stdout to %s\n", dest);
    fflush(stdout);
    outbak = dup(1);
    outtmp = open(dest, O_WRONLY);
    dup2(outtmp, 1);
    close(outtmp);
  }
  void Restore() {
    // restore stdout...
    fflush(stdout);
    dup2(outbak, 1);
    close(outbak);
    outbak=outtmp=-1;
  }
};

class MdbmHolder {
private:
  string name;
  MDBM*  db;
public:
  MdbmHolder(const string& filename="") : name(filename), db(NULL) { }
  MdbmHolder(MDBM* dbh=NULL) : db(dbh) { }
  // TODO add copy-constructor and operator=
  virtual ~MdbmHolder() {
    //fprintf(stderr, "@@@ destroying MdbmHolder at %p\n", this);
    Close();
    Remove();
  }
  int Open(int flags, int mode, int pagesize, int dbsize) {
    db = mdbm_open(name.c_str(), flags, mode, pagesize, dbsize);
    return (db!=NULL) ? 0 : -1;
  }
  void Close() {
    if (db) { mdbm_close(db); }
    db = NULL;
  }
  void Remove() {
    if (name.size()) { unlink(name.c_str()); }
    name="";
  }
  MDBM* Release() {
    MDBM* tmp = db;
    db = NULL;
    name="";
    return tmp;
  }
  operator const char*() { return name.c_str(); } // questionable
  operator MDBM*() { return db; }
};

class ScopeAutoTracer {
public:
  string   msg; ///< the message to print out
  ostream& out; ///< where to log it
public:
  ScopeAutoTracer(ostream& outStream, const string baseMsg) : msg(baseMsg), out(outStream) {
    out << msg << " Starting..." << endl << flush;
  }
  ~ScopeAutoTracer() {
    out << msg << " Finished." << endl << flush;
  }
};

#define SUITE_PREFIX() \
  (GetSuiteName() + " - " + versionString + ": ")
#define TRACE_TEST_CASE(msg) \
  ScopeAutoTracer tracer(GetLogStream(), SUITE_PREFIX() + msg);

#define TRACE_TEST(variable, expression) \
    variable.out << variable.msg << " " << expression << std::endl << std::flush

/* Variable `tracer' is used in conjuction with TRACE_TEST_CASE. */
#define TRACE_LOG(expression) \
    TRACE_TEST(tracer, expression)

inline double getnow() {
    struct timeval now;
    gettimeofday(&now, NULL);
    double ftnow = ((double)now.tv_sec) + ((double)now.tv_usec)/1000000.0;
    return ftnow;
}

extern double startTime;

#define TRACE_TEST_LOCATION_TIMED()                     \
  {                                                     \
    char buf[512];                                      \
    double t = getnow() - startTime;                     \
    sprintf(buf, "+++++++++  %s .. %s:%d at %7.5f +++++++\n", \
        SUITE_PREFIX().c_str(), __func__, __LINE__, t); \
    GetLogStream() << buf << flush;                     \
  }

#define SKIP_IF_VALGRIND()                                    \
  if (TestBase::IsInValgrind()) {                             \
    fprintf(stderr, "RUNNING UNDER VALGRIND. SKIPPING...\n"); \
    return;                                                   \
  }

#define SKIP_IF_FAST()                                      \
  if (TestBase::RunFastOnly()) {                           \
    fprintf(stderr, "RUNNING IN FAST MODE. SKIPPING...\n"); \
    return;                                                 \
  }

#define SKIP_IF_FAST_VALGRIND()                                      \
  if (TestBase::IsInValgrind() && TestBase::RunFastOnly()) {         \
    fprintf(stderr, "RUNNING IN FAST VALGRIND MODE. SKIPPING...\n"); \
    return;                                                          \
  }

typedef map<string, MdbmHolder*> MdbmMap;

class TestBase {
public:
    int    logLevel;      ///< test log level verbosity (from environment)
    int    testLevel;     ///< depth of tests to run (from environment)
    string suiteName;     ///< short, descriptive test-suite name
    string testName;      ///< short, descriptive test-case name
    int    versionFlag;   ///< mdbm_open option MDBM_CREATE_V3 
    string versionString; ///< mdbm version string "v3"
private:
    bool   autoclean;     ///< controls automatic deletion of temp test directory
    static int    tmpIndex;      ///< index for generating temporary filenames
    int    pid;           ///< starting pid, so tmp dir name is consistent across fork
      // TODO deal with cleanup properly when forking...
    MdbmMap createdMdbms; ///< list of mdbms to be auto-cleaned
    ofstream *log;
    datum   testValueDatum;

public:
    TestBase(int vFlag=MDBM_CREATE_V3, const string& suite="");
    virtual ~TestBase();
    static bool IsInValgrind();
    static bool RunFastOnly();
    int GetTestLevel();
    int GetLogLevel();
    ostream& GetLogStream();
    void SetSuiteName(const string& name) { suiteName = name; }
    const string& GetSuiteName() { return suiteName; }
    void SetTestName(const string& name) { testName = name; }
    const string& GetTestName() { return testName; }

    /// turns automatic cleaning of temp directories on or off
    void SetAutoClean(bool clean);
    /// Populates dirname with the temp directory.
    /// Also ensures that the temp directory exists (mkdir -p).
    /// Returns non-zero if the directory can't be created.
    int GetTmpDir(string &dirname, bool create=true);
    /// returns a one-time-use temporary name, different on every call.
    string GetTmpName(const string &suffix="");

    /// Opens an mdbm in the temp directory, with the given parameters.
    /// NOTE: this relies on cleaning up directory
    //MDBM* GetTmpMdbm(int flags, int mode, int pagesize=-1, int dbsize=-1, string& filename);
    MDBM* GetTmpMdbm(int flags, int mode=0644, int pagesize=-1, int dbsize=-1, string* filename=NULL);
    // as above but asserts that the mdbm was created;
    MDBM* EnsureTmpMdbm(const string& prefix, int flags, int mode=0644, int pagesize=-1, int dbsize=-1, string* filename=NULL);

    MDBM* GetTmpPopulatedMdbm(const string& prefix, int flags, int mode=0644, int pagesize=-1, int dbsize=-1, string* filename=NULL);
    MDBM* GetTmpFilledMdbm(const string& prefix, int flags, int mode=0644, int pagesize=-1, int dbsize=-1, string* filename=NULL);

    /// opens the named mdbm
    MDBM* EnsureNamedMdbm(const string& filename, int flags, int mode=0644, int pagesize=-1, int dbsize=-1);
    /// opens the named mdbm with O_CREAT, O_TRUNC, O_RDWR
    MDBM* EnsureNewNamedMdbm(const string& filename, int flags, int mode=0644, int pagesize=-1, int dbsize=-1);

    // Copies the file at 'filname' to temp dir and returns the new temp filename
    string CopyFileToTmp(const string& filename);

    // TODO generate/store/fetch/validate deterministic and random keys and values of given size
    //        allow keys to be put in an STL container
    // generates pseudo-random (deterministic) data based on 'index' and 'fast'
    //'round' allows for multiple rounds of modifying the data in a deterministic way.
    static void GenerateData(uint32_t size, char* buffer, bool fast, int index);
    int InsertData(MDBM* db, uint32_t keySize=DEFAULT_KEY_SIZE, uint32_t valSize=DEFAULT_VAL_SIZE, uint32_t count=DEFAULT_ENTRY_COUNT, bool fast=true, int offset=0, int insertMode=MDBM_REPLACE, uint32_t round=DEFAULT_INSERT_ROUND);
    int VerifyData(MDBM* db, uint32_t keySize=DEFAULT_KEY_SIZE, uint32_t valSize=DEFAULT_VAL_SIZE, uint32_t count=DEFAULT_ENTRY_COUNT, bool fast=true, int offset=0, uint32_t round=DEFAULT_INSERT_ROUND);
    int DeleteData(MDBM* db, uint32_t keySize=DEFAULT_KEY_SIZE, uint32_t valSize=DEFAULT_VAL_SIZE, uint32_t count=DEFAULT_ENTRY_COUNT, bool fast=true, int offset=0);

    // The following methods were adapted from lib/mtst.c
    // StoreKnownValues, FetchKnownValuesAndVerify, StoreFetchKnownValues
    // These 3 methods work together - they will CPPUNIT assert if error detected.
    // They determine errors based on whether there is large object support.
    // StoreKnownValues will try to store a large object on every 100th store
    // via method MakeKnownValue(). FetchKnownValuesAndVerify will know
    // what any computed value and key will be for a given increment.
    static void StoreKnownValues(MDBM *dbh, int numvalues, int openflags, int pagesize, const string &prefix);
    static void FetchKnownValuesAndVerify(MDBM *dbh, int numvalues, int openflags, int pagesize, const string &prefix);
    static string MakeKnownKey(int num);
    static string MakeKnownValue(int num);

    // Acts on the range of page sizes found in pageSizeRange.
    // Will pre-split the DB if presplitpages > 0.
    void StoreFetchKnownValues(int openflags, vector<int> &pageSizeRange, int presplitpages, bool compresstree, const string &tcprefix);


    void GetTestStats(MDBM *db, TestStats *statsPtr);
    // Fills specified DB with dummy filler records until the DB is full.
    // User should have called mdbm_limit_size before calling this method.
    // numRecs : -1 = fill to capacity, else fill at most numRecs records
    // RETURN: number of records inserted
    int FillDb(MDBM* dbh, int numRecs=-1);

    // mdbm_store wrapper
    // Only asserts if user provides a msg and mdbm_store returns an error.
    // key : MDBM key name used to store the associated value
    // val : value to store in the DB
    // assertMsg : true = CPPUNIT assert if fails to store the value
    // RETURN : 0 = success; -1 = error (if assertMsg == 0)
    int store(MDBM *dbh, const char* key, long long val, const char* assertMsg=0);
    int store(MDBM *dbh, const char* key, float val, const char* assertMsg=0);
    int store(MDBM *dbh, const char* key, const char* val, const char* assertMsg=0);
    int store(MDBM* dbh, const char* key, char* val, int valLen, const char* assertMsg);
    int store(MDBM* dbh, const string& key, const string& val, const char* assertMsg=0);

    // mdbm_fetch wrapper
    // Only asserts if user provides a msg and mdbm_fetch returns an error.
    // key : MDBM key name used to fetch the associated value
    // val : value to fetch from the DB
    // assertMsg : true = CPPUNIT assert if fails to fetch the value
    // RETURN : -1 = error (if assertMsg == 0); else number bytes of data returned
    int fetch(MDBM* dbh, const string& key, long long &val, const char* assertMsg=0);
    int fetch(MDBM* dbh, const string& key, float &val, const char* assertMsg=0);
    int fetch(MDBM* dbh, const string& key, char* &val, const char* assertMsg=0);
    int fetch(MDBM* dbh, const string& key, string &val, const char* assertMsg=0);
    datum fetch(MDBM* dbh, const string& key,  const char* assertMsg);

    // returns the keys in pre-"populated" mdbms
    static string GetDefaultKey(int index);
    // returns the values in pre-"populated" mdbms
    static string GetDefaultValue(int index);

    int CleanupTmpDir();
    int CleanupTmpDirLocks();
    int CleanupTmpFile(string suffix);

    static int GetExpectedPageSize(int dbVersionFlag, int pagesize);
    // MDBM v3 limits for number of pages and page size
    static uint64_t GetMaxNumPages(int vFlag);
    static uint64_t GetMaxDBsize(int vFlag);

    static uint64_t GetMaxFileSize();
    static uint64_t GetMaxVirtualAddress();

    void CopyFile(const string &filenm);
    datum CreateTestValue(const string& prefix, size_t length, datum &value);

    string ToStr(int val);
};

#define RETURN_IF_WINMODE_LIMITED   if (remap_is_limited(sysconf(_SC_PAGESIZE)))  return;



#endif //DONT_MULTI_INCLUDE_TEST_BASE
