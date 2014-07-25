/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <string.h>
#include <assert.h>
#include <string.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <cppunit/TestAssert.h>

#include "TestBase.hh"

// TODO conditionally set. Some OS (i.e. Fedora 19) make /tmp a small tmpfs partition
//#define TEMPDIR "/tmp"
#define TEMPDIR "/var/tmp"

#define UNITTEST_LOG_LEVEL_ENV "MDBM_LOG_LEVEL"
#define ENV_UNITTEST_LEVEL     "MDBM_UNITTEST_LEVEL"
#define MAX_VALID_LEVEL_VALUE  20


double startTime = getnow();

//const int      MinPowerForV2 = 7; // minimum power factor to determine V2 page sizes
//const int      MaxPowerForV2 = 16; // maximum power factor to determine V2 page sizes

const uint64_t MaxDBsizeV3 = (1ULL<<36);
// NOTE: MDBM_DIRSHIFT_MAX not accessable
const uint64_t V3MaxDBsize = MaxDBsizeV3; // 68gig hi, posted is 16gig: too big --> _v3MaxNumPages* (uint64_t)MDBM_MAXPAGE;


    // MDBM v3 limits for number of pages and page size
uint64_t TestBase::GetMaxNumPages(int vFlag) {
  return MDBM_NUMPAGES_MAX;
}
uint64_t TestBase::GetMaxDBsize(int vFlag) {
  return V3MaxDBsize;
}

//  Get the system limit - maximum file size allowed for this process.
uint64_t TestBase::GetMaxFileSize()
{
    rlimit rlim;
    rlim.rlim_cur = 0;
    rlim.rlim_max = 0;

    int ret = getrlimit(RLIMIT_FSIZE, &rlim);
    if (ret == -1)
    {
        throw("getrlimit RLIMIT_FSIZE failed");
    }
//    else cout << "The maximum size of file the process can create is: soft=" << rlim.rlim_cur << " and hard=" << rlim.rlim_max << endl;

    return  rlim.rlim_cur;
}

//  Get the system limit - maximum virtual address space allowed for this process.
uint64_t TestBase::GetMaxVirtualAddress()
{
    rlimit rlim;
    rlim.rlim_cur = 0;
    rlim.rlim_max = 0;

// this is for FreeBSD
#ifndef RLIMIT_AS
    throw("getrlimit RLIMIT_AS not supported on this OS");
#else
    int ret = getrlimit(RLIMIT_AS, &rlim);
    if (ret == -1)
    {
        throw("getrlimit RLIMIT_AS failed");
    }
#endif

    return rlim.rlim_cur;
}


int GetUnitTestLevelEnv() {
    int value = 0;
    char *retval = getenv(ENV_UNITTEST_LEVEL);

    if (retval && retval[0]) { value = atoi(retval); }
    if (value > MAX_VALID_LEVEL_VALUE) { value = MAX_VALID_LEVEL_VALUE; }
    return value;
}

#define LOG_OFF -1
int GetUnitTestLogLevelEnv() {
    int logLevel = -1;
    char *val = getenv(UNITTEST_LOG_LEVEL_ENV);

    if (!(val && val[0])) {
      return LOG_OFF;
    }
    logLevel = strtol(val, NULL, 0);

    switch (logLevel) {
        // fall thru, by necessity these all map directly to ysys
        case LOG_OFF:
        case LOG_EMERG:
        case LOG_ALERT:
        case LOG_CRIT:
        case LOG_ERR:
        case LOG_WARNING:
        case LOG_NOTICE:
        case LOG_INFO:
        case LOG_DEBUG:
        case LOG_DEBUG+1:
        case LOG_DEBUG+2:
            break;
        default:
            logLevel = LOG_DEBUG+3;
    }

    return logLevel;
}



int TestBase::tmpIndex = 0;

TestBase::TestBase(int vFlag, const string& suite)
      : logLevel(0), testLevel(0), suiteName(suite),
      versionFlag(vFlag), versionString((vFlag&MDBM_CREATE_V3) ? "v3" : "v2"),
      autoclean(true),
      //tmpIndex(0),
      pid(getpid()), log(NULL) {
    // TODO get test and log-level from environment
    logLevel = GetUnitTestLogLevelEnv();
    testLevel = GetUnitTestLevelEnv();

    testValueDatum.dptr = NULL;
    testValueDatum.dsize = 0;
}
TestBase::~TestBase() {
  if (autoclean) {
    CleanupTmpDir();
  }
  if (log) {
    delete log;
    log = NULL;
  }
  if (testValueDatum.dptr) {
    free(testValueDatum.dptr);
    testValueDatum.dptr = NULL;
    testValueDatum.dsize = 0;
  }
  suiteName.clear();
  testName.clear();
  versionString.clear();
}
bool TestBase::IsInValgrind() {
  const char* valg = getenv("VALG");
  // for now, any setting for environment variable "VALG" indicates valgrind
  return (valg && valg[0]);
}
bool TestBase::RunFastOnly() {
  const char* fast = getenv("FAST");
  // for now, any setting for environment variable "VALG" indicates valgrind
  return (fast && fast[0]);
}
int TestBase::GetTestLevel() {
  return testLevel;
}
int TestBase::GetLogLevel() {
  return logLevel;
}
ostream& TestBase::GetLogStream() {
  if (logLevel <= LOG_NOTICE) {
    if (!log) {
      log = new ofstream("/dev/null");
    }
    return *log;
  }
  return cout;
}


void TestBase::SetAutoClean(bool clean) {
  autoclean = clean;
}
int TestBase::GetTmpDir(string &dirname, bool create) {
    char buf[512];
    uid_t uid = geteuid();
    struct passwd *pw = getpwuid(uid);
    const char *usrname = "unknown-user";
    int ret = 0;

    memset(buf, 0, sizeof(buf));
    if (pw) { usrname = pw->pw_name; }
    //snprintf(buf, sizeof(buf), TEMPDIR"/mdbm/%s-%d", usrname, getpid());
    snprintf(buf, sizeof(buf), TEMPDIR"/mdbm/%s-%d", usrname, pid);
    dirname = buf;

    if (create) {
        string cmd = "mkdir -p ";
        cmd += dirname;
        //cerr << "++++ CREATING DIRECTORY WITH [[" << cmd << "]]" << endl;
        ret = system(cmd.c_str()); // TODO assert return ok...
    }
    return ret;
}
string TestBase::GetTmpName(const string &suffix) {
  char buf[512];
  string filename;

  memset(buf, 0, sizeof(buf));
  GetTmpDir(filename);
  filename += "/";
  snprintf(buf, sizeof(buf), "mdbm-%05d%s", tmpIndex++, suffix.c_str());
  filename += buf;
  return filename;
}

//// NOTE: this relies on cleaning up directory
//MDBM* TestBase::GetTmpMdbm(int flags, int mode, int pagesize, int dbsize, string& filename) {
//  string name;
//  GetTmpDir(name);
//  name += "/"; name += filename;
//
//  return mdbm_open(name.c_str(), flags, mode, pagesize, dbsize);
//}

// NOTE: this relies on cleaning up directory
MDBM* TestBase::GetTmpMdbm(int flags, int mode, int pagesize, int dbsize, string* filename) {
  string name = GetTmpName(".mdbm");

  if (filename) { *filename = name; }
  if (pagesize<0) { pagesize=DEFAULT_PAGE_SIZE; }
  if (dbsize<0) { dbsize=DEFAULT_DB_SIZE; }
  return mdbm_open(name.c_str(), flags, mode, pagesize, dbsize);
}
MDBM* TestBase::EnsureTmpMdbm(const string& prefix, int flags, int mode, int pagesize, int dbsize, string* filename) {
    MDBM* db = GetTmpMdbm(flags, mode, pagesize, dbsize, filename);

    if (!db) {
        stringstream openmsg;
        openmsg << prefix << "Failed to mdbm_open DB(errno=" << errno
                << ") using pagesize(" << pagesize << ") dbsize(" << dbsize
                << ((flags & MDBM_DBSIZE_MB) ? ") MB's" : ") bytes");
        openmsg << endl;
        cerr << openmsg.str();
        assert(db);
    }

    return db;
}

MDBM* TestBase::EnsureNewNamedMdbm(const string& filename, int flags, int mode, int pagesize, int dbsize) {
    int newFlags = MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_O_RDWR;
    return  EnsureNamedMdbm(filename, flags|newFlags, mode, pagesize, dbsize);

}
MDBM* TestBase::EnsureNamedMdbm(const string& filename, int flags, int mode, int pagesize, int dbsize) {
    if (pagesize<0) { pagesize=DEFAULT_PAGE_SIZE; }
    if (dbsize<0) { dbsize=DEFAULT_DB_SIZE; }

    MDBM* db = mdbm_open(filename.c_str(), flags, mode, pagesize, dbsize);

    if (!db) {
        stringstream openmsg;
        openmsg << filename << "Failed to mdbm_open ["<<filename<<"] DB(errno=" << errno
                << ") using pagesize(" << pagesize << ") dbsize(" << dbsize
                << ((flags & MDBM_DBSIZE_MB) ? ") MB's" : ") bytes");
        openmsg << endl;
        cerr << openmsg.str();
        assert(db);
    }

    return db;
}

MDBM* TestBase::GetTmpPopulatedMdbm(const string& prefix, int flags, int mode, int pagesize, int dbsize, string* filename) {
    string tmpFilename;
    MDBM* db = EnsureTmpMdbm(prefix, flags|MDBM_O_CREAT|MDBM_O_TRUNC, mode, pagesize, dbsize, &tmpFilename);
    int ret = InsertData(db, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT);
    if (ret) {
        stringstream openmsg;
        openmsg << prefix << "Failed to populate DB " << tmpFilename << " (errno=" << errno
                << ") using pagesize(" << pagesize << ") dbsize(" << dbsize
                << ((flags & MDBM_DBSIZE_MB) ? ") MB's" : ") bytes");
        openmsg << endl;

        mdbm_close(db);
        // TODO unlink
        cerr << openmsg.str();
        assert(!ret);
        return NULL;
    }
    if (filename) { *filename = tmpFilename; }
    return db;
}

MDBM* TestBase::GetTmpFilledMdbm(const string& prefix, int flags, int mode, int pagesize, int dbsize, string* filename) {
    int ret;
    string tmpFname;
    MDBM* db = EnsureTmpMdbm(prefix, flags|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_O_RDWR, mode, pagesize, dbsize, &tmpFname);
    assert(db);
    if (filename) { *filename = tmpFname; }
    mdbm_limit_size_v3(db, 1, 0, 0); // set to 1 page and no shake function
    ret = FillDb(db);
    if (!ret) {
        TestStats stats;
        GetTestStats(db, &stats);
        stringstream openmsg;
        cerr << prefix << "Failed to fill DB(errno=" << errno
             << ") using pagesize(" << pagesize << ") dbsize(" << dbsize
             << ((flags & MDBM_DBSIZE_MB) ? ") MB's" : ") bytes") << " file:" << tmpFname
             << endl;
        cerr << ">>>>>>>>>>>>>>> stats: PageSize=" << stats.psize
             << " FileSize=" << stats.fileSize
             << " largeObj=" << stats.largeObj
             << " alignment=" << stats.alignment
             << endl;


        mdbm_close(db);
        // TODO unlink
        assert(!ret);
        return NULL;
    }
    return db;
}



// returns new filename
string TestBase::CopyFileToTmp(const string& filename) {
  size_t slash = filename.find_last_of('/');
  if (!slash) { slash=0; }
  else { ++slash; }
  string suffix = filename.substr(slash);

  string tmpname = GetTmpName(suffix);
  string cmd = "cp "+filename+" "+tmpname;
  int ret = system(cmd.c_str());
  if (0 != ret) { //failed
    tmpname = "";
  }
  return tmpname;
}

// TODO generate/store/fetch/validate deterministic and random keys and values of given size
//        allow keys to be put in an STL container
// generates pseudo-random (deterministic) data based on 'index' and 'fast'
void TestBase::GenerateData(uint32_t size, char* buffer, bool fast, int index) {
  if (fast) {
    int i=0;
    int len = (size<4) ? size : 4;
    uint32_t val = index;
    char* cur = buffer;
    memset(buffer, 0, size);
    // Fast: write the index at the beginning and end
    for (; i<len; ++i) {
      cur[i] = val&0xff;
      cur[size-(i+1)] = val&0xff;
      val >>= 8;
    }
  } else {
    // complex: generate random data with the index as a seed
    char* cur = buffer;
    srand48(index);
    uint32_t val = (uint32_t) mrand48();
    for (; size>=4; cur+=4, size-=4) {
      *((uint32_t*)cur) = val;
      val = (uint32_t) mrand48();
    }
    for (; size>0; ++cur, --size) {
      *cur = val&0xff;
      val >>= 8;
    }
  }

}
int TestBase::InsertData(MDBM* db, uint32_t keySize, uint32_t valSize, uint32_t count, bool fast, int offset, int insertMode, uint32_t round) {

  //fprintf(stderr, "InsertData(db=%p, ksz=%u, vsz=%u, count=%u, fast=%d, off=%d, insMode=%d)\n",
  //     db, keySize, valSize, count, fast, offset, insertMode);
  int store_ret = 0;
  datum key = {new char[keySize], (int)keySize};
  datum val = {new char[valSize], (int)valSize};
  int i, last=count+offset;
  for (i=offset; i<last; ++i) {
     GenerateData(key.dsize, key.dptr, fast, i);
     // todo: do we care about value data being different from key?
     GenerateData(val.dsize, val.dptr, fast, i+round);
     store_ret = mdbm_store(db, key, val, insertMode);
     if (store_ret) {
       //fprintf(stderr, "FAIL!: ret=%d, InsertData(db=%p, ksz=%u, vsz=%u, count=%u, fast=%d, off=%d, insMode=%d) index=%d\n", store_ret, (void*)db, keySize, valSize, count, fast, offset, insertMode, i);
       break;
     }
  }
  delete[] key.dptr;
  delete[] val.dptr;
  return store_ret;
}
int TestBase::VerifyData(MDBM* db, uint32_t keySize, uint32_t valSize, uint32_t count, bool fast, int offset, uint32_t round) {
  int retval = 0;
  datum key = {new char[keySize], (int)keySize};
  datum val = {new char[valSize], (int)valSize};
  datum found;
  int i, last=count+offset;
  for (i=offset; i<last; ++i) {
     GenerateData(key.dsize, key.dptr, fast, i);
     // todo: do we care about value data being different from key?
     GenerateData(val.dsize, val.dptr, fast, i+round);
     found = mdbm_fetch(db, key);
     if ( (val.dsize != found.dsize) || memcmp(val.dptr, found.dptr, val.dsize) ) {
       //fprintf(stderr, "DIFFERENCE FOUND... item:%d sz:%d vs %d, ptr:%p and %p\n", i, val.dsize, found.dsize, (void*)val.dptr, (void*)found.dptr);
       retval = -1;
       break;
     }
  }
  delete[] key.dptr;
  delete[] val.dptr;
  return retval;
}

int TestBase::DeleteData(MDBM* db, uint32_t keySize, uint32_t valSize, uint32_t count, bool fast, int offset) {
  int delete_ret = 0;
  datum key = {new char[keySize], (int)keySize};
  int i, last=count+offset;
  for (i=offset; i<last; ++i) {
     GenerateData(key.dsize, key.dptr, fast, i);
     // TODO: do we care about value data being different from key?
     int delete_ret = mdbm_delete(db, key);
     if (delete_ret) { break; }
  }
  delete[] key.dptr;
  return delete_ret;
}

int TestBase::store(MDBM* dbh, const char* key, long long num, const char* assertMsg)
{
    long long val = num;
    return store(dbh, key, reinterpret_cast<char*>(&val), sizeof(val), assertMsg);
}
int TestBase::store(MDBM* dbh, const char* key, float num, const char* assertMsg)
{
    float val = num;
    return store(dbh, key, reinterpret_cast<char*>(&val), sizeof(val), assertMsg);
}
int TestBase::store(MDBM* dbh, const char* key, const char* val, const char* assertMsg)
{
    return store(dbh, key, const_cast<char*>(val), strlen(val)+1, assertMsg);
}
int TestBase::store(MDBM* dbh, const char* key, char* val, int valLen, const char* assertMsg)
{
    datum dkey;
    dkey.dptr  = const_cast<char *>(key);
    dkey.dsize = strlen(dkey.dptr);

    datum dval;
    dval.dptr  = val;
    dval.dsize = valLen;

    errno = 0;
    int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    int errnum = errno;
    if (ret == -1 && assertMsg)
    {
        stringstream sstrm;
        sstrm << assertMsg << "mdbm_store Failed for key=" << key
              << " storing data of size=" << valLen << ": errno=" << errnum << endl;
        CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (ret == 0));
    }

    return ret;
}
int TestBase::store(MDBM* dbh, const string& key, const string& val, const char* assertMsg)
{
    datum dkey;
    dkey.dptr  = const_cast<char *>(key.data());
    dkey.dsize = key.size();

    datum dval;
    dval.dptr  =  const_cast<char *>(val.data());
    dval.dsize = val.size();

    errno = 0;
    int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    int errnum = errno;
    if (ret == -1 && assertMsg)
    {
        stringstream sstrm;
        sstrm << assertMsg << "mdbm_store Failed for key=" << key
              << " storing data of size=" << val.size() << ": errno=" << errnum << endl;
        CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (ret == 0));
    }

    return ret;
}


int TestBase::fetch(MDBM* dbh, const string& key, long long &val, const char* assertMsg)
{
    datum fdatum = fetch(dbh, key, assertMsg);
    if (!fdatum.dptr)
        return -1;

    val = *(reinterpret_cast<long long *>(fdatum.dptr));
    return fdatum.dsize;
}
int TestBase::fetch(MDBM* dbh, const string& key, float &val, const char* assertMsg)
{
    datum fdatum = fetch(dbh, key, assertMsg);
    if (!fdatum.dptr)
        return -1;

    val = *(reinterpret_cast<float *>(fdatum.dptr));
    return fdatum.dsize;
}
int TestBase::fetch(MDBM* dbh, const string& key, char* &val, const char* assertMsg)
{
    datum fdatum = fetch(dbh, key, assertMsg);
    if (!fdatum.dptr)
        return -1;

    val = fdatum.dptr;
    return fdatum.dsize - 1;
}
int TestBase::fetch(MDBM* dbh, const string& key, string &val, const char* assertMsg)
{
    datum fdatum = fetch(dbh, key, assertMsg);
    if (!fdatum.dptr)
        return -1;

    val.assign(fdatum.dptr, fdatum.dsize);
    return fdatum.dsize;
}
datum TestBase::fetch(MDBM* dbh, const string& key,  const char* assertMsg)
{
    datum dkey;
    dkey.dptr  = const_cast<char *>(key.data());
    dkey.dsize = key.size();

    datum fdatum = mdbm_fetch(dbh, dkey);
    if (!fdatum.dptr && assertMsg)
    {
        stringstream sstrm;
        sstrm << assertMsg << "mdbm_fetch Failed for key=" << key << endl;
        CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (fdatum.dptr == 0));
    }
    return fdatum;
}

void TestBase::GetTestStats(MDBM *db, TestStats *statsPtr) {
    statsPtr->psize = mdbm_get_page_size(db);
    statsPtr->fileSize = mdbm_get_size(db);
    statsPtr->hashFunc = mdbm_get_hash(db);
    statsPtr->alignment = mdbm_get_alignment(db);
    statsPtr->largeObj = false;
    mdbm_stats_t stats;
    if (mdbm_get_stats (db, &stats, sizeof (stats)) != 0 )
        return;
    statsPtr->largeObj = (stats.s_large_page_count != 0);
}


// Fills specified DB with dummy filler records until the DB is full.
// User should have called mdbm_limit_size before calling this method.
// numRecs : -1 = fill to capacity, else fill at most numRecs records
// RETURN: number of records inserted
int TestBase::FillDb(MDBM* dbh, int numRecs) {
    int cnt = 0;

    if (!dbh) { return 0; }
    // if user specified numRecs < 0, use get limit size to determine that
    // a limit was set, else it will fill til the system limits
    if (numRecs != 0) {
        uint64_t limitsize = mdbm_get_limit_size(dbh);
        if (limitsize == 0) { // no limit was set
          cerr << "FillDb Error: No LimitSize set" << endl;
          return 0;
        }
    }

    for (; numRecs != 0; --numRecs,++cnt) {
        //int offset =  DEFAULT_ENTRY_COUNT+cnt;
        int offset = cnt;
        //cerr << "FillDb Info: Trying entry:" << cnt << endl;
        int ret = InsertData(dbh, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, 1, true, offset);
        if (ret) {
          //cerr << "FillDb Info: Stopped at entry:" << cnt << endl;
          break;
        }
    }

    return cnt;
}


string TestBase::GetDefaultKey(int index) {
  string ret;
  char buf[DEFAULT_KEY_SIZE];
  // Caveat Emptor... want to allow generating dummys
  //if (index<0 || index>=DEFAULT_ENTRY_COUNT) { return ret; }
  GenerateData(DEFAULT_KEY_SIZE, buf, true, index);
  ret.assign(buf, DEFAULT_KEY_SIZE);
  return ret;
}

string TestBase::GetDefaultValue(int index) {
  string ret;
  char buf[DEFAULT_VAL_SIZE];
  // Caveat Emptor... want to allow generating dummys
  //if (index<0 || index>=DEFAULT_ENTRY_COUNT) { return ret; }
  GenerateData(DEFAULT_VAL_SIZE, buf, true, index);
  ret.assign(buf, DEFAULT_VAL_SIZE);
  return ret;
}


// TODO get test and log-level from environment, setup as necessary

int TestBase::CleanupTmpDirLocks() {
  string tmpdir;
  string cmd;

  GetTmpDir(tmpdir, false);
  if (strncmp(tmpdir.c_str(), TEMPDIR, strlen(TEMPDIR))) { // make sure any damage is confined to tmp :D
    return -1;
  }

  // try to clean up mlock files
  cmd = "rm -rf /tmp/.mlock-named/" + tmpdir;
  int ret = system(cmd.c_str());
  (void)ret;

  return 0;
}
int TestBase::CleanupTmpDir() {
  string tmpdir;

  GetTmpDir(tmpdir, false);
  if (strncmp(tmpdir.c_str(), TEMPDIR, strlen(TEMPDIR))
      || !strlen(TEMPDIR)) { // make sure any damage is confined to tmp :D
    fprintf(stderr, "CleanupTmpDir BAILING! (%s) (%s:%d)\n", tmpdir.c_str(), TEMPDIR, (int)strlen(TEMPDIR));
    return -1;
  }
  //fprintf(stderr, "CleanupTmpDir RUNNING!\n");

  // first try to clean up lock files
  CleanupTmpDirLocks();

  string cmd = "rm -rf " + tmpdir;
  //cerr << "---- DESTROYING DIRECTORY WITH [[" << cmd << "]]" << endl;
  return system(cmd.c_str());
}
int TestBase::CleanupTmpFile(string suffix) {
  string tmpdir;

  GetTmpDir(tmpdir, false);
  if (strncmp(tmpdir.c_str(), TEMPDIR, strlen(TEMPDIR))
      || !strlen(TEMPDIR)) { // make sure any damage is confined to tmp :D
    return -1;
  }
  string cmd = "rm ";
  cmd += tmpdir;
  return system(cmd.c_str());
}

// Calculate expected page size for given pagesize according to DB version
// For v2, page sizes used will be a power of 2
// dbVersionFlag : MDBM_CREATE_V3
// pagesize : in bytes
// RETURN: expected page size for given pagesize
int TestBase::GetExpectedPageSize(int dbVersionFlag, int pagesize) {
    if (pagesize == 0)
        return MDBM_PAGESIZ;

    //if (dbVersionFlag == MDBM_CREATE_V3)
    //    return pagesize;

    //// calculate power of 2 that is >= to pagesize
    ////
    //for (int cnt = MinPowerForV2; cnt <= MaxPowerForV2; ++cnt) {
    //    //int power = static_cast<int>(pow(2, cnt));
    //    int power = 1<<cnt;
    //    if (power >= pagesize)
    //        return power;
    //}

    return pagesize; // invalid page size, too large
}



void TestBase::CopyFile(const string &filenm) {
    string buf("cp ");
    buf +=  filenm + "_cp " + filenm;
    if (system(buf.c_str())) {
        cerr << "CopyFile: unable to copy file, errno= " << errno << "," << strerror(errno) << " cmd: " << buf << endl;
    }
}

// Creates the value in the valueRef, and returns the key.  prefix gets appended to the key
// so that we get a unique key if CreateTestValue is called in a loop
datum TestBase::CreateTestValue(const string& prefix, size_t length, datum &value) {
    datum& key=testValueDatum;
    int prefLen = prefix.size();
    int klen = prefLen + length;

    value.dsize = length;

    if (key.dsize < klen) {
        char* tmp = (char *) realloc(key.dptr, klen + 10);
        if (!tmp) {
            cerr << "CreateTestValue: Unable to realloc()" << endl;
            free(key.dptr);
            key.dptr = NULL;
            key.dsize = 0;
            return key;
        }
        key.dptr = tmp;
        key.dsize = klen;
        strncpy(key.dptr, prefix.c_str(), klen);
    }

    value.dptr = key.dptr + prefLen;

    int pos = prefLen;
    char curChar = 'A';
    while ( pos < klen ) {
        if (curChar > 'z') {
            curChar = 'A';
        }
        key.dptr[pos] = curChar++;
        ++pos;
    }

    key.dptr[pos] = '\0';

    return key;
}

string TestBase::ToStr(int val)
{
    stringstream s;
    s << val;
    string tmp = s.str();
    return tmp;
}

string TestBase::MakeKnownKey(int num)
{
    stringstream keyss;
    keyss << num;
    return keyss.str();
}

string TestBase::MakeKnownValue(int num)
{
    static char val_buf[128*1024];
    if ((num % 100) != 0) {
        snprintf(val_buf, sizeof(val_buf), "<%04d>", num);
    } else {
        char *bufp = val_buf;
        size_t len = sizeof(val_buf);
        int cnt = 5000 + (10 * (num % 1000));
        for (int vnum = 0; vnum < cnt; vnum++) {
            int bytes = snprintf(bufp, len, "%08x", num+vnum);
            bufp += bytes;
            len -= bytes;
        }
    }
    string val(val_buf);
    return val;
}

void TestBase::StoreKnownValues(MDBM *dbh, int numvalues, int openflags, int pagesize, const string &prefix)
{
    datum dkey;
    datum dval;
    string key;
    stringstream stss;
    stss << prefix;
    for (int cnt = 0; cnt < numvalues; ++cnt) {
        string key = MakeKnownKey(cnt);
        dkey.dptr  = const_cast<char*>(key.c_str());
        dkey.dsize = key.size();
        string val = MakeKnownValue(cnt);
        dval.dptr  = const_cast<char*>(val.c_str());
        dval.dsize = val.size();
        if (dbh == NULL)
           return;
        int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
        if (ret != 0 && (openflags & MDBM_LARGE_OBJECTS)) {
            stss << "mdbm_store Failed to store key(size=" << dkey.dsize
                 << " name=" << dkey.dptr << ") and value(size=" << dval.dsize << ")" << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
        } else if (ret == 0 && dval.dsize > pagesize && (openflags & MDBM_LARGE_OBJECTS) == 0) {
            stss << "mdbm_store Succeeded to store key(size=" << dkey.dsize
                 << " name=" << dkey.dptr << ") with very large value(size=" << dval.dsize
                 << ") but should have FAILed" << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret != 0));
        }
    }
}

void TestBase::FetchKnownValuesAndVerify(MDBM *dbh, int numvalues, int openflags, int pagesize, const string &prefix)
{
    datum dkey;
    datum dval;
    stringstream fkss;
    fkss << prefix;
    for (int cnt = numvalues - 1; cnt > -1; --cnt) {
        string key = MakeKnownKey(cnt);
        dkey.dptr  = const_cast<char*>(key.c_str());
        dkey.dsize = key.size();
        string val = MakeKnownValue(cnt);
        dval.dsize = val.size();
        if (dbh == NULL)
           return;
        // do the fetch and compare values
        datum retval = mdbm_fetch(dbh, dkey);
        if (retval.dptr == NULL) {
            if ((openflags & MDBM_LARGE_OBJECTS) || (dval.dsize < pagesize))
            {
                fkss << "mdbm_fetch Failed to fetch(" << cnt << ") key(size="
                     << dkey.dsize << " name=" << dkey.dptr << ") Expected value size=" << dval.dsize << endl;
                CPPUNIT_ASSERT_MESSAGE(fkss.str(), (retval.dptr != NULL));
            } else {
                continue; // DB without large object support expected to FAIL on very large data
            }
        }

        string vstr(retval.dptr, retval.dsize);
        if (vstr.compare(val)) {
            fkss << "The fetched key(size=" << dkey.dsize
                 << ") did not have the expected value(size=" << dval.dsize << ")" << endl;
            CPPUNIT_ASSERT_MESSAGE(fkss.str(), (retval.dptr != NULL));
        }
    }
}

void TestBase::StoreFetchKnownValues(int openflags, vector<int> &pageSizeRange, int presplitpages, bool compresstree, const string &tcprefix)
{
    stringstream prefix;
    prefix << SUITE_PREFIX() << tcprefix;
    TRACE_TEST_CASE(tcprefix);

    int maxkeys = 10000;

    // test using different page sizes
    for (size_t cnt = 0; cnt < pageSizeRange.size(); ++cnt) {
        stringstream psss;
        psss << "Pagesize=" << pageSizeRange[cnt] << ": ";

        MDBM *dbh;
        if (openflags) {
            dbh = GetTmpMdbm(openflags|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC, 0644, pageSizeRange[cnt]);
        } else {
            int numPages = presplitpages ? presplitpages : 8;
            dbh = mdbm_open(NULL, 0, 0, pageSizeRange[cnt], pageSizeRange[cnt] * numPages); // memory only
            if (dbh == NULL) {
                prefix << psss.str()
                       << "Memory-only test FAILed: mdbm_open returned null" << endl;
                CPPUNIT_ASSERT_MESSAGE(prefix.str(), (dbh != NULL));
            }
            //maxkeys = 500; // reset smaller since memory-only db cannot grow
            maxkeys = 100; // reset smaller since memory-only db cannot grow
        }
        MdbmHolder dbholder(dbh);

        if (presplitpages > 0) {
            int ret = mdbm_pre_split(dbh, presplitpages);
            if (ret == -1) {
                prefix << psss.str()
                       << "mdbm_pre_split FAILed to pre split DB to 32 pages. "
                       << "Got errno=" << errno << endl;
                CPPUNIT_ASSERT_MESSAGE(prefix.str(), (ret == 0));
            }
        }

        // store maxkeys key/values
        StoreKnownValues(dbh, maxkeys, openflags, pageSizeRange[cnt], prefix.str() + psss.str());

        //dump_mdbm_header(dbh);
        //dump_chunk_headers(dbh);
        //mdbm_dump_all_page(dbh);

        if (compresstree) {
            mdbm_compress_tree(dbh);
        }

        // fetch the values in reverse order, comparing to the original computed stored value
        FetchKnownValuesAndVerify(dbh, maxkeys, openflags, pageSizeRange[cnt], prefix.str() + psss.str());

        // check MDBM_INSERT of duplicate, expect it to return 1
        // create a dup known key
        string dupkey = MakeKnownKey(5);
        string repkey;
        datum dkey;
        dkey.dptr  = const_cast<char*>(dupkey.c_str());
        dkey.dsize = dupkey.size();
        datum dval;
        dval.dptr  = dkey.dptr;
        dval.dsize = dkey.dsize;
        int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
        if (ret != 1) {
            prefix << psss.str()
                   << "mdbm_store of duplicate key(" << dupkey << ") "
                   << "should have returned 1, instead it returned=" << ret << endl;
            CPPUNIT_ASSERT_MESSAGE(prefix.str(), (ret == 1));
        }

        // do some MDBM_REPLACE stores, then fetch the data and verify it == to what was stored
        char repbuf[8];
        dval.dptr  = repbuf;
        for (int rcnt = 5; rcnt < 15; ++rcnt) {
            repkey = MakeKnownKey(rcnt);
            dkey.dptr  = const_cast<char*>(repkey.c_str());
            dkey.dsize = repkey.size();
            dval.dsize = sprintf(repbuf, "#%04d#", rcnt);
            ret = mdbm_store(dbh, dkey, dval, MDBM_REPLACE);
            if (ret != 0) {
                prefix << psss.str()
                       << "mdbm_store to replace value for key(" << repkey << ") "
                       << "should have returned 1, instead it returned=" << ret << endl;
                CPPUNIT_ASSERT_MESSAGE(prefix.str(), (ret == 0));
            }

            datum dret = mdbm_fetch(dbh, dkey);
            if (dret.dptr == NULL) {
                prefix << psss.str()
                       << "mdbm_fetch FAILed to fetch replaced value for key(" << repkey << ") "
                       << endl;
                CPPUNIT_ASSERT_MESSAGE(prefix.str(), (dret.dptr != NULL));
            }

            string vstr(dret.dptr, dret.dsize);
            if (vstr.compare(repbuf)) {
                prefix << psss.str()
                       << "Fetched key(" << repkey << ") should have contained value(" << repbuf
                       << ") of size=" << dval.dsize << endl
                       << ") Instead fetched the value(" << vstr
                       << ") of size=" << vstr.size() << endl;
                CPPUNIT_ASSERT_MESSAGE(prefix.str(), (vstr.compare(repbuf) == 0));
            }
        }

        // delete a known key, then try to fetch it and expect failure
        // use dupkey above
        ret = mdbm_delete(dbh, dkey);
        if (ret != 0) {
            prefix << psss.str()
                   << "mdbm_delete FAILed to delete known key(" << dkey.dptr
                   << ") Got errno=" << errno << endl;
            CPPUNIT_ASSERT_MESSAGE(prefix.str(), (ret == 0));
        }
        datum dret = mdbm_fetch(dbh, dkey);
        if (dret.dptr != NULL) {
            prefix << psss.str()
                   << "mdbm_fetch Succeeded to fetch deleted value for key(" << dkey.dptr << ") "
                   << " but should NOT have!" << endl;
            CPPUNIT_ASSERT_MESSAGE(prefix.str(), (dret.dptr == NULL));
        }

        // call mdbm_chk_all_page to check for errors
        ret = mdbm_chk_all_page(dbh);
        if (ret != 0) {
            prefix << psss.str()
                   << "Should be no errors reported for the DB. Got errno=" << errno << endl;
            CPPUNIT_ASSERT_MESSAGE(prefix.str(), (ret == 0));
        }
    }
}

