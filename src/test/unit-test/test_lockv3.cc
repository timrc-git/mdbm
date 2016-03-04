/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_lock mdbm_trylock mdbm_unlock
//   mdbm_lock_shared mdbm_trylock_shared mdbm_islocked
//   mdbm_plock mdbm_tryplock mdbm_punlock mdbm_lock_reset

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <sys/uio.h>
#include <signal.h>
#include <pthread.h>
#include <fcntl.h>

// for usage
#include <sys/time.h>
#include <sys/resource.h>

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

#include "mdbm.h"
#include "test_lockbase.hh"

#define main delete_lockfiles_main_wrapper
#include "../../tools/mdbm_delete_lockfiles.c"
#undef main

// bugzilla tickets:
// FIX BZ 5384701 - v2, v3: mdbm_islocked: returns NOT locked after mdbm_plock performed
// FIX BZ 5395602 - v3: mdbm_plock: partition locking: auto-unlock test fails - iteration broken
class LockV3TestSuite : public CppUnit::TestFixture, public LockBaseTestSuite
{
public:
    LockV3TestSuite(int flags) : LockBaseTestSuite(flags) {}
    void setUp();
    void deleteLockfile();

    // unit tests in this suite
    void GetLockMode();
    void LockModeTest();
    void LockNewDB();  // TC A-1
    void ThreadThenLock(); // TC A-2
    void ForkThenLock(); // TC A-3
    void ForkAndLockWithPartFlag(); // TC A-4
    void ThreadAndLockWithNoLockFlag(); // TC A-5
    void ForkAndLockWithNoLockFlag(); // TC A-6
    void ForkAndLockWithChildNoLockFlag(); // TC A-7
    void ParentNestsChildLocksOnce(); // TC A-8
    void ThreadNestsChildLocksOnce(); // TC A-9
    void ThreadsShareHandleNestVsBlock(); // TC A-10
    void LockCloseReopenRelock(); // TC A-11
    void MemoryOnlyParentLockChildBlock(); // A-12

    void UnlockWithoutLocking(); // B-1
    void ParentLocksChildUnlocks(); // B-2
    void ParentLockSharedChildUnlocks(); // B-3
    void ThreadThenChildUnlock(); // B-4
    void ParentLocksChildUnlocksSharedHandle(); // B-5
    void ThreadsShareHandleNestVsBlockUnlock(); // B-6
    void ThreadNestLocksThenUnwindUnlock(); // B-7
    // Obsolete(interleaving) void MismatchLockingPlockThenUnlock();  // B-8
    void MismatchLockingSharedLockThenUnlock();  // B-9

    void DupTestIsLocked(); // C-1
    void TestIsLocked(); // C-2
    void TestUnlockAndIsLocked(); // C-3
    // Obsolete(interleaving) void TestPlockAndIsLocked(); // C-4
    void TestPunlockAndIsLocked(); // C-5
    void LockSharedAndIsLocked(); // C-6
    void PremadeIslocked(); // C-7
    void NewDbIslocked(); // C-8
    void PremadeNoLockFlagIslocked(); // C-9
    void NewDbNoLockFlagIslocked(); // C-10
    void PremadePartFlagIslocked(); // C-11
    void NewDbPartFlagIslocked(); // C-12
    void PremadeRWFlagIslocked(); // C-13
    void NewDbRWFlagIslocked(); // C-14
    void PremadeUnlockLockIslocked(); // C-15
    void NewDbUnlockLockIslocked(); // C-16
    void DupLockShareAndIsLocked(); // C-17
    void NoRWFlagLockSharedAndIsLocked(); // C-18
    void NoPartFlagTestPunlockAndIsLocked(); // C-19
    void DupLockIslockedCloseDbIslocked(); // C-20
    void TestPlockAndChildIsLocked(); // C-21

    void ThreadThenTryLock(); // D-1
    void ForkThenTryLock(); // D-2
    void OpenPartFlagTryLockThenFork();  // D-3
    void ThreadAndTryLockWithNoLockFlag();  // D-4
    void OpenNoLockTryLockThenFork(); // D-5
    void ThreadNestsChildTryLocks(); // D-6
    void ThreadsShareHandleNestVsBlockTryLock(); // D-7
    void TryLockCloseReopenReTryLock(); // D-8
    void TryLockCloseChildReopenReTryLock(); // D-9
    void ForkAndTryLockThenLock(); // D-10
    void ForkAndLockThenTryLock(); // D-11

    void SharedLockNewDBnoFlag(); // E-1
    void SharedLockNewDB(); // E-2
    void LockSharedThenChildLockShared(); // E-3
    void ThreadAndLockShared(); // E-4
    void ThreadLockSharedChildReads(); // E-5
    void LockSharedChildReads(); // E-6
    void ThreadLockSharedChildWrites(); // E-7
    void LockSharedChildWrites(); // E-8
    void ThreadReadThenLockShared(); // E-9
    void ThreadsShareHandleShLockVsLock(); // E-10
    void ThreadAndLockSharedVsLockDupHandle(); // E-11
    void ThreadAndLockSharedVsLock(); // E-12
    void LockSharedThenForkAndLock(); // E-13
    void ThreadOpenSharedChildOpensNotShared(); // E-14
    void OpenSharedChildOpensNotShared(); // E-15
    void ThreadShareLockChildIterates(); // E-16
    void LockSharedChildIterates(); // E-17
    void ParentNestsSL_ChildLocksOnce(); // E-18
    void ThreadsShareHandleShLockVsShLock(); // E-19
    void ThreadsShareHandleLockVsShLock(); // E-20
    void MemoryOnlyParentSharedLockChildBlock(); // E-21

    void SharedTryLockNewDBnoFlag(); // F-1
    void ThreadTryLockSharedChildReads(); // F-2
    void TryLockSharedChildReads(); // F-3
    void ThreadTryLockSharedChildWrites(); // F-4
    void TryLockSharedChildWrites(); // F-5
    void ThreadReadThenTryLockShared(); // F-6
    void ThreadsShareHandleShLockVsTryLockShared(); // F-7
    void ThreadAndLockSharedVsTryLockShareddDupHandle(); // F-8
    void LockSharedThenForkTryLockShared(); // F-9
    void TryShared_ThreadOpenSharedChildOpensNotShared(); // F-10
    void TryShared_OpenSharedChildOpensNotShared(); // F-11
    void ThreadTryShareLockChildIterates(); // F-12
    void TryLockSharedChildIterates(); // F-13

    void PartLockNewDBnoFlag(); // G-1
    void PartLockNewDB(); // G-2
    void ThreadPlockChildIterates(); // G-3
    void PlockChildIterates(); // G-4
    void ThreadParentChildKeysInSamePartion(); // G-5
    void ParentChildKeysInSamePartion(); // G-6
    void ThrdParentPlockChildPlockKeysInDiffPart(); // G-7
    void ParentChildKeysInDiffPartions(); // G-8
    void PlockIterationAndAutoUnlock(); // G-9
    void ThreadOpenPartChildOpensNotPart(); // G-10
    void OpenPartChildOpensNotPart(); // G-11
    void ParentNestsChildPlocksOnce(); // G-12
    void MemoryOnlyParentPartLockChildBlock(); // G-13

    void PunlockWithoutLocking(); // H-1
    void ParentIteratePunlockChildIterate(); // H-2
    void ParentPlocksChildPunlocks(); // H-3
    void ThreadNestPlocksThenUnwindPunlock(); // H-4
    // Obsolete(interleaving) void ParentLocksChildPunlocks(); // H-5

    void TryPartLockNewDBnoFlag(); // I-1
    void ThreadTryPlockChildIterates(); // I-2
    void TryPlockChildIterates(); // I-3
    void ThreadTryPlockParentChildKeysInSamePartion(); // I-4
    void TryPlockParentChildKeysInSamePartion(); // I-5
    void ThrdParentTryPlockChildTryPlockKeysInDiffPart(); // I-6
    void TryLockParentChildKeysInDiffPartions(); // I-7
    void TryPlockIterationAndAutoUnlock(); // I-8
    void ThreadOpenPartFlagChildOpenNoPartFlagTry(); // I-9
    void OpenPartFlagChildOpenNoPartFlagTryPlock(); // I-10
    void ThrdParentTryPlockChildPlockKeysInSamePartion(); // I-11
    void ParentPlockChildTryPlockKeysInSamePartion(); // I-12
    void ThrdParentTryPlockChildPlockKeysInDiffPart(); // I-13
    void ParentPlockChildTryPlockKeysInDiffPartions(); // I-14

    void test_OtherAG7();  // General/Other test cases, TC AG7
    void test_OtherAG8();  // General/Other test cases, TC AG8

    void test_OtherAJ8();  // General/Other test cases, TC AJ8
    void test_OtherAJ9();  // General/Other test cases, TC AJ9
    void test_OtherAJ10(); // General/Other test cases, TC AJ10
    void test_OtherAJ11(); // General/Other test cases, TC AJ11
    void test_OtherAJ26(); // General/Other test cases, TC AJ26

    void LockPagesInMemoryFullDB();
    void LockPagesInMemoryGrowDB();
    void LockPagesInMemoryGrowAndCheck();
    void LockPagesInMemoryEmptyDB();
    void LockPagesInMemoryNULL();
    void LockPagesCheckPageFaultsLargeDB();
    void LockPagesInMemoryWindowed();
    void TestDeleteLockfiles(); // Test of mdbm_delete_lockfiles API
    void TestDeleteLockfilesUtil(); // Test of mdbm_delete_lockfiles utility
    void TestDeleteLockfilesNonExistent();



//protected:

    void dupTestIsLocked(string &prefix, int openFlags, LockerFunc &locker, bool closeDB);

    void memoryOnlyParentLockChildBlock(string &prefix, int lockFlag);
    int  makeChild(string &prefix, MDBM *dbh, int lockFlag, string &parentsKey, string &childsKey, FD_RAII &childSendFD);

    u_int getLockedMemory();   // Returns the amount of Virtual memory locked into RAM in Kbytes

    void deleteLockfiles(bool useAPI, const string &fname);
    void DeleteLockfilesTest(bool useAPI);   // shared code for mdbm_delete_lockfiles tests
    void checkLocks(const string &filename);
    void DeleteLockfilesNonExistentTester(const string &prefix, bool useAPI);


//private:

    // package for thread parameters
    struct ThrdParams : public CommonParams
    {
        ThrdParams(string &prefix, int childOpenFlags,
                MDBM *dbh, LockerFunc &lockFunc,
                int childSendPipe, int parSendPipe, bool shouldBeBlocked) :
            CommonParams(prefix, childOpenFlags, lockFunc, 1, shouldBeBlocked),
            _dbh(dbh), _childSendPipe(childSendPipe), _parSendPipe(parSendPipe),
            _status(true), _nestedLocks(0), _expectSuccess(true),
            _statusMsg(""), _quitTest(false)
        {
            pthread_mutex_init(&_mutex, 0);
        }

        MDBM* dbh() { return _dbh; }
        int sendPipe() { return _childSendPipe; }
        int readPipe() { return _parSendPipe; }
        void status(bool success) { MutexRAII excl(_mutex); _status = success; }
        bool status() { return _status; }
        void nestLocks(int cnt) { _nestedLocks = cnt; }
        int  nestLocks() { return _nestedLocks; }
        void expectSuccess(bool success) { _expectSuccess = success; }
        bool expectSuccess() { return _expectSuccess; }
        void   statusMsg(const string &msg) { MutexRAII excl(_mutex); _statusMsg = msg; }
        string statusMsg() { MutexRAII excl(_mutex); return _statusMsg; }

        void quitTest() { MutexRAII excl(_mutex); _quitTest = true; }
        bool shouldQuit() { return _quitTest; }

        MDBM       *_dbh;
        int         _childSendPipe;
        int         _parSendPipe;
        bool        _status;
        int         _nestedLocks;
        bool        _expectSuccess;
        string      _statusMsg;
        bool        _quitTest;

        // lets protect against multi-thread access to the params
        pthread_mutex_t _mutex;
        struct MutexRAII
        {
            MutexRAII(pthread_mutex_t &mutex) : _mutex(mutex)
                { pthread_mutex_lock(&_mutex); }
            ~MutexRAII() { pthread_mutex_unlock(&_mutex); }
            pthread_mutex_t &_mutex;
        };
    };

    struct ThrdMonitor_RAII
    {
        ThrdMonitor_RAII(pthread_t tid) :
            _tid(tid) {}
        ~ThrdMonitor_RAII() { release(0, 0); }
        void set(pthread_t tid) { _tid = tid; }
        void release(ThrdParams *params, MDBM *dbh, bool detach = false)
        {
            if (_tid) {
                if (params) { params->quitTest(); }
                if (dbh) { mdbm_unlock(dbh); } // perhaps? mdbm_lock_reset
                if (_tid && detach) {
                  pthread_detach(_tid); // we wont be waiting for this child
                } else {
                    sleep(1); // give child a chance to unblock and exit
                }
            }
            _tid = 0;
        }

        static void exit(const string &errMsg, ThrdParams *params)
        {
            params->_status = false;
            cout << params->_prefix << errMsg << endl << flush;
            params->_statusMsg = errMsg;
            pthread_exit(0);
        }

        pthread_t  _tid;
    };

#if 0
    mdmb_lock_reset will not be Unit Tested in this story since it is an
    internal interface and may possibly go away in v4.

    struct LockReset : public UnLocker
    {
        LockReset(const string &dbName) : UnLocker(), _dbName(dbName) {}
        int operator()(MDBM *dbh) {
            int ret = mdbm_lock_reset(_dbName, 0);
            return ret == -1 ? -1 : 1;
        }
        bool blockable() { return false; }
        string _dbName;
    };
#endif

    void lockModeTestHelper(const string& prefix, int flags1, int flags2, bool expectFail);
    static void* thrdLockAndAck(void *arg);
    void lockingAndThreading(string &prefix, int parOpenFlags, int childOpenFlags,
        LockerFunc &lockFunc, UnLocker &unlockFunc, LockerFunc &childLockFunc,
        bool dupHandle = false);
    void thrdNestLocksChildLockOnce(string &prefix, int openFlags,
        LockerFunc &parlocker, UnLocker &unlocker, LockerFunc &childLocker);

    static void *thrdLockOrUnlockAndAck(void *arg);
    void thrdShareHandleLock(string &prefix, int openFlags,
        LockerFunc &parlocker, LockerFunc &childLocker, bool expectSuccess);

    static void* thrdCheckLockLoop(void *arg);
    void thrdNestLockUnwind(string &prefix, int openFlags, LockerFunc &locker, UnLocker &unocker);

    void thrdParChildLockPartitions(CommonParams &parParams, const string &_dbName, UnLocker &unlocker, CommonParams &childParams);

    static void* thrdIterates(void *arg);
    void thrdLockAndChildIterates(CommonParams &parParams, int childOpenFlags, UnLocker &unlocker, const string &dbName, bool unlockElseIterate = true);

    // cppunit related design issue:
    // The following are static because I want the work done once but
    // cppunit will make and instance of this class for every unit test
    // instead of re-using the same instance.
    // They are in this class and not the base class since they will be specific
    // to the instance of this class and should not be used in the v3 class.
    static string _dbName;
    static string _utKeyPart1;
    static string _utKeyPart2;

    static string _fetchAck;
    static string _storeAck;
};



string LockV3TestSuite::_dbName;
string LockV3TestSuite::_utKeyPart1;
string LockV3TestSuite::_utKeyPart2;
string LockV3TestSuite::_fetchAck("fetched key:");
string LockV3TestSuite::_storeAck("stored key:");

void
LockV3TestSuite::setUp()
{
    //LockBaseTestSuite::setUp("locktestv2", "locktestv3", "Locking V3 Test Suite");
    LockBaseTestSuite::setUp();

    // now setup a v3 multi partition DB
    string prefix = "Locking V3 Test Suite: init the multi-partition DB: ";

    if (_dbName.empty()) {
        GetTmpDir(_dbName);
        _dbName += "/locktest"+versionString+"tmp.mdbm";
    }

    if (_utKeyPart1.empty() && _utKeyPart2.empty()) {
        string utKeyPart1, utKeyPart2;
        if (setupKeysInDifferentPartitions(prefix, versionFlag, _dbName,
                utKeyPart1, utKeyPart2)) {
            _utKeyPart1 = utKeyPart1;
            _utKeyPart2 = utKeyPart2;
        }
    }
}

void LockV3TestSuite::deleteLockfile()
{
  // wrapped-pthreads all-in-one lock
  string cmd;
  cmd = "rm -rf ";
  cmd += "/tmp/.mlock-named";
  cmd += _dbName;
  cmd += "._int_";
  //fprintf(stderr, "Deleting PthrLock Lockfile via [[%s]]\n", cmd.c_str());
  system(cmd.c_str());
}


#define CHECK_CHILD_EXIT(p) \
  if (p->shouldQuit()) { pthread_exit(0); }

// flexibly emulate CPPUNIT_ASSERT_MESSAGE(msg, test);
// can't just throw, that would delete things the
// child thread is still using....
#define GRACEFUL_ASSERT(msg, test)    \
 if (!(test)) {                       \
   string foo=msg;                    \
   fprintf(stderr, "ASSERT_FAILED! %s:%d :: %s \n", __FILE__, __LINE__, foo.c_str());  \
   fflush(stderr);                    \
   errStr = foo; goto error_handler;  \
 }

void
LockV3TestSuite::lockModeTestHelper(const string& prefix, int flags1, int flags2, bool expectFail) {
  { // try opening exclusive (default), then shared
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    MdbmHolder mdbm1(mdbm_open(_dbName.c_str(), flags1, 0644, 0, 0));
    MdbmHolder mdbm2(mdbm_open(_dbName.c_str(), flags2, 0644, 0, 0));
    int mode1 =  mdbm_get_lockmode(mdbm1);
    int expected=0;
    if (flags1&MDBM_RW_LOCKS) { expected = MDBM_RW_LOCKS; }
    if (flags1&MDBM_PARTITIONED_LOCKS) { expected = MDBM_PARTITIONED_LOCKS; }
    CPPUNIT_ASSERT_MESSAGE(prefix+" first mdbm should open!", NULL != (MDBM*)mdbm1);
    CPPUNIT_ASSERT_MESSAGE(prefix+" wrong lockmode", mode1 == expected);

    if (expectFail) {
      CPPUNIT_ASSERT_MESSAGE(prefix+" second mdbm should not open!", NULL == (MDBM*)mdbm2);
    } else {
      CPPUNIT_ASSERT_MESSAGE(prefix+" second mdbm should open!", NULL != (MDBM*)mdbm2);
      int mode2 = mdbm_get_lockmode(mdbm2);
      CPPUNIT_ASSERT_MESSAGE(prefix+" wrong lockmode on second mdbm", mode2 == expected);
    }
  }
}
void
LockV3TestSuite::LockModeTest()
{
#ifndef DYNAMIC_LOCK_EXPANSION
  // try opening exclusive (default), then shared
  lockModeTestHelper("LockModeTest excl, shar",
      versionFlag, versionFlag|MDBM_RW_LOCKS, true);
  // try opening exclusive (default), then partition
  lockModeTestHelper("LockModeTest excl, part",
      versionFlag, versionFlag|MDBM_PARTITIONED_LOCKS, true);

  // try opening shared, then partition
  lockModeTestHelper("LockModeTest shar, part",
      versionFlag|MDBM_RW_LOCKS, versionFlag|MDBM_PARTITIONED_LOCKS, true);

  // try opening shared, then partition
  lockModeTestHelper("LockModeTest part, shar",
      versionFlag|MDBM_PARTITIONED_LOCKS, versionFlag|MDBM_RW_LOCKS, true);



  // try opening exclusive (default), then shared
  lockModeTestHelper("LockModeTest excl, shar any",
      versionFlag, versionFlag|MDBM_RW_LOCKS|MDBM_ANY_LOCKS, false);

  // try opening exclusive (default), then partition
  lockModeTestHelper("LockModeTest excl, part any",
      versionFlag, versionFlag|MDBM_PARTITIONED_LOCKS|MDBM_ANY_LOCKS, false);

  // try opening shared, then partition
  lockModeTestHelper("LockModeTest shar, part any",
      versionFlag|MDBM_RW_LOCKS, versionFlag|MDBM_PARTITIONED_LOCKS|MDBM_ANY_LOCKS, false);

  // try opening shared, then partition
  lockModeTestHelper("LockModeTest part, shar any",
      versionFlag|MDBM_PARTITIONED_LOCKS, versionFlag|MDBM_RW_LOCKS|MDBM_ANY_LOCKS, false);


  deleteLockfile();
#endif
}

void
LockV3TestSuite::GetLockMode()  // Test Case J-1
{
    string prefix = "TC J-1: GetLockMode: ";
    TRACE_TEST_CASE(prefix);
    int flags  = MDBM_O_CREAT | versionFlag;

    {
        MdbmHolder db = EnsureTmpMdbm("lockmode-none", flags | MDBM_OPEN_NOLOCK );
        uint32_t mode = mdbm_get_lockmode(db);
        CPPUNIT_ASSERT_EQUAL(MDBM_OPEN_NOLOCK, mode);
    }

    {
        MdbmHolder db = EnsureTmpMdbm("lockmode-excl", flags);
        uint32_t mode = mdbm_get_lockmode(db);
        CPPUNIT_ASSERT_EQUAL(0U, mode);
    }

    {
        MdbmHolder db = EnsureTmpMdbm("lockmode-rw", flags | MDBM_RW_LOCKS);
        uint32_t mode = mdbm_get_lockmode(db);
        CPPUNIT_ASSERT(MDBM_RW_LOCKS == mode);
    }

    {
        MdbmHolder db = EnsureTmpMdbm("lockmode-part", flags | MDBM_PARTITIONED_LOCKS);
        uint32_t mode = mdbm_get_lockmode(db);
        CPPUNIT_ASSERT(MDBM_PARTITIONED_LOCKS == mode);
    }
}

void
LockV3TestSuite::LockNewDB()  // Test Case A-1
{
    string prefix = "TC A-1: LockNewDB: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag;
    Locker locker;
    lockNewDB(prefix, openflags, locker);
}

void* LockV3TestSuite::LockV3TestSuite::thrdLockAndAck(void *arg)
{
    ThrdParams *params = static_cast<ThrdParams*>(arg);
    params->_status = false;
    //MdbmHolder db(params->dbh());

    string resp;
    string errMsg = "Parent should have told us to continue: ";
    CHECK_CHILD_EXIT(params);
    bool selret = waitForResponse(params->_parSendPipe, resp, errMsg);
    if (selret == false) {
        ThrdMonitor_RAII::exit(errMsg, params);
    }

    CHECK_CHILD_EXIT(params);
    int ret = params->lockFunc()(params->dbh());
    CHECK_CHILD_EXIT(params);

    if (params->shouldBlock() == true) {
        //mdbm_lock_dump(params->dbh());
        FD_RAII::sendMsg(params->_childSendPipe, _Ack);
    } else {
        if (ret == -1) { // Ok, trylock should have failed
            //mdbm_lock_dump(params->dbh());
            FD_RAII::sendMsg(params->_childSendPipe, _TryLockFailed);
        } else { // shared locks
            FD_RAII::sendMsg(params->_childSendPipe, _Ack);
        }

        CHECK_CHILD_EXIT(params);
        // nested trylock testing
        if (params->nestLocks() && params->_lockFunc.blockable() == false) {
            // parent will make us try again and we should fail again
            resp = "";
            errMsg = "Child: parent should have told us to try again: ";
            selret = waitForResponse(params->_parSendPipe, resp, errMsg);
            if (selret == false) {
                ThrdMonitor_RAII::exit(errMsg, params);
            }

            CHECK_CHILD_EXIT(params);
            ret = params->_lockFunc(params->dbh());
            errMsg = "Child: Tried to lock again and it succeeded but should NOT have: ";
            if (ret != -1) {
                ThrdMonitor_RAII::exit(errMsg, params);
            }
        }

        CHECK_CHILD_EXIT(params);
        // read msg from parent when they tell us they unlocked
        errMsg = "Parent should have told us they unlocked: ";
        selret = waitForResponse(params->_parSendPipe, resp, errMsg, 2);
        if (selret == false) {
            ThrdMonitor_RAII::exit(errMsg, params);
        }

        CHECK_CHILD_EXIT(params);
        ret = params->_lockFunc(params->dbh());  // so trylock should succeed now
        if (ret != 1) {
            cout << params->_prefix
                 << ": Should have successfully trylocked or share-locked" << endl << flush;
            params->_status = false;
        }

        CHECK_CHILD_EXIT(params);
        FD_RAII::sendMsg(params->_childSendPipe, _Ack);
    }

    params->_status = true;
    return 0;
}


void
LockV3TestSuite::lockingAndThreading(string &prefix, int parOpenFlags, int childOpenFlags, LockerFunc &lockFunc, UnLocker &unlockFunc, LockerFunc &childLockFunc, bool dupHandle)
{
    string suffix = "locking" + versionString + ".mdbm";
    const string dbName = GetTmpName(suffix);
    string errStr;
    stringstream retmsg;
    stringstream msghdrss;
    vector<int> fds;
    vector<int> readyFds;
    string errMsg;
    string msghdr;
    bool killChild = false;

    MdbmHolder dbh(EnsureNewNamedMdbm(dbName, parOpenFlags | MDBM_O_RDWR));
    InsertData(dbh);

    MdbmHolder c_dbh(
        dupHandle
        ? mdbm_dup_handle(dbh, childOpenFlags | MDBM_O_RDWR)
        :  EnsureNamedMdbm(dbName, childOpenFlags | MDBM_O_RDWR) );

    // use pipes as fairly easy way to synchronize parent with child
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    bool shouldBeBlocked = childLockFunc.blockable();
    if (lockFunc.sharable() && childLockFunc.sharable()) {
        // all shared locking must play by same open rules else same as mutex
        if (parOpenFlags & childOpenFlags & MDBM_RW_LOCKS) {
            shouldBeBlocked = false;
        }
    }

    // package childs parameters
    ThrdParams params(prefix, childOpenFlags, c_dbh, childLockFunc,
        childSendFD, parReadFD, shouldBeBlocked);

    // create child
    pthread_t tid;
    int ret = pthread_create(&tid, 0, thrdLockAndAck, &params);
    stringstream frkss;
    frkss << prefix
          << " Could not thread the child - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 0));
//    c_dbh.Release();

    ThrdMonitor_RAII child(tid); // have to create here to alow GRACEFUL_ASSERT() jump

    msghdrss << prefix << "Parent:child(" << tid << "): ";
    msghdr = msghdrss.str();

    ret = lockFunc(dbh);
    if (ret != 1) {
        child.release(&params, dbh, true);
        errMsg = "Parent: lock FAILed. Cannot continue this test";
        GRACEFUL_ASSERT(errMsg.insert(0, prefix), (ret == 1));
    }

    // signal child that it may lock the DB
    ret = parSendFD.sendMsg(_Continue);

    fds.push_back(childReadFD);
    try {
        readyFds = readSelect(fds, 1);

        stringstream selss;
        selss << msghdr;
        if ((parOpenFlags | childOpenFlags) & MDBM_OPEN_NOLOCK) {
            // if anyone specified no lock, then there will be no
            // enforced locking
            selss << " Child should NOT have been blocked" << endl;
            killChild = (readyFds.size() <= 0);
            GRACEFUL_ASSERT(selss.str(), (!killChild));
        } else if (shouldBeBlocked) {
            selss << " Child should have been blocked" << endl;
            killChild = (readyFds.size() != 0);
            GRACEFUL_ASSERT(selss.str(), (!killChild));
        } else { // shared locks and trylock type functions should not have blocked
            selss << " Child should NOT have been blocked {trylock or parent/child shared locking}" << endl;
            killChild = (readyFds.size() <= 0);
            GRACEFUL_ASSERT(selss.str(), (!killChild));

            char buff[64];
            int ret = read(childReadFD, buff, sizeof(buff));

            string ack = _TryLockFailed;
            if (childLockFunc.sharable()) {
                if (childLockFunc.blockable()) {
                    ack = _Ack;
                } else if (parOpenFlags & childOpenFlags & MDBM_RW_LOCKS) {
                    ack = _Ack;
                }
            }

            stringstream rbss;
            rbss << prefix
                 << " Child reported(" << buff
                 << ") but should have reported lock failure" << endl;
            killChild = (ret <= 0 && strcmp(ack.c_str(), buff) != 0);
            GRACEFUL_ASSERT(rbss.str(), (!killChild));
        }
    } catch (SelectError &serr) {
        // ohoh! problem with select on the pipe
        stringstream selss;
        selss << prefix << " Problem selecting on the pipe - cannot continue this test" << endl;
        killChild = true;
        GRACEFUL_ASSERT(selss.str(), (!killChild));
    }

    ret = unlockFunc(dbh);

    if (shouldBeBlocked == false) {
        ret = parSendFD.sendMsg(_Continue);
    }

    try {
        string resp;
        string errMsg = "Waiting on child to lock DB: ";
        bool selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        GRACEFUL_ASSERT(errMsg.insert(0, prefix), (!killChild));

        // yay! child acknowledged it got hold of the DB
        GetLogStream() << prefix
                     << " parent: child=" << tid
                     << ": Child ACKnowledged it locked the DB: " << resp << endl;
    } catch (SelectError &serr) {
        // boo! child is blocked or something else went wrong
        // perhaps send kill to child pid
        child.release(&params, dbh, true);
    }

    ret = pthread_join(tid, 0);
    retmsg << prefix
           << "Child thread(" << tid
           << ") reports FAILure: " << params.statusMsg() << endl;
    CPPUNIT_ASSERT_MESSAGE(retmsg.str(), (params._status == true));

   return;

error_handler:
    dbh.Close(); // should release locks
    params.quitTest();
    ret = pthread_join(tid, 0);
    CPPUNIT_ASSERT_MESSAGE(errStr, (true==false));
}

void
LockV3TestSuite::ThreadThenLock() // TC A-2
{
    string prefix = "TC A-2: ThreadThenLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   parlocker;
    UnLocker unlocker;
    Locker   childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::ForkThenLock()  // Test Case A-3
{
    string prefix = "TC A-3: ForkThenLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags =  versionFlag;
    Locker locker;
    UnLocker unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ForkAndLockWithPartFlag()  // Test Case A-4
{
    string prefix = "TC A-4: ForkAndLockWithPartFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = MDBM_PARTITIONED_LOCKS | versionFlag;
    Locker locker;
    UnLocker unlocker;
    // FIXME fail here....
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}

void
LockV3TestSuite::ThreadAndLockWithNoLockFlag()  // Test Case A-5
{
    string prefix = "TC A-5: ThreadAndLockWithNoLockFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = MDBM_OPEN_NOLOCK | versionFlag;
    Locker   parlocker;
    UnLocker unlocker;
    Locker   childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::ForkAndLockWithNoLockFlag()  // Test Case A-6
{
    string prefix = "TC A-6: ForkAndLockWithNoLockFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = MDBM_OPEN_NOLOCK | versionFlag;
    Locker locker;
    UnLocker unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ForkAndLockWithChildNoLockFlag()  // Test Case A-7
{
    string prefix = "TC A-7: ForkAndLockWithChildNoLockFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenFlags   = versionFlag;
    int childOpenFlags = MDBM_OPEN_NOLOCK | versionFlag;
    Locker locker;
    UnLocker unlocker;
    lockingAndForking(prefix, parOpenFlags, childOpenFlags, locker, unlocker, locker);
}
void
LockV3TestSuite::ParentNestsChildLocksOnce()  // TC A-8
{
    string prefix = "TC A-8: ParentNestsChildLocksOnce: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    Locker   locker;
    UnLocker unlocker;
    Locker   childlocker;
    nestLocksChildLockOnce(prefix, versionFlag, locker, unlocker, childlocker);
}
void
LockV3TestSuite::thrdNestLocksChildLockOnce(string &prefix, int openFlags, LockerFunc &parlocker, UnLocker &unlocker, LockerFunc &childLocker)
{
    string suffix = "locking" + versionString + ".mdbm";
    const string dbName = GetTmpName(suffix);

    MdbmHolder dbh(EnsureNewNamedMdbm(dbName, openFlags | MDBM_O_RDWR));
    InsertData(dbh);

    MdbmHolder c_dbh(EnsureNamedMdbm(dbName, openFlags | MDBM_O_RDWR));

    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    // package childs parameters
    ThrdParams params(prefix, openFlags, c_dbh, childLocker,
        childSendFD, parReadFD, childLocker.blockable());
    params.nestLocks(3);

    // create child
    pthread_t tid;
    int ret = pthread_create(&tid, 0, thrdLockAndAck, &params);
    stringstream frkss;
    frkss << prefix
          << " Could not thread the child - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 0));

    bool killChild = false;
    ThrdMonitor_RAII child(tid);

    ret = parlocker(dbh); // first locking

    // signal child to perform lock
    parSendFD.sendMsg(_Continue);

    ret = parlocker(dbh); // nested lock
    ret = parlocker(dbh); // nested lock again - 3rd locking

    // wait for child to say they got the lock - should time-out
    string resp;
    string lockErrMsg = "Parent set nested locks. Child should not be able to obtain Lock: ";
    bool selerr = waitForResponse(childReadFD, resp, lockErrMsg);
    killChild = (selerr == true);
    if (childLocker.blockable() == false && selerr == true) {
        // ensure it sent the right message
        if (resp.compare(_TryLockFailed) == 0) {
            killChild = false;
        }
    }
    CPPUNIT_ASSERT_MESSAGE(lockErrMsg.insert(0, prefix), (!killChild));

    // unlock twice and verify child is still blocked
    ret = unlocker(dbh);
    ret = unlocker(dbh);

    if (childLocker.blockable() == false) { // tell child to trylock again
        parSendFD.sendMsg(_Continue);
    }

    // wait for child to say they got the lock - should time-out
    string stillNestedErrMsg = "Locks still nested. Child should still not be able to obtain Lock: ";
    selerr = waitForResponse(childReadFD, resp, stillNestedErrMsg);
    killChild = (selerr == true);
    CPPUNIT_ASSERT_MESSAGE(stillNestedErrMsg.insert(0, prefix), (!killChild));

    // unlock for 3rd and last time, now child should unblock
    ret = unlocker(dbh);

    if (childLocker.blockable() == false) { // tell child to trylock again
        parSendFD.sendMsg(_Continue);
    }

    string unlockErrMsg = "NO Locks nested. Child should have been able to obtain Lock: ";
    selerr = waitForResponse(childReadFD, resp, unlockErrMsg);
    killChild = (selerr == false);
    if (selerr == false) {
        child.release(&params, dbh, true);
    }
    CPPUNIT_ASSERT_MESSAGE(unlockErrMsg.insert(0, prefix), (!killChild));

    ret = pthread_join(tid, 0);
    stringstream retmsg;
    retmsg << prefix
           << "Child thread(" << tid
           << ") reports FAILure: " << params.statusMsg() << endl;
    CPPUNIT_ASSERT_MESSAGE(retmsg.str(), (params._status == true));
}
void
LockV3TestSuite::ThreadNestsChildLocksOnce()  // TC A-9
{
    string prefix = "TC A-9: ThreadNestsChildLocksOnce: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    Locker   parlocker;
    UnLocker unlocker;
    Locker   childlocker;
    thrdNestLocksChildLockOnce(prefix, versionFlag, parlocker, unlocker, childlocker);
}
void* LockV3TestSuite::LockV3TestSuite::thrdLockOrUnlockAndAck(void *arg)
{
    ThrdParams *params = static_cast<ThrdParams*>(arg);
    params->_status = false;
    MdbmHolder dbh(params->dbh());

    string resp;
    string errMsg = "Parent should have told us to continue: ";
    //GetLogStream() << errMsg;
    CHECK_CHILD_EXIT(params);
    bool selret = waitForResponse(params->_parSendPipe, resp, errMsg);
    if (selret == false)
        ThrdMonitor_RAII::exit(errMsg, params);

    CHECK_CHILD_EXIT(params);
    int ret = params->_lockFunc(params->dbh());

    params->_status = true;
    if (params->expectSuccess()) {
        if (ret == -1) {
            errMsg = "Expected success but the lock/unlock FAILed";
            params->_status = false;
        }
    } else if (ret == 1) {
        errMsg = "Expected failure but the lock/unlock succeeded and should NOT have";
        params->_status = false;
    }

    CHECK_CHILD_EXIT(params);
    if (params->_status == false) {
        FD_RAII::sendMsg(params->_childSendPipe, _NAck);
        ThrdMonitor_RAII::exit(errMsg, params);
    }

    CHECK_CHILD_EXIT(params);
    FD_RAII::sendMsg(params->_childSendPipe, _Ack);
    return 0;
}
void
LockV3TestSuite::thrdShareHandleLock(string &prefix, int openFlags, LockerFunc &parlocker, LockerFunc &childLocker, bool expectSuccess)
{
    // parent locks; tells child to lock
    // child locks; reports success or failure to parent
    // parent waits for child report
    string suffix = "locking" + versionString + ".mdbm";
    const string dbName = GetTmpName(suffix);

    MdbmHolder dbh(EnsureNewNamedMdbm(dbName, openFlags | MDBM_O_RDWR));
    InsertData(dbh);

    // use pipes as fairly easy way to synchronize parent with child
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    // package childs parameters
    ThrdParams params(prefix, openFlags, dbh, childLocker,
        childSendFD, parReadFD, childLocker.blockable());
    params._expectSuccess = expectSuccess;

    // create child
    pthread_t tid;
    int ret = pthread_create(&tid, 0, thrdLockOrUnlockAndAck, &params);
    stringstream frkss;
    frkss << prefix
          << " Could not thread the child - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 0));

    bool killChild = false;
    ThrdMonitor_RAII child(tid);

    ret = parlocker(dbh);
    string errLckMsg = "Parent: FAILed to lock DB. Cannot continue this test: ";
    killChild = (ret == -1);
    CPPUNIT_ASSERT_MESSAGE(errLckMsg.insert(0, prefix), (!killChild));

    // signal child that it may lock/unlock the DB
    ret = parSendFD.sendMsg(_Continue);

    string resp;
    string errMsg = "Parent: child should NOT block doing nested lock/unlock DB: ";
    if (childLocker.blockable()) {
        errMsg = "Parent: child should block doing nested lock of DB: ";
    }

    bool selret = waitForResponse(childReadFD, resp, errMsg);
    killChild = (selret == false);
    if (selret == false) { // child is blocked
        if (childLocker.blockable()) { // OK, child is supposed to have blocked
            killChild = false;
            mdbm_unlock(dbh); // release the blocked child

            errMsg = "Parent: child should now be unblocked doing nested lock of DB: ";
            selret = waitForResponse(childReadFD, resp, errMsg);
            killChild = (selret == false);
        } else {
            killChild = true; // Ohoh, child should NOT have blocked
            child.release(&params, dbh, true);
        }
    } else if (childLocker.blockable()) { // child should have blocked ?
        // if both are sharable type locks then it wont block
        // otherwise there should be mutex locking to cause blocking
        if (!childLocker.sharable() || !parlocker.sharable()) {
            killChild = true;
        }
    }

    CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

    ret = pthread_join(tid, 0);
    stringstream retmsg;
    retmsg << prefix
           << "Child thread(" << tid
           << ") reports FAILure: " << params.statusMsg() << endl;
    CPPUNIT_ASSERT_MESSAGE(retmsg.str(), (params._status == true));
}
void
LockV3TestSuite::ThreadsShareHandleNestVsBlock() // TC A-10
{
    string prefix = "TC A-10: ThreadsShareHandleNestVsBlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    Locker   parlocker;
    Locker   childlocker;
    thrdShareHandleLock(prefix, versionFlag, parlocker, childlocker, true);
}
void
LockV3TestSuite::LockCloseReopenRelock()  // Test Case A-11
{
    string prefix = "TC A-11: LockCloseReopenRelock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    Locker locker;
    lockCloseReopenRelock(prefix, versionFlag, locker);
}

void
LockV3TestSuite::ThreadThenTryLock() // TC D-1
{
    string prefix = "TC D-1: ThreadThenTryLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    TryLocker  parlocker;
    UnLocker   unlocker;
    TryLocker  childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::ForkThenTryLock()  // Test Case D-2
{
    string prefix = "TC D-2: ForkThenTryLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags =  versionFlag;
    TryLocker locker;
    UnLocker  unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}

void
LockV3TestSuite::OpenPartFlagTryLockThenFork()  // Test Case D-3
{
    string prefix = "TC D-3: OpenPartFlagTryLockThenFork: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag | MDBM_PARTITIONED_LOCKS;
    TryLocker locker;
    UnLocker  unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ThreadAndTryLockWithNoLockFlag()  // Test Case D-4
{
    string prefix = "TC D-4: ThreadAndTryLockWithNoLockFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = MDBM_OPEN_NOLOCK | versionFlag;
    TryLocker parlocker;
    UnLocker  unlocker;
    TryLocker childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::OpenNoLockTryLockThenFork()  // Test Case D-5
{
    string prefix = "TC D-5: OpenNoLockTryLockThenFork: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = MDBM_OPEN_NOLOCK | versionFlag;
    TryLocker locker;
    UnLocker unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ThreadNestsChildTryLocks()  // TC D-6
{
    if (GetTestLevel() < RunAllTestCases) {
       return;
    }

    string prefix = "TC D-6: ThreadNestsChildTryLocks: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    TryLocker   parlocker;
    UnLocker    unlocker;
    TryLocker   childlocker;
    thrdNestLocksChildLockOnce(prefix, versionFlag, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::ThreadsShareHandleNestVsBlockTryLock() // TC D-7
{
    string prefix = "TC D-7: ThreadsShareHandleNestVsBlockTryLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    TryLocker   parlocker;
    TryLocker   childlocker;
    thrdShareHandleLock(prefix, versionFlag, parlocker, childlocker, false);
}
void
LockV3TestSuite::TryLockCloseReopenReTryLock() // Test Case D-8
{
    string prefix = "TC D-8: TryLockCloseReopenReTryLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    TryLocker locker;
    lockCloseReopenRelock(prefix, versionFlag, locker);
}
void
LockV3TestSuite::TryLockCloseChildReopenReTryLock() // Test Case D-9
{
    string prefix = "TC D-9: TryLockCloseChildReopenReTryLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    TryLocker locker;
    lockCloseChildReopenRelock(prefix, versionFlag, locker);
}
void
LockV3TestSuite::ForkAndTryLockThenLock()  // Test Case D-10
{
    string prefix = "TC D-10: ForkAndTryLockThenLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags =  versionFlag;
    TryLocker trylocker;
    Locker    locker;
    UnLocker  unlocker;
    lockingAndForking(prefix, openflags, openflags, trylocker, unlocker, locker);
}
void
LockV3TestSuite::ForkAndLockThenTryLock()  // Test Case D-11
{
    string prefix = "TC D-11: ForkAndLockThenTryLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags =  versionFlag;
    Locker    locker;
    TryLocker trylocker;
    UnLocker  unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, trylocker);
}
#if 0
void
LockV3TestSuite::UnlockWithoutLocking() // B-1
{
    string prefix = "TC B-1: UnlockWithoutLocking: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags =  versionFlag;
    UnLocker unlocker;
    lockNewDB(prefix, openflags, unlocker, -1);
}
void
LockV3TestSuite::ParentLocksChildUnlocks() // B-2
{
    string prefix = "TC B-2: ParentLocksChildUnlocks: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   locker;
    UnLocker unlocker;
    CommonParams params(prefix, openFlags, locker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, unlocker, false);
}
void
LockV3TestSuite::ParentLockSharedChildUnlocks() // B-3
{
    string prefix = "TC B-3: ParentLockSharedChildUnlocks: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    CommonParams params(prefix, openFlags, locker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, unlocker, false);
}
void
LockV3TestSuite::ThreadThenChildUnlock() // TC B-4
{
    string prefix = "TC B-4: ThreadThenChildUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   parlocker;
    UnLocker unlocker;
    UnLocker childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::ParentLocksChildUnlocksSharedHandle() // B-5
{
    string prefix = "TC B-5: ParentLocksChildUnlocksSharedHandle: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   locker;
    UnLocker unlocker;
    CommonParams params(prefix, openFlags, locker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, unlocker, true);
}
#endif

void
LockV3TestSuite::ThreadsShareHandleNestVsBlockUnlock() // TC B-6
{
    string prefix = "TC B-6: ThreadsShareHandleNestVsBlockUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    Locker   parlocker;
    UnLocker childunlocker;
    thrdShareHandleLock(prefix, versionFlag, parlocker, childunlocker, false);
}
void* LockV3TestSuite::LockV3TestSuite::thrdCheckLockLoop(void *arg)
{
    ThrdParams *params = static_cast<ThrdParams*>(arg);
    params->_status = true;
    MdbmHolder dbh(params->dbh());

    // in loop (_nestedCnt + 1 times)
    // - wait for parent to tell us to check lock
    // - do mdbm_islocked
    // - report locked or unlocked
    string resp;
    string errMsg = "Parent should have told us to test the lock: ";
    for (int cnt = 0; cnt < params->nestLocks(); ++cnt) {
        bool selret = waitForResponse(params->_parSendPipe, resp, errMsg);
        if (selret == false) {
            ThrdMonitor_RAII::exit(errMsg, params);
        }

        resp = "";

        int ret = mdbm_islocked(params->dbh()); // DB should be locked
        if (ret == 1) {
            FD_RAII::sendMsg(params->_childSendPipe, _Ack);
        } else {
            //mdbm_lock_dump(params->dbh());
            FD_RAII::sendMsg(params->_childSendPipe, _NAck);
            errMsg = "Child: mdbm_islocked reported NOT locked but the DB should have been locked";
            ThrdMonitor_RAII::exit(errMsg, params);
        }
    }

    resp = "";
    bool selret = waitForResponse(params->_parSendPipe, resp, errMsg);
    if (selret == false) {
        ThrdMonitor_RAII::exit(errMsg, params);
    }

    int ret = mdbm_islocked(params->dbh()); // now DB should NOT be locked
    if (ret == 0) {
        FD_RAII::sendMsg(params->_childSendPipe, _Ack);
    } else {
        FD_RAII::sendMsg(params->_childSendPipe, _NAck);
        errMsg = "Child: mdbm_islocked reported locked but the DB should NOT have been locked";
        ThrdMonitor_RAII::exit(errMsg, params);
    }

    return 0;
}
void
LockV3TestSuite::thrdNestLockUnwind(string &prefix, int openFlags, LockerFunc &locker, UnLocker &unlocker)
{
    // use the multi-page DB
    MdbmHolder dbh(EnsureNamedMdbm(_dbName, openFlags | MDBM_O_RDWR));
    if (!(MDBM*)dbh) {
        cout << "WARNING: " << prefix
             << "thrdNestLockUnwind: mdbm_open FAILed for DB=" << _dbName.c_str()
             << " so cannot continue the test" << endl << flush;
        return;
    }

    MdbmHolder c_dbh(EnsureNamedMdbm(_dbName.c_str(), openFlags | MDBM_O_RDWR));

    // use pipes as fairly easy way to synchronize parent with child
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    // package childs parameters
    IsLocked islocked;
    ThrdParams params(prefix, openFlags, c_dbh, islocked,
        childSendFD, parReadFD, islocked.blockable());
    params.nestLocks(3);

    // create child
    pthread_t tid;
    int ret = pthread_create(&tid, 0, thrdCheckLockLoop, &params);
    stringstream frkss;
    frkss << prefix
          << " Could not thread the child - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 0));
    c_dbh.Release();

    ThrdMonitor_RAII child(tid);

    // nest the locks
    for (int cnt = 0; cnt < params.nestLocks(); ++cnt) {
        int ret = locker(dbh);
        if (ret == -1) {
            stringstream lkss;
            lkss << prefix
                 << "WARNING: Failed to lock the DB, cannot continue with this unit test" << endl;
            child.release(&params, dbh, true);
            CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 1));
        }
    }

    // loop 3 times, each time thru:
    // - tell child to check if locked
    // - verify child reports it is locked
    // - unlock
    string resp;
    string lockErrMsg = "Parent set nested locks. Child should not be able to obtain Lock: ";
    for (int cnt = 0; cnt < params.nestLocks(); ++cnt) {
        parSendFD.sendMsg(_Continue);

        resp = "";
        bool selret = waitForResponse(childReadFD, resp, lockErrMsg);
        if (selret == false) {
            //mdbm_lock_dump(dbh);
            child.release(&params, dbh, true);
        } else if (resp.compare(_Ack) != 0) {
            //mdbm_lock_dump(dbh);
            selret = false;
            child.release(&params, dbh, true);
        }
        CPPUNIT_ASSERT_MESSAGE(lockErrMsg.insert(0, prefix), (selret == true));

        int ret = unlocker(dbh);
        if (ret == -1) {
            stringstream lkss;
            lkss << prefix
                 << "WARNING: Failed to unlock the DB, cannot continue with this unit test" << endl;
            child.release(&params, dbh, true);
            CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 1));
        }
    }

    // after loop, tell child to check if locked
    // - verify child reports it is locked
    parSendFD.sendMsg(_Continue);

    resp = "";
    bool selret = waitForResponse(childReadFD, resp, lockErrMsg);
    if (selret == false) {
        child.release(&params, dbh, true);
    } else if (resp.compare(_Ack) != 0) {
        selret = false;
        child.release(&params, dbh, true);
    }
    CPPUNIT_ASSERT_MESSAGE(lockErrMsg.insert(0, prefix), (selret == true));

    ret = pthread_join(tid, 0);
    stringstream retmsg;
    retmsg << prefix
           << "Child thread(" << tid
           << ") reports FAILure: " << params.statusMsg() << endl;
    CPPUNIT_ASSERT_MESSAGE(retmsg.str(), (params._status == true));
}
void
LockV3TestSuite::ThreadNestLocksThenUnwindUnlock() // B-7
{
    string prefix = "TC B-7: ThreadNestLocksThenUnwindUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    Locker   parlocker;
    UnLocker childunlocker;
    thrdNestLockUnwind(prefix, versionFlag, parlocker, childunlocker);
}
#if 0
// Obsolete(interleaving)
void
LockV3TestSuite::MismatchLockingPlockThenUnlock() // B-8
{
    string prefix = "TC B-8: MismatchLockingPlockThenUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    UnLocker   unlocker;
    int openflags =  versionFlag | MDBM_PARTITIONED_LOCKS;
    lockAndUnlock(prefix, openflags, locker, unlocker);
}
#endif
void
LockV3TestSuite::MismatchLockingSharedLockThenUnlock() // B-9
{
    string prefix = "TC B-9: MismatchLockingSharedLockThenUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    SharedLocker locker;
    UnLocker   unlocker;
    int openflags =  versionFlag | MDBM_RW_LOCKS;
    lockAndUnlock(prefix, openflags, locker, unlocker);
}

void
LockV3TestSuite::dupTestIsLocked(string &prefix, int openFlags, LockerFunc &locker, bool closeDB)
{
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | openFlags));

    MDBM *dupdbh = mdbm_dup_handle(dbh, openFlags | MDBM_O_RDWR);
    int ret = locker(dbh);
    if (ret == -1) {
        cout << "WARNING: " << prefix << " Failed to lock the DB, cannot continue with this unit test" << endl << flush;
        mdbm_close(dupdbh);
        return;
    }

    ret = mdbm_islocked(dupdbh);
    stringstream lkmsg;
    lkmsg << prefix
              << " DB should be locked, but mdbm_islocked reports DB is unlocked" << endl;
    CPPUNIT_ASSERT_MESSAGE(lkmsg.str(), (ret == 1));

    if (closeDB) {
        mdbm_close(dbh);
        dbh.Release();
    } else if (mdbm_unlock(dbh) == -1) {
        cout << "WARNING: " << prefix << " Failed to unlock the DB, cannot continue with this unit test" << endl << flush;
        mdbm_close(dupdbh);
        return;
    }

    ret = mdbm_islocked(dupdbh);
    stringstream ulmsg;
    ulmsg << prefix
          << " DB should not be locked, but mdbm_islocked reports DB is locked" << endl;
    CPPUNIT_ASSERT_MESSAGE(ulmsg.str(), (ret == 0));
    mdbm_close(dupdbh);
}

void
LockV3TestSuite::DupTestIsLocked() // C-1
{
    string prefix = "TC C-1: DupTestIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   locker;
    dupTestIsLocked(prefix, openFlags, locker, false);
}
void
LockV3TestSuite::TestIsLocked() // C-2
{
    string prefix = "TC C-2: TestIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   locker;
    UnLocker unlocker;
    CommonParams params(prefix, openFlags, locker, 1, false);
    testIsLocked(params, unlocker, 1, 1, 0, true);
}
void
LockV3TestSuite::TestUnlockAndIsLocked() // C-3
{
    string prefix = "TC C-3: TestUnlockAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   locker;
    UnLocker unlocker;
    testIsLockedParentChild(prefix, openFlags, locker, unlocker, openFlags);
}
#if 0
// Obsolete(interleaving)
void
LockV3TestSuite::TestPlockAndIsLocked() // C-4
{
    string prefix = "TC C-4: TestPlockAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(2);
    PartLocker locker(keyname);
    UnLocker   unlocker;
    CommonParams params(prefix, openFlags, locker, 1, false);
    testIsLocked(params, unlocker, 1, 0, 0, true);
}
#endif
void
LockV3TestSuite::TestPunlockAndIsLocked() // C-5
{
    string prefix = "TC C-5: TestPunlockAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(2);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    CommonParams params(prefix, openFlags, locker, 1, false);
    testIsLocked(params, unlocker, 1, 0, 0, true);
}
void
LockV3TestSuite::LockSharedAndIsLocked() // C-6
{
    string prefix = "TC C-6: LockSharedAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    SharedLocker locker;
    UnLocker     unlocker;
    CommonParams params(prefix, openFlags, locker, 1, false);
    testIsLocked(params, unlocker, 1, 1, 0, true);
}
void
LockV3TestSuite::PremadeIslocked() // C-7
{
    string prefix = "TC C-7: PremadeIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag;
    openIsLocked(prefix, openFlags, false);
}
void
LockV3TestSuite::NewDbIslocked() // C-8
{
    string prefix = "TC C-8: NewDbIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag;
    openIsLocked(prefix, openFlags, true);
}
void
LockV3TestSuite::PremadeNoLockFlagIslocked() // C-9
{
    string prefix = "TC C-9: PremadeNoLockFlagIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_OPEN_NOLOCK;
    openIsLocked(prefix, openFlags, false);
}
void
LockV3TestSuite::NewDbNoLockFlagIslocked() // C-10
{
    string prefix = "TC C-10: NewDbNoLockFlagIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_OPEN_NOLOCK;
    openIsLocked(prefix, openFlags, true);
}
void
LockV3TestSuite::PremadePartFlagIslocked() // C-11
{
    string prefix = "TC C-11: PremadePartFlagIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    openIsLocked(prefix, openFlags, false);
}
void
LockV3TestSuite::NewDbPartFlagIslocked() // C-12
{
    string prefix = "TC C-12: NewDbPartFlagIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    openIsLocked(prefix, openFlags, true);
}
void
LockV3TestSuite::PremadeRWFlagIslocked() // C-13
{
    string prefix = "TC C-13: PremadeRWFlagIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    openIsLocked(prefix, openFlags, false);
}
void
LockV3TestSuite::NewDbRWFlagIslocked() // C-14
{
    string prefix = "TC C-14: NewDbRWFlagIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    openIsLocked(prefix, openFlags, true);
}
void
LockV3TestSuite::PremadeUnlockLockIslocked() // C-15
{
    string prefix = "TC C-15: PremadeUnlockLockIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag;
    UnLocker unlocker;
    Locker   locker;
    CommonParams params(prefix, openFlags, unlocker, -1, false);
    testIsLocked(params, locker, 1, 0, 1, false);
}
void
LockV3TestSuite::NewDbUnlockLockIslocked() // C-16
{
    string prefix = "TC C-16: NewDbUnlockLockIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag;
    UnLocker unlocker;
    Locker   locker;
    CommonParams params(prefix, openFlags, unlocker, -1, false);
    testIsLocked(params, locker, 1, 0, 1, true);
}
void
LockV3TestSuite::DupLockShareAndIsLocked() // C-17
{
    string prefix = "TC C-17: DupLockShareAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    SharedLocker   locker;
    dupTestIsLocked(prefix, openFlags, locker, false);
}
void
LockV3TestSuite::NoRWFlagLockSharedAndIsLocked() // C-18
{
    string prefix = "TC C-18: NoRWFlagLockSharedAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag;
    SharedLocker locker;
    UnLocker     unlocker;
    CommonParams params(prefix, openFlags, locker, 1, false);
    testIsLocked(params, unlocker, 1, 1, 0, true);
}
void
LockV3TestSuite::NoPartFlagTestPunlockAndIsLocked() // C-19
{
    string prefix = "TC C-19: NoPartFlagTestPunlockAndIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag;
    string keyname = GetDefaultKey(2);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    CommonParams params(prefix, openFlags, locker, 1, false);
    testIsLocked(params, unlocker, 1, 1, 0, true);
    // different from v2 behaviour, this will lock the DB but v2 wont
}
void
LockV3TestSuite::DupLockIslockedCloseDbIslocked() // C-20
{
    string prefix = "TC C-20: DupLockIslockedCloseDbIslocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag;
    Locker   locker;
    dupTestIsLocked(prefix, openFlags, locker, true);
}
void
LockV3TestSuite::TestPlockAndChildIsLocked() // C-21
{
#if 0
// FIX BZ 5384701
    string prefix = "TC C-21: TestPlockAndChildIsLocked: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags  =  versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    testIsLockedParentChild(prefix, openFlags, locker, unlocker, openFlags);
#endif
}

void
LockV3TestSuite::SharedLockNewDBnoFlag()  // Test Case E-1
{
    string prefix = "TC E-1: SharedLockNewDBnoFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag;
    SharedLocker locker;
    lockNewDB(prefix, openflags, locker);
}
void
LockV3TestSuite::SharedLockNewDB()  // Test Case E-2
{
    string prefix = "TC E-2: SharedLockNewDB: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    lockNewDB(prefix, openflags, locker);
}
void
LockV3TestSuite::LockSharedThenChildLockShared()  // Test Case E-3
{
    string prefix = "TC E-3: LockSharedThenChildLockShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ThreadAndLockShared()  // Test Case E-4
{
    string prefix = "TC E-4: ThreadAndLockShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker parlocker;
    UnLocker     unlocker;
    SharedLocker childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::ThreadLockSharedChildReads() // Test Case E-5
{
    // child should not block
    string prefix = "TC E-5: ThreadLockSharedChildReads: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    string keyname = GetDefaultKey(3);
    SharedReader reader(keyname);
    lockingAndThreading(prefix, openFlags, openFlags, locker, unlocker, reader);
}
void
LockV3TestSuite::LockSharedChildReads() // Test Case E-6
{
    // child should not block
    string prefix = "TC E-6: LockSharedChildReads: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    string keyname = GetDefaultKey(3);
    SharedReader reader(keyname);
    lockingAndForking(prefix, openFlags, openFlags, locker, unlocker, reader);
}
void
LockV3TestSuite::ThreadLockSharedChildWrites() // Test Case E-7
{
    // child should block
    string prefix = "TC E-7: ThreadLockSharedChildWrites: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    string keyname = GetDefaultKey(3);
    UnsharedWriter writer(keyname);
    lockingAndThreading(prefix, openFlags, openFlags, locker, unlocker, writer);
}
void
LockV3TestSuite::LockSharedChildWrites() // Test Case E-8
{
    // child should block until parent unlocks
    string prefix = "TC E-8: LockSharedChildWrites: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    string keyname = GetDefaultKey(3);
    UnsharedWriter writer(keyname);
    lockingAndForking(prefix, openFlags, openFlags, locker, unlocker, writer);
}
void
LockV3TestSuite::ThreadReadThenLockShared() // Test Case E-9
{
    string prefix = "TC E-9: ThreadReadThenLockShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    KeyReader    reader;
    UnLocker     unlocker;
    SharedLocker locker;
    lockingAndThreading(prefix, openFlags, openFlags, reader, unlocker, locker);
}
void
LockV3TestSuite::ThreadsShareHandleShLockVsLock() // TC E-10
{
    string prefix = "TC E-10: ThreadsShareHandleShLockVsLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker parlocker;
    Locker       childlocker;
    thrdShareHandleLock(prefix, openFlags, parlocker, childlocker, true);
}
void
LockV3TestSuite::ThreadAndLockSharedVsLockDupHandle()  // Test Case E-11
{
    string prefix = "TC E-11: ThreadAndLockSharedVsLockDupHandle: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker parlocker;
    UnLocker     unlocker;
    Locker       childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker, true);
}
void
LockV3TestSuite::ThreadAndLockSharedVsLock()  // Test Case E-12
{
    string prefix = "TC E-12: ThreadAndLockSharedVsLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker parlocker;
    UnLocker     unlocker;
    Locker       childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::LockSharedThenForkAndLock()  // Test Case E-13
{
    string prefix = "TC E-13: LockSharedThenForkAndLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker shlocker;
    Locker       locker;
    UnLocker     unlocker;
    lockingAndForking(prefix, openflags, openflags, shlocker, unlocker, locker);
}
void
LockV3TestSuite::ThreadOpenSharedChildOpensNotShared()  // Test Case E-14
{
    string prefix = "TC E-14: ThreadOpenSharedChildOpensNotShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenFlags   = versionFlag | MDBM_RW_LOCKS;
    int childOpenFlags = versionFlag;
    SharedLocker parlocker;
    UnLocker     unlocker;
    SharedLocker childlocker;
    lockingAndThreading(prefix, parOpenFlags, childOpenFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::OpenSharedChildOpensNotShared()  // Test Case E-15
{
    string prefix = "TC E-15: OpenSharedChildOpensNotShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenflags   = versionFlag | MDBM_RW_LOCKS;
    int childOpenflags = versionFlag;
    SharedLocker shlocker;
    UnLocker     unlocker;
    lockingAndForking(prefix, parOpenflags, childOpenflags, shlocker, unlocker, shlocker);
}
void
LockV3TestSuite::ThreadShareLockChildIterates() // E-16
{
    string prefix = "TC E-16: ThreadShareLockChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    CommonParams parParams(prefix, openFlags, locker, 1, false);
    thrdLockAndChildIterates(parParams, versionFlag, unlocker, _dbName);
}
void
LockV3TestSuite::LockSharedChildIterates() // Test Case E-17
{
    string prefix = "TC E-17: LockSharedChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker locker;
    UnLocker     unlocker;
    lockAndChildIterates(prefix, openFlags, _dbName, locker, unlocker, false);
}
void
LockV3TestSuite::ParentNestsSL_ChildLocksOnce()  // TC E-18
{
    string prefix = "TC E-18: ParentNestsSL_ChildLocksOnce: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    SharedLocker locker;
    UnLocker     unlocker;
    Locker childlocker;
    nestLocksChildLockOnce(prefix, versionFlag | MDBM_RW_LOCKS, locker, unlocker, childlocker);
}
void
LockV3TestSuite::ThreadsShareHandleShLockVsShLock() // TC E-19
{
    string prefix = "TC E-19: ThreadsShareHandleShLockVsShLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker parlocker;
    SharedLocker childlocker;
    thrdShareHandleLock(prefix, openFlags, parlocker, childlocker, true);
}
void
LockV3TestSuite::ThreadsShareHandleLockVsShLock() // TC E-20
{
    string prefix = "TC E-20: ThreadsShareHandleLockVsShLock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    Locker       parlocker;
    SharedLocker childlocker;
    thrdShareHandleLock(prefix, openFlags, parlocker, childlocker, true);
}

void
LockV3TestSuite::SharedTryLockNewDBnoFlag()  // Test Case F-1
{
    string prefix = "TC F-1: SharedTryLockNewDBnoFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag;
    SharedTryLocker locker;
    lockNewDB(prefix, openflags, locker);
}
void
LockV3TestSuite::ThreadTryLockSharedChildReads() // Test Case F-2
{
    // child should not block
    string prefix = "TC F-2: ThreadTryLockSharedChildReads: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedTryLocker locker;
    UnLocker        unlocker;
    string keyname = GetDefaultKey(3);
    SharedReader reader(keyname);
    lockingAndThreading(prefix, openFlags, openFlags, locker, unlocker, reader);
}
void
LockV3TestSuite::TryLockSharedChildReads() // Test Case F-3
{
    // child should not block
    string prefix = "TC F-3: TryLockSharedChildReads: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedTryLocker locker;
    UnLocker        unlocker;
    string keyname = GetDefaultKey(3);
    SharedReader reader(keyname);
    lockingAndForking(prefix, openFlags, openFlags, locker, unlocker, reader);
}
void
LockV3TestSuite::ThreadTryLockSharedChildWrites() // Test Case F-4
{
    // child should block
    string prefix = "TC F-4: ThreadTryLockSharedChildWrites: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedTryLocker locker;
    UnLocker        unlocker;
    string keyname = GetDefaultKey(3);
    UnsharedWriter writer(keyname);
    lockingAndThreading(prefix, openFlags, openFlags, locker, unlocker, writer);
}
void
LockV3TestSuite::TryLockSharedChildWrites() // Test Case F-5
{
    // child should block until parent unlocks
    string prefix = "TC F-5: TryLockSharedChildWrites: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedTryLocker locker;
    UnLocker        unlocker;
    string keyname = GetDefaultKey(3);
    UnsharedWriter writer(keyname);
    lockingAndForking(prefix, openFlags, openFlags, locker, unlocker, writer);
}
void
LockV3TestSuite::ThreadReadThenTryLockShared() // Test Case F-6
{
    string prefix = "TC F-6: ThreadReadThenTryLockShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    KeyReader       reader;
    UnLocker        unlocker;
    SharedTryLocker locker;
    lockingAndThreading(prefix, openFlags, openFlags, reader, unlocker, locker);
}
void
LockV3TestSuite::ThreadsShareHandleShLockVsTryLockShared() // TC F-7
{
    string prefix = "TC F-7: ThreadsShareHandleShLockVsTryLockShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();
    int openFlags =  versionFlag | MDBM_RW_LOCKS;
    SharedLocker    parlocker;
    SharedTryLocker childlocker;
    thrdShareHandleLock(prefix, openFlags, parlocker, childlocker, true);
}
void
LockV3TestSuite::ThreadAndLockSharedVsTryLockShareddDupHandle()  // TC F-8
{
    string prefix = "TC F-8: ThreadAndLockSharedVsTryLockSharedDupHandle: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker    parlocker;
    UnLocker        unlocker;
    SharedTryLocker childlocker;
    lockingAndThreading(prefix, openFlags, openFlags, parlocker, unlocker, childlocker, true);
}
void
LockV3TestSuite::LockSharedThenForkTryLockShared()  // Test Case F-9
{
    string prefix = "TC F-9: LockSharedThenForkTryLockShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag | MDBM_RW_LOCKS;
    SharedLocker    locker;
    SharedTryLocker trylocker;
    UnLocker        unlocker;
    lockingAndForking(prefix, openflags, openflags, locker, unlocker, trylocker);
}
void
LockV3TestSuite::TryShared_ThreadOpenSharedChildOpensNotShared()  // Test Case F-10
{
    string prefix = "TC F-10: TryShared_ThreadOpenSharedChildOpensNotShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenFlags   = versionFlag | MDBM_RW_LOCKS;
    int childOpenFlags = versionFlag;
    SharedTryLocker parlocker;
    UnLocker        unlocker;
    SharedTryLocker childlocker;
    lockingAndThreading(prefix, parOpenFlags, childOpenFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::TryShared_OpenSharedChildOpensNotShared()  // Test Case F-11
{
    string prefix = "TC F-11: TryShared_OpenSharedChildOpensNotShared: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenflags   = versionFlag | MDBM_RW_LOCKS;
    int childOpenflags = versionFlag;
    SharedTryLocker locker;
    UnLocker        unlocker;
    lockingAndForking(prefix, parOpenflags, childOpenflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ThreadTryShareLockChildIterates() // F-12
{
    string prefix = "TC F-12: ThreadTryShareLockChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedTryLocker locker;
    UnLocker        unlocker;
    CommonParams parParams(prefix, openFlags, locker, 1, false);
    thrdLockAndChildIterates(parParams, versionFlag, unlocker, _dbName);
}
void
LockV3TestSuite::TryLockSharedChildIterates() // Test Case F-13
{
    string prefix = "TC F-13: TryLockSharedChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_RW_LOCKS;
    SharedTryLocker locker;
    UnLocker        unlocker;
    lockAndChildIterates(prefix, openFlags, _dbName, locker, unlocker, false);
}

void
LockV3TestSuite::PartLockNewDBnoFlag()  // Test Case G-1
{
    string prefix = "TC G-1: PartLockNewDBnoFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag;
    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    lockNewDB(prefix, openflags, locker, 1);
}
void
LockV3TestSuite::PartLockNewDB()  // Test Case G-2
{
    string prefix = "TC G-2: PartLockNewDB: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    lockNewDB(prefix, openflags, locker);
}

// wait for parent to start iteration
// iterate - for non-blocker, wait for parent message to continue
// upon completion send message to parent
void* LockV3TestSuite::LockV3TestSuite::thrdIterates(void *arg)
{
    ThrdParams *params = static_cast<ThrdParams*>(arg);
    params->_status = true;

    string resp;
    string errMsg = "Child: parent should have told us to continue: ";
    bool selret = waitForResponse(params->readPipe(), resp, errMsg);
    if (selret == false) {
        ThrdMonitor_RAII::exit(errMsg, params);
    }

    // params->lockFunc()(params->dbh()); // BZ 5395602 testing

    // iterate the DB - when done send an ACK to the parent
    MDBM_ITER iter;
    //fprintf(stderr, "@@@ === child pid:%d tid:%d about to iterate\n", getpid(), gettid());
    //mdbm_lock_dump(params->dbh());
    datum     key = mdbm_firstkey_r(params->dbh(), &iter);
    while (key.dsize != 0) {
        key = mdbm_nextkey_r(params->dbh(), &iter);
    }

    FD_RAII::sendMsg(params->sendPipe(), _Ack);
    return 0;
}
// parent locks - tell child to iterate
// child iterates - should block
// parent unlocks
// child should complete iteration - send message to parent
// parent awaits child completion message
void
LockV3TestSuite::thrdLockAndChildIterates(CommonParams &parParams, int childOpenFlags, UnLocker &unlocker, const string &dbName, bool unlockElseIterate)
{
    string prefix = parParams.prefix();

    MdbmHolder dbh(EnsureNamedMdbm(dbName.c_str(), parParams.openFlags() | MDBM_O_RDWR));

    MdbmHolder c_dbh(EnsureNamedMdbm(dbName.c_str(), childOpenFlags | MDBM_O_RDWR));

    // use pipes as fairly easy way to synchronize parent with child
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    // package childs parameters
    ThrdParams params(parParams.prefix(), childOpenFlags, c_dbh,
        parParams.lockFunc(), childSendFD, parReadFD,
        parParams.lockFunc().blockable());

    // create child
    pthread_t tid;
    int ret = pthread_create(&tid, 0, thrdIterates, &params);
    stringstream frkss;
    frkss << prefix
          << " Could not thread the child - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 0));

    ThrdMonitor_RAII child(tid);

    string errMsg;
    ret = parParams.lockFunc()(dbh);
    if (ret != parParams.expectedRet()) {
        child.release(&params, dbh, true);
        errMsg = "Parent: lock FAILed. Cannot continue this test";
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (ret == parParams.expectedRet()));
    }

    //fprintf(stderr, "@@@ === parent pid:%d tid:%d about to tell child to iterate\n", getpid(), gettid());
    //mdbm_lock_dump(dbh);
    // tell child to iterate
    parSendFD.sendMsg(_Continue);

    string resp;
    errMsg = "Waiting on child to iterate: ";
    bool selret = waitForResponse(childReadFD, resp, errMsg);
    if (selret == true) {
        child.release(&params, dbh, true);
        errMsg = "Parent: child was NOT blocked but should have been: ";
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == false));
    }

    // unlock or iterate and wait for child to complete iteration
    if (unlockElseIterate) {
        ret = unlocker(dbh);
        if (ret == -1) {
            child.release(&params, dbh, true);
            errMsg = "Parent: unlock FAILed. Cannot continue this test";
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (ret == -1));
        }
    } else { // iterate
        MDBM_ITER iter;
        datum     key = mdbm_firstkey_r(dbh, &iter);
        while (key.dsize != 0) {
            key = mdbm_nextkey_r(dbh, &iter);
        }
    }

    resp = "";
    errMsg = "Waiting on child to complete iteration: ";
    selret = waitForResponse(childReadFD, resp, errMsg);
    if (selret == false) {
        child.release(&params, dbh, true);
        errMsg = "Parent: child was blocked but should NOT have been: ";
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));
    }

    ret = pthread_join(tid, 0);
    stringstream retmsg;
    retmsg << prefix
           << "Child thread(" << tid
           << ") reports FAILure: " << params.statusMsg() << endl;
    CPPUNIT_ASSERT_MESSAGE(retmsg.str(), (params._status == true));
}
void
LockV3TestSuite::ThreadPlockChildIterates() // G-3
{
    string prefix = "TC G-3: ThreadPlockChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    PartLocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, locker, 1, false);
    thrdLockAndChildIterates(parParams, openFlags, unlocker, _dbName);
}
void
LockV3TestSuite::PlockChildIterates() // Test Case G-4
{
    string prefix = "TC G-4: PlockChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    PartLocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    lockAndChildIterates(prefix, openFlags, _dbName, locker, unlocker, true);
}
void
LockV3TestSuite::thrdParChildLockPartitions(CommonParams &parParams, const string &dbName, UnLocker &unlocker, CommonParams &childParams)
{
    string prefix = parParams.prefix();
    string resp;
    string errStr;
    string errMsg;
    stringstream retmsg;
    bool selret;

    // use the multi-page DB
    MdbmHolder dbh(EnsureNamedMdbm(dbName.c_str(), parParams.openFlags() | MDBM_O_RDWR));

    MdbmHolder c_dbh(EnsureNamedMdbm(dbName.c_str(), childParams.openFlags() | MDBM_O_RDWR));

    // use pipes as fairly easy way to synchronize parent with child
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    // package childs parameters
    ThrdParams params(childParams.prefix(), childParams.openFlags(), c_dbh,
        childParams.lockFunc(), childSendFD, parReadFD,
        childParams.lockFunc().blockable());
    params.expectSuccess(childParams.expectedRet() == -1 ? false : true);

    // create child
    pthread_t tid;
    int ret = pthread_create(&tid, 0, thrdLockOrUnlockAndAck, &params);
    stringstream frkss;
    frkss << prefix
          << " Could not thread the child - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (ret == 0));
    c_dbh.Release(); // child has to close (and release any locks held)

    ThrdMonitor_RAII child(tid);

    ret = parParams.lockFunc()(dbh);
    if (ret != parParams.expectedRet()) {
        child.release(&params, dbh, true);
        stringstream lkss;
        lkss << "WARNING: " << prefix
             << "thrdParChildLockPartitions: lock FAILed for DB=" << dbName
             << " Cannot continue this test" << endl;
        GRACEFUL_ASSERT(lkss.str(), (ret == parParams.expectedRet()));
    }

    // tell child to perform plock
    parSendFD.sendMsg(_Continue);

    errMsg = "Waiting on child to perform lock: ";
    GetLogStream() << errMsg;
    selret = waitForResponse(childReadFD, resp, errMsg);

    if (childParams.shouldBlock()) {
        if (selret == true) {
            child.release(&params, dbh, true);
            errMsg = "Child was not blocked but should have been: ";
            child.release(&params, dbh, true);
            GRACEFUL_ASSERT(errMsg.insert(0, prefix), (selret == false));
        }

        // unlock and wait for child response again
        ret = unlocker(dbh);
        if (ret == -1) {
            child.release(&params, dbh, true);
            stringstream ulss;
            ulss << "WARNING: " << prefix
                 << "parChildLockPartitions: unlock FAILed for DB=" << dbName
                 << " Cannot continue this test" << endl;

            GRACEFUL_ASSERT(ulss.str(), (ret == -1));
        }

        resp = "";
        errMsg = "Waiting again for child to lock the DB: ";
        GetLogStream() << errMsg;
        selret = waitForResponse(childReadFD, resp, errMsg);
        if (selret == false) {
            child.release(&params, dbh, true);
            GRACEFUL_ASSERT(errMsg.insert(0, prefix), (selret == true));
        }
    } else if (selret == false) {
        errMsg = "Child was blocked but should NOT have been: ";
        child.release(&params, dbh, true);
        GRACEFUL_ASSERT(errMsg.insert(0, prefix), (selret == true));
    }

    ret = pthread_join(tid, 0);
    retmsg << prefix
           << "Child thread(" << tid
           << ") reports FAILure: " << params.statusMsg() << endl;
    CPPUNIT_ASSERT_MESSAGE(retmsg.str(), (params._status == true));
    return;

error_handler:
    dbh.Close(); // should release locks
    params.quitTest();
    ret = pthread_join(tid, 0);
    CPPUNIT_ASSERT_MESSAGE(errStr, (true==false));
}
void
LockV3TestSuite::ThreadParentChildKeysInSamePartion() // Test Case G-5
{
    string prefix = "TC G-5: ThreadParentChildKeysInSamePartion: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    PartLocker parlocker(_utKeyPart2);
    PartLocker childlocker(_utKeyPart2);
    PunLocker  unlocker(_utKeyPart2);
    CommonParams parParams(prefix, openFlags, parlocker, 1, false);
    CommonParams childParams(prefix, openFlags, childlocker, 1, true);
    thrdParChildLockPartitions(parParams, _dbName, unlocker, childParams);
}
void
LockV3TestSuite::ParentChildKeysInSamePartion() // Test Case G-6
{
    string prefix = "TC G-6: ParentChildKeysInSamePartion: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    PartLocker locker(_utKeyPart2);
    PunLocker  unlocker(_utKeyPart2);
    CommonParams params(prefix, openFlags, locker, 1, true);
    parChildLockPartitions(params, _dbName, unlocker, locker, false);
}
void
LockV3TestSuite::ThrdParentPlockChildPlockKeysInDiffPart() // Test Case G-7
{
    string prefix = "TC G-7: ThrdParentPlockChildPlockKeysInDiffPart: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    PartLocker parlocker(_utKeyPart1);
    PartLocker childlocker(_utKeyPart2);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, parlocker, 1, false);
    CommonParams childParams(prefix, openFlags, childlocker, 1, false);
    thrdParChildLockPartitions(parParams, _dbName, unlocker, childParams);
}
void
LockV3TestSuite::ParentChildKeysInDiffPartions() // Test Case G-8
{
    string prefix = "TC G-8: ParentChildKeysInDiffPartions: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    PartLocker locker1(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    PartLocker locker2(_utKeyPart2);
    CommonParams params(prefix, openFlags, locker1, 1, false);
    parChildLockPartitions(params, _dbName, unlocker, locker2, false);
}
void
LockV3TestSuite::PlockIterationAndAutoUnlock() // G-9
{
#if 0
// FIX BZ 5395602
    string prefix = "TC G-9: PlockIterationAndAutoUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    PartLocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, locker, 1, false);
    thrdLockAndChildIterates(parParams, openFlags, unlocker, _dbName, false);
#endif
}
void
LockV3TestSuite::ThreadOpenPartChildOpensNotPart()  // Test Case G-10
{
    string prefix = "TC G-10: ThreadOpenPartChildOpensNotPart: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenFlags   = versionFlag | MDBM_PARTITIONED_LOCKS;
    int childOpenFlags = versionFlag;
    string keyname = GetDefaultKey(3);
    PartLocker parlocker(keyname);
    PunLocker  unlocker(keyname);
    PartLocker childlocker(keyname);
    lockingAndThreading(prefix, parOpenFlags, childOpenFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::OpenPartChildOpensNotPart()  // Test Case G-11
{
    string prefix = "TC G-11: OpenPartChildOpensNotPart: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenflags   = versionFlag | MDBM_PARTITIONED_LOCKS;
    int childOpenflags = versionFlag;
    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    lockingAndForking(prefix, parOpenflags, childOpenflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ParentNestsChildPlocksOnce()  // TC G-12
{
    string prefix = "TC G-12: ParentNestsChildPlocksOnce: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    nestLocksChildLockOnce(prefix, openFlags, locker, unlocker, locker);
}
void
LockV3TestSuite::PunlockWithoutLocking() // H-1
{
    string prefix = "TC H-1: PunlockWithoutLocking: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(3);
    PunLocker unlocker(keyname);
    lockNewDB(prefix, openFlags, unlocker, -1);
}
void
LockV3TestSuite::ParentIteratePunlockChildIterate() // H-2
{
    string prefix = "TC H-2: ParentIteratePunlockChildIterate: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_PARTITIONED_LOCKS;
    partTestIterateAndChildIterates(prefix, openFlags, _dbName);
}
void
LockV3TestSuite::ParentPlocksChildPunlocks() // H-3
{
    string prefix = "TC H-3: ParentPlocksChildPunlocks: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_PARTITIONED_LOCKS;
    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    CommonParams params(prefix, openFlags, locker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, unlocker, false);
}
void
LockV3TestSuite::ThreadNestPlocksThenUnwindPunlock() // H-4
{
#if 0
// FIX BZ 5384701
    string prefix = "TC H-4: ThreadNestPlocksThenUnwindPunlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    string keyname = GetDefaultKey(3);
    PartLocker locker(keyname);
    PunLocker  unlocker(keyname);
    int openFlags =  versionFlag | MDBM_PARTITIONED_LOCKS;
    thrdNestLockUnwind(prefix, openFlags, locker, unlocker);
#endif
}
#if 0
// Obsolete(interleaving)
void
LockV3TestSuite::ParentLocksChildPunlocks() // H-5
{
    string prefix = "TC H-5: ParentLocksChildPunlocks: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags =  versionFlag | MDBM_PARTITIONED_LOCKS;
    Locker locker;
    string keyname = GetDefaultKey(2);
    PunLocker  unlocker(keyname);
    CommonParams params(prefix, openFlags, locker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, unlocker, false);
}
#endif
void
LockV3TestSuite::TryPartLockNewDBnoFlag()  // Test Case I-1
{
    string prefix = "TC I-1: TryPartLockNewDBnoFlag: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openflags = versionFlag;
    string keyname = GetDefaultKey(2);
    PartLocker locker(keyname);
    lockNewDB(prefix, openflags, locker, 1); // exclusive locks without PART flag
}
void
LockV3TestSuite::ThreadTryPlockChildIterates() // I-2
{
    string prefix = "TC I-2: ThreadTryPlockChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    TryPlocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, locker, 1, false);
    thrdLockAndChildIterates(parParams, openFlags, unlocker, _dbName);
}
void
LockV3TestSuite::TryPlockChildIterates() // Test Case I-3
{
    string prefix = "TC I-3: TryPlockChildIterates: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    TryPlocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    lockAndChildIterates(prefix, openFlags, _dbName, locker, unlocker, true);
}
void
LockV3TestSuite::ThreadTryPlockParentChildKeysInSamePartion() // Test Case I-4
{
    string prefix = "TC I-4: ThreadTryPlockParentChildKeysInSamePartion: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker parlocker(_utKeyPart1);
    TryPlocker childlocker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, parlocker, 1, false);
    CommonParams childParams(prefix, openFlags, childlocker, -1, false);
    thrdParChildLockPartitions(parParams, _dbName, unlocker, childParams);
}
void
LockV3TestSuite::TryPlockParentChildKeysInSamePartion() // Test Case I-5
{
    string prefix = "TC I-5: TryPlockParentChildKeysInSamePartion: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams params(prefix, openFlags, locker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, locker, false);
}
void
LockV3TestSuite::ThrdParentTryPlockChildTryPlockKeysInDiffPart() // Test Case I-6
{
    string prefix = "TC I-6: ThrdParentTryPlockChildTryPlockKeysInDiffPart: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker parlocker(_utKeyPart1);
    TryPlocker childlocker(_utKeyPart2);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, parlocker, 1, false);
    CommonParams childParams(prefix, openFlags, childlocker, 1, false);
    thrdParChildLockPartitions(parParams, _dbName, unlocker, childParams);
}
void
LockV3TestSuite::TryLockParentChildKeysInDiffPartions() // Test Case I-7
{
    string prefix = "TC I-7: TryLockParentChildKeysInDiffPartions: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker locker1(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    TryPlocker locker2(_utKeyPart2);
    CommonParams params(prefix, openFlags, locker1, 1, false);
    parChildLockPartitions(params, _dbName, unlocker, locker2, false);
}
void
LockV3TestSuite::TryPlockIterationAndAutoUnlock() // I-8
{
#if 0
// FIX BZ 5395602
    string prefix = "TC I-8: TryPlockIterationAndAutoUnlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;
    TryPlocker locker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, locker, 1, false);
    thrdLockAndChildIterates(parParams, openFlags, unlocker, _dbName, false);
#endif
}
void
LockV3TestSuite::ThreadOpenPartFlagChildOpenNoPartFlagTry()  // Test Case I-9
{
    string prefix = "TC I-9: ThreadOpenPartFlagChildOpenNoPartFlagTry: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenFlags   = versionFlag | MDBM_PARTITIONED_LOCKS;
    int childOpenFlags = versionFlag;
    string keyname = GetDefaultKey(3);
    TryPlocker parlocker(keyname);
    PunLocker  unlocker(keyname);
    TryPlocker childlocker(keyname);
    lockingAndThreading(prefix, parOpenFlags, childOpenFlags, parlocker, unlocker, childlocker);
}
void
LockV3TestSuite::OpenPartFlagChildOpenNoPartFlagTryPlock()  // Test Case I-10
{
    string prefix = "TC I-10: OpenPartFlagChildOpenNoPartFlagTryPlock: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int parOpenflags   = versionFlag | MDBM_PARTITIONED_LOCKS;
    int childOpenflags = versionFlag;
    TryPlocker locker(_utKeyPart2);
    PunLocker  unlocker(_utKeyPart2);
    lockingAndForking(prefix, parOpenflags, childOpenflags, locker, unlocker, locker);
}
void
LockV3TestSuite::ThrdParentTryPlockChildPlockKeysInSamePartion() // Test Case I-11
{
    string prefix = "TC I-11: ThrdParentTryPlockChildPlockKeysInSamePartion: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker parlocker(_utKeyPart1);
    PartLocker childlocker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, parlocker, 1, false);
    CommonParams childParams(prefix, openFlags, childlocker, 1, true);
    thrdParChildLockPartitions(parParams, _dbName, unlocker, childParams);
}
void
LockV3TestSuite::ParentPlockChildTryPlockKeysInSamePartion() // Test Case I-12
{
    string prefix = "TC I-12: ParentPlockChildTryPlockKeysInSamePartion: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    PartLocker plocker(_utKeyPart1);
    TryPlocker tryplocker(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams params(prefix, openFlags, plocker, -1, false);
    parChildLockPartitions(params, _dbName, unlocker, tryplocker, false);
}
void
LockV3TestSuite::ThrdParentTryPlockChildPlockKeysInDiffPart() // Test Case I-13
{
    string prefix = "TC I-13: ThrdParentTryPlockChildPlockKeysInDiffPart: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker parlocker(_utKeyPart1);
    PartLocker childlocker(_utKeyPart2);
    PunLocker  unlocker(_utKeyPart1);
    CommonParams parParams(prefix, openFlags, parlocker, 1, false);
    CommonParams childParams(prefix, openFlags, childlocker, 1, false);
    thrdParChildLockPartitions(parParams, _dbName, unlocker, childParams);
}
void
LockV3TestSuite::ParentPlockChildTryPlockKeysInDiffPartions() // Test Case I-14
{
    string prefix = "TC I-14: ParentPlockChildTryPlockKeysInDiffPartions: ";
    TRACE_TEST_CASE(prefix);
    deleteLockfile();

    int openFlags = versionFlag | MDBM_PARTITIONED_LOCKS;

    TryPlocker locker1(_utKeyPart1);
    PunLocker  unlocker(_utKeyPart1);
    TryPlocker locker2(_utKeyPart2);
    CommonParams params(prefix, openFlags, locker1, 1, false);
    parChildLockPartitions(params, _dbName, unlocker, locker2, false);
}

void
LockV3TestSuite::test_OtherAG7()
{
    string prefix = string("OtherAG7") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, versionFlag | MDBM_O_RDWR | MDBM_RW_LOCKS,
                                        0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_shared(mdbm));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_isowned(mdbm));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
}


void
LockV3TestSuite::test_OtherAG8()
{
    string prefix = string("OtherAG8") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    SharedLocker   shrlocker;
    UnLocker unlocker;
    // Should not be owned by child - locked by parent
    CPPUNIT_ASSERT_EQUAL(0, testIsOwnedParentChild(prefix, versionFlag, shrlocker, unlocker, versionFlag));
}

//mdbm_trylock_smart
void
LockV3TestSuite::test_OtherAJ8()
{
    string prefix = string("OtherAJ8: Partition lock") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    datum key;

    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDONLY | MDBM_PARTITIONED_LOCKS;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, flags,
                    0644, DEFAULT_PAGE_SIZE, 0));

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
}

void
LockV3TestSuite::test_OtherAJ9()
{
    string prefix = string("OtherAJ9: Shared lock") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    datum key;

    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDONLY | MDBM_RW_LOCKS;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, flags,
                    0644, DEFAULT_PAGE_SIZE, 0));

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
}

void
LockV3TestSuite::test_OtherAJ10()
{
    string prefix = string("OtherAJ10: exclusive lock") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    datum key;

    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDONLY;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    MdbmHolder mdbm(EnsureTmpMdbm(prefix, flags,
                    0644, DEFAULT_PAGE_SIZE, 0));

    CPPUNIT_ASSERT_EQUAL(1, mdbm_trylock_smart(mdbm, &key, flags));
}

void
LockV3TestSuite::test_OtherAJ11()
{
    string prefix = string("OtherAJ11: NULL MDBM") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    datum key;
    MDBM* mdbm=NULL;

    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDWR;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_trylock_smart(mdbm, &key, flags));
}

//mdbm_lock_smart Error
void
LockV3TestSuite::test_OtherAJ26()
{
    string prefix = string("OtherAJ26: mdbm_lock_smart Error") + versionString + ":";
    TRACE_TEST_CASE(__func__)
    MDBM *mdbm=NULL;
    datum key;
    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDWR;

    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_lock_smart(mdbm, &key, flags));
}

void
LockV3TestSuite::memoryOnlyParentLockChildBlock(string &prefix, int lockFlag)
{
    int flags = MDBM_CREATE_V3|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|lockFlag;
    int pagesz = 4096;
    string myKey    = "parent key";
    string childKey = "child key";

    MDBM *dbh = mdbm_open(NULL, flags, 0666, pagesz, 8*pagesz);
    stringstream openss;
    openss << prefix
           << " Cannot open memory-only db: " << strerror(errno) << endl;
    CPPUNIT_ASSERT_MESSAGE(openss.str(), (dbh != NULL));

    int ret = mdbm_store_str(dbh, myKey.c_str(), "parent says go to bed", MDBM_INSERT);
    stringstream storess;
    storess << prefix
            << " Parent: cannot store key=" << myKey << " to db" << endl;
    CPPUNIT_ASSERT_MESSAGE(storess.str(), (ret == 0));
    char *fstr = mdbm_fetch_str(dbh, myKey.c_str());
    stringstream fetchss;
    fetchss << prefix
            << " Parent: cannot fetch key=" << myKey << " from db" << endl;
    CPPUNIT_ASSERT_MESSAGE(fetchss.str(), (fstr != NULL));

    // Parent: make the child
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);
    int pid = makeChild(prefix, dbh, lockFlag, myKey, childKey, childSendFD);

    // Parent: Lock the DB
    datum dkey;
    bool expectResp = false;
    if (lockFlag == 0) {
        mdbm_lock(dbh);
    } else if (lockFlag == MDBM_PARTITIONED_LOCKS) {
        dkey.dptr  = const_cast<char*>(myKey.c_str());
        dkey.dsize = myKey.size() + 1;
        mdbm_plock(dbh, &dkey, 0);
    } else if (lockFlag == MDBM_RW_LOCKS) {
        mdbm_lock_shared(dbh);
        expectResp = true;
    }

    // Parent: I will sleep at most 4 winks
    string resp;
    string errMsg = prefix;
    bool lockingError = false;
    int maxCount = 5;
    for (int cnt = 0; cnt <= maxCount; ++cnt) {
        bool haveResp = waitForResponse(childReadFD, resp, errMsg, 1);
        if (haveResp) {
            if (!lockFlag || lockFlag == MDBM_PARTITIONED_LOCKS) {
                errMsg += "Exclusive/Partitioned locking in play, child should have been blocked";
                lockingError = true;;
                break;
            } else {
                // ok, if resp contains the fetch msg, we are good, so turn off
                // looking for that message
                if (resp.compare(_fetchAck) != 0) {
                    errMsg += "Shared locking in play, child should have sent fetch ACK only";
                    lockingError = true;;
                    break;
                }
                expectResp = false;
            }
        } else if (cnt==maxCount && !haveResp && expectResp) {
            errMsg += "Shared locking in play, child should NOT have been blocked";
            lockingError = true;
            break;
        }
    }

    // if error due to blocked/not-blocked, kill child then assert here
    if (lockingError) {
        kill(pid, SIGINT);
        CPPUNIT_ASSERT_MESSAGE(errMsg, (lockingError == false));
    }

    // Parent: Yawn....now unlock db and wait
    if (lockFlag == 0) {
        mdbm_unlock(dbh);
    } else if (lockFlag == MDBM_PARTITIONED_LOCKS) {
        mdbm_punlock(dbh, &dkey, 0);
    } else if (lockFlag == MDBM_RW_LOCKS) {
        mdbm_unlock(dbh);
    }

    resp = "";
    bool haveResp = waitForResponse(childReadFD, resp, errMsg, 2);
    if (haveResp) {
        if (lockFlag == MDBM_RW_LOCKS) {
            errMsg += "Did not receive correct ACK from child storing to db, ack=" + resp;
            CPPUNIT_ASSERT_MESSAGE(errMsg, (resp.compare(_storeAck) == 0));
        } else {
            errMsg += "Did not receive correct ACK from child fetching from db, ack=" + resp;
            CPPUNIT_ASSERT_MESSAGE(errMsg, (resp.find(_fetchAck) != string::npos));
        }
    } else {
        kill(pid, SIGINT);
        errMsg += "Error child should have sent an ACK since db was unlocked";
        CPPUNIT_ASSERT_MESSAGE(errMsg, (haveResp == true));
    }

    waitpid(pid, NULL, 0);

    fstr = mdbm_fetch_str(dbh, childKey.c_str());
    stringstream fetchchildss;
    fetchchildss << prefix
                 << " Parent: cannot fetch childs key=" << childKey
                 << " from db" << endl;
    CPPUNIT_ASSERT_MESSAGE(fetchchildss.str(), (fstr != NULL));

    mdbm_close(dbh);
}

int
LockV3TestSuite::makeChild(string &prefix, MDBM *dbh, int lockFlag, string &parentKey, string &childKey, FD_RAII &childSendFD)
{
    int pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    //CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));
    if (pid < 0) {
      fprintf(stderr, frkss.str().c_str());
      exit(1);
    }

    if (pid > 0) {
        //mdbm_close(cdbh);
        return pid; // parent returns here
    }
    MDBM *cdbh = dbh;

    sleep(1); // let parent get lock

    datum dkey;
    if (lockFlag == 0) {
        mdbm_lock(dbh);
    } else if (lockFlag == MDBM_PARTITIONED_LOCKS) {
        dkey.dptr  = const_cast<char*>(parentKey.c_str());
        dkey.dsize = parentKey.size() + 1;
        mdbm_plock(dbh, &dkey, 0);
    } else if (lockFlag == MDBM_RW_LOCKS) {
        mdbm_lock_shared(dbh);
    }
    char *fstr = mdbm_fetch_str(cdbh, parentKey.c_str());
    stringstream fetchss;
    fetchss << prefix
            << " Child: cannot fetch parents key=" << parentKey
            << " from db" << endl;
    //CPPUNIT_ASSERT_MESSAGE(fetchss.str(), (fstr != NULL));
    if (fstr == NULL) {
      fprintf(stderr, fetchss.str().c_str());
      mdbm_close(cdbh);
      exit(1);
    }

    if (lockFlag == 0) {
        mdbm_unlock(dbh);
    } else if (lockFlag == MDBM_PARTITIONED_LOCKS) {
        mdbm_punlock(dbh, &dkey, 0);
    } else if (lockFlag == MDBM_RW_LOCKS) {
        mdbm_unlock(dbh);
    }

    childSendFD.sendMsg(_fetchAck);

    int ret = mdbm_store_str(cdbh, childKey.c_str(), "children write on walls", MDBM_INSERT);
    stringstream storess;
    storess << prefix
            << " Child: cannot store key=" << childKey
            << " to db" << endl;
    //CPPUNIT_ASSERT_MESSAGE(storess.str(), (ret == 0));
    if (ret != 0) {
      fprintf(stderr, storess.str().c_str());
      mdbm_close(cdbh);
      exit(1);
    }

    childSendFD.sendMsg(_storeAck);

    mdbm_close(cdbh);
    sleep(1);
    exit(0);
}

void
LockV3TestSuite::MemoryOnlyParentLockChildBlock()
{
    string prefix = "TC A-12: MemoryOnlyParentLockChildBlock: ";
    memoryOnlyParentLockChildBlock(prefix, 0);
}
void
LockV3TestSuite::MemoryOnlyParentSharedLockChildBlock()
{
    string prefix = "TC E-21: MemoryOnlyParentSharedLockChildBlock: ";
    memoryOnlyParentLockChildBlock(prefix, MDBM_RW_LOCKS);
}
void
LockV3TestSuite::MemoryOnlyParentPartLockChildBlock()
{
    string prefix = "TC G-13: MemoryOnlyParentPartLockChildBlock: ";
    memoryOnlyParentLockChildBlock(prefix, MDBM_PARTITIONED_LOCKS);
}

// Returns the amount of Virtual memory locked into RAM in Kbytes
u_int
LockV3TestSuite::getLockedMemory()
{
    char line[MAXPATHLEN];

    snprintf(line, MAXPATHLEN, "/proc/%d/status", getpid());
    FILE *fp = fopen(line, "r");

    if (fp == NULL) {
        cerr << "Unable to open /proc/" << getpid() << "/status" << endl;
        return 0;
    }

    char *search;
    u_int lockedMem = 0;
    while (fgets(line, MAXPATHLEN, fp) != NULL) {
        if ((search = strstr(line, "VmLck")) != NULL) {
            lockedMem = atoi(search + strlen("VmLck") + 1);
        }
    }

    fclose(fp);

    return lockedMem;
}

void
LockV3TestSuite::LockPagesInMemoryFullDB()
{
    string prefix = "TC LockPagesInMemoryFullDB: ";

    MdbmHolder db(GetTmpFilledMdbm(prefix, MDBM_O_RDWR, 0666, 10240));
    CPPUNIT_ASSERT(NULL != (MDBM*)db);

    if (mdbm_lock_pages(db) != 0) {
        mdbm_unlock_pages(db);   // Unlock just in case we locked some but not all pages
        CPPUNIT_FAIL("mdbm_lock_pages failed");
    }

    CPPUNIT_ASSERT(getLockedMemory() > 0);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_unlock_pages(db));
}

void
LockV3TestSuite::LockPagesInMemoryGrowDB()
{
    string prefix = "TC LockPagesInMemoryGrowDB: ";

    MdbmHolder db(GetTmpFilledMdbm(prefix, MDBM_O_RDWR, 0666, 1024));
    CPPUNIT_ASSERT(NULL != (MDBM*)db);

    mdbm_limit_size_v3(db, 2, NULL, NULL);
    FillDb(db, 500);
    if (mdbm_lock_pages(db) != 0) {
        mdbm_unlock_pages(db);   // Unlock just in case we locked some but not all pages
        CPPUNIT_FAIL("mdbm_lock_pages failed");
    }

    CPPUNIT_ASSERT(getLockedMemory() > 0);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_unlock_pages(db));
}

void
LockV3TestSuite::LockPagesInMemoryGrowAndCheck()
{
    string prefix = "TC LockPagesInMemoryGrowAndCheck: ";

    MdbmHolder db(GetTmpFilledMdbm(prefix, MDBM_O_RDWR, 0666, 2048));
    CPPUNIT_ASSERT(NULL != (MDBM*)db);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_lock_pages(db));
    u_int memory1 = getLockedMemory();
    CPPUNIT_ASSERT(memory1 > 0);

    mdbm_limit_size_v3(db, 3, NULL, NULL);
    FillDb(db, 2500);
    if (mdbm_lock_pages(db) != 0) {
        mdbm_unlock_pages(db);   // Unlock just in case we locked some but not all pages
        CPPUNIT_FAIL("mdbm_lock_pages failed");
    }

    CPPUNIT_ASSERT(getLockedMemory() > memory1);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_unlock_pages(db));
}

void
LockV3TestSuite::LockPagesInMemoryEmptyDB()
{
    string prefix = "TC LockPagesInMemoryEmptyDB: ";

    MdbmHolder db(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT, 0644, 28672));  // 28K
    CPPUNIT_ASSERT(NULL != (MDBM*)db);

    if (mdbm_lock_pages(db) != 0) {
        mdbm_unlock_pages(db);   // Unlock just in case we locked some but not all pages
        CPPUNIT_FAIL("mdbm_lock_pages failed");
    }

    CPPUNIT_ASSERT(getLockedMemory() > 0);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_unlock_pages(db));
}

void
LockV3TestSuite::LockPagesInMemoryNULL()
{
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_lock_pages(NULL));
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_unlock_pages(NULL));
}

void
LockV3TestSuite::LockPagesCheckPageFaultsLargeDB()
{
    string prefix = "TC LockPagesCheckPageFaultsLargeDB: ";

    string fname;
    MdbmHolder db(GetTmpFilledMdbm(prefix, MDBM_O_RDWR, 0666, 5120, 0, &fname));
    CPPUNIT_ASSERT(NULL != (MDBM*)db);

    mdbm_limit_size_v3(db, 6, NULL, NULL);
    FillDb(db, 3000);
    db.Close();

    struct rusage resusage;
    CPPUNIT_ASSERT_EQUAL(0, getrusage(RUSAGE_SELF, &resusage));
    long startFaults = resusage.ru_majflt;

    MdbmHolder db2(fname);
    db2.Open(MDBM_O_RDWR, 0666, 5120, 0);

    if (mdbm_lock_pages(db2) != 0) {
        mdbm_unlock_pages(db2);   // Unlock just in case we locked some but not all pages
        CPPUNIT_FAIL("mdbm_lock_pages of db2 failed");
    }

    CPPUNIT_ASSERT_EQUAL(0, getrusage(RUSAGE_SELF, &resusage));
    long faultDelta = resusage.ru_majflt - startFaults;

    CPPUNIT_ASSERT(faultDelta >= 0);

    CPPUNIT_ASSERT(getLockedMemory() > 0);

    CPPUNIT_ASSERT_EQUAL(0, mdbm_unlock_pages(db2));
}

void
LockV3TestSuite::LockPagesInMemoryWindowed()
{
    RETURN_IF_WINMODE_LIMITED

    string prefix = "TC LockPagesInMemoryWindowed: ";

    MdbmHolder db(GetTmpFilledMdbm(prefix, MDBM_O_RDWR | MDBM_OPEN_WINDOWED, 0666, 4096));
    CPPUNIT_ASSERT(NULL != (MDBM*)db);

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_lock_pages(db));
}

void
LockV3TestSuite::deleteLockfiles(bool useAPI, const string &fname)
{
    if (useAPI) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_delete_lockfiles(fname.c_str()));
    } else {
      const char* args[] = { "foo", fname.c_str(), NULL };
      reset_getopt();
      int ret = delete_lockfiles_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
      CPPUNIT_ASSERT(ret == 0);
    }
}

void
LockV3TestSuite::DeleteLockfilesTest(bool useAPI)
{
    string prefix = string("DeleteLockfiles") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string filename;
    int basicFlags = versionFlag | MDBM_O_CREAT | MDBM_O_RDWR;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix, basicFlags, 0644, DEFAULT_PAGE_SIZE, 0, &filename));
    datum key;
    key.dptr = (char*)"key";
    key.dsize = strlen(key.dptr);
    // Exclusive locks, then delete lockfiles
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_smart(mdbm, &key, basicFlags));
    mdbm.Close();
    deleteLockfiles(useAPI, filename);
    checkLocks(filename);

    // Partition locks, then delete lockfiles
    int flags = basicFlags | MDBM_PARTITIONED_LOCKS;
    MdbmHolder mdbmpart(EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE, 0, &filename));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_smart(mdbmpart, &key, flags));
    mdbmpart.Close();
    deleteLockfiles(useAPI, filename);
    checkLocks(filename);

    // Shared locks, then delete lockfiles
    flags = basicFlags | MDBM_RW_LOCKS;
    MdbmHolder mdbmshared(EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE, 0, &filename));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_smart(mdbmshared, &key, flags));
    mdbmshared.Close();
    deleteLockfiles(useAPI, filename);
    checkLocks(filename);
}

void
LockV3TestSuite::TestDeleteLockfiles()
{
   DeleteLockfilesTest(true);  // Use the mdbm_delete_lockfiles API
}

void
LockV3TestSuite::TestDeleteLockfilesUtil()
{
   DeleteLockfilesTest(false);  // Do not use the mdbm_delete_lockfiles API (use util code)
}

void
LockV3TestSuite::checkLocks(const string &filename)
{
    string lockfile = string("/tmp/.mlock-named/") + filename;
    CPPUNIT_ASSERT_EQUAL(-1, open(lockfile.c_str(), O_RDONLY));
    lockfile += string("._int_");

    MDBM *reopenedMdbm = mdbm_open(filename.c_str(), MDBM_O_RDONLY, 0644, DEFAULT_PAGE_SIZE, 0);
    CPPUNIT_ASSERT(NULL != reopenedMdbm);
    mdbm_close(reopenedMdbm);
}

void
LockV3TestSuite::DeleteLockfilesNonExistentTester(const string &prefix, bool useAPI)
{
    string filename;
    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDWR;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix, flags, 0644, DEFAULT_PAGE_SIZE, 0, &filename));
    datum key;
    key.dptr = (char *) "key";
    key.dsize = strlen(key.dptr);
    // Exclusive locks, then delete lockfiles
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock_smart(mdbm, &key, 0));
    mdbm.Close();
    deleteLockfiles(useAPI, filename);
    checkLocks(filename);

}

void
LockV3TestSuite::TestDeleteLockfilesNonExistent()
{
    string prefix = string("DeleteLockfilesNonExistent") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    DeleteLockfilesNonExistentTester(prefix, true);
    DeleteLockfilesNonExistentTester(prefix, false);
}



class LockV3TestSuitePLock : public LockV3TestSuite
{
    CPPUNIT_TEST_SUITE(LockV3TestSuitePLock);

    CPPUNIT_TEST(LockModeTest);

    CPPUNIT_TEST(MemoryOnlyParentLockChildBlock);
    CPPUNIT_TEST(GetLockMode); // J-1
    CPPUNIT_TEST(LockNewDB); // A-1
    CPPUNIT_TEST(ThreadThenLock);
    CPPUNIT_TEST(ForkThenLock);
    CPPUNIT_TEST(ForkAndLockWithPartFlag);
    CPPUNIT_TEST(ThreadAndLockWithNoLockFlag);
    CPPUNIT_TEST(ForkAndLockWithNoLockFlag);
    CPPUNIT_TEST(ForkAndLockWithChildNoLockFlag);
    CPPUNIT_TEST(ParentNestsChildLocksOnce);
    CPPUNIT_TEST(ThreadNestsChildLocksOnce);
    //CPPUNIT_TEST(ThreadsShareHandleNestVsBlock); // shouldn't share handles x threads
    CPPUNIT_TEST(LockCloseReopenRelock);

#if 0 // pthreads requires same proc/thread to unlock
    CPPUNIT_TEST(UnlockWithoutLocking); // B-1
    CPPUNIT_TEST(ParentLocksChildUnlocks);
    CPPUNIT_TEST(ParentLockSharedChildUnlocks);
    CPPUNIT_TEST(ThreadThenChildUnlock);
    CPPUNIT_TEST(ParentLocksChildUnlocksSharedHandle);
#endif
    //CPPUNIT_TEST(ThreadsShareHandleNestVsBlockUnlock); // shouldn't share handles x threads
    CPPUNIT_TEST(ThreadNestLocksThenUnwindUnlock);
    // Obsolete(interleaving) CPPUNIT_TEST(MismatchLockingPlockThenUnlock);
    CPPUNIT_TEST(MismatchLockingSharedLockThenUnlock);

    CPPUNIT_TEST(DupTestIsLocked); // C-1
    CPPUNIT_TEST(TestIsLocked);
    CPPUNIT_TEST(TestUnlockAndIsLocked);
    // Obsolete(interleaving) CPPUNIT_TEST(TestPlockAndIsLocked);
    CPPUNIT_TEST(TestPunlockAndIsLocked);
    CPPUNIT_TEST(LockSharedAndIsLocked);
    CPPUNIT_TEST(PremadeIslocked);
    CPPUNIT_TEST(NewDbIslocked);
    CPPUNIT_TEST(PremadeNoLockFlagIslocked);
    CPPUNIT_TEST(NewDbNoLockFlagIslocked);
    CPPUNIT_TEST(PremadePartFlagIslocked);
    CPPUNIT_TEST(NewDbPartFlagIslocked);
    CPPUNIT_TEST(PremadeRWFlagIslocked);
    CPPUNIT_TEST(NewDbRWFlagIslocked);
    CPPUNIT_TEST(PremadeUnlockLockIslocked);
    CPPUNIT_TEST(NewDbUnlockLockIslocked);
    CPPUNIT_TEST(DupLockShareAndIsLocked);
    CPPUNIT_TEST(NoRWFlagLockSharedAndIsLocked);
    CPPUNIT_TEST(NoPartFlagTestPunlockAndIsLocked);
    CPPUNIT_TEST(DupLockIslockedCloseDbIslocked);
    CPPUNIT_TEST(TestPlockAndChildIsLocked);

    CPPUNIT_TEST(ThreadThenTryLock); // D-1
    CPPUNIT_TEST(ForkThenTryLock); // D-2
    CPPUNIT_TEST(OpenPartFlagTryLockThenFork);
    CPPUNIT_TEST(ThreadAndTryLockWithNoLockFlag);
    CPPUNIT_TEST(OpenNoLockTryLockThenFork);
    CPPUNIT_TEST(ThreadNestsChildTryLocks);
    //CPPUNIT_TEST(ThreadsShareHandleNestVsBlockTryLock); //shouldn't share handles x threads
    CPPUNIT_TEST(TryLockCloseReopenReTryLock);
    CPPUNIT_TEST(TryLockCloseChildReopenReTryLock);
    CPPUNIT_TEST(ForkAndTryLockThenLock);
    CPPUNIT_TEST(ForkAndLockThenTryLock);

    CPPUNIT_TEST(SharedLockNewDBnoFlag); // E-1
    CPPUNIT_TEST(SharedLockNewDB);
    CPPUNIT_TEST(LockSharedThenChildLockShared);
    CPPUNIT_TEST(ThreadAndLockShared);
    CPPUNIT_TEST(ThreadLockSharedChildReads);
    CPPUNIT_TEST(LockSharedChildReads);
    CPPUNIT_TEST(ThreadLockSharedChildWrites);
    CPPUNIT_TEST(LockSharedChildWrites);
    CPPUNIT_TEST(ThreadReadThenLockShared);
    //CPPUNIT_TEST(ThreadsShareHandleShLockVsLock); //shouldn't share handles x threads
    CPPUNIT_TEST(LockSharedThenForkAndLock);
    CPPUNIT_TEST(ThreadAndLockSharedVsLockDupHandle);
    CPPUNIT_TEST(ThreadAndLockSharedVsLock);
    //CPPUNIT_TEST(ThreadOpenSharedChildOpensNotShared); // FAIL, behavior differs in V4
    //CPPUNIT_TEST(OpenSharedChildOpensNotShared); // FAIL, behavior differs in V4

    //CPPUNIT_TEST(ThreadShareLockChildIterates); // FAIL, behavior differs in V4
    CPPUNIT_TEST(LockSharedChildIterates);
    CPPUNIT_TEST(ParentNestsSL_ChildLocksOnce);
    //CPPUNIT_TEST(ThreadsShareHandleShLockVsShLock); // shouldn't share handles x threads
    //CPPUNIT_TEST(ThreadsShareHandleLockVsShLock); // shouldn't share handles x threads
    CPPUNIT_TEST(MemoryOnlyParentSharedLockChildBlock); // E-21

    CPPUNIT_TEST(SharedTryLockNewDBnoFlag); // F-1
    CPPUNIT_TEST(ThreadTryLockSharedChildReads);
    CPPUNIT_TEST(TryLockSharedChildReads);
    CPPUNIT_TEST(ThreadTryLockSharedChildWrites);
    CPPUNIT_TEST(TryLockSharedChildWrites);
    CPPUNIT_TEST(ThreadReadThenTryLockShared);
    //CPPUNIT_TEST(ThreadsShareHandleShLockVsTryLockShared); // shouldn't share handles x threads
    CPPUNIT_TEST(ThreadAndLockSharedVsTryLockShareddDupHandle);
    CPPUNIT_TEST(LockSharedThenForkTryLockShared);
#ifdef DYNAMIC_LOCK_EXPANSION
    CPPUNIT_TEST(TryShared_ThreadOpenSharedChildOpensNotShared);
    CPPUNIT_TEST(TryShared_OpenSharedChildOpensNotShared);
#endif
    //CPPUNIT_TEST(ThreadTryShareLockChildIterates); // FAIL, behaviour differs in V4
    CPPUNIT_TEST(TryLockSharedChildIterates);

    CPPUNIT_TEST(PartLockNewDBnoFlag); // G-1
    CPPUNIT_TEST(PartLockNewDB);
    CPPUNIT_TEST(ThreadPlockChildIterates);
    CPPUNIT_TEST(PlockChildIterates);
    CPPUNIT_TEST(ThreadParentChildKeysInSamePartion);
    CPPUNIT_TEST(ParentChildKeysInSamePartion);
    CPPUNIT_TEST(ThrdParentPlockChildPlockKeysInDiffPart);
    CPPUNIT_TEST(ParentChildKeysInDiffPartions);
    CPPUNIT_TEST(PlockIterationAndAutoUnlock);
#ifdef DYNAMIC_LOCK_EXPANSION
    CPPUNIT_TEST(ThreadOpenPartChildOpensNotPart);
    CPPUNIT_TEST(OpenPartChildOpensNotPart);
#endif
    CPPUNIT_TEST(ParentNestsChildPlocksOnce);
    CPPUNIT_TEST(MemoryOnlyParentPartLockChildBlock); // G-13

    CPPUNIT_TEST(PunlockWithoutLocking); // H-1
    CPPUNIT_TEST(ParentIteratePunlockChildIterate);
    CPPUNIT_TEST(ParentPlocksChildPunlocks);
    CPPUNIT_TEST(ThreadNestPlocksThenUnwindPunlock);
    // Obsolete(interleaving) CPPUNIT_TEST(ParentLocksChildPunlocks);

    CPPUNIT_TEST(TryPartLockNewDBnoFlag); // I-1
    CPPUNIT_TEST(ThreadTryPlockChildIterates);
    CPPUNIT_TEST(TryPlockChildIterates);
    CPPUNIT_TEST(ThreadTryPlockParentChildKeysInSamePartion);
    CPPUNIT_TEST(TryPlockParentChildKeysInSamePartion);
    CPPUNIT_TEST(ThrdParentTryPlockChildTryPlockKeysInDiffPart);
    CPPUNIT_TEST(TryLockParentChildKeysInDiffPartions);
    CPPUNIT_TEST(TryPlockIterationAndAutoUnlock);
#ifdef DYNAMIC_LOCK_EXPANSION
    CPPUNIT_TEST(ThreadOpenPartFlagChildOpenNoPartFlagTry);
    CPPUNIT_TEST(OpenPartFlagChildOpenNoPartFlagTryPlock);
#endif
    CPPUNIT_TEST(ThrdParentTryPlockChildPlockKeysInSamePartion);
    CPPUNIT_TEST(ParentPlockChildTryPlockKeysInSamePartion);
    CPPUNIT_TEST(ThrdParentTryPlockChildPlockKeysInDiffPart);
    CPPUNIT_TEST(ParentPlockChildTryPlockKeysInDiffPartions);

    // Tests of mdbm_isowned() 
    CPPUNIT_TEST(test_OtherAG1);
    CPPUNIT_TEST(test_OtherAG2);
    CPPUNIT_TEST(test_OtherAG3);
    CPPUNIT_TEST(test_OtherAG4);
    CPPUNIT_TEST(test_OtherAG5);
    CPPUNIT_TEST(test_OtherAG6);
    CPPUNIT_TEST(test_OtherAG7);
    CPPUNIT_TEST(test_OtherAG8);

    //Tests of mdbm_trylock_smart
    CPPUNIT_TEST(test_OtherAJ8);
    CPPUNIT_TEST(test_OtherAJ9);
    CPPUNIT_TEST(test_OtherAJ10);
    CPPUNIT_TEST(test_OtherAJ11);

    CPPUNIT_TEST(TestDeleteLockfiles);   // Test of mdbm_delete_lockfiles API

    CPPUNIT_TEST_SUITE_END();

public:
    LockV3TestSuitePLock() : LockV3TestSuite(MDBM_CREATE_V3|MDBM_SINGLE_ARCH) {}
};

CPPUNIT_TEST_SUITE_REGISTRATION(LockV3TestSuitePLock);
