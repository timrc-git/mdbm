/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_lock mdbm_trylock mdbm_unlock
//   mdbm_lock_shared mdbm_trylock_shared mdbm_islocked
//   mdbm_plock mdbm_tryplock mdbm_punlock mdbm_lock_reset
#ifndef TEST_LOCKBASE_H
#define TEST_LOCKBASE_H

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

#include <iostream>
#include <vector>
#include <algorithm>

#include "atomic.h"
#include "mdbm.h"
//#include "configstoryutil.hh"
#include "TestBase.hh"


//class LockBaseTestSuite : public CppUnit::TestFixture, public TestBase
class LockBaseTestSuite : public TestBase
{
public:
    LockBaseTestSuite(int vFlag) : TestBase(vFlag, "LockBaseTestSuite") {}
    //void setUp(const char *tempv2dbname, const char *tempv3dbname, const char *tsTitle);
    void setUp();
    //void tearDown() { }

    struct SelectError
    {
        SelectError() : _errno(-1) {}
        SelectError(int errnum) : _errno(errnum) {}

        int _errno;
    };

    // readSelect will do a select on the file descriptors specified
    // as a set of read fd's
    // timeoutSecs can default to 1 second for the select timeout
    // RETURNS: 0=timeout, >0 fd's are ready for reading
    // readSelect will throw a SelectError if select returns an error
    static vector<int> readSelect(vector<int> &fds, int timeoutSecs = 1);

    // wait for a message on the specified fd, set errMsg if error
    // if there is a message, set it in the resp
    // RETURN: true = got a message, false = time-out or error
    static bool        waitForResponse(int readFD, string &resp, string &errMsg, int timeoutSecs = 1);

    // IP-comm messages used between parent and child
    static string _Continue;
    static string _Ack;
    static string _NAck;
    static string _TryLockOK;
    static string _TryLockFailed;


    // Perform auto close of file descriptor.
    // Used for making and pipes and cleaning up the associated file descriptors
    struct FD_RAII
    {
        FD_RAII(const int fd) : _fd(fd) {}
        FD_RAII() : _fd(-1) {}
        ~FD_RAII() { if (_fd != -1) { close(_fd); } _fd = -1; }
        operator int() { return _fd; }
        void operator = (int fd) { _fd = fd; }

        // send the message string via contained file descriptor
        // RETURN: -1 upon error, else number of bytes sent
        int        sendMsg(const string &msg);
        static int sendMsg(int fd, const string &msg);

        // make a pipe and set the read and write ends with the given params
        static void makePipe(FD_RAII &readFD, FD_RAII &sendFD);

        int _fd;
    };

    // Function Object base class to be implemented by various lock API.
    struct LockerFunc
    {
        LockerFunc() {}
        virtual ~LockerFunc() {}
        virtual int operator()(MDBM *dbh) = 0; // perform locking against the DB
        virtual bool blockable() { return true; }
        virtual bool sharable() { return false; }
    };
    struct IsLocked : public LockerFunc
    {
        IsLocked() : LockerFunc() {}
        int operator()(MDBM *dbh) { return mdbm_islocked(dbh); }
        bool blockable() { return false; }
    };
    struct Locker : public LockerFunc
    {
        Locker() : LockerFunc() {}
        int operator()(MDBM *dbh) { return mdbm_lock(dbh); }
    };
    struct UnLocker : public LockerFunc
    {
        UnLocker() : LockerFunc() {}
        int operator()(MDBM *dbh) { return mdbm_unlock(dbh); }
        bool blockable() { return false; }
    };
    struct TryLocker : public LockerFunc
    {
        TryLocker() : LockerFunc() {}
        int operator()(MDBM *dbh) { return mdbm_trylock(dbh); }
        bool blockable() { return false; }
    };
    struct PartLocker : public LockerFunc
    {
        PartLocker(const string &key) : LockerFunc(), _key(key) {}
        int operator()(MDBM *dbh) {
           datum key;
           key.dsize = _key.size();
           key.dptr  = const_cast<char*>(_key.c_str());
           return mdbm_plock(dbh, &key, 0);
        }
        string _key;
    };
    struct TryPlocker : public PartLocker
    {
        TryPlocker(const string &key) : PartLocker(key) {}
        int operator()(MDBM *dbh) {
           datum key;
           key.dsize = _key.size();
           key.dptr  = const_cast<char*>(_key.c_str());
           return mdbm_tryplock(dbh, &key, 0);
        }
        bool blockable() { return false; }
    };
    struct PunLocker : public UnLocker
    {
        PunLocker(const string &key) : UnLocker(), _key(key) {}
        int operator()(MDBM *dbh) {
           datum key;
           key.dsize = _key.size();
           key.dptr  = const_cast<char*>(_key.c_str());
           return mdbm_punlock(dbh, &key, 0);
        }
        string _key;
    };
    struct SharedLocker : public LockerFunc
    {
        SharedLocker() : LockerFunc() {}
        int operator()(MDBM *dbh) { return mdbm_lock_shared(dbh); }
        bool sharable() { return true; }
    };
    struct SharedTryLocker : public SharedLocker
    {
        SharedTryLocker() : SharedLocker() {}
        int operator()(MDBM *dbh) { return mdbm_trylock_shared(dbh); }
        bool blockable() { return false; }
    };
    struct SharedReader : public LockerFunc
    {
        SharedReader(const string &key) : LockerFunc(), _key(key) {}
        int operator()(MDBM *dbh) {
           datum key;
           key.dsize = _key.size();
           key.dptr  = const_cast<char*>(_key.c_str());
           datum ret = mdbm_fetch(dbh, key);
           return ret.dsize == 0 ? -1 : 1;
        }
        bool sharable() { return true; }
        string _key;
    };
    struct KeyReader : public LockerFunc
    {
        KeyReader() : LockerFunc() {}
        int operator()(MDBM *dbh) {
            datum key = mdbm_firstkey(dbh);
            return key.dsize == 0 ? -1 : 1;
        }
        bool sharable() { return true; }
    };
    struct UnsharedWriter : public SharedReader
    {
        UnsharedWriter(const string &key) : SharedReader(key) {}
        int operator()(MDBM *dbh) {
           datum key;
           key.dsize = _key.size();
           key.dptr  = const_cast<char*>(_key.c_str());
           datum val;
           val.dptr  = (char*)"shwriter";
           val.dsize = strlen(val.dptr) + 1;
           int ret = mdbm_store(dbh, key, val, MDBM_REPLACE);
           return ret == -1 ? -1 : 1;
        }
        bool sharable() { return false; }
    };

    struct CommonParams
    {
        CommonParams(string &prefix, int openFlags, LockerFunc &lockFunc,
            int expectedRet, bool shouldBlock) :
                _prefix(prefix), _openFlags(openFlags), _lockFunc(lockFunc),
                _expectedRet(expectedRet), _shouldBlock(shouldBlock)
        {}

        string& prefix() { return _prefix; }
        int  openFlags() { return _openFlags; }
        LockerFunc& lockFunc() { return _lockFunc; }
        int  expectedRet() { return _expectedRet; }
        bool shouldBlock() { return _shouldBlock; }

        string     &_prefix;
        int         _openFlags;
        LockerFunc &_lockFunc;
        int         _expectedRet;
        bool        _shouldBlock;
    };

    // setup a multi-page DB
    bool setupKeysInDifferentPartitions(string &prefix, int openFlags, string &dbName,
        string &keyPart1, string &keyPart2);

    void lockNewDB(string &prefix, int openFlags, LockerFunc &locker, int expectedReturn = 1);

    void lockingAndForking(string &prefix, int parOpenFlags, int childOpenFlags,
        LockerFunc &lockFunc, UnLocker &unlockFunc, LockerFunc &childLockFunc);

    void nestLocksChildLockOnce(string &prefix, int openFlags,
             LockerFunc &parlocker, UnLocker &parunlocker, LockerFunc &childLocker);

    void lockCloseReopenRelock(string &prefix, int dbOpenFlags, LockerFunc &lockFunc);
    void lockCloseChildReopenRelock(string &prefix, int dbOpenFlags, LockerFunc &lockFunc);
    void lockAndUnlock(string &prefix, int dbOpenFlags, LockerFunc &locker, UnLocker &unlocker);

    void testIsLocked(CommonParams &params, LockerFunc &unlockOrLock, int unlockerRet,
         int firstIslockRet, int secIslockRet, bool newDB);

    void testIsLockedParentChild(string &prefix, int parOpenFlags,
        LockerFunc &locker, UnLocker &unlocker, int childOpenFlags);
    void openIsLocked(string &prefix, int openFlags, bool newDB);
    void openUnlockIslocked(string &prefix, int openFlags, UnLocker &unlocker, bool newDB);

    void parChildLockPartitions(CommonParams &params, const string &dbName,
        UnLocker &unlocker, LockerFunc &part2Locker, bool shareHandle);

    void lockAndChildIterates(string &prefix, int openFlags, string &dbName,
        LockerFunc &lockFunc, UnLocker &unlocker, bool childShouldBlock);
    void partTestIterateAndChildIterates(string &prefix, int openFlags, string &dbName);

    string createEmptyMdbm(const string &prefix);
    int testIsOwnedParentChild(string &prefix, int parOpenFlags,
         LockerFunc &locker, UnLocker &unlocker, int childOpenFlags);

    void test_OtherAG1();
    void test_OtherAG2();
    void test_OtherAG3();
    void test_OtherAG4();
    void test_OtherAG5();
    void test_OtherAG6();

protected:
    // user may either specify a name or a name will be created and set in dbName
    MDBM* lockThenClose(string &prefix, int dbOpenFlags, string &dbName, LockerFunc &lockFunc, bool closeDB = true, int expectedRet = 1);

    // user may either specify a name or a name will be created and set in dbName
    void openThenLock(string &prefix, int dbOpenFlags, string &dbName, LockerFunc &lockFunc);

    void parentWaitOnChildStatus(string &prefix, int pid);

    MDBM* createMultipageDB(string &prefix, int openFlags, string &dbName, int &numRecs);
    void getKeysInDifferentPartitions(MDBM *dbh, int openFlags, string &keyAlpha, string &keyBeta);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
};

#endif
