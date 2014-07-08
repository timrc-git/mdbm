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
#include "mdbm_internal.h"
#include "test_lockbase.hh"




int LockBaseTestSuite::_setupCnt = 0;

string LockBaseTestSuite::_Continue("CONTINUE");
string LockBaseTestSuite::_Ack("ACK");
string LockBaseTestSuite::_NAck("NACK");
string LockBaseTestSuite::_TryLockOK("TRYLOCKOK");
string LockBaseTestSuite::_TryLockFailed("TRYLOCKFAILED");

namespace UTLockBase
{
    struct ChildMonitor_RAII
    {
        ChildMonitor_RAII(int pid, bool &killChild) :
            _pid(pid), _killChild(killChild) {}
        ~ChildMonitor_RAII()
        {
            kill();
        }
        void kill()
        {
            if (_pid > 0 && _killChild) {
                ::kill(_pid, SIGINT);
            }
            _pid = 0;
            _killChild = false;
        }

        int   _pid;
        bool &_killChild;
    };

}

using namespace UTLockBase;


void
LockBaseTestSuite::setUp()
{
    //stringstream title;
    //title << tsTitle << ": ";
    //_cfgStory = new ConfigStoryUtil(tempv2dbname, tempv3dbname, title.str().c_str());

    // create v2 and v3 databases if they dont exist
    //ConfigStoryUtil::setUpDBs();

    if (_setupCnt++ == 0) {
        cout << endl << SUITE_PREFIX() << " Beginning..." << endl << flush;
    }
}

int LockBaseTestSuite::FD_RAII::sendMsg(const string &msg)
{
    return sendMsg(_fd, msg);
}
int LockBaseTestSuite::FD_RAII::sendMsg(int fd, const string &msg)
{
    return write(fd, msg.c_str(), msg.size() + 1);
}

void LockBaseTestSuite::FD_RAII::makePipe(FD_RAII &readFD, FD_RAII &sendFD)
{
    int compipe[2];
    int ret = pipe(compipe);
    (void)ret;
    readFD = compipe[0];
    sendFD = compipe[1];
}

// Perform timed select on the given set of fd's for reading.
// Return the fd's that are ready for reading.
vector<int>
LockBaseTestSuite::readSelect(vector<int> &fds, int timeoutSecs)
{
    fd_set rfds;
    FD_ZERO(&rfds);
    int nfds = 0;
    for (size_t cnt = 0; cnt < fds.size(); ++cnt) {
        if (fds[cnt] > nfds) {
            nfds = fds[cnt];
        }
        FD_SET(fds[cnt], &rfds);
    }

    struct timeval tv;
    tv.tv_sec = timeoutSecs;
    tv.tv_usec = 0;

    vector<int> readyFds;
    int ret = select(nfds+1, &rfds, 0, 0, &tv);
    if (ret == -1) {
        throw SelectError(errno);
    } else if (ret == 0) { // timeout
    } else { // some descriptors are ready
        for (size_t cnt = 0; cnt < fds.size(); ++cnt) {
            if (FD_ISSET(fds[cnt], &rfds)) {
                readyFds.push_back(fds[cnt]);
            }
        }
    }
    return readyFds;
}

bool
LockBaseTestSuite::waitForResponse(int readFD, string &resp, string &errMsg, int timeoutSecs)
{
    try {
        vector<int> fds;
        fds.push_back(readFD);
        vector<int> readyFds = readSelect(fds, timeoutSecs);
        if (readyFds.size() == 0) {
            errMsg.append("TIMEOUT: No message sent");
            return false;
        }
    } catch (SelectError &serr) {
        // ohoh! problem with select on the pipe
        errMsg.append("ERROR: Problem selecting on the pipe - cannot continue this test");
        return false;
    }

    char buff[512];
    memset(buff, 0, sizeof(buff));
    int ret = read(readFD, buff, sizeof(buff));
    if (ret > 0) {
        resp.append(buff);
    }

    return true;
}

void
LockBaseTestSuite::lockNewDB(string &prefix, int openFlags, LockerFunc &locker, int expectedRet)
{
    string dbName;
    lockThenClose(prefix, openFlags, dbName, locker, true, expectedRet);
    unlink(dbName.c_str());
}

void
LockBaseTestSuite::lockingAndForking(string &prefix, int parOpenFlags, int childOpenFlags,
    LockerFunc &lockFunc, UnLocker &unlockFunc, LockerFunc &childLockFunc)
{
    // create pipes - using separate pipes so no timing issues as there can be
    // if using same pipe for reading and writing
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    string dbName;
    // FIXME fail here....
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, parOpenFlags | MDBM_O_RDWR, 0644, -1, -1, &dbName));

    int pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    bool shouldBeBlocked = childLockFunc.blockable();
    if (lockFunc.sharable() && childLockFunc.sharable()) {
        // all shared locking must play by same open rules else same as mutex
        if (parOpenFlags & childOpenFlags & MDBM_RW_LOCKS) {
            shouldBeBlocked = false;
        }
    }

    if (pid) { // parent
        // parent: lock the DB; send continue to child;
        //         select on pipe for a second;
        //         timeout on pipe select; unlock the DB;
        //         select on pipe again to receive ACK from child
        bool killChild = false;
        ChildMonitor_RAII child(pid, killChild);

        stringstream msghdrss;
        msghdrss << prefix << "Parent:child(" << pid << "): ";
        string msghdr = msghdrss.str();

        // wait for child to get the DB
        string resp;
        string errMsg = msghdr + "Waiting on child to obtain DB: ";
        bool selerr = waitForResponse(childReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg, (selerr == true));

        lockFunc(dbh);   // now lock

        // signal child that it may lock the DB
        int ret = parSendFD.sendMsg(_Continue);
        (void)ret;

        vector<int> fds;
        fds.push_back(childReadFD);
        try {
            vector<int> readyFds = readSelect(fds, 1);

            stringstream selss;
            selss << msghdr;
            if ((parOpenFlags | childOpenFlags) & MDBM_OPEN_NOLOCK) {
                // if anyone specified no lock, then there will be no
                // enforced locking
                selss << " Child should NOT have been blocked" << endl;
                killChild = (readyFds.size() <= 0);
                CPPUNIT_ASSERT_MESSAGE(selss.str(), (!killChild));
            } else if (shouldBeBlocked) {
                selss << " Child should have been blocked" << endl;
                killChild = (readyFds.size() != 0);
                CPPUNIT_ASSERT_MESSAGE(selss.str(), (!killChild));
            } else { // shared locks and trylock type functions should not have blocked
                selss << " Child should NOT have been blocked {trylock or parent/child shared locking}" << endl;
                killChild = (readyFds.size() <= 0);
                CPPUNIT_ASSERT_MESSAGE(selss.str(), (!killChild));

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
                CPPUNIT_ASSERT_MESSAGE(rbss.str(), (!killChild));
            }
        } catch (SelectError &serr) {
            // ohoh! problem with select on the pipe
            stringstream selss;
            selss << prefix << " Problem selecting on the pipe - cannot continue this test" << endl;
            killChild = true;
            CPPUNIT_ASSERT_MESSAGE(selss.str(), (!killChild));
        }

        unlockFunc(dbh);

        if (shouldBeBlocked == false) {
            ret = parSendFD.sendMsg(_Continue);
        }

        try {
            string resp;
            string errMsg = "Waiting on child to lock DB: ";
            bool selret = waitForResponse(childReadFD, resp, errMsg);
            killChild = (selret == false);
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

            // yay! child acknowledged it got hold of the DB
            GetLogStream() << prefix << " parent: child=" << pid
                           << ": Child ACKnowledged it locked the DB: " << resp << endl;
        } catch (SelectError &serr) {
            // boo! child is blocked or something else went wrong
            // perhaps send kill to child pid
            child.kill();
        }

        parentWaitOnChildStatus(prefix, pid);
    } else  { // child
        // child: open its own handle; read continue on pipe; lock the DB
        //        when unblock send an ACK thru the pipe; exit
        //MDBM *c_dbh = EnsureNamedMdbm(dbName, childOpenFlags | MDBM_O_RDWR);
        MdbmHolder c_dbh(EnsureNamedMdbm(dbName, childOpenFlags | MDBM_O_RDWR));

        // tell parent we got the DB
        childSendFD.sendMsg(_Ack);

        string resp;
        string errMsg = "Parent should have told us to continue: ";
        bool selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        GetLogStream() << prefix
                     << " parent locked the DB and sent us the message=" << resp << endl;

        int ret = childLockFunc(c_dbh);

        if (shouldBeBlocked == true) {
            childSendFD.sendMsg(_Ack);
        } else {
            if (ret == -1) { // trylock should have failed
                childSendFD.sendMsg(_TryLockFailed);
            } else  {
                childSendFD.sendMsg(_Ack);
            }

            // read msg from parent when they tell us they unlocked
            errMsg = "Parent should have told us they unlocked: ";
            selret = waitForResponse(parReadFD, resp, errMsg);
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

            ret = childLockFunc(c_dbh);  // so trylock should succeed now
            if (ret != 1) {
                cout << prefix
                     << ": Should have successfully trylocked or share-locked" << endl << flush;
                exit(1);
            }

            childSendFD.sendMsg(_Ack);
        }
        exit(0);
    }
}

//  Parent will lock the DB several times; signal child to lock;
//  then unwind the locks and test the child doesnt get the lock
//  until fully unwound.
void
LockBaseTestSuite::nestLocksChildLockOnce(string &prefix, int openFlags, LockerFunc &parlocker, UnLocker &unlocker, LockerFunc &childLocker)
{
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    string dbName;
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, openFlags | MDBM_O_RDWR, 0644, -1, -1, &dbName));

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid) { // parent
        // wait for child to get the DB
        string resp;
        string errMsg = "Waiting on child to obtain DB: ";
        bool selerr = waitForResponse(childReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selerr == true));

        int ret = parlocker(dbh); // first locking

        // signal child to perform lock
        parSendFD.sendMsg(_Continue);

        ret = parlocker(dbh); // nested lock
        ret = parlocker(dbh); // nested lock again - 3rd locking

        // wait for child to say they got the lock - should time-out
        string lockErrMsg = "Parent set nested locks. Child should not be able to obtain Lock: ";
        selerr = waitForResponse(childReadFD, resp, lockErrMsg);
        CPPUNIT_ASSERT_MESSAGE(lockErrMsg.insert(0, prefix), (selerr == false));

        // unlock twice and verify child is still blocked
        ret = unlocker(dbh);
        ret = unlocker(dbh);

        // wait for child to say they got the lock - should time-out
        string stillNestedErrMsg = "Locks still nested. Child should still not be able to obtain Lock: ";
        selerr = waitForResponse(childReadFD, resp, stillNestedErrMsg);
        CPPUNIT_ASSERT_MESSAGE(stillNestedErrMsg.insert(0, prefix), (selerr == false));

        // unlock for 3rd and last time, now child should unblock
        ret = unlocker(dbh);
        (void)ret;

        string unlockErrMsg = "NO Locks nested. Child should be able to obtain Lock: ";
        selerr = waitForResponse(childReadFD, resp, unlockErrMsg);
        CPPUNIT_ASSERT_MESSAGE(unlockErrMsg.insert(0, prefix), (selerr == true));

        parentWaitOnChildStatus(prefix += "Unknown ERROR: Child FAILed to exit: ", pid);

    } else { // child
        MdbmHolder c_dbh = EnsureNamedMdbm(dbName, openFlags | MDBM_O_RDWR);

        // signal parent we have the DB
        childSendFD.sendMsg(_Continue);

        // wait for parent to tell us to obtain the lock
        string resp;
        string errMsg = "Waiting on parent to tell us to continue: ";
        if (waitForResponse(parReadFD, resp, errMsg) == false) {
            cout << errMsg.insert(0, prefix) << endl << flush;
            exit(1);
        }

        // obtain lock once
        if (childLocker(c_dbh) == -1) {
            cout << prefix
                 << ": Should have successfully locked" << endl << flush;
            exit(1);
        }

        // signal parent we have the lock
        childSendFD.sendMsg(_Continue);

        exit(0);
    }
}

//  Complements openThenLock.
//  Creates a new DB, calls the lock function on it, then closes it if requested.
//  Does not delete the newly created DB file.
MDBM*
LockBaseTestSuite::lockThenClose(string &prefix, int dbOpenFlags, string &dbName, LockerFunc &lockFunc, bool closeDB, int expectedRet)
{
    if (dbName.empty()) {
        string suffix = "locking" + versionString + ".mdbm";
        dbName = GetTmpName(suffix);
    }

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | dbOpenFlags;
    MDBM *dbh = mdbm_open(dbName.c_str(), openflags, 0644, 0, 0);
    if (!dbh) {
        int errnum = errno;
        cout << "WARNING:(LTC): " << prefix
             << " mdbm_open FAILed with errno=" << errnum
             << " to open DB=" << dbName.c_str() << endl << flush;
        return 0;
    }

    int ret = lockFunc(dbh);

    if (closeDB)     {
        mdbm_close(dbh);
        dbh = 0;
    }

    if (ret != expectedRet)     {
        unlink(dbName.c_str());

        stringstream ssmsg;
        ssmsg << prefix
              << " FAIL: Lock function returned=" << ret
              << " but was expected to return=" << expectedRet << endl;
        CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (ret == expectedRet));
    }

    return dbh;
}
//  Complements lockThenClose.
//  Opens the DB, locks(or unlocks) it.
//  Deletes the DB file.
void
LockBaseTestSuite::openThenLock(string &prefix, int dbOpenFlags, string &dbName, LockerFunc &lockFunc)
{
    if (dbName.empty())     {
        string suffix = "locking" + versionString + ".mdbm";
        dbName = GetTmpName(suffix);
    }

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | dbOpenFlags;
    MdbmHolder dbh(EnsureNamedMdbm(dbName, openflags));

    int ret = lockFunc(dbh);

    stringstream ssmsg;
    ssmsg << prefix << " FAILed to (un)lock DB" << endl;
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (ret == 1));
}
void
LockBaseTestSuite::lockCloseReopenRelock(string &prefix, int dbOpenFlags, LockerFunc &lockFunc)
{
    string dbName;
    lockThenClose(prefix, dbOpenFlags, dbName, lockFunc);

    MdbmHolder db(dbName);

    openThenLock(prefix, dbOpenFlags, dbName, lockFunc);
}

void
LockBaseTestSuite::lockCloseChildReopenRelock(string &prefix, int dbOpenFlags, LockerFunc &lockFunc)
{
    string dbName;
    lockThenClose(prefix, dbOpenFlags, dbName, lockFunc);

    MdbmHolder db(dbName);

    int pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    prefix += " Child: FAILed to lock DB: ";

    if (pid) { // parent
        parentWaitOnChildStatus(prefix, pid);
    } else { // child
        try {
            openThenLock(prefix, dbOpenFlags, dbName, lockFunc);
        } catch(...) {
            cout << prefix << endl << flush;
            exit(1);
        }
        exit(0);
    }
}

void
LockBaseTestSuite::parentWaitOnChildStatus(string &prefix, int pid)
{
    int status;
    pid_t pidret = waitpid(pid, &status, 0);  // wait for child

    stringstream ssmsg;
    ssmsg << prefix;
    if (pidret == -1) {
        perror("LockBaseTestSuite:parent wait on child failed");
        int errnum = errno;
        ssmsg << "waitpid(" << pid << ") returned errno=" << errnum << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (pidret != -1));

    int signo = 0;
    if (WIFSIGNALED(status)) {
        signo = WTERMSIG(status);
    }

    if (pidret == pid) {
        if (WIFEXITED(status)) { // normal termination - aka child called exit
            int exstatus = WEXITSTATUS(status);
            ssmsg << "Child exit value=" << exstatus << endl;
            CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (exstatus == 0));
        }
        ssmsg << "The child was BLOCKed therefore parent terminated it with signal=" << SIGINT
              << " System reports it was terminated with signal=" << signo << endl;
    } else {
        ssmsg << "Indeterminate what happened with the child. Parent attempted to terminate it with signal=" << SIGINT << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (pidret == pid && WIFEXITED(status)));
}

void
LockBaseTestSuite::lockAndUnlock(string &prefix, int dbOpenFlags, LockerFunc &locker, UnLocker &unlocker)
{
    // use pre-made DB
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, dbOpenFlags | MDBM_O_RDWR));

    int ret = locker(dbh);

    stringstream lkmsg;
    lkmsg << prefix << " FAILed to lock DB" << endl;
    CPPUNIT_ASSERT_MESSAGE(lkmsg.str(), (ret == 1));

    ret = unlocker(dbh);

    stringstream ulmsg;
    ulmsg << prefix << " FAILed to unlock DB" << endl;
    CPPUNIT_ASSERT_MESSAGE(ulmsg.str(), (ret == 1));
}

void
LockBaseTestSuite::testIsLocked(CommonParams &params, LockerFunc &unlockOrLock, int unlockerRet, int firstIslockRet, int secIslockRet, bool newDB)
{
    string prefix = params.prefix();
    LockerFunc &lockOrUnlock = params.lockFunc();
    int lockerRet = params.expectedRet();
    int openFlags = params.openFlags();

    MdbmHolder dbh(
        newDB
        ? EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | openFlags)
        : GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | openFlags) );

    int ret = lockOrUnlock(dbh);
    stringstream lkmsg;
    lkmsg << prefix
          << " FAILed to (un)lock the DB. ret=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(lkmsg.str(), (ret == lockerRet));

    ret = mdbm_islocked(dbh);
    stringstream ilmsg;
    ilmsg << prefix
          << " FAIL: mdbm_islocked returned=" << ret
          << " but was expected to return=" << firstIslockRet << endl;
    CPPUNIT_ASSERT_MESSAGE(ilmsg.str(), (ret == firstIslockRet));

    ret = unlockOrLock(dbh);
    stringstream ukmsg;
    ukmsg << prefix
          << " FAILed to (un)lock the DB. ret=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(ukmsg.str(), (ret == unlockerRet));

    ret = mdbm_islocked(dbh);
    stringstream il2msg;
    il2msg << prefix
          << " FAIL: mdbm_islocked returned=" << ret
          << " but was expected to return=" << secIslockRet << endl;
    CPPUNIT_ASSERT_MESSAGE(il2msg.str(), (ret == secIslockRet));
}

void
LockBaseTestSuite::testIsLockedParentChild(string &prefix, int parOpenFlags, LockerFunc &locker, UnLocker &unlocker, int childOpenFlags)
{
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    string dbName;
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | parOpenFlags, 0644, -1, -1, &dbName));

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid) { // parent
        bool killChild = false;
        ChildMonitor_RAII child(pid, killChild);

        // wait for child to get the DB
        string resp;
        string errMsg = "Waiting on child to obtain DB: ";
        bool selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        int ret = locker(dbh);
        errMsg = "WARNING: Parent: lock failed, cannot continue the test";
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (ret == 1));

        // signal child to perform islocked
        parSendFD.sendMsg(_Continue);

        errMsg = "Waiting on child to perform islocked: ";
        resp = "";
        selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        if (resp.compare(_NAck) == 0) { // child reports a problem
            killChild = true;
            errMsg = "Parent: child reports DB is NOT locked: ";
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));
        }

        unlocker(dbh);

        // signal child to perform islocked
        parSendFD.sendMsg(_Continue);

        parentWaitOnChildStatus(prefix += " Child FAILed performing mdbm_islocked: ", pid);
    } else {  // child
        // child: open its own handle; read continue on pipe; lock the DB
        //        when unblock send an ACK thru the pipe; exit
        MDBM *c_dbh = EnsureNamedMdbm(dbName, childOpenFlags | MDBM_O_RDWR);

        // tell parent we got the DB
        childSendFD.sendMsg(_Ack);

        string resp;
        string errMsg = "Child: parent should have told us to continue: ";
        bool selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        GetLogStream() << prefix
                     << "Child: parent locked the DB and sent us the message=" << resp
                     << endl << flush;

        int ret = mdbm_islocked(c_dbh);
        errMsg = "WARNING: Child: testIsLockedParentChild: FAIL: mdbm_islocked returned NOT locked. exiting...";
        if (ret == 0) {
            cout << endl << prefix << errMsg << endl << endl << flush;
            childSendFD.sendMsg(_NAck);
        }
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (ret == 1));

        // tell parent we performed the islocked
        childSendFD.sendMsg(_Ack);

        // waiting for parent to tell us to try again
        errMsg = "Child: parent should have told us to perform 2nd mdbm_islocked : ";
        selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        ret = mdbm_islocked(c_dbh); // should return 0
        exit(ret);
    }
}

void
LockBaseTestSuite::openIsLocked(string &prefix, int openFlags, bool newDB)
{
    MdbmHolder dbh(
        newDB
        ? EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | openFlags)
        : GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | openFlags) );

    int ret = mdbm_islocked(dbh);

    stringstream ikmsg;
    ikmsg << prefix << " DB should NOT be locked, but mdbm_islocked reports it is locked" << endl;
    CPPUNIT_ASSERT_MESSAGE(ikmsg.str(), (ret == 0));
}

void
LockBaseTestSuite::openUnlockIslocked(string &prefix, int openFlags, UnLocker &unlocker, bool newDB)
{
    MdbmHolder dbh(
        newDB
        ? EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | openFlags)
        : GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | openFlags) );


    int ret = unlocker(dbh);  // should always fail

    ret = mdbm_islocked(dbh);

    stringstream ikmsg;
    ikmsg << prefix << " DB should NOT be locked, but mdbm_islocked reports it is locked" << endl;
    CPPUNIT_ASSERT_MESSAGE(ikmsg.str(), (ret == 0));
}

// Create a multi-page DB and fill with dummy records
// dbName: may be specified else a name will be chosen for the new DB
// numRecs: number of records inserted into the DB
MDBM*
LockBaseTestSuite::createMultipageDB(string &prefix, int openFlags, string &dbName, int &numRecs)
{
    // if the DB file exists then just open it and return the handle
    if (!dbName.empty()) {
        struct stat statbuf;
        memset(&statbuf, 0, sizeof(statbuf));
        if (stat(dbName.c_str(), &statbuf) == 0 && statbuf.st_size > 0) {
            MDBM *dbh = mdbm_open(dbName.c_str(), openFlags | MDBM_O_RDWR,
                0644, 0, 0);
            if (dbh) {
                return dbh;
            }
        }
    } else { // get a name for the new DB
        CPPUNIT_ASSERT_MESSAGE("dbName is empty", !dbName.empty());
    }

    int pageSize = 512;
    int numPages = 5;
    int preSize = pageSize * numPages;

    MDBM *dbh = mdbm_open(dbName.c_str(),
        MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | openFlags, 0644, pageSize, preSize);
    if (!dbh) {
        cout << "WARNING: " << prefix
             << "createMultipageDB: mdbm_open FAILed for DB=" << dbName.c_str()
             << endl << flush;
        return dbh;
    }

    if (mdbm_limit_size_v3(dbh, numPages, 0, 0) == -1) {
        cout << "WARNING: " << prefix << "createMultipageDB: mdbm_limit_size FAILed" << endl << flush;
        mdbm_close(dbh);
        return 0;
    }

    numRecs = FillDb(dbh, -1);

    return dbh;
}

// Iterate the DB to find 2 keys in different partitions
// Throw exception if DB has less than 2 partitions
// FIX TODO translating page number to partition number has a couple of issues
void
LockBaseTestSuite:: getKeysInDifferentPartitions(MDBM *dbh, int openFlags, string &keyAlpha, string &keyBeta)
{
//v3: int partNum = MDBM_PAGENUM_TO_PARTITION(dbh, pageNum);
//v2: int partNum = pageNum % dbh->m_mlock_num; // problem how to access v2 struct
// FIX    int getPartitionNumV2(MDBM *dbh, mdbm_ubig_t pageNum);

    int       alphaPart = -1;
    int       betaPart  = -1;
    int       numRecs   = 0;
    MDBM_ITER iter;
    datum     key       = mdbm_firstkey_r(dbh, &iter);
    for (; key.dsize != 0; ++numRecs) {
        mdbm_ubig_t pageNum = mdbm_get_page(dbh, &key);
// FIX for now just use page number as partition number
//        int         partNum = pageNum; // FIX MDBM_PAGENUM_TO_PARTITION(dbh, pageNum);

        if (alphaPart == -1) {
            alphaPart = pageNum; // FIX partNum;
            keyAlpha.assign(key.dptr, key.dsize);
        } else if (betaPart == -1 && (pageNum%128 != (mdbm_ubig_t)alphaPart%128)) { // FIX partNum
            betaPart = pageNum; // FIX partNum;
            keyBeta.assign(key.dptr, key.dsize);
            return;
        }

        key = mdbm_nextkey_r(dbh, &iter);
    }

    throw numRecs;
}

bool
LockBaseTestSuite::setupKeysInDifferentPartitions(string &prefix, int openFlags, string &dbName, string &keyPart1, string &keyPart2)
{
    int numRecs = 0;
    bool ret = false;
    MDBM *dbh = createMultipageDB(prefix, openFlags, dbName, numRecs);
    if (dbh) {
        try {
            getKeysInDifferentPartitions(dbh, openFlags, keyPart1, keyPart2);
            cout << "INFO: " << prefix
                 << "Found keys in different partitions."
                 //<< " Key1=" << keyPart1
                 //<< " Key2=" << keyPart2
                 << endl << flush;
            ret = true;
        } catch(int numRecs) {
            cout << "WARNING: " << prefix
                 << "FAILed to find keys in different partitions. Number of keys in DB=" << numRecs
                 << endl << flush;
        }

        mdbm_close(dbh);
    }

    return ret;
}

void
LockBaseTestSuite::parChildLockPartitions(CommonParams &params, const string &dbName, UnLocker &unlocker, LockerFunc &part2Locker, bool shareHandle)
{
    string prefix = params.prefix();
    LockerFunc &part1Locker = params.lockFunc();
    bool shouldBlock = params.shouldBlock();
    int openFlags = params.openFlags();

    MdbmHolder dbh(EnsureNamedMdbm(dbName.c_str(), openFlags | MDBM_O_RDWR, 0644, 0, 0));

    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid) { // parent
        bool killChild = false;
        ChildMonitor_RAII child(pid, killChild);

        // wait for child to get the DB
        string resp;
        string errMsg = "Waiting on child to obtain DB: ";
        GetLogStream() << errMsg;
        bool selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        //mdbm_lock_dump(dbh);
        int ret = part1Locker(dbh);
        //mdbm_lock_dump(dbh);
        if (ret == -1) {
            killChild = true;
            cout << "WARNING: " << prefix
                 << "parChildLockPartitions: lock FAILed for DB=" << dbName.c_str()
                 << " Cannot continue this test" << endl << flush;
            return;
        }

        // tell child to perform plock
        parSendFD.sendMsg(_Continue);

        resp = "";
        errMsg = "Waiting on child to perform lock: ";
        GetLogStream() << errMsg;
        selret = waitForResponse(childReadFD, resp, errMsg);

        if (shouldBlock) {
            errMsg = "Child was not blocked but should have been: ";
            killChild = (selret == true);
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

            // unlock and wait for child response again
            ret = unlocker(dbh);
            if (ret == -1) {
                cout << "WARNING: " << prefix
                     << "parChildLockPartitions: unlock FAILed for DB=" << dbName.c_str()
                     << " Cannot continue this test" << endl << flush;
                return;
            }

            resp = "";
            errMsg = "Waiting again for child to lock the DB: ";
            GetLogStream() << errMsg;
            selret = waitForResponse(childReadFD, resp, errMsg);
            killChild = (selret == false);
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));
        } else {
            errMsg = "Child was blocked but should NOT have been: ";
            killChild = (selret == false);
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));
        }

        prefix += " Child FAILed plock DB test: ";
        parentWaitOnChildStatus(prefix, pid);
    } else { // child
        // child: open its own handle if not shared; read continue on pipe;
        // lock the DB; when unblock send an ACK thru the pipe; exit
        MdbmHolder c_dbh(
            shareHandle
            ? (MDBM*)dbh
            : mdbm_open(dbName.c_str(), openFlags | MDBM_O_RDWR, 0644, 0, 0) );

        // tell parent we got the DB
        childSendFD.sendMsg(_Ack);

        string resp;
        string errMsg = "Parent should have told us to continue: ";
        GetLogStream() << errMsg;
        bool selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        GetLogStream() << prefix
                     << " parent locked the db and sent us the message=" << resp
                     << endl << flush;

        //mdbm_lock_dump(dbh);
        int ret = part2Locker(dbh);
        GetLogStream() << prefix
                     << " child locker got ret="<<ret<< " expected=" << params.expectedRet()
                     << " dbName=" << dbName
                     << " pid=" << getpid()
                     << " tid=" << gettid()
                     << endl << flush;

        if (ret == params.expectedRet()) {
            childSendFD.sendMsg(_Ack); // tell parent we performed lock
            ret = 0;
        } else {
            childSendFD.sendMsg(_NAck); // tell parent we performed lock
            //mdbm_lock_dump(dbh);
            sleep(30);
            ret = 1;
        }

        if (shareHandle == true) {
          c_dbh.Release();
        }
        exit(ret);
    }
}

void
LockBaseTestSuite::lockAndChildIterates(string &prefix, int openFlags, string &dbName, LockerFunc &locker, UnLocker &unlocker, bool childShouldBlock)
{
    MdbmHolder dbh(EnsureNamedMdbm(dbName.c_str(), openFlags | MDBM_O_RDWR, 0644, 0, 0));

    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid) { // parent
        bool killChild = false;
        ChildMonitor_RAII child(pid, killChild);

        // wait for child to get the DB
        string resp;
        string errMsg = "Waiting on child to obtain DB: ";
        bool selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        int ret = locker(dbh);
        if (ret == -1) {
            killChild = true;
            cout << "WARNING: " << prefix
                 << "lockAndChildIterates: lock FAILed for DB=" << dbName.c_str()
                 << " Cannot continue this test" << endl << flush;
            return;
        }

        // tell child to iterate
        parSendFD.sendMsg(_Continue);

        if (childShouldBlock) { // for child that should be blocked
            // child should get blocked at some point, so wait for response
            // should time-out
            resp = "";
            errMsg = "Child should have been blocked, therefore should not have completed iterating the DB: ";
            selret = waitForResponse(childReadFD, resp, errMsg);
            killChild = (selret == true);
            CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

            // after time-out so unlock the DB and tell child to continue
            ret = unlocker(dbh);
            if (ret == -1) {
                killChild = true;
                cout << "WARNING: " << prefix
                     << "Parent: unlock FAILed, therefore cannot continue this test"
                     << endl << flush;
                return;
            }
        }

        // wait for child to tell us they finished iterating
        resp = "";
        errMsg = "Child FAILed to complete iterating the DB: ";
        selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        parentWaitOnChildStatus(prefix += " Child FAILed lock DB test: ", pid);
    } else { // child
        // child: open its own handle; read continue on pipe; lock the DB
        //        when unblock send an ACK thru the pipe; exit
        MdbmHolder c_dbh(mdbm_open(dbName.c_str(), openFlags | MDBM_O_RDWR, 0644, 0, 0));

        // tell parent we got the DB
        childSendFD.sendMsg(_Ack);

        string resp;
        string errMsg = "Parent should have told us to continue: ";
        bool selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        GetLogStream() << prefix
                     << " parent locked the DB and sent us the message=" << resp
                     << endl << flush;

        // iterate the DB - when done send an ACK to the parent
        MDBM_ITER iter;
        datum     key = mdbm_firstkey_r(c_dbh, &iter);
        while (key.dsize != 0) {
            key = mdbm_nextkey_r(c_dbh, &iter);
        }

        childSendFD.sendMsg(_Ack);
        mdbm_close(c_dbh);
        exit(0);
    }
}

// parent iterate n times, tell child to iterate
// child should block
// parent should punlock and wait for child to tell it to iterate
// child should iterate n times, then tell parent to iterate
// parent should block
// child should complete iteration then tell parent its done
// parent should complete iteration, then read childs completion status
// then tell child to exit
void
LockBaseTestSuite::partTestIterateAndChildIterates(string &prefix, int openFlags, string &dbName)
{
    MdbmHolder dbh(EnsureNamedMdbm(dbName.c_str(), openFlags | MDBM_O_RDWR, 0644, 0, 0));

    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    // Both parent and child iterate this many times before telling the other
    // to continue.
    int   iterMsgTimeCnt = 3;

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid) { // parent
        bool killChild = false;
        ChildMonitor_RAII child(pid, killChild);

        // wait for child to get the DB
        string resp;
        string errMsg = "Waiting on child to obtain DB: ";
        bool selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        // iterate the DB
        MDBM_ITER iter;
        datum     key = mdbm_firstkey_r(dbh, &iter);

        // tell child to begin iteration
        parSendFD.sendMsg(_Continue);

        for (int cnt = 0; key.dsize != 0; ++cnt) {
            key = mdbm_nextkey_r(dbh, &iter);
            if (cnt == iterMsgTimeCnt) { // tell child to iterate
                resp = "";
                errMsg = "Child should have been blocked trying to iterate the DB: ";
                selret = waitForResponse(childReadFD, resp, errMsg);
                killChild = (selret == true);
                CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

                int ret = mdbm_punlock(dbh, &key, 0); // now unlock so child will unblock and iterate
                if (ret == -1) {
                    killChild = true;
                    cout << "WARNING: " << prefix
                         << "partTestIterateAndChildIterates: punlock FAILed for DB=" << dbName.c_str()
                         << " Cannot continue this test" << endl << flush;
                    return;
                }

                // wait for child to tell us to iterate again
                resp = "";
                errMsg = "Parent: Child should have told us to continue to iterate the DB: ";
                selret = waitForResponse(childReadFD, resp, errMsg);
                killChild = (selret == false);
                CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

                // tell child we will continue
                parSendFD.sendMsg(_Ack);
            }
        }

        // read childs completion status
        resp = "";
        errMsg = "Parent: Child should have completed iterating the DB: ";
        selret = waitForResponse(childReadFD, resp, errMsg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (!killChild));

        // tell child to exit
        parSendFD.sendMsg(_Continue);

        parentWaitOnChildStatus(prefix += " Child FAILed parition iteration DB test: ", pid);
    } else { // child
        // child: open its own handle; read continue on pipe; lock the DB
        //        when unblock send an ACK thru the pipe; exit
        MdbmHolder c_dbh(mdbm_open(dbName.c_str(), openFlags | MDBM_O_RDWR, 0644, 0, 0));

        // tell parent we got the DB
        childSendFD.sendMsg(_Ack);

        string resp;
        string errMsg = "Parent should have told us to continue: ";
        bool selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        GetLogStream() << prefix
                     << "Child: parent began iteration and told us to perform iteration: message=" << resp
                     << endl << flush;

        // iterate the DB - when done send an ACK to the parent
        MDBM_ITER iter;
        datum     key = mdbm_firstkey_r(c_dbh, &iter);
        for (int cnt = 0; key.dsize != 0; ++cnt) {
            key = mdbm_nextkey_r(c_dbh, &iter);
            if (cnt == iterMsgTimeCnt) { // tell parent to iterate
                childSendFD.sendMsg(_Continue);

                resp = "";
                errMsg = "Child: parent said it will continue: ";
                selret = waitForResponse(parReadFD, resp, errMsg);
                if (selret == true) {
                    errMsg += resp;
                }
                CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));
            }
        }

        // tell parent we are done iterating
        childSendFD.sendMsg(_Ack);

        resp = "";
        errMsg = "Child: parent should have told us to exit: ";
        selret = waitForResponse(parReadFD, resp, errMsg);

        mdbm_close(c_dbh); // clean up before exiting

        if (selret == false) {
            cout << "WARNING: " << errMsg << endl << flush;
        }

        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        exit(0);
    }
}

string
LockBaseTestSuite::createEmptyMdbm(const string &prefix)
{
    string fname;
    MdbmHolder mdbm(EnsureTmpMdbm(prefix, versionFlag | MDBM_O_CREAT | MDBM_O_RDWR, 0644,
                                  DEFAULT_PAGE_SIZE, 0, &fname));
    return fname;
}


void
LockBaseTestSuite::test_OtherAG1()
{
    string prefix = string("OtherAG1") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    string emptyMdbm(createEmptyMdbm(prefix));
    MdbmHolder mdbm(mdbm_open(emptyMdbm.c_str(), MDBM_O_RDWR, 0644, 0, 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) mdbm);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_isowned(mdbm));
}


void
LockBaseTestSuite::test_OtherAG2()
{
    string prefix = string("OtherAG2") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, versionFlag | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_isowned(mdbm));
}

void
LockBaseTestSuite::test_OtherAG3()
{
    string prefix = string("OtherAG3") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, versionFlag | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_isowned(mdbm));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
}

void
LockBaseTestSuite::test_OtherAG4()
{
    string prefix = string("OtherAG4") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, versionFlag | MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    for (int i = 0; i < 4; ++i) {
        CPPUNIT_ASSERT_EQUAL(1, mdbm_lock(mdbm));
    }
    CPPUNIT_ASSERT_EQUAL(1, mdbm_isowned(mdbm));
    for (int i = 0; i < 4; ++i) {
        CPPUNIT_ASSERT_EQUAL(1, mdbm_unlock(mdbm));
    }
}


void
LockBaseTestSuite::test_OtherAG5()
{
    string prefix = string("OtherAG5") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, versionFlag | MDBM_O_RDWR | MDBM_PARTITIONED_LOCKS,
                                        0644, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE * 8));
    CPPUNIT_ASSERT_EQUAL(0, VerifyData(mdbm, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT));
    string ky(GetDefaultKey(0));
    const datum key = {(char *)ky.c_str(), (int)ky.size()};
    datum val = mdbm_fetch(mdbm, key);
    (void)val;
    CPPUNIT_ASSERT_EQUAL(1, mdbm_plock(mdbm, &key, 0));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_isowned(mdbm));
    CPPUNIT_ASSERT_EQUAL(1, mdbm_punlock(mdbm, &key, 0));
}

void
LockBaseTestSuite::test_OtherAG6()
{
    string prefix = string("OtherAG6") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    Locker   locker;
    UnLocker unlocker;
    // Should not be owned by child - locked by parent
    CPPUNIT_ASSERT_EQUAL(0, testIsOwnedParentChild(prefix, versionFlag, locker, unlocker, versionFlag));
}

int
LockBaseTestSuite::testIsOwnedParentChild(string &prefix, int parOpenFlags, LockerFunc &locker, UnLocker &unlocker, int childOpenFlags)
{
    FD_RAII parReadFD, parSendFD;
    FD_RAII::makePipe(parReadFD, parSendFD);
    FD_RAII childReadFD, childSendFD;
    FD_RAII::makePipe(childReadFD, childSendFD);

    string dbName;
    MdbmHolder dbh(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | parOpenFlags, 0644, -1, -1, &dbName));

    int ret = -1, pid = fork();
    string msg = prefix + string(" Could not fork the child process: cannot continue");
    CPPUNIT_ASSERT_MESSAGE(msg, (pid != -1));

    if (pid) { // parent
        bool killChild = false;
        ChildMonitor_RAII child(pid, killChild);

        // wait for child to get the DB
        string resp;
        msg = "Waiting on child to obtain DB: ";
        bool selret = waitForResponse(childReadFD, resp, msg);
        killChild = (selret == false);
        CPPUNIT_ASSERT_MESSAGE(msg.insert(0, prefix), (!killChild));

        int status;
        ret = locker(dbh);
        msg = "Parent: locking failed, cannot continue";
        CPPUNIT_ASSERT_MESSAGE(msg.insert(0, prefix), (ret == 1));

        // signal child to perform isowned
        parSendFD.sendMsg(_Continue);

        waitpid(pid, &status, 0);
        ret = WEXITSTATUS(status);  // exit() status contains mdbm_isowned's return value

        unlocker(dbh);
    } else {  // child
        // child: open its own handle; read continue on pipe; lock the DB
        //        when unblock send an ACK thru the pipe; exit
        MDBM *c_dbh = EnsureNamedMdbm(dbName, childOpenFlags | MDBM_O_RDWR);

        // tell parent we got the DB
        childSendFD.sendMsg(_Ack);

        string resp;
        string errMsg = "Child: should have received continue: ";
        bool selret = waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        GetLogStream() << prefix
                     << "Child: parent locked the DB and sent us message: " << resp
                     << endl << flush;

        int isowned = mdbm_isowned(c_dbh);
        mdbm_close(c_dbh);
        exit(isowned);   // return the value of mdbm_isowned in exit's status
    }

    return ret;
}

