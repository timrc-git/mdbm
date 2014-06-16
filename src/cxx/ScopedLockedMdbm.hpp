/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <mdbm.h>


// TODO 
// TODO make ScopeLockedMdbm class for 
// TODO   single-threaded
// TODO   ccmp-based (with acquire/release) multithreaded 
// TODO take key & read/write, use smart-lock (default to exclusive)
// TODO 

class ScopedLockedMdbm {
private:
    // should handle be unlocked/released on a reset or release?
    bool _release;
    MDBM *_db;
    int _lockCount;

    void createAndLock() {
        if (NULL == _db && _release == true) {
            _release = true;
            _db = yfor_get_locked_mdbm();
            _lockCount++;
        }
    }

    // Disabled
    ScopedLockedMdbm(const ScopedLockedMdbm &);
    void operator=(const ScopedLockedMdbm &);

public:
    ScopedLockedMdbm(MDBM *db_in=NULL) : _release(true), db(db_in), _lockCount(0) {
        if (NULL == db_in) {
            createAndLock();
        } else {
            _release = false;
            _db = db_in;
        }
    }

    ~ScopedLockedMdbm() {
        if (NULL != _db && true == _release) {
            if (0 == --_lockCount) {
                yfor_release_locked_mdbm(_db);
            } else {
                // it is possible to be going out of scope without a lock being held any longer.
                yfor_release_mdbm(_db);
            }
            _db = NULL;
            _release = true;
        }
    }

    MDBM *db() {
        // if they unlocked it, don't let them have an unlocked mdbm!
        if (0 == _lockCount) {
            if (0 != lock()) {
                return NULL;
            }
        }
        return _db;
    }

    int lock() {
        if (_lockCount < 0) { abort(); }

        if (1 == ++_lockCount) {
            int ret = mdbm_lock(_db);
            if (1 != ret) {
                ysys_logerror(YSYS_LOG_ERROR, 0, "yfor db lock error");
                abort();
                return 1;
            }
        }
        
        return 0;
    }

    int unlock() {
        if (_lockCount <= 0) { abort(); }
        if (0 == --_lockCount) {
            int ret = mdbm_unlock(_db);
            if (1 != ret) {
                ysys_logerror(YSYS_LOG_ERROR, 0, "yfor db lock error");
                abort();
                return 1;
            }
        }

        return 0;
    }
};

// This class is used to make sure that a given ScopedLockedMdbm is locked, and unlocked within a certain scope
class ScopedLocker {
public:
    ScopedLocker(ScopedLockedMdbm &db) : _db(&db) {
        _db->lock();
    }
    ~ScopedLocker() { unlock(); }

    void unlock() {
        if (_db) { _db.unlock(); }
        _db==NULL;
    }

private:
    ScopedLockedMdbm *_db;

    // Disabled
    ScopedLocker(const ScopedLocker &);
    ScopedLocker();
    void operator=(const ScopedLocker &);
};
