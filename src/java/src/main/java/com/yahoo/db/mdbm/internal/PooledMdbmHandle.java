/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.HashFunctionCode;
import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmIterator;
import com.yahoo.db.mdbm.MdbmKvPair;
import com.yahoo.db.mdbm.MdbmPoolInterface;
import com.yahoo.db.mdbm.PruneCallback;
import com.yahoo.db.mdbm.ShakeCallback;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;
import com.yahoo.db.mdbm.exceptions.MdbmLockFailedException;
import com.yahoo.db.mdbm.exceptions.UncloseableMdbmException;

public class PooledMdbmHandle extends ClosedBaseChecked implements MdbmInterface {
    protected final UncloseableMdbm proxy;
    protected final MdbmPoolInterface pool;
    protected boolean checkedOut;

    PooledMdbmHandle(MdbmPoolInterface pool, UncloseableMdbm proxy) {
        // we don't want isClosed throwing and giving us greif!
        super(false);

        if (null == proxy || null == pool) {
            throw new NullPointerException();
        }

        this.pool = pool;
        this.proxy = proxy;
        this.checkedOut = false;
    }

    @Override
    public boolean store(MdbmDatum key, MdbmDatum value, int flags, MdbmIterator iter) throws MdbmException {
        return proxy.store(key, value, flags, iter);
    }

    @Override
    public MdbmDatum fetch(MdbmDatum key, MdbmIterator iter) throws MdbmException {
        return proxy.fetch(key, iter);
    }

    @Override
    public MdbmDatum fetchDup(MdbmDatum key, MdbmIterator iter) throws MdbmException {
        return proxy.fetchDup(key, iter);
    }

    @Override
    public void delete(MdbmDatum key) throws MdbmException {
        proxy.delete(key);
    }

    @Override
    public void delete(MdbmIterator iter) throws MdbmException {
        proxy.delete(iter);
    }

    @Override
    public void lock() throws MdbmException {
        proxy.lock();
    }

    @Override
    public void unlock() throws MdbmException {
        proxy.unlock();
    }

    @Override
    public void plock(MdbmDatum key, int flags) throws MdbmException {
        proxy.plock(key, flags);
    }

    @Override
    public int tryPlockNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException {
        return proxy.tryPlockNoThrow(key, flags);
    }

    @Override
    public void tryPlock(MdbmDatum key, int flags) throws MdbmException {
        proxy.tryPlock(key, flags);
    }

    @Override
    public void punlock(MdbmDatum key, int flags) throws MdbmException {
        proxy.punlock(key, flags);
    }

    @Override
    public void sharedLock() throws MdbmException {
        proxy.sharedLock();
    }

    @Override
    public int trySharedLockNoThrow() throws MdbmInvalidStateException {
        return proxy.trySharedLockNoThrow();
    }

    @Override
    public void trySharedLock() throws MdbmException {
        proxy.trySharedLock();
    }

    @Override
    public void sharedUnlock() throws MdbmException {
        proxy.sharedUnlock();
    }

    @Override
    public void setHash(int hashid) throws MdbmException {
        proxy.setHash(hashid);
    }

    @Override
    public void setHash(HashFunctionCode hashid) throws MdbmException {
        proxy.setHash(hashid);
    }

    @Override
    public void sync() throws MdbmException {
        proxy.sync();
    }

    @Override
    public void compressTree() throws MdbmException {
        proxy.compressTree();
    }

    @Override
    public void closeFd() throws MdbmException {
        throw new UncloseableMdbmException();
    }

    @Override
    public void truncate() throws MdbmException {
        proxy.truncate();
    }

    @Override
    public void replaceFile(String oldfile, String newfile) throws MdbmException {
        proxy.replaceFile(oldfile, newfile);
    }

    @Override
    public void replaceDb(String newfile) throws MdbmException {
        proxy.replaceDb(newfile);
    }

    @Override
    public void preSplit(int size) throws MdbmException {
        proxy.preSplit(size);
    }

    @Override
    public void setSpillSize(int size) throws MdbmException {
        proxy.setSpillSize(size);
    }

    @Override
    public void storeString(String key, String value, int flags) throws MdbmException {
        proxy.storeString(key, value, flags);
    }

    @Override
    public String fetchString(String key) throws MdbmException {
        return proxy.fetchString(key);
    }

    @Override
    public void deleteString(String key) throws MdbmException {
        proxy.deleteString(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see yjava.db.mdbm.MdbmInterface#prune(PruneCallback prune, Object param)
     */
    @Override
    public void prune(PruneCallback prune) throws MdbmException {
        proxy.prune(prune);
    }

    @Override
    public void purge() throws MdbmException {
        proxy.purge();
    }

    @Override
    public void fsync() throws MdbmException {
        proxy.fsync();
    }

    @Override
    public int tryLockNoThrow() throws MdbmInvalidStateException {
        return proxy.tryLockNoThrow();
    }

    @Override
    public void tryLock() throws MdbmException {
        proxy.tryLock();
    }

    @Override
    public boolean isLocked() throws MdbmException {
        return proxy.isLocked();
    }

    @Override
    public boolean isOwned() throws MdbmException {
        return proxy.isOwned();
    }

    /**
     * ITERATION
     * 
     * @throws MdbmException
     **/

    @Override
    public MdbmKvPair first(MdbmIterator iter) throws MdbmException {
        return proxy.first(iter);
    }

    @Override
    public MdbmKvPair next(MdbmIterator iter) throws MdbmException {
        return proxy.next(iter);
    }

    @Override
    public MdbmDatum firstKey(MdbmIterator iter) throws MdbmException {
        return proxy.firstKey(iter);
    }

    @Override
    public MdbmDatum nextKey(MdbmIterator iter) throws MdbmException {
        return proxy.nextKey(iter);
    }

    @Override
    public long getSize() throws MdbmException {
        return proxy.getSize();
    }

    @Override
    public long getLimitSize() throws MdbmException {
        return proxy.getLimitSize();
    }

    @Override
    public int getPageSize() throws MdbmException {
        return proxy.getPageSize();
    }

    @Override
    public HashFunctionCode getHash() throws MdbmException {
        return proxy.getHash();
    }

    @Override
    public int getHashInt() throws MdbmException {
        return proxy.getHashInt();
    }

    @Override
    public int getAlignment() throws MdbmException {
        return proxy.getAlignment();
    }

    @Override
    public String getPath() {
        return proxy.getPath();
    }

    @Override
    public void validate() throws MdbmInvalidStateException {
        proxy.validate();
    }

    @Override
    protected boolean release() {
        try {
            // we need to return this, and not the proxy or it won't work.
            pool.returnMdbmHandle(this);
        } catch (MdbmInvalidStateException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public MdbmIterator iterator() throws MdbmException {
        return proxy.iterator();
    }

    @Override
    public void store(MdbmDatum key, MdbmDatum value, int flags) throws MdbmException {
        proxy.store(key, value, flags);
    }

    @Override
    public MdbmDatum fetch(MdbmDatum key) throws MdbmException {
        return proxy.fetch(key);
    }

    @Override
    public MdbmDatum fetchDup(MdbmDatum key) throws MdbmException {
        return proxy.fetchDup(key);
    }

    @Override
    public void lockSmart(MdbmDatum key, int flags) throws MdbmException {
        proxy.lockSmart(key, flags);
    }

    @Override
    public int tryLockSmartNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException {
        return proxy.tryLockSmartNoThrow(key, flags);
    }

    @Override
    public void tryLockSmart(MdbmDatum key, int flags) throws MdbmInvalidStateException, MdbmLockFailedException {
        proxy.tryLockSmartNoThrow(key, flags);
    }

    @Override
    public void unlockSmart(MdbmDatum key, int flags) throws MdbmException {
        proxy.unlockSmart(key, flags);
    }

    @Override
    public void limitSize(int max_page, ShakeCallback shakeCb, Object param) throws MdbmException {
        proxy.limitSize(max_page, shakeCb, param);
    }

    @Override
    public long getHashValue(MdbmDatum key, int hashFunctionCode) throws MdbmException {
        return proxy.getHashValue(key, hashFunctionCode);
    }

    @Override
    public long getHashValue(MdbmDatum key, HashFunctionCode hashFunctionCode) throws MdbmException {
        return proxy.getHashValue(key, hashFunctionCode);
    }

    long getPointer() {
        return proxy.getPointer();
    }

    public boolean isCheckedOut() {
        return checkedOut;
    }

    public void setCheckedOut(boolean b) {
        this.checkedOut = b;
    }
}
