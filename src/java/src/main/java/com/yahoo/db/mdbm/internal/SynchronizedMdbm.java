/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.HashFunctionCode;
import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmIterator;
import com.yahoo.db.mdbm.MdbmKvPair;
import com.yahoo.db.mdbm.PruneCallback;
import com.yahoo.db.mdbm.ShakeCallback;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;
import com.yahoo.db.mdbm.exceptions.MdbmLockFailedException;

/**
 * A synchronized wrapper for any mdbm object to allow people to block when they share the object between threads.
 * 
 * You probably shouldn't be using this.
 * 
 * @author areese
 * 
 */
public class SynchronizedMdbm extends ClosedBaseChecked implements MdbmInterface {
    protected final MdbmInterface proxy;

    public SynchronizedMdbm(MdbmInterface proxy) {
        if (null == proxy) {
            throw new NullPointerException();
        }
        this.proxy = proxy;
    }

    @Override
    public synchronized boolean store(MdbmDatum key, MdbmDatum value, int flags, MdbmIterator iter)
                    throws MdbmException {
        return proxy.store(key, value, flags, iter);
    }

    @Override
    public synchronized MdbmDatum fetch(MdbmDatum key, MdbmIterator iter) throws MdbmException {
        return proxy.fetch(key, iter);
    }

    @Override
    public synchronized MdbmDatum fetchDup(MdbmDatum key, MdbmIterator iter) throws MdbmException {
        return proxy.fetchDup(key, iter);
    }

    @Override
    public synchronized void delete(MdbmDatum key) throws MdbmException {
        proxy.delete(key);
    }

    @Override
    public synchronized void delete(MdbmIterator iter) throws MdbmException {
        proxy.delete(iter);
    }

    @Override
    public synchronized void lock() throws MdbmException {
        proxy.lock();
    }

    @Override
    public synchronized void unlock() throws MdbmException {
        proxy.unlock();
    }

    @Override
    public synchronized void plock(MdbmDatum key, int flags) throws MdbmException {
        proxy.plock(key, flags);
    }

    @Override
    public synchronized int tryPlockNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException {
        return proxy.tryPlockNoThrow(key, flags);
    }

    @Override
    public synchronized void tryPlock(MdbmDatum key, int flags) throws MdbmException {
        proxy.tryPlock(key, flags);
    }

    @Override
    public synchronized void punlock(MdbmDatum key, int flags) throws MdbmException {
        proxy.punlock(key, flags);
    }

    @Override
    public synchronized void sharedLock() throws MdbmException {
        proxy.sharedLock();
    }

    @Override
    public synchronized int trySharedLockNoThrow() throws MdbmInvalidStateException {
        return proxy.trySharedLockNoThrow();
    }

    @Override
    public synchronized void trySharedLock() throws MdbmException {
        proxy.trySharedLock();
    }

    @Override
    public synchronized void sharedUnlock() throws MdbmException {
        proxy.sharedUnlock();
    }

    @Override
    public synchronized void setHash(int hashid) throws MdbmException {
        proxy.setHash(hashid);
    }

    @Override
    public synchronized void setHash(HashFunctionCode hashid) throws MdbmException {
        proxy.setHash(hashid);
    }

    @Override
    public synchronized void sync() throws MdbmException {
        proxy.sync();
    }

    @Override
    public synchronized void compressTree() throws MdbmException {
        proxy.compressTree();
    }

    @Override
    public synchronized void closeFd() throws MdbmException {
        proxy.closeFd();
    }

    @Override
    public synchronized void truncate() throws MdbmException {
        proxy.truncate();
    }

    @Override
    public synchronized void replaceFile(String oldfile, String newfile) throws MdbmException {
        proxy.replaceFile(oldfile, newfile);
    }

    @Override
    public synchronized void replaceDb(String newfile) throws MdbmException {
        proxy.replaceDb(newfile);
    }

    @Override
    public synchronized void preSplit(int size) throws MdbmException {
        proxy.preSplit(size);
    }

    @Override
    public synchronized void setSpillSize(int size) throws MdbmException {
        proxy.setSpillSize(size);
    }

    @Override
    public synchronized void storeString(String key, String value, int flags) throws MdbmException {
        proxy.storeString(key, value, flags);
    }

    @Override
    public synchronized String fetchString(String key) throws MdbmException {
        return proxy.fetchString(key);
    }

    @Override
    public synchronized void deleteString(String key) throws MdbmException {
        proxy.deleteString(key);
    }

    /*
     * (non-Javadoc)
     * 
     * @see yjava.db.mdbm.MdbmInterface#prune(PruneCallback prune, Object param)
     */
    @Override
    public synchronized void prune(PruneCallback prune) throws MdbmException {
        proxy.prune(prune);
    }

    @Override
    public synchronized void purge() throws MdbmException {
        proxy.purge();
    }

    @Override
    public synchronized void fsync() throws MdbmException {
        proxy.fsync();
    }

    @Override
    public synchronized int tryLockNoThrow() throws MdbmInvalidStateException {
        return proxy.tryLockNoThrow();
    }

    @Override
    public synchronized void tryLock() throws MdbmException {
        proxy.tryLock();
    }

    @Override
    public synchronized boolean isLocked() throws MdbmException {
        return proxy.isLocked();
    }

    @Override
    public synchronized boolean isOwned() throws MdbmException {
        return proxy.isOwned();
    }

    /**
     * ITERATION
     * 
     * @throws MdbmException
     **/

    @Override
    public synchronized MdbmKvPair first(MdbmIterator iter) throws MdbmException {
        return proxy.first(iter);
    }

    @Override
    public synchronized MdbmKvPair next(MdbmIterator iter) throws MdbmException {
        return proxy.next(iter);
    }

    @Override
    public synchronized MdbmDatum firstKey(MdbmIterator iter) throws MdbmException {
        return proxy.firstKey(iter);
    }

    @Override
    public synchronized MdbmDatum nextKey(MdbmIterator iter) throws MdbmException {
        return proxy.nextKey(iter);
    }

    @Override
    public synchronized long getSize() throws MdbmException {
        return proxy.getSize();
    }

    @Override
    public synchronized long getLimitSize() throws MdbmException {
        return proxy.getLimitSize();
    }

    @Override
    public synchronized int getPageSize() throws MdbmException {
        return proxy.getPageSize();
    }

    @Override
    public synchronized HashFunctionCode getHash() throws MdbmException {
        return proxy.getHash();
    }

    @Override
    public synchronized int getHashInt() throws MdbmException {
        return proxy.getHashInt();
    }

    @Override
    public synchronized int getAlignment() throws MdbmException {
        return proxy.getAlignment();
    }

    @Override
    public synchronized String getPath() {
        return proxy.getPath();
    }

    @Override
    public synchronized void validate() throws MdbmInvalidStateException {
        proxy.validate();
    }

    @Override
    protected synchronized boolean release() {
        if (null != proxy) {
            proxy.close();
        }

        return true;
    }

    @Override
    public synchronized MdbmIterator iterator() throws MdbmException {
        return proxy.iterator();
    }

    @Override
    public synchronized void store(MdbmDatum key, MdbmDatum value, int flags) throws MdbmException {
        proxy.store(key, value, flags);
    }

    @Override
    public synchronized MdbmDatum fetch(MdbmDatum key) throws MdbmException {
        return proxy.fetch(key);
    }

    @Override
    public synchronized MdbmDatum fetchDup(MdbmDatum key) throws MdbmException {
        return proxy.fetchDup(key);
    }

    @Override
    public synchronized void lockSmart(MdbmDatum key, int flags) throws MdbmException {
        proxy.lockSmart(key, flags);
    }

    @Override
    public synchronized int tryLockSmartNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException {
        return proxy.tryLockSmartNoThrow(key, flags);
    }

    @Override
    public synchronized void tryLockSmart(MdbmDatum key, int flags) throws MdbmInvalidStateException,
                    MdbmLockFailedException {
        proxy.tryLockSmartNoThrow(key, flags);
    }

    @Override
    public synchronized void unlockSmart(MdbmDatum key, int flags) throws MdbmException {
        proxy.unlockSmart(key, flags);
    }

    @Override
    public synchronized void limitSize(int max_page, ShakeCallback shakeCb, Object param) throws MdbmException {
        proxy.limitSize(max_page, shakeCb, param);
    }

    @Override
    public synchronized long getHashValue(MdbmDatum key, int hashFunctionCode) throws MdbmException {
        return proxy.getHashValue(key, hashFunctionCode);
    }

    @Override
    public synchronized long getHashValue(MdbmDatum key, HashFunctionCode hashFunctionCode) throws MdbmException {
        return proxy.getHashValue(key, hashFunctionCode);
    }

}
