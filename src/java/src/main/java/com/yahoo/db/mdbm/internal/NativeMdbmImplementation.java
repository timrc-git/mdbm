/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.HashFunctionCode;
import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmIterator;
import com.yahoo.db.mdbm.MdbmKvPair;
import com.yahoo.db.mdbm.MdbmPoolInterface;
import com.yahoo.db.mdbm.Open;
import com.yahoo.db.mdbm.PruneCallback;
import com.yahoo.db.mdbm.ShakeCallback;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;
import com.yahoo.db.mdbm.exceptions.MdbmLockFailedException;
import com.yahoo.db.mdbm.exceptions.SharedLockViolationException;

public class NativeMdbmImplementation extends DeallocatingClosedBase implements MdbmInterface {

    private static final Deallocator.Dealloc DESTRUCTOR = new Deallocator.Dealloc() {
        @Override
        public void delete(long pointer) {
            if (pointer != 0) {
                NativeMdbmAccess.mdbm_close(pointer);
            }
        }
    };

    protected final String path;
    protected final boolean shared;

    protected NativeMdbmImplementation(final long pointer, String path, int flags) throws MdbmException {
        this(pointer, path, flags, true);
    }

    protected NativeMdbmImplementation(final long pointer, String path, int flags, boolean ownPointer)
                    throws MdbmException {
        // if we don't own the pointer, disable the deallocator.
        super(pointer, (ownPointer) ? DESTRUCTOR : null);

        if (null == path) {
            throw new NullPointerException("path was null");
        }
        // JAVAPLATF-1882 this can contend badly
        // this.path = new File(path).getCanonicalPath();
        this.path = path;

        // set the shared flag so we can stop people with wayward axes.
        this.shared = (Open.MDBM_RW_LOCKS == (flags & Open.MDBM_RW_LOCKS));
        this.setOpen();
    }

    @Override
    public void release(long pointer) {
        if (0 == pointer) {
            return;
        }

        NativeMdbmAccess.mdbm_close(pointer);
    }

    @Override
    public boolean store(MdbmDatum key, MdbmDatum value, int flags, MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_store_r(this, key, value, flags, (NativeMdbmIterator) iter);
    }

    @Override
    public void store(MdbmDatum key, MdbmDatum value, int flags) throws MdbmException {
        store(key, value, flags, null);
    }

    @Override
    public MdbmDatum fetch(MdbmDatum key, MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_fetch_r(this, key, (NativeMdbmIterator) iter);
    }

    @Override
    public MdbmDatum fetch(MdbmDatum key) throws MdbmException {
        return fetch(key, null);
    }

    @Override
    public MdbmDatum fetchDup(MdbmDatum key, MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_fetch_dup_r(this, key, (NativeMdbmIterator) iter);
    }

    @Override
    public MdbmDatum fetchDup(MdbmDatum key) throws MdbmException {
        return fetchDup(key, null);
    }

    @Override
    public void delete(MdbmDatum key) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_delete(this, key);
    }

    @Override
    public void delete(MdbmIterator iter) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_delete_r(this, (NativeMdbmIterator) iter);
    }

    @Override
    public void lock() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_lock(this);
    }

    @Override
    public void unlock() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_unlock(this);
    }

    @Override
    public void plock(MdbmDatum key, int flags) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_plock(this, key, flags);
    }

    @Override
    public int tryPlockNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException {
        validate();
        return NativeMdbmAccess.mdbm_tryplock(this, key, flags);
    }

    @Override
    public void tryPlock(MdbmDatum key, int flags) throws MdbmException {
        if (1 == tryPlockNoThrow(key, flags)) {
            // it's locked return
            return;
        }

        throw new MdbmLockFailedException("failed to plock " + key + " with flags " + flags);
    }

    @Override
    public void punlock(MdbmDatum key, int flags) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_punlock(this, key, flags);
    }

    @Override
    public void sharedLock() throws MdbmException {
        if (!shared) {
            throw new SharedLockViolationException();
        }

        validate();
        NativeMdbmAccess.mdbm_lock_shared(this);
    }

    @Override
    public int trySharedLockNoThrow() throws MdbmInvalidStateException {
        if (!shared) {
            throw new SharedLockViolationException();
        }

        validate();
        return NativeMdbmAccess.mdbm_trylock_shared(this);
    }

    @Override
    public void trySharedLock() throws MdbmException {
        if (1 == trySharedLockNoThrow()) {
            // it's locked return
            return;
        }

        throw new MdbmLockFailedException("failed to lock shared");
    }

    @Override
    public void sharedUnlock() throws MdbmException {
        if (!shared) {
            throw new SharedLockViolationException();
        }

        validate();
        NativeMdbmAccess.mdbm_unlock(this);
    }

    @Override
    public void lockSmart(MdbmDatum key, int flags) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_lock_smart(this, key, flags);
    }

    @Override
    public int tryLockSmartNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException {
        validate();
        return NativeMdbmAccess.mdbm_trylock_smart(this, key, flags);
    }

    @Override
    public void tryLockSmart(MdbmDatum key, int flags) throws MdbmInvalidStateException, MdbmLockFailedException {
        if (1 == tryLockSmartNoThrow(key, flags)) {
            // it's locked return
            return;
        }

        throw new MdbmLockFailedException("failed to lock smart " + key + " with flags " + flags);
    }

    @Override
    public void unlockSmart(MdbmDatum key, int flags) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_unlock_smart(this, key, flags);
    }

    @Override
    public void setHash(int hashid) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_sethash(this, hashid);
    }

    @Override
    public void setHash(HashFunctionCode hashid) throws MdbmException {
        setHash(hashid.v);
    }

    @Override
    public void sync() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_sync(this);
    }

    @Override
    public void limitSize(int max_page, ShakeCallback shakeCb, Object param) throws MdbmException {
        validate();

        // NativeMdbmAccess.mdbm_limit_size_v3(this, max_page, shakeCb, param);
        throw new UnsupportedOperationException("mdbm_limit_size_v3 isn't supported");
    }

    @Override
    public void compressTree() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_compress_tree(this);
    }

    @Override
    public void closeFd() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_close_fd(this);
    }

    @Override
    public void truncate() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_truncate(this);
    }

    @Override
    public void replaceFile(String oldfile, String newfile) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_replace_file(oldfile, newfile);
    }

    @Override
    public void replaceDb(String newfile) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_replace_db(this, newfile);
    }

    @Override
    public void preSplit(int size) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_pre_split(this, size);
    }

    @Override
    public void setSpillSize(int size) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_setspillsize(this, size);
    }

    @Override
    public void storeString(String key, String value, int flags) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_store_str(this, key, value, flags);
    }

    @Override
    public String fetchString(String key) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_fetch_str(this, key);
    }

    @Override
    public void deleteString(String key) throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_delete_str(this, key);
    }

    @Override
    public void prune(PruneCallback prune) throws MdbmException {
        validate();
        // NativeMdbmAccess.mdbm_prune(this, prune);
        throw new UnsupportedOperationException("mdbm_prune isn't supported");
    }

    @Override
    public void purge() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_purge(this);
    }

    @Override
    public void fsync() throws MdbmException {
        validate();
        NativeMdbmAccess.mdbm_fsync(this);
    }

    @Override
    public int tryLockNoThrow() throws MdbmInvalidStateException {
        validate();
        return NativeMdbmAccess.mdbm_trylock(this);
    }

    @Override
    public void tryLock() throws MdbmException {
        if (1 == tryLockNoThrow()) {
            // it's locked return
            return;
        }

        throw new MdbmLockFailedException("failed to lock ");
    }

    @Override
    public boolean isLocked() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_islocked(this);
    }

    @Override
    public boolean isOwned() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_isowned(this);
    }

    @Override
    public MdbmKvPair first(MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_first_r(this, (NativeMdbmIterator) iter);
    }

    @Override
    public MdbmKvPair next(MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_next_r(this, (NativeMdbmIterator) iter);
    }

    @Override
    public MdbmDatum firstKey(MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_firstkey_r(this, (NativeMdbmIterator) iter);
    }

    @Override
    public MdbmDatum nextKey(MdbmIterator iter) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_nextkey_r(this, (NativeMdbmIterator) iter);
    }

    @Override
    public long getSize() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_get_size(this);
    }

    @Override
    public long getLimitSize() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_get_limit_size(this);
    }

    @Override
    public int getPageSize() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_get_page_size(this);
    }

    @Override
    public int getHashInt() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_get_hash(this);
    }

    @Override
    public HashFunctionCode getHash() throws MdbmException {
        return HashFunctionCode.valueOf(getHashInt());
    }

    @Override
    public int getAlignment() throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_get_alignment(this);
    }

    @Override
    public long getHashValue(MdbmDatum key, int hashFunctionCode) throws MdbmException {
        validate();
        return NativeMdbmAccess.mdbm_get_hash_value(key, hashFunctionCode);
    }

    @Override
    public long getHashValue(MdbmDatum key, HashFunctionCode hashFunctionCode) throws MdbmException {
        return getHashValue(key, hashFunctionCode.v);
    }

    @Override
    public MdbmIterator iterator() throws MdbmException {
        validate();
        return NativeMdbmAccess.createIter();
    }

    /** Non-wrapper functions **/

    long getPointer() {
        return pointer;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public void validate() throws MdbmInvalidStateException {
        boolean closed = isClosed();
        if (0 == pointer || closed) {
            throw new MdbmInvalidStateException("Mdbm is closed or not open: " + pointer + " closed: " + closed);
        }
    }

    public static NativeMdbmImplementation mdbm_open(String file, int flags, int mode, int psize, int presize) {
        return NativeMdbmAccess.mdbm_open(file, flags, mode, psize, presize);
    }

    public static long mdbm_get_hash_value(MdbmDatum key, int hashFunctionCode) {
        return NativeMdbmAccess.mdbm_get_hash_value(key, hashFunctionCode);
    }

    public static MdbmPoolInterface create_pool(MdbmInterface mdbm, int poolSize) {
        return NativeMdbmAccess.create_pool(mdbm, poolSize);
    }
}
