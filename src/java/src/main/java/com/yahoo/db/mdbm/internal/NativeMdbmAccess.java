/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm.internal;

import com.yahoo.db.mdbm.MdbmDatum;
import com.yahoo.db.mdbm.MdbmInterface;
import com.yahoo.db.mdbm.MdbmPoolInterface;
import com.yahoo.db.mdbm.ShakeCallback;
import com.yahoo.db.mdbm.exceptions.MdbmException;

class NativeMdbmAccess {

    static {
        try {
            System.loadLibrary("mdbm_java");
            System.err.println("mdbm_java loaded");
        } catch (UnsatisfiedLinkError error) {
            String javaLibraryPath = System.getProperty("java.library.path");

            System.err.println("Cannot load mdbm_java native library, -Djava.library.path="
                            + ((null == javaLibraryPath) ? "unset" : javaLibraryPath));
            throw error;
        }
    }

    // Not certain we need this. create should init it.
    // static native void mdbm_init_iterator(NativeMdbmIterator iter);

    static native NativeMdbmIterator createIter();

    static native void freeIter(long pointer);

    static native NativeMdbmImplementation mdbm_open(String file, int flags, int mode, int psize, int presize);

    static native void mdbm_close(long pointer);

    static native boolean mdbm_store_r(NativeMdbmImplementation db, MdbmDatum key, MdbmDatum val, int flags,
                    NativeMdbmIterator iter) throws MdbmException;

    static native MdbmDatum mdbm_fetch_r(NativeMdbmImplementation db, MdbmDatum key, NativeMdbmIterator iter)
                    throws MdbmException;

    static native MdbmDatum mdbm_fetch_r_locked(NativeMdbmImplementation db, MdbmDatum key, NativeMdbmIterator iter)
                    throws MdbmException;

    static native MdbmDatum mdbm_fetch_dup_r(NativeMdbmImplementation db, MdbmDatum key, NativeMdbmIterator iter)
                    throws MdbmException;

    static native void mdbm_delete(NativeMdbmImplementation db, MdbmDatum key) throws MdbmException;

    static native void mdbm_delete_r(NativeMdbmImplementation db, NativeMdbmIterator iter) throws MdbmException;

    static native void mdbm_lock(NativeMdbmImplementation db) throws MdbmException;

    static native void mdbm_unlock(NativeMdbmImplementation db) throws MdbmException;

    static native void mdbm_plock(NativeMdbmImplementation db, MdbmDatum key, int flags) throws MdbmException;

    static native int mdbm_tryplock(NativeMdbmImplementation db, MdbmDatum key, int flags);

    static native void mdbm_punlock(NativeMdbmImplementation db, MdbmDatum key, int flags) throws MdbmException;

    static native void mdbm_lock_shared(NativeMdbmImplementation db) throws MdbmException;

    static native int mdbm_trylock_shared(NativeMdbmImplementation db);

    static native void mdbm_lock_smart(NativeMdbmImplementation db, MdbmDatum key, int flags);

    static native int mdbm_trylock_smart(NativeMdbmImplementation db, MdbmDatum key, int flags);

    static native void mdbm_unlock_smart(NativeMdbmImplementation db, MdbmDatum key, int flags);

    static native void mdbm_sethash(NativeMdbmImplementation db, int hashid) throws MdbmException;

    static native void mdbm_sync(NativeMdbmImplementation db);

    static native void mdbm_limit_size_v3(NativeMdbmImplementation db, long max_page, ShakeCallback shakeCb,
                    Object param);

    static native void mdbm_compress_tree(NativeMdbmImplementation db);

    static native void mdbm_close_fd(NativeMdbmImplementation db);

    static native void mdbm_truncate(NativeMdbmImplementation db);

    static native void mdbm_replace_file(String oldfile, String newfile);

    static native void mdbm_replace_db(NativeMdbmImplementation db, String newfile);

    static native void mdbm_pre_split(NativeMdbmImplementation db, long size);

    static native void mdbm_setspillsize(NativeMdbmImplementation db, int size);

    static native void mdbm_store_str(NativeMdbmImplementation db, String key, String val, int flags)
                    throws MdbmException;

    static native String mdbm_fetch_str(NativeMdbmImplementation db, String key) throws MdbmException;

    static native void mdbm_delete_str(NativeMdbmImplementation db, String key) throws MdbmException;

    static native void mdbm_prune(NativeMdbmImplementation db, com.yahoo.db.mdbm.PruneCallback prune);

    static native void mdbm_purge(NativeMdbmImplementation db);

    static native void mdbm_fsync(NativeMdbmImplementation db);

    static native int mdbm_trylock(NativeMdbmImplementation db);

    static native boolean mdbm_islocked(NativeMdbmImplementation db);

    static native boolean mdbm_isowned(NativeMdbmImplementation db);

    static native com.yahoo.db.mdbm.MdbmKvPair mdbm_first_r(NativeMdbmImplementation db, NativeMdbmIterator iter);

    static native com.yahoo.db.mdbm.MdbmKvPair mdbm_next_r(NativeMdbmImplementation db, NativeMdbmIterator iter);

    static native MdbmDatum mdbm_firstkey_r(NativeMdbmImplementation db, NativeMdbmIterator iter);

    static native MdbmDatum mdbm_nextkey_r(NativeMdbmImplementation db, NativeMdbmIterator iter);

    static native long mdbm_get_size(NativeMdbmImplementation db);

    static native long mdbm_get_limit_size(NativeMdbmImplementation db);

    static native int mdbm_get_page_size(NativeMdbmImplementation db);

    static native int mdbm_get_hash(NativeMdbmImplementation db);

    static native int mdbm_get_alignment(NativeMdbmImplementation db);

    static native long mdbm_get_version(NativeMdbmImplementation db);

    static native NativeMdbmImplementation mdbm_dup_handle(NativeMdbmImplementation db, int flags);

    static native long mdbm_get_hash_value(MdbmDatum key, int hashFunctionCode);

    static native MdbmPoolInterface create_pool(MdbmInterface mdbm, int poolSize);

    static native void destroy_pool(long pointer);

    static native MdbmInterface acquire_handle(NativeMdbmPoolImplementation nativeMdbmPoolImplementation);

    static native void release_handle(NativeMdbmPoolImplementation nativeMdbmPoolImplementation, MdbmInterface mdbm);

}
