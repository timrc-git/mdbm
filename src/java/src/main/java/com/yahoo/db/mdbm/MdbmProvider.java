/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import com.yahoo.db.mdbm.exceptions.MdbmCreatePoolException;
import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.internal.NativeMdbmImplementation;
import com.yahoo.db.mdbm.internal.SynchronizedMdbm;

/**
 * Returns a registered implementation of the {@link MdbmInterface} interface.
 * 
 * The best way to use this is to call com.yahoo.db.mdbm.MdbmProvider.openPool(String, int, int, int, int, int) This
 * will open a pool of mdbm handles, and you can acquire and release a handle from the pool for use within a single
 * thread. Mdbm handles are not thread safe and must not be shared.
 * 
 * The mdbm handle pool is blocking and will fail to close if you haven't returned the handles to the pool. You can
 * either close the handle or return it to the pool, it will do the right thing underneath.
 * 
 * <p>
 * 
 * @author areese
 * 
 */
public final class MdbmProvider {

    /**
     * Open an mdbm and create a pool of duplicated handles. A handle cannot be shared between threads, but a pool can
     * be. get a handle for operations, and return it when done.
     * 
     * @see {yjava.db.mdbm.MdbmProvider.open(String, int, int, int, int)} Creates an mdbm pool of poolSize
     * 
     * 
     * @param file name of the backing file for the database. <br>
     * @param flags specifies the open-mode for the file, usually either (MDBM_O_RDWR|MDBM_O_CREAT) or (MDBM_O_RDONLY).
     *        Flag MDBM_LARGE_OBJECTS may be used to enable large object support. Large object support can only be
     *        enabled when the database is first created. Subsequent mdbm_open calls will ignore the flag. (See \ref
     *        mdbm_xobj) Flag MDBM_PARTITIONED_LOCKS may be used to enable page locking (See \ref mdbm_plock). <br>
     * @param mode used to set the file permissions if the file needs to be created. <br>
     * @param psize specifies the page size for the database and is set when the database is created. The minimum
     *        pagesize is 512 and the maximum is 64K (default, if 0 is specified, is 4096). <br>
     * @param presize specifies the initial size for the database. The database will dynamically grow as records are
     *        added, but specifying an initial size may improve efficiency. If this is not a multiple of \em psize, it
     *        will be increased to the next \em psize multiple. <br>
     *        \return Upon success, the database handle (which must be passed to all other mdbm functions) is returned.
     *        Otherwise, NULL is returned and errno indicates the type of error that occurred.
     * 
     *        Values for \em flags param include:
     * 
     *        <ul>
     *        <li>MDBM_O_RDONLY open for reading only</li>
     *        <li>MDBM_O_RDWR open for reading and writing</li>
     *        <li>MDBM_O_WRONLY same as RDWR (deprecated)</li>
     *        <li>MDBM_O_CREAT create file if it does not exist</li>
     *        <li>MDBM_O_TRUNC truncate size to 0</li>
     *        <li>MDBM_O_ASYNC enable asynchronous sync'ing by the kernel syncer process.</li>
     *        <li>MDBM_O_FSYNC sync mdbm upon mdbm_close()</li>
     *        <li>MDBM_O_DIRECT Use O_DIRECT when accessing backing-store files</li>
     *        <li>MDBM_NO_DIRTY do not not track clean/dirty status</li>
     *        <li>MDBM_OPEN_WINDOWED_MIN use windowing (minimum footprint)</li>
     *        <li>MDBM_OPEN_WINDOWED use windowing to access db, only available with MDBM_O_RDWR</li>
     *        <li>MDBM_PROTECT protect database except when locked</li>
     *        <li>MDBM_DBSIZE_MB dbsize is specific in MB</li>
     *        <li>MDBM_LARGE_OBJECTS support large objects - obsolete</li>
     *        <li>MDBM_PARTITIONED_LOCKS partitioned locks</li>
     *        <li>MDBM_RW_LOCKS read-write locks</li>
     *        <li>MDBM_CREATE_V3 create a V3 db</li>
     *        <li>MDBM_HEADER_ONLY map header only (internal use)</li>
     *        <li>MDBM_OPEN_NOLOCK don't lock during open</li>
     *        </ul>
     *        For more information about the effects of MDBM_O_ASYNC and MDBM_O_FSYNC, see \ref mdbm_faq_async_fsync
     *        "MDBM FAQ: ASYNC and FSYNC"
     * @param poolSize # of mdbm handles to duplicate * @return
     * @throws MdbmException on failure.
     */
    @SuppressWarnings("resource")
    public static MdbmPoolInterface openPool(String file, int flags, int mode, int psize, int presize, int poolSize)
                    throws MdbmException {
        if (poolSize <= 0) {
            throw new MdbmCreatePoolException("Poolsize must be > 0, was " + poolSize);
        }

        MdbmInterface mdbm = open(file, flags, mode, psize, presize);
        if (null == mdbm) {
            return null;
        }

        return NativeMdbmImplementation.create_pool(mdbm, poolSize);
    }

    /**
     * create and/or open a database <br>
     * 
     * @param file name of the backing file for the database. <br>
     * @param flags specifies the open-mode for the file, usually either (MDBM_O_RDWR|MDBM_O_CREAT) or (MDBM_O_RDONLY).
     *        Flag MDBM_LARGE_OBJECTS may be used to enable large object support. Large object support can only be
     *        enabled when the database is first created. Subsequent mdbm_open calls will ignore the flag. (See \ref
     *        mdbm_xobj) Flag MDBM_PARTITIONED_LOCKS may be used to enable page locking (See \ref mdbm_plock). <br>
     * @param mode used to set the file permissions if the file needs to be created. <br>
     * @param psize specifies the page size for the database and is set when the database is created. The minimum
     *        pagesize is 512 and the maximum is 64K (default, if 0 is specified, is 4096). <br>
     * @param presize specifies the initial size for the database. The database will dynamically grow as records are
     *        added, but specifying an initial size may improve efficiency. If this is not a multiple of \em psize, it
     *        will be increased to the next \em psize multiple. <br>
     *        \return Upon success, the database handle (which must be passed to all other mdbm functions) is returned.
     *        Otherwise, NULL is returned and errno indicates the type of error that occurred.
     * 
     *        Values for \em flags param include:
     * 
     *        <ul>
     *        <li>MDBM_O_RDONLY open for reading only</li>
     *        <li>MDBM_O_RDWR open for reading and writing</li>
     *        <li>MDBM_O_WRONLY same as RDWR (deprecated)</li>
     *        <li>MDBM_O_CREAT create file if it does not exist</li>
     *        <li>MDBM_O_TRUNC truncate size to 0</li>
     *        <li>MDBM_O_ASYNC enable asynchronous sync'ing by the kernel syncer process.</li>
     *        <li>MDBM_O_FSYNC sync mdbm upon mdbm_close()</li>
     *        <li>MDBM_O_DIRECT Use O_DIRECT when accessing backing-store files</li>
     *        <li>MDBM_NO_DIRTY do not not track clean/dirty status</li>
     *        <li>MDBM_OPEN_WINDOWED_MIN use windowing (minimum footprint)</li>
     *        <li>MDBM_OPEN_WINDOWED use windowing to access db, only available with MDBM_O_RDWR</li>
     *        <li>MDBM_PROTECT protect database except when locked</li>
     *        <li>MDBM_DBSIZE_MB dbsize is specific in MB</li>
     *        <li>MDBM_LARGE_OBJECTS support large objects - obsolete</li>
     *        <li>MDBM_PARTITIONED_LOCKS partitioned locks</li>
     *        <li>MDBM_RW_LOCKS read-write locks</li>
     *        <li>MDBM_CREATE_V3 create a V3 db</li>
     *        <li>MDBM_HEADER_ONLY map header only (internal use)</li>
     *        <li>MDBM_OPEN_NOLOCK don't lock during open</li>
     *        </ul>
     *        For more information about the effects of MDBM_O_ASYNC and MDBM_O_FSYNC, see \ref mdbm_faq_async_fsync
     *        "MDBM FAQ: ASYNC and FSYNC"
     */
    public static MdbmInterface open(String file, int flags, int mode, int psize, int presize) throws MdbmException {
        try {
            /**
             * We need to be careful here, if a use is opening a v3, then we expect them to not share MdbmInterfaces
             * between threads. If they are opening a v2, then we must synchronize all handles to it.
             */
            NativeMdbmImplementation mdbm = NativeMdbmImplementation.mdbm_open(file, flags, mode, psize, presize);

            if (null == mdbm) {
                throw new MdbmException("Unable to open mdbm " + file + " flags: " + flags + " mode: " + mode
                                + " psize: " + psize + " presize " + presize);
            }

            return mdbm;

        } catch (MdbmException e) {
            e.setPath(file);
            e.setInfo(String.format("flags: 0x%1$X mode: 0%2$o (octal) psize: 0x%3$X presize: 0x%4$X",
                            Integer.valueOf(flags), Integer.valueOf(mode), Integer.valueOf(psize),
                            Integer.valueOf(presize)));
            throw e;
        }
    }

    /**
     * @see {yjava.db.mdbm.MdbmProvider.open(String, int, int, int, int)}
     * @param file
     * @param flags
     * @param mode
     * @param psize
     * @param presize
     * @return
     * @throws MdbmException
     */
    public static MdbmInterface openSynchronized(String file, int flags, int mode, int psize, int presize)
                    throws MdbmException {
        return new SynchronizedMdbm(MdbmProvider.open(file, flags, mode, psize, presize));
    }

    /**
     * This function will return a synchronized mdbm from a handle. This uses Java synchronized to perform locking. You
     * must discard oldInterface after calling this.
     * 
     * @param oldInterface
     * @return
     */
    public static MdbmInterface createSynchronizedMdbm(MdbmInterface oldInterface) {
        return new SynchronizedMdbm(oldInterface);
    }

    public static long getHashValue(MdbmDatum key, HashFunctionCode hashFunctionCode) throws MdbmException {
        return getHashValue(key, hashFunctionCode.v);

    }

    public static long getHashValue(MdbmDatum key, int hashFunctionCode) throws MdbmException {
        return NativeMdbmImplementation.mdbm_get_hash_value(key, hashFunctionCode);
    }

}
