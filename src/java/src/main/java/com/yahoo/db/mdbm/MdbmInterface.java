/* Copyright 2014 Yahoo! Inc. */
/* Licensed under the terms of the 3-Clause BSD license. See LICENSE file in the project root for details. */
package com.yahoo.db.mdbm;

import java.io.Closeable;

import com.yahoo.db.mdbm.exceptions.MdbmException;
import com.yahoo.db.mdbm.exceptions.MdbmInvalidStateException;
import com.yahoo.db.mdbm.exceptions.MdbmLockFailedException;

/**
 * 
 * MDBM wrapper.
 * 
 * MDBM instances must not be shared between threads.
 * 
 * Opening multiple instances of the same mdbm is a bad idea, consider opening a correctly sized pool instead using
 * 
 * You cannot use Read/Write locks to protect an Mdbm.
 * 
 * For more details: <a href= "http://docs.corp.yahoo.com/svn/yahoo/stack/core/mdbm/docs/mdbm_faq.html#seq_faq_MROW"
 * >mdbm_locking_1</a> <a href=
 * "http://docs.corp.yahoo.com/svn/yahoo/stack/core/mdbm/docs/mdbm_faq.html#seq_faq_readWriteLocks" >mdbm_locking_2</a>
 * 
 * Close must be called when done using an MDBM
 * 
 * @author areese
 * 
 */
public interface MdbmInterface extends Closeable {
    public static final int MDBM_KEYLEN_MAX = (1 << 15);
    public static final int MDBM_VALLEN_MAX = (1 << 24);

    public static final int MDBM_LOC_NORMAL = 0;
    public static final int MDBM_LOC_ARENA = 1;

    /**
     * 
     */
    // TODO: mdbmv2 interface.
    // public static final int MDBM_DEMAND_PAGING = 0x04000000;/* (v2 only) */
    // public static final int MDBM_DBSIZE_MB_OLD = 0x04000000;/* (don't use --
    // conflicts with above) */

    /**
     * close the database. <br>
     * <br>
     * mdbm_close closes the database specified by the <em>db</em> arg. The in-memory pages are not flushed to disk by
     * close. They will be written to disk over time as they are paged out, but an explicit mdbm_sync() call is
     * necessary before closing if on-disk consistency is required.
     */
    @Override
    void close();

    /**
     * Stores the record specified by the <em>key</em> and <em>val</em> params. <br>
     * 
     * @param key
     * @see datum to be used as the index key
     * @param value
     * @see datum containing the value to be stored
     * @param flags indicates the type of store operation to be performed. <br>
     *        Values for <em>flags</em> param:
     *        <ul>
     *        <li>MDBM_INSERT operation will fail if a record with the same key</li>
     *        already exists in the database.
     *        <li>MDBM_REPLACE a record with the same key will be replaced. If the key does not exist, the record will
     *        be created.</li>
     *        <li>MDBM_INSERT_DUP allows multiple records with the same key to be inserted. Fetching a record with the
     *        key will return only one of the duplicate records, and which record is returned is not defined.</li>
     *        <li>MDBM_MODIFY Store only if matching entry already exists.</li>
     *        <li>MDBM_RESERVE reserve space; value not copied (mdbm_store_r only)</li>
     *        </ul>
     * @param iter MDBM Iterator (@see MdbmIterator)
     * @return
     * 
     * @throws MdbmException If MDBM_INSERT is specified and a record with the same key already exists, 1 is returned.
     *         Otherwise, -1 is returned, and errno is set.
     * 
     *         Insertion with flag <em>MDBM_MODIFY</em> set will fail if the key does not already exist.
     * 
     *         Insertion with flag <em>MDBM_RESERVE</em> set will allocate space within the mdbm for the object; but
     *         will not copy it. The caller is responsible for copying the value into the indicated position.
     * 
     *         Quick option summary: <code>
     *  MDBM_INSERT           add if missing,    fail if present
     *  MDBM_REPLACE          add if missing, replace if present
     *  MDBM_MODIFY          fail if missing, replace if present
     * </code>
     * 
     *         NOTE: Ensure that the key and value datum pointers reference storage that is external to the db and not
     *         the result of, for example, an mdbm_fetch or mdbm_first/next. A store operation can result in various db
     *         internal modifications that invalidate previously returned key and value pointers.
     * 
     * @return true on SUCCESS, false Flag MDBM_INSERT was specified, and the key already exists
     * @throws MdbmException on error.
     */
    boolean store(MdbmDatum key, MdbmDatum value, int flags, MdbmIterator iter) throws MdbmException;

    /**
     * Stores the record specified by the <em>key</em> and <em>val</em> params. <br>
     * 
     * @param key
     * @see datum to be used as the index key
     * @param value
     * @see datum containing the value to be stored
     * @param flags indicates the type of store operation to be performed. <br>
     *        Values for <em>flags</em> param:
     *        <ul>
     *        <li>MDBM_INSERT operation will fail if a record with the same key</li>
     *        already exists in the database.
     *        <li>MDBM_REPLACE a record with the same key will be replaced. If the key does not exist, the record will
     *        be created.</li>
     *        <li>MDBM_INSERT_DUP allows multiple records with the same key to be inserted. Fetching a record with the
     *        key will return only one of the duplicate records, and which record is returned is not defined.</li>
     *        <li>MDBM_MODIFY Store only if matching entry already exists.</li>
     *        <li>MDBM_RESERVE reserve space; value not copied (mdbm_store_r only)</li>
     *        </ul>
     * @throws MdbmException Upon success, 0 is returned. If MDBM_INSERT is specified and a record with the same key
     *         already exists, 1 is returned. Otherwise, -1 is returned, and errno is set.
     * 
     *         Insertion with flag <em>MDBM_MODIFY</em> set will fail if the key does not already exist.
     * 
     *         Insertion with flag <em>MDBM_RESERVE</em> set will allocate space within the mdbm for the object; but
     *         will not copy it. The caller is responsible for copying the value into the indicated position.
     * 
     *         Quick option summary: <code>
     *  MDBM_INSERT           add if missing,    fail if present
     *  MDBM_REPLACE          add if missing, replace if present
     *  MDBM_MODIFY          fail if missing, replace if present
     * </code>
     * 
     *         NOTE: Ensure that the key and value datum pointers reference storage that is external to the db and not
     *         the result of, for example, an mdbm_fetch or mdbm_first/next. A store operation can result in various db
     *         internal modifications that invalidate previously returned key and value pointers.
     */
    void store(MdbmDatum key, MdbmDatum value, int flags) throws MdbmException;

    /**
     * Fetches the record specified by the <em>key</em> argument. If such a record exists in the database, the size and
     * location are stored in the datum pointed to by val. If no matching record exists, a MdbmNoEntryException is thrown.
     * <br>
     * <b> Unlike the underlying mdbm_fetch function, this does not allow in place updates of the data.</b>
     * 
     * @param key ptr to @see MdbmDatum used as the index key
     * @param iter MDBM Iterator (see @see MdbmIterator)
     * @return MdbmDatum stored value.
     * @throws MdbmNoEntryException no matching recoed exists.
     * @throws MdbmException upon error.
     */
    MdbmDatum fetch(MdbmDatum key, MdbmIterator iter) throws MdbmException;

    /**
     * fetches the record specified by the <em>key</em> argument. If such a record exists in the database, the size and
     * location are stored in the datum pointed to by val. If no matching record exists, a MdbmNoEntryException is thrown.
     * <br>
     * <b> Unlike the underlying mdbm_fetch function, this does not allow in place updates of the data.</b>
     * 
     * @param key ptr to @see datum used as the index key
     * @return MdbmDatum stored value.
     * @throws MdbmNoEntryException no matching recoed exists.
     * @throws MdbmException upon error.
     */
    MdbmDatum fetch(MdbmDatum key) throws MdbmException;

    /**
     * Fetches the next value for a key inserted via mdbm_store_r with the MDBM_INSERT_DUP flag set. The order of values
     * returned by iterating via this function is not guaranteed to be the same order as the values were inserted.
     * 
     * As with any db iteration, record insertion and deletion during iteration may cause the iteration to skip and/or
     * repeat records.
     * 
     * Calling this function with an iterator initialized via MDBM_ITER_INIT() will cause this function to return the
     * first value for the given key.
     * 
     * See example 7: @see ex7 for an example.
     * 
     * @param key ptr to @see datum used as the index key <br>
     * @param iter MDBM Iterator (see @see MdbmIterator)
     * @return null when not found
     * @throws MdbmException upon error.
     */
    MdbmDatum fetchDup(MdbmDatum key, MdbmIterator iter) throws MdbmException;

    /**
     * Fetches the next value for a key inserted via mdbm_store_r with the MDBM_INSERT_DUP flag set. The order of values
     * returned by iterating via this function is not guaranteed to be the same order as the values were inserted.
     * 
     * As with any db iteration, record insertion and deletion during iteration may cause the iteration to skip and/or
     * repeat records.
     * 
     * Calling this function with an iterator initialized via MDBM_ITER_INIT() will cause this function to return the
     * first value for the given key.
     * 
     * See example 7: @see ex7 for an example.
     * 
     * @param key ptr to @see datum used as the index key <br>
     * @return null when not found
     * @throws MdbmException upon error.
     */
    MdbmDatum fetchDup(MdbmDatum key) throws MdbmException;

    /**
     * deletes the record currently addressed by the <em>iter</em> argument. After deletion, the key and/or value
     * returned by the iterating function is no longer valid. Calling @see mdbm_next_r on the iterator will return the
     * key/value for the entry following the entry that was deleted.
     * 
     * @param key key for the item to be deleted <br>
     * @throws MdbmException upon error.
     * 
     *         (see @see MdbmIterator)
     */
    void delete(MdbmDatum key) throws MdbmException;

    /**
     * deletes the record currently addressed by the <em>iter</em> argument. After deletion, the key and/or value
     * returned by the iterating function is no longer valid. Calling @see mdbm_next_r on the iterator will return the
     * key/value for the entry following the entry that was deleted.
     * 
     * @param iter MDBM Iterator pointing to item to be deleted <br>
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     * 
     *         (see @see MdbmIterator)
     */
    void delete(MdbmIterator iter) throws MdbmException;

    /**
     * locks the database for exclusive access by the caller.
     * 
     * The lock is nestable, so a caller already holding the lock may call mdbm_lock again as long as an equal number of
     * calls to mdbm_unlock are made to release the lock. See @see mdbm_lock for usage.
     * 
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void lock() throws MdbmException;

    /**
     * unlocks the database, releasing exclusive access by the caller.
     * 
     * If the caller has called mdbm_lock multiple times in a row, an equal number of unlock calls are required. See @see
     * mdbm_lock for usage.
     * 
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void unlock() throws MdbmException;

    /**
     * locks a specific page in the database for exclusive access by the caller.
     * 
     * The lock is nestable, so a caller already holding the lock may call mdbm_plock again as long as an equal number
     * of calls to mdbm_punlock are made to release the lock. See @see mdbm_plock for usage.
     * 
     * @param key Key to be hashed to determine page to lock
     * @param flags Ignored.
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void plock(MdbmDatum key, int flags) throws MdbmException;

    /**
     * locks a specific page in the database for exclusive access by the caller.
     * 
     * The lock is nestable, so a caller already holding the lock may call mdbm_plock again as long as an equal number
     * of calls to mdbm_punlock are made to release the lock. See @see mdbm_plock for usage.
     * 
     * @param key Key to be hashed to determine page to lock
     * @param flags Ignored.
     * @throws MdbmInvalidStateException
     */
    int tryPlockNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException;

    /**
     * locks a specific page in the database for exclusive access by the caller.
     * 
     * The lock is nestable, so a caller already holding the lock may call mdbm_plock again as long as an equal number
     * of calls to mdbm_punlock are made to release the lock. See @see mdbm_plock for usage.
     * 
     * @param key Key to be hashed to determine page to lock
     * @param flags Ignored.
     * @throws MdbmInvalidStateException
     */
    void tryPlock(MdbmDatum key, int flags) throws MdbmException;

    /**
     * unlocks the database, releasing exclusive access by the caller.
     * 
     * If the caller has called mdbm_plock multiple times in a row, an equal number of unlock calls are required. See @see
     * mdbm_plock for usage.
     * 
     * @param key Ignored.
     * @param flags Ignored.
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void punlock(MdbmDatum key, int flags) throws MdbmException;

    /**
     * locks a specific page in the database for exclusive access by the caller.
     * 
     * The lock is nestable, so a caller already holding the lock may call mdbm_plock again as long as an equal number
     * of calls to mdbm_punlock are made to release the lock. See \ref mdbm_plock for usage.
     * 
     * \param db Database to be locked. \param key Key to be hashed to determine page to lock \param flags Ignored.
     * \return Upon success, 1 is returned. Otherwise, -1 is returned and errno is set. Typically, an error could only
     * occur if the database has been corrupted.
     */
    void sharedLock() throws MdbmException;

    /**
     * @see MdbmInterface#sharedLock() does not throw
     * @return 1 upon success.
     * @throws MdbmInvalidStateException
     */
    int trySharedLockNoThrow() throws MdbmInvalidStateException;

    /**
     * @see MdbmInterface#sharedLock() does not throw
     * @return 1 upon success.
     * @throws MdbmLockFailedException if locking failed.
     */
    void trySharedLock() throws MdbmException;

    /**
     */
    void sharedUnlock() throws MdbmException;

    public void lockSmart(MdbmDatum key, int flags) throws MdbmException;

    public int tryLockSmartNoThrow(MdbmDatum key, int flags) throws MdbmInvalidStateException;

    public void tryLockSmart(MdbmDatum key, int flags) throws MdbmInvalidStateException, MdbmLockFailedException;

    public void unlockSmart(MdbmDatum key, int flags) throws MdbmException;

    /**
     * Set the hashing function for a given MDBM. The hash function must be set before storing anything to the db (this
     * is not enforced, but entries stored before the hash change will become inaccessible if the hash function is
     * changed). <br>
     * 
     * @param hashid Numeric identifier for new hash function. <br>
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     * 
     *         Available Hash IDs are:
     * 
     *         <code> 
     *     MDBM_HASH_CRC32        Table based 32bit crc
     *     MDBM_HASH_EJB          From hsearch
     *     MDBM_HASH_PHONG        Congruential hash
     *     MDBM_HASH_OZ           From sdbm
     *     MDBM_HASH_TOREK        From Berkeley db
     *     MDBM_HASH_FNV          Fowler/Vo/Noll hash (DEFAULT)
     *     MDBM_HASH_STL          STL string hash
     *     MDBM_HASH_MD5          MD5
     *     MDBM_HASH_SHA_1        SHA_1
     * </code>
     * 
     */
    void setHash(int hashid) throws MdbmException;

    void setHash(HashFunctionCode hashid) throws MdbmException;

    /**
     * Sync all pages to disk. <br>
     * 
     * @throws MdbmException upon error (and errno is set)
     */
    void sync() throws MdbmException;

    /**
     * Limit the size of a DB to a maximum number of pages.
     * 
     * This function causes the mdbm to shake whenever a full page would cause the db to grow beyond the specified max
     * number of pages. For more information, see mdbm_shake.
     * 
     * This function uses the newshake.
     * 
     * @param max_page Maximum number of pages
     * @param shake Shake function. See @see newshake
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void limitSize(int max_page, ShakeCallback shakeCb, Object param) throws MdbmException;

    /**
     * Compress the existing MDBM directory. Rebalances the b-tree and compresses the db to the smallest possible size.
     * 
     * @throws MdbmException
     */
    void compressTree() throws MdbmException;

    /**
     * Close the MDBM's underlying fd.
     * 
     * @throws MdbmException
     */
    void closeFd() throws MdbmException;

    /**
     * Truncate the MDBM. Truncates the database to a single empty page.
     * 
     * @throws MdbmException
     */
    void truncate() throws MdbmException;

    /**
     * Seamlessly and atomically replaces the database currently in oldfile with the new database in newfile. The old
     * database is locked while the new database is renamed from newfile to oldfile and the old database is marked as
     * having been replaced. This causes all processes that have the old database open to reopen the new database on
     * their next access.
     * 
     * This function will delete the old file; and rename the new file.
     * 
     * @param oldfile File to be replaced
     * @param newfile Path of new file
     * 
     *        Returns 0 upon success; -1 upon error (and sets errno)
     */
    // TODO: this should be static or in a helper.
    void replaceFile(String oldfile, String newfile) throws MdbmException;

    /**
     * {@link MdbmInterface#replaceFile}, but replaces a currently open database.
     */
    void replaceDb(String newfile) throws MdbmException;

    /**
     * Force a db to split, creating N pages.
     * 
     * Must be called before any data is inserted.
     * 
     * If N is not a multiple of 2, it will be rounded up.
     * 
     * @param size Target number of pages post split.
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void preSplit(int size) throws MdbmException;

    /**
     * Set the size of item data value which will be put on the large-object heap rather than inline. (See @see
     * mdbm_xobj) The spill size can be changed at any point after the db has been created.
     * 
     * @param size new large-object threshold size
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void setSpillSize(int size) throws MdbmException;

    /**
     * Store a string into the MDBM. <br>
     * 
     * @param key Key for storage <br>
     * @param value String to store <br>
     * @param flags indicates the type of store operation to be performed.
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     * 
     *         For more information about the return code, or flags, see mdbm_store_r().
     */
    void storeString(String key, String value, int flags) throws MdbmException;

    /**
     * Fetch a string from the MDBM. <br>
     * 
     * @param key Stored key
     * @return Returns the string, or NULL.
     */
    String fetchString(String key) throws MdbmException;

    /**
     * Delete a string from the MDBM. <br>
     * 
     * @param key Stored key
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     */
    void deleteString(String key) throws MdbmException;

    /**
     * Prune's a MDBM.
     * 
     * Iterates through the database calling the supplied prune function for each item in the database. Prune function:
     * 
     * <code>
     *  int (*prune)(MDBM *, datum, datum, void *)
     * </code>
     * 
     * The user-supplied param pointer is passed as the 4th parameter to the prune function. If the prune function
     * returns 1, the item is deleted. If the prune function returns 0, the item is retained.
     * 
     * @param prune Prune function
     * @throws MdbmInvalidStateException
     */
    void prune(PruneCallback prune) throws MdbmException;

    /**
     * Purge all entries from a MDBM.
     * 
     * @throws MdbmException
     */
    void purge() throws MdbmException;

    /**
     * fsync this mdbm.
     * 
     * @throws MdbmException upon error (and errno is set)
     */
    void fsync() throws MdbmException;

    /**
     * attempt to lock a mdbm.
     * 
     * @return 1 if lock is acquired; -1 otherwise (and sets errno)
     * @throws MdbmInvalidStateException
     */
    int tryLockNoThrow() throws MdbmInvalidStateException;

    /**
     * attempt to lock a mdbm.
     * 
     * @throws MdbmException if errno is set. Typically, an error could only occur if the database has been corrupted.
     * @throws MdbmInvalidStateException
     */
    void tryLock() throws MdbmException;

    /**
     * Returns whether or not mdbm is locked.
     * 
     * @return true if db is locked; false otherwise.
     * @throws MdbmException
     */
    boolean isLocked() throws MdbmException;

    /**
     * Returns whether or not mdbm is currently owned by a process. <em>Owned</em> MDBMs have multiple nested locks in
     * place.
     * 
     * @return 1 if db is owned; 0 otherwise.
     * @throws MdbmException
     */
    boolean isOwned() throws MdbmException;

    /** ITERATION **/

    /**
     * fetch first record
     * 
     * Initializes the iterator, and returns the first key/value pair from the db. Subsequent calls to next() or
     * nextKey() with this iterator will loop through the entire db.
     * 
     * @param iter MDBM Iterator (see @see MdbmIterator)
     * @return kvpair. If db is empty, NULL.
     */
    MdbmKvPair first(MdbmIterator iter) throws MdbmException;

    /**
     * fetch next record
     * 
     * Returns the next key/value pair from the db, based on the iterator.
     * 
     * @param iter MDBM Iterator (see @see MdbmIterator)
     * @return kvpair. If end of db is reached, NULL.
     */
    MdbmKvPair next(MdbmIterator iter) throws MdbmException;

    /**
     * fetch first key
     * 
     * Initializes the iterator, and returns the first key from the db. Subsequent calls to next() or
     * nextKey() with this iterator will loop through the entire db.
     * 
     * @param iter MDBM Iterator (see @see MdbmIterator)
     * @return datum. If db is empty, NULL.
     */
    MdbmDatum firstKey(MdbmIterator iter) throws MdbmException;

    /**
     * fetch next key
     * 
     * returns the next key from the db, based on the iterator.
     *
     * @param iter MDBM Iterator (see @see MdbmIterator)
     * @return datum. If end of db is reached, NULL.
     * @throws MdbmException
     */
    MdbmDatum nextKey(MdbmIterator iter) throws MdbmException;

    /**
     * get current db size
     * 
     * returns the current size of the db.
     * 
     * @return Size of database in bytes.
     * @throws MdbmException
     */
    long getSize() throws MdbmException;

    /**
     * get db size limit
     * 
     * returns the limit set for the size of the db using mdbm_limit_size.
     * 
     * @return Size limit of database in bytes (0 if no limit set).
     * @throws MdbmException
     */
    long getLimitSize() throws MdbmException;

    /**
     * get db page size
     * 
     * returns the page size of the db.
     * 
     * @return Size of db pages in bytes.
     */
    int getPageSize() throws MdbmException;

    /**
     * get hash
     * 
     * returns the hash function code for the db.
     * 
     * @return Hash code (see mdbm_sethash for values).
     */
    HashFunctionCode getHash() throws MdbmException;

    int getHashInt() throws MdbmException;

    /**
     * get alignment
     * 
     * returns the record alignment for the db
     * 
     * @return Alignment mask.
     */
    int getAlignment() throws MdbmException;

    /**
     * Create a new iterator for use with first.
     * 
     * @throws MdbmException
     */
    MdbmIterator iterator() throws MdbmException;

    /**
     * Given a hash function code, get the hash value for the given key.
     * 
     * @see MdbmInterface#setHash(int) for the list of valid hash function codes.
     * 
     * @param key Key
     * @param hashFunctionCode for a valid hash function (below)
     * @return hashValue is calculated according to the \a hashFunctionCode
     * @throws MdbmException on failure.
     * 
     *         <pre>
     * Values for \a hashFunctionCode:
     *   - MDBM_HASH_CRC32   - Table based 32bit CRC
     *   - MDBM_HASH_EJB     - From hsearch
     *   - MDBM_HASH_PHONG   - Congruential hash
     *   - MDBM_HASH_OZ      - From sdbm
     *   - MDBM_HASH_TOREK   - From BerkeleyDB
     *   - MDBM_HASH_FNV     - Fowler/Vo/Noll hash (DEFAULT)
     *   - MDBM_HASH_STL     - STL string hash
     *   - MDBM_HASH_MD5     - MD5
     *   - MDBM_HASH_SHA_1   - SHA_1
     *   - MDBM_HASH_JENKINS - Jenkins string
     *   - MDBM_HASH_HSIEH   - Hsieh SuperFast
     * </pre>
     */
    long getHashValue(MdbmDatum key, HashFunctionCode hashFunctionCode) throws MdbmException;

    long getHashValue(MdbmDatum key, int hashFunctionCode) throws MdbmException;

    /** Non-wrapper functions **/

    /**
     * @return Returns the canonical path to the mdbm.
     */
    String getPath();

    /**
     * @return Has the mdbm been closed?
     */
    boolean isClosed();

    /**
     * Validate the internal state of the Mdbm Object
     * 
     * @throws MdbmInvalidStateException
     */
    void validate() throws MdbmInvalidStateException;

}
