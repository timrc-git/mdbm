.. $Id$
   $URL$

.. _concepts:

Concepts
========

MDBM is an ndbm work-alike hashed database library  based on sdbm which is based
on Per-Aake Larson's Dynamic Hashing algorithms.

MDBM is a high-performance, memory-mapped hash database similar to the homegrown
libhash.  The records stored in a mdbm database may have keys and values of
arbitrary and variable lengths.

Larger value sizes may be stored by using MDBM's support for a Large Object Store.
Every key+value pair that is not a large object, plus some MDBM overhead, must fit
inside an MDBM page.  MDBM supports a maximum page size of 16777152 Bytes (16MB minus 64 bytes).


An MDBM's hash function specifies the algorithm to be used to decide how to parcel
out key-value pairs into particular MDBM pages.

Note: Testing the actual application dataset and access pattern is probably
important in determining whether mdbm will work well.


.. _size-limits:

Size Limits
-----------

  - A 32-bit process can access 4G of memory (2^32), so depending on system 
    configuration around 3.5G may be allocated for MDBM use.
    On a 64-bit machine, this limit is much higher (2^64), so practical
    MDBM size is more constrained by the size of your data's working set that
    needs to be memory-mapped into RAM.
  - You may establish a size limit on an MDBM to make sure MDBM will not use
    more RAM and/or disk resources than you'd like.  Once that size is exceeded,
    attempts to store data will start to fail.


.. _insertion:

Insertion
---------

When an insert occurs, and the item hashes to a page with insufficient room for
the data, a few things occur:

  - If an element of the same size had been previously removed from
    the page, that space is reallocated
  - The page is then 'compressed', moving all empty space to the end, and if
    that doesn't result in enough space for the new data
  - The page may be 'shaken', allowing your app to remove elements from the page

If there's still no room in the page, the MDBM may grow and a new page is
allocated.  All elements on the current page are re-hashed, either to the same
page, or the new page.  This step is repeated until there's room, or the
database grows as large as it can.


.. _key-structures-used-by-mdbm:

Key structures used by MDBM
---------------------------

Key structures used by MDBM.  These are declared in <mdbm.h>::

    typedef struct {
         char    *dptr;
         int     dsize;
     } datum;

     typedef struct {
         datum key;
         datum val;
    } kvpair;


.. _iteration:

Iteration
---------

.. TODO insert description of iteration

Use the ``MDBM_ITER_INIT()`` macro will initialize an iterator.

See FAQ :ref:`faq-iteration` entries for additional information.


.. _performance:

Performance
-----------

Sample database: key=yahoo-id (<=32 chars) val=4-byte status word.  Performance
for other database schemas may vary significantly from these stats.

Results are given as peak rate (records/sec and percentage change from base)::

                    libhash   libhash(ylock)     mdbm(4K page)      db3(4k page)

  store
   insert (w/dups)   64275    204338 (+218%)    318424 (+395%)    9860 (-85%)
   update (new)       23103     33880 (+47%)      87506 (+278%)
   update (existing)  32344     39278 (+21%)     161437 (+399%)
   replace (worst)    11856     16231 (+37%)      19980 (+68%)
           (avg)      17352     28483 (+64%)      35355 (+104%)

  fetch
   iterate           299033   1280540 (+330%)   1744610 (+483%)
   lookup (seq)       33449     40823 (+22%)     198463 (+493%)
          (random)    32601     39523 (+21%)     197881 (+506%)   16444 (-50%)

  delete              30109     33565 (+12%)      59609 (+98%)


.. _tips-and-tricks:

Tips and Tricks
---------------

Database Full
~~~~~~~~~~~~~

If your database is full, try looking at the page-size histogram (via
mdbm_stat).  If a very small number of pages are very full, you might want to
consider changing hash functions (md5 and especially sha-1 perform a lot better
on some key sets).

You can also consider enabling the large-object support (which does double-duty
as a spill-over mechanism when pages fill).


.. _data-integrity-checking:

Data Integrity Checking
-----------------------

If during an mdbm_lock() call it is determined that a process died while holding
the database lock, a full check of the database structure is performed, and if
any errors are discovered, all subsequent fetch, store, or delete operations
will return -1 with errno set to EFAULT.


.. _partition-locking:

Partition Locking
-----------------

Y! has added mdbm_plock() (sometimes incorrectly referred to as page locking)
support to MDBM.  Partition lock support enables mdbm to lock a partition (i.e.,
a section) of a database, rather than the entire database which provides a
significant performance improvement, at the cost of flexibility.

To use plock support:

  - V2 Only: The database must be pre-sized to it's final size.  plock support
    removes the ability for the mdbm to grow.
  - V2 and V3: mdbm_open() must be called with the MDBM_PARTITIONED_LOCKS flag

Two additional functions (mdbm_plock(), mdbm_punlock()) are available to allow
users to lock specific partitions within the database.  Care must be taken if
locks are nested. If partition locks are nested, then they must be unwound with
an equal number of mdbm_punlock() calls before any other type of locking can be
done, else indeterminate database state will result.  Interleaving of lock types
is not permitted. See the following example for how to correctly handle nested
partition locks.

Ex: partition lock nesting::

    ret = mdbm_plock(dbh, &key1, 0);
    ret = mdbm_plock(dbh, &key1, 0);
    // now we unwind
    ret = mdbm_punlock(dbh, &key1, 0);
    ret = mdbm_punlock(dbh, &key1, 0);
    // now we can do some other kind of locking
    ret = mdbm_lock(dbh);


.. _iteration-locking:

Iteration and locking
---------------------

For exclusive locking, users must explicitly lock the db around iteration.

When partitioned locking is enabled, the iteration API, mdbm_first(),
mdbm_next(), mdbm_firstkey(), and mdbm_nextkey() will automatically lock the
correct partition for you.

Once you have completely iterated through a partition lock enabled MDBM, it will
automatically unlock the database.  You do not need to make an explicit call to
mdbm_punlock().

However, if you break out of an iteration loop before completely iterating the
database, then you must call mdbm_punlock() on the last record obtained via
mdbm_first(), mdbm_next(), mdbm_firstkey(), or mdbm_nextkey().

When shared locking is enabled, the iteration API will automatically take
out a shared lock for you. Once you have completely iterated through the MDBM,
it will automatically unlock the database.
As with partition locking, the user must call mdbm_unlock() if they terminate
iteration early.
Write operations will still require an explicit write lock from the user.


.. _record-insertion:

Record Insertion
----------------

Several tactics are used to insert or replace records.  Replacing a record of
the same size will re-use the same space on a page.  Replacing a record with a
smaller one, however, will essentially insert a new record and delete the old
record.

Inserting a new record may require several tactics to find space before giving
up.  The following is the ordered list of tactics for finding space:

  #. If Large Objects are enabled, and the record size is >= spill-size, use the Large Object Store
  #. Find large enough free space on the page
  #. Find a direct singly usable (soft) deleted record on the page
  #. Garbage Collect (compact) the page
  #. Split the page (if that MDBM is able to grow)
  #. Shake the page
  #. Make the page an Oversized Page, or grow that existing Oversized Page by PAGESIZE


.. _large-objects:

Large Objects
-------------

Y! has added Large Object support to MDBM.  This allows records that are larger
than an MDBM's PAGESIZE (or some threshold value) to be stored and managed
separately.

Large object support is enabled by adding MDBM_LARGE_OBJECTS to the flags
parameter of mdbm_open() when creating an mdbm.  You cannot add or remove large
object support after an mdbm has been created.

A record size (key-size + value-size) is considered to be large when it greater
than or equal to a *spill-size*.  This causes that record to be stored as a
large object.  This spill-size threshold, may be set by using
mdbm_setspillsize().  The default spill-size is 3/4 of the size for that MDBM's
PAGESIZE.

In the general case, a large object is created when:

  - Large Objects are enabled for that MDBM
  - The record size (key-size + value-size) is greater or equal to the
    spill-size.  The default spill-size is 3/4 the size of the PAGESIZE
  - In MDBM v2, if a page is full, and that MDBM cannot split,
    a record smaller than the spill-size will be stored in the Large Object Store
    (LOS) if the record will not fit on the main store's page.

The page(s) used to store a large object does share that storage with any other record.

Currently, there are no interfaces to determine whether an object returned from
a fetch or saved by a store operation is a large object.

Large Objects in MDBM V2
~~~~~~~~~~~~~~~~~~~~~~~~

In MDBM V2, a large object is created by:

  - Allocating page(s) out of the large object table to store the object
  - Storing the key in a normal (main store) MDBM page
  - Storing a pointer to the head of the Large Object on that allocated MDBM page

mdbm_store(), mdbm_store_r() will fail to insert a large object if:

  - No room exists in the mdbm for that key
  - No room exists in the large object table

Large object pages are contiguously allocated until there is enough space to
contain the entire object.  Pages are 4096 bytes; a 4098 byte object will
consume 2 pages.

In general, MDBMs are sparse files, but large object support can create MDBMs
with large object support enabled may contain large expanses of empty pages
(e.g. it is a sparse file).  Because MDBM V2 manages the LOS as a separate
entity in the MDBM file, that file is more sparse.  Distribution notes for
sparse files:

  - rsync/rdist will be time consuming on some platforms if these tools do not
    efficiently handle sparse files.  For rsync on RHEL, use the *--sparse* option.
  - Calling GNU tar (or derivatives) with the *--sparse* flag will
    efficiently tar the file
  - As of version 2.24.8, the mdbm_save
    utility may be used to export the MDBM into a compact, binary object for
    transport; and mdbm_restore must be
    used to restore that datafile.

Facts about MDBM V2 Large Objects:

  - Large Objects cannot be used with partition locking
  - At most (2^18 -- 256K) large object pages may be used
  - Large object page size is fixed at 4096 bytes
  - Contiguous large object pages are used for objects larger than 4096 bytes
  - The default threshold is ( PAGESIZE * 0.75 )
  - The file on the local disk will be > 4G.  Space allocated for
    large objects is in the same file; but at an offset of 4G.  There's
    a hole between the normal database area and the large object area to
    allow the database to grow.

Large Objects in MDBM V3
~~~~~~~~~~~~~~~~~~~~~~~~

In MDBM V3, a large object is created by:

  - Allocating page(s) out of the free-list, and marking it as a Large Object page
  - Storing the key in a normal MDBM page
  - Storing a pointer to the Large Object page

In general, MDBMs are sparse files, but MDBM V3 files are usually not as
sparse as MDBM V2 files because there is not a separate store for large objects.

Facts about MDBM V3 Large Objects:

  - Large Objects can be used with partition locking
  - At most (2^18) large object pages may be used --- To Be Verified
  - A large object is comprised of 1 or more contiguously allocated pages of PAGESIZE
  - The default threshold is ( PAGESIZE * 0.75 )
  - Large objects reside in the same store as non-large objects

Using Large Objects in MDBM V3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MDBM V3 changed how large objects are stored.  In V2, large objects are stored
in a separately mapped fixed-size region, while in V3, large objects are
integrated into the main region.

For MDBMs that are allowed to grow dynamically, there is no pre-determined limit on
the number of large objects.  However, once an mdbm has reached its maximum size, it
is limited by the chunks of contiguous pages on the free list.  If you run out
of free pages, or the remaining contiguous chunks are smaller than your large
objects, store operations will fail.

The rest of this section addresses the situation of using large objects for a
fixed-size mdbm.

In the case of using a fixed size mdbm, where the maximum size is set at
creation time, it is necessary for you to set aside sufficient space for large
objects for the lifetime of your mdbm.  This is necessary because V3 has large
objects integrated into the main store.

An MDBM always has a page-directory that is able to address a power-of-2 number
of pages.  However in V3, the number of allocated pages does not have to be a
power-of-2.  This logical space between the size of the allocated pages, and the
power-of-2 page directory size is what defines the size of the large object
space.  The actual large objects and normal objects reside is the same virtual
space.  This artificial gap between the 2 logical spaces is only used for
reserving large object space.

Consequently, when using large objects in a fixed-size mdbm, it is *REQUIRED*
to specify a db pre-size that is *NOT* a power of 2.  Otherwise, there would be
no space set aside for large objects.

For example, assume that you want to allocate a 6GB data store with
approximately 1GB set aside for large objects and normal objects should occupy
5GB.  Also assuming that you want a data store page size around 100K, then page size
of 81920 would imply 64K pages and a main data store of 5GB.

To configure this mdbm::

  MDBM* db =
      mdbm_open(filename,
                MDBM_O_RDWR|MDBM_O_CREAT|MDBM_LARGE_OBJECTS|MDBM_DBSIZE_MB,
                0600, 81920, 6144);

or::

  ``mdbm_create -3 -d 6144m -L -p 81920 foo.mdbm``

Corresponding mdbm_stat::

  Page size    =        80K               Num pages    =      78648
  Hash func    =          5 FNV             used       =      65540 (83.33%)
  Max dir size =          0                 free       =      13108
  Max pages    =          0 0             Db size      =        6.0G
  Spill size   =      61440

  Directory                               Data
   width       =      65536                entries     =          0
   pages       =          4 ( 0.01%)         normal    =          0
   min level   =         16                  large-obj =          0
   max level   =         16                bytes       =           0 ( 0.00% utilization)
   nodes       =      65535                  normal    =           0 ( 0.00%)
                                             large-obj =           0 ( 0.00%)
  Chunks       =      65536                  overhead  =        1.3M ( 0.02%)
   normal      =      65536                  unused    =        6.0G (99.98%)
   oversized   =          0                entry sizes (min/mean/max)
   large-obj   =          0                  key+val   = 0/0/0
                                             key       = 0/0/0
                                             val       = 0/0/0
                                             lob       = 0/0/0
                                           entries/page (min/mean/max)
                                                       = 0/0/0

Note that there are 13108 free data store pages that can be used for large
objects or oversized pages.

.. _oversized-pages:

Oversized Pages
---------------

Y! has added Oversized Page support to MDBM, starting in V3.  This allows a page
to dynamically grow in size.  Previously, pages in the main store were all the
same PAGESIZE.  With this change, a page will grow as needed to store a record.
When a page grows, it grows by a PAGESIZE at a time.  There is no preset limit
for the number of times that a page can grow.

Oversized Page support is integral to MDBM, and there is no interface to enable
it.

In MDBM V2, if a room could not be made on page, and a Large Object Store were
not enabled, that store would fail.  With Oversized Page support, a page will
grow to store a new record.  However, it is still possible for a store to fail
if there is no room left in that MDBM, or that MDBM cannot grow, or the
available space is too fragmented to grow that page.

One aspect of this implementation is that the key-value index on an over-sized
page can be larger than PAGESIZE.  This allows many records to reside on the
same over-sized page.

Oversized Pages and Large Objects may be used together.  Records smaller than
the spill-size will be stored on the main page, which will become an over-sized
page as necessary.  Records that are greater than or equal to the spill-size
will be stored as Large Objects (on a large-object page that is not shared with
other records).


.. _shaking:

Shaking
-------

The way mdbm works, the key is hashed to a value, and a certain range of hash
values is stored on a certain page of the database.  When that page fills up, in
order to make more room, the database key space is split which doubles the size
of the db.  When you reach the maximum size for the database (i.e., how much
contiguous address space is available) and a page fills up, any further stores
for keys that hash to that page will fail.

When building a cache, you need to handle cache overflow somehow.  A common way
to do that is LRU, but such a solution doesn't work very well for mdbm because
you could fill up a database page with very recent stuff and you'd basically
have to delete every other entry in the database before you got to the end of
the LRU list and removed enough items from the filled page to allow the store to
proceed.  With mdbm, you really want to handle page overflow, not cache
overflow.

To handle that, you can specify the maximum size (as mapped address space, not
the number of bytes of records actually stored) that you want the database to
grow to.  You also supply a "shake" function.  Here's the prototype::

  int (*func)(struct mdbm *, datum, datum, void *)

When you attempt a store and the target page doesn't have enough space for the
new record and the database is already at the maximum size, then mdbm iterates through
all the existing records on that page and calls the shake function for each
record.  If you want to keep the record, you return 0.  If you want to delete
the record, you return 1.

mdbm actually will make up to 3 passes through the page passing as the last
argument the pass count (cast to a void*).  This allows the shake function to
start out conservatively and gradually get more aggressive in choosing what to
delete.  If there's still not enough free space after the first pass through the
page, the shaker will get called for each record again, this time with (void*)1
as the last argument.  And so on.


Original Shake Function
~~~~~~~~~~~~~~~~~~~~~~~

::

 int orig_shake_fn( MDBM *db, datum key, datum val, void *urgency )

Arguments::

    db       Reference to the db
    key      Key for the entry being considered
    val      Value of the entry being considered
    urgency  size_t (0-2) indicating the urgency of this request.
             "urgency" is a (size_t), passed as a (void *).

The urgency argument may be used to implement progressively aggressive deletion
of hash entries.  If you don't free up enough space by the end of the last page
scan (urgency == 2), the insert/modify operation will fail with a fatal error.

Using the original shake function will generate a warning message requesting that you
start using the V3 shake function (below).

New Shake Function
~~~~~~~~~~~~~~~~~~

::

 int new_shake_fn( MDBM *db, const datum *key, const datum *val, struct mdbm_shake_data *d );

Arguments::

    db   reference to the db
    key  key to be inserted
    val  value to be inserted
    d    data about the page

This function should return 1 if any changes are made; 0 otherwise.

mdbm_shake_data contains::

    mdbm_ubig_t page_num;        Index number of overflowing page
    const char* page_begin;      Beginning address of page
    const char* page_end;        One byte past last byte of page
    uint16_t    page_free_space; Current free space on page
    uint16_t    space_needed;    Space needed for current insert
    uint16_t    page_num_items;  Number of items on page
    uint16_t    reserved;
    kvpair*     page_items;      Key-Value pairs for all items on page
    uint16_t*   page_item_sizes; total in-page size for each item on page
    void*       user_data;       User-Data pointer passed to mdbm_limit_size_new()

The key-value arguments represent the item being inserted that triggered the
page shake.

When traversing the kvpairs in the mdbm_shake_data struct, a kvpair may be
marked as "deleted" by setting the key size to 0.

All information in the mdbm_shake_data struct may be used to help
determine how many / what records may be removed.

If the call to this shake function fails to free up enough space,
the insert/modify operation will fail with a fatal error.

Using the original shake function will generate a warning message requesting that you
start using the V3 shake function (below).

Shake Function for V3
~~~~~~~~~~~~~~~~~~~~~

::

 int shake_fn_v3( MDBM *db, const datum *key, const datum *val, struct mdbm_shake_data_v3 *d );

Arguments:

    db    reference to the db
    key   key to be inserted
    val   value to be inserted
    d     data about the page

The v3 shake function accommodates large page size which can be up to 16MB.

This function should return 1 if any changes are made; 0 otherwise.

mdbm_shake_data_v3 contains::

    uint32_t    page_num;        Index number of overflowing page
    const char* page_begin;      Beginning address of page
    const char* page_end;        One byte past last byte of page
    uint32_t    page_free_space; Current free space on page
    uint32_t    space_needed;    Space needed for current insert
    uint32_t    page_num_items;  Number of items on page
    uint32_t    reserved;
    kvpair*     page_items;      Key-Value pairs for all items on page
    uint32_t*   page_item_sizes; Total in-page size for each item on page
    void*       user_data;       User-data pointer passed to mdbm_limit_size_v3()

The key-value arguments represent the item being inserted that triggered the
page shake.

When traversing the kvpairs in the mdbm_shake_data_v3 struct, a kvpair may be
marked as "deleted" by setting the key size to 0.

All information in the mdbm_shake_data_v3 struct may be used to help determine
how many / what records may be removed.

If the call to this shake function fails to free up enough space, the
insert/modify operation will fail with a fatal error.


.. _locking:

Locking
-------

It is important to note that MDBM does basic locking internally, but users
of MDBM APIs should always lock around MDBM operations, if fetching or if
locking is required across mdbm calls (e.g. while fetching or iterating
through the mdbm), then processes must use the mdbm_lock() and mdbm_unlock().

For example, mdbm_lock() should always be called before calling mdbm_fetch(),
mdbm_first(), or mdbm_next().  mdbm_unlock() should not be called to release a
lock until all access to the returned record has completed.  Because the
data (key and value) that are returned contain pointers to the actual memory
locations where the record is stored in the database page, it is critical to
hold the database lock while making any type of access (read or write) to that
record data.

V3 has locking API for shared locking, known as MROW locking.  This is to allow
Multiple concurrent Readers but only One Writer.  So when a Writer has access
all Readers and other Writers are blocked.  To enable this locking mode, the
database must be opened with the flag MDBM_RW_LOCKS.  The API added to support
this feature is: mdbm_lock_shared() and mdbm_trylock_shared(), which should be
used before performing reads.  To write to or delete from an MDBM opened with
MDBM_RW_LOCKS, use mdbm_lock().  To unlock after both mdbm_lock_shared() and
mdbm_lock(), use mdbm_unlock().

A new set of APIs, mdbm_lock_smart(), mdbm_unlock_smart() and mdbm_trylock_smart()
have been added to simplify locking.  For more information, see the C API docs.

In V3, multi-threading is supported. However note that if multi-threads share
the same MDBM handle, blocking can still take place. This can happen if one
thread calls mdbm_lock on the database and later a 2nd thread calls mdbm_lock
using the shared handle. In this scenario, the 2nd thread will block until the
first thread calls mdbm_unlock.

For Partition locking, see mdbm_plock()

Partition locking and Shared locking may not be used on the same MDBM at the
same time.  All processes that open an MDBM must open it with the same locking
mode.  If you want to change between Partition and Shared locking, you must
close all open handles (ex., stopping your application), and re-open all handles
(ex., restart your application) with the changed locking mode.

NOTE: the locking mode is "sticky". If you use partitioned or shared
locking, it marks the exclusive lock as "terminated". So any future access
will default to partitioned locking (shared locking was added later, and
ysys_mmutex doesn't have a way to determine the type of a lock file).
This means that if you use shared locking, then you must specify it
everywhere that MDBM is used.

Locking scenarios
~~~~~~~~~~~~~~~~~

The following examples show *incorrect* and correct usage of the
locking API.

A process may be blocked upon opening a database if another process already has
a lock on the database.

Example, A: Blocking on mdbm_open()

  - Process 1: calls mdbm_open() to open the database and then calls mdbm_lock()

  - Process 2: calls mdbm_open() on the same database - will block in mdbm_open()

  - Process 1: calls mdbm_unlock() on the database

  - Process 2: will unblock and mdbm_open() returns with a handle to the database

**IMPORTANT**: Care must be taken to use locking within a process correctly.
The process should not interleave locking/unlocking calls of different types.
Incorrect mixing of locking/unlocking calls within a process can lead to an
unstable runtime environment.

Example B: Invalid interleaving of lock/unlock calls within a process:

  - mdbm_lock(dbh)
  -     mdbm_plock(dbh, key, flags)
  - mdbm_unlock(dbh)
  -     mdbm_punlock(dbh, key, flags)

Example C: Correct embedding of lock/unlock calls within a process:

  - mdbm_lock(dbh)
  -     mdbm_plock(dbh, key, flags)
  -     mdbm_punlock(dbh, key, flags)
  - mdbm_unlock(dbh)

Care must be taken to avoid deadlock as well.

Example D: Deadlock using shared(read/write) locks on same database.

  - Process 1: calls mdbm_open() with the MDBM_RW_LOCKS flag

  - Process 2: calls mdbm_open() with the MDBM_RW_LOCKS flag

  - Process 1: calls mdbm_lock_shared() on the database

  - Process 2: calls mdbm_lock_shared() on the database

  - Process 1: calls mdbm_store() - blocks trying to obtain exclusive lock
    since Process 2 holds the shared lock on the database

  - Process 2: calls mdbm_store() - blocks trying to obtain exclusive lock
    since Process 1 holds the shared lock on the database

  - Both processes blocked since each hold the shared lock and both try to
    obtain the exclusive lock - Deadlock.


Example E: Correct usage to avoid deadlock using shared (read/write) locks on same database.

  - Process 1: calls mdbm_open() with the MDBM_RW_LOCKS flag

  - Process 2: calls mdbm_open() with the MDBM_RW_LOCKS flag

  - Process 1: calls mdbm_lock_shared() on the database

  - Process 2: calls mdbm_lock() on the database and blocks since Process 1
    has the shared lock

  - Process 1: calls mdbm_unlock() on the database when it has finished
    reading (ex: mdbm_fetch()) from the database

  - Process 2: unblocks and obtains the exclusive lock and can call
    mdbm_store() without blocking

Example F: Incorrect usage under multi-threaded applications

You must make sure that only 1 thread at a time calls *any* function on a single MDBM handle.
The following example shows an invalid multi-threaded usage of MDBM:

  - Thread A: mdbm_lock(handle1)
  - Thread B: mdbm_dup_handle(handle1), or any other operation
  - Other MDBM operations, by both threads
  - Thread A: mdbm_unlock(handle1)
 
Example G: Correct usage under multi-threaded applications

  - "Parent Thread" creates an MDBM handle pool, using mdbm_dup_handle.
  -  Once you have your dup'd handles, you can do:
  - Thread A: mdbm_lock(handle1)
  - Thread B: mdbm_dup_handle(handle2), or any other operation
  - Other MDBM operations, by both threads
  - ThreadA: mdbm_unlock(handle1)

V4 Locking
~~~~~~~~~~

V4 has a plugin system for locking, based on pthreads.
Pthreads locking is up to twice as fast for high-concurrency cases.
However, pthreads structures are not compatible between 32 and 64-bit.

MDBM V4 has two new mdbm_open() flags for locking.

  - MDBM_SINGLE_ARCH indicates to mdbm that you will only use 64-bit or 32-bit
    access (but not both!) to any particular mdbm file.

  - MDBM_ANY_LOCKS indicates that other locking flags passed to mdbm_open() are
    just a suggestion, and if there are existing locks, the mdbm will be opened
    with the existing lock mode (and plugin type).

So, you can not use mixed architecture (32/64) access to the same mdbm fil.

V4 remembers the locking mode, and attempts to open with a different mode
are considered an error (unless the MDBM_ANY_LOCKS flag is passed).


The locations of lock files in V4 are:
   /tmp/.mlock-named/*dbname*._int_


Because of the profusion of possible lock locations, manually cleaning up after
an mdbm (when it will no longer be used) is more tedious.
So V4 adds mdbm_delete_lockfiles(), which takes the mdbm filename and
deletes any of the lockfiles that exist for it.

Note: mdbm_delete_lockfiles() should ONLY be called when users are completely
done with the mdbm file and deleting it. Other uses may result in corruption
and undefined behavior.

Note: MDBM has no way of knowing when the intervening directories are no
longer needed. So, it can't delete them. Users creating many temporary
directories may need to do additional cleanup.


.. These are exported, but not documented here:
    [mdbm.c]
    xm_init
    xm_remap
    xm_alloc
    xm_free
    mdbm_page_addr()
    minimum_level()
    mdbm_alloc_page()
    page_number()
    get_dir()

  I don't believe they should be documented for external use.


.. _threading-support:

Threading support
-----------------

The following restrictions apply when using MDBM in a threaded process
(this is not exhaustive):

  - Multiple handles may be created with ``mdbm_open`` by a process, but each
    handle will have a duplicate map of the db, wasting VM page-table entries
    and address space
  - ``mdbm_dup_handle`` may be used to create a new handle which shares a single
    data store mapping
  - All locking types are supported on dup'd handles,
    as long as a handle isn't shared across threads
  - ``mdbm_replace`` is supported using dup'd handles
    (again, no sharing of handles across threads)

Since an MDBM handle may be used by only one thread at a
time.  Some multi-threaded approaches are:

  #. Use a global MDBM handle, and an application lock to control access.
     This is very slow but easy to do for testing.
  #. Use ``mdbm_dup_handle`` on a per-thread basis.  As mentioned above,
     a handle may be used by only one thread at a time, so threaded applications
     should use pthread_mutex_lock and unlock around calls to mdbm_dup_handle.
  #. Each thread in your application could have an MDBM handle as thread-specific data.
     A handle is created the first time that it's needed, and it
     remains for the life of that thread.
  #. Use a thread pool where each thread has an MDBM handle
     with thread-specific data

Options 3 and 4 above are recommended for high-performance applications.

Code for a ccmp_pool is included in the distribution.
This is one easy, tested, and tuned way to implement option 4.

.. // at startup
   ccmp_pool_t* pool = ccmp_create_pool(db, int size, log);
   ...
   // in each thread...
   MDBM* db = ccmp_acquire_handle(pool, log)
   mdbm_lock(db);
   do_mdbm_stuff(db);
   mdbm_unlock(db);
   ccmp_release_handle(pool, db, log)
   ...
   // at shut-down
   ccmp_destroy_pool(pool, log);



.. _dynamic-mdbm-replacement:

Dynamic MDBM replacement
------------------------

An MDBM that is actively in use can be dynamically replaced with a new MDBM.
The ``mdbm_replace`` program and the ``mdbm_replace_file`` API provide atomic
replacement.

For applications that have write (store/delete) accesses, the MDBM must be
opened with locking (i.e., you may not use ``mdbm_open`` with the
``MDBM_OPEN_NOLOCK`` open flag).  An application lock (ex., ``mdbm_lock``, or
``mdbm_plock``) is required around each access.  This lock must be done around
read accesses (fetches) as well as write accesses.

For high-performance read-only applications, where all accesses to an MDBM are
fetches (across all processes that open the MDBM), an optimization is available:

  - The ``mdbm_open`` call may optionally specify the ``MDBM_OPEN_NOLOCK`` flag.
    This flag prevents any lock objects from being created, which makes the
    ``mdbm_open`` call faster.  For highly parallel applications that use
    ylock-based locking (all v3 applications use ylock), this option avoids
    contention on the ylock arena lock that must be acquired to create new lock
    objects.  It's not necessary to use the ``MDBM_OPEN_NOLOCK`` option for
    non-high-performance applications.
  - It is not necessary to acquire a lock around fetch operations.

Note: Currently (2013-04-01) dynamic replacement is only supported for
applications with a processed-based programming model.  It is not supported for
multi-threaded models, but it will be supported in a future release.

.. _mdbm_import-and-mdbm_export:

``mdbm_import`` and ``mdbm_export``
-----------------------------------

``mdbm_import`` and ``mdbm_export`` are utilities for converting from .mdbm
files into a textual representation and back.  These tools are
similar in concept to the BerkeleyDB database _{dump,load} utilities:

  - http://www.sleepycat.com/docs/utility/db_dump.html
  - http://www.sleepycat.com/docs/utility/db_load.html

In fact, the default format for the textual representation is essentially the
same as the "printable" format used by Berkeley DB (which means that it should
be relatively easy to convert between MDBM and BerkeleyDB formats).

This simple 3-key database should give you an idea of what the tools do::

    [smiles] ~$ mdbm_export /tmp/test.mdbm
    format=print
    type=hash
    mdbm_pagesize=4096
    mdbm_pagecount=1
    HEADER=END
    key1
    value1
    key20
    some_value 20
    noval ky

    [smiles] ~$ mdbm_import -i temp.dump /tmp/test2.mdbm
    [smiles] ~$ mdbm_export -o temp2.dump /tmp/test2.mdbm
    [smiles] ~$ diff temp.dump temp2.dump
    [smiles] ~$ mdbm_export /tmp/test2.mdbm | mdbm_import /tmp/test3.mdbm
    [smiles] ~$

Both tools also support the cdbmake/cdbdump format, which is a little more
compact because it uses binary representation instead of escaped hex sequences,
but not as easy to edit by hand.

http://cr.yp.to/cdb/cdbmake.html::

    [smiles] ~$ mdbm_export -c /tmp/test.mdbm
    +4,6:key1->value1
    +5,13:key20->some_value 20
    +8,0:noval ky->
    [smiles] ~$


.. _mdbm-data-store-protection:

MDBM data store protection to catch wild-pointer reads/writes
-------------------------------------------------------------

Buggy code somewhere in an application has the potential to corrupt the
structure or data contained in an MDBM because that MDBM is mapped into a
process's address space.  An application write through a bogus pointer can alter
the MDBM data.  Of course, the same buggy code could potentially corrupt the
stack or heap data.  Because MDBMs are often the largest chunk of writable data
in the address space, that's often where the issue arises.

mdbm-3.11.0 added a protection mode which automatically protects (from both
reading and writing) the MDBM when the data store is not locked.  This can be
used to catch both buggy code which is doing reads or writes via bogus pointers
that happen to point into an MDBM as well as buggy code which is not holding an
MDBM lock when needed to access data from the db.  The result of either type of
unwanted access is a SIGSEGV or SIGBUS.  Hopefully you have core dumps enabled
(I'm not a fan of simply ignoring these errors because the core dumps cause too
much system overhead).

This feature is pretty costly in performance terms, so it cannot be used for
general production runs.  However, if an easily reproducible MDBM corruption
issue arises in either testing or production, the protect feature can be enabled
for a short time to attempt to trap the errant code.

mdbm-3.27.0.69 added a warning that is displayed during ``mdbm_open`` when the
protect feature is enabled.  For example::

   *your-mdbm* : protect feature enabled -- performance might be degraded

Protection might not be compatible with ``mdbm_replace``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using MDBM protection is generally not compatible with using ``mdbm_replace`` at
the same time.  However, ``mdbm_replace`` can be used in limited circumstances:

    - The old and new MDBMs must be owned by root
    - The old and new MDBMs permissions must be 0664 or 0644
    - The old and new MDBMs must have the same permissions

Using a .protect file to enable protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable protection for an MDBM, create a symlink in the same
directory as the MDBM, using them same name as the db with a ``.protect`` suffix.
The symlink should point to /dev/null and it must be owned by root.  For example::

    # Given the following as your mdbm:
    /home/y/var/cache/foo/foo.mdbm

    # Enable protection with a symlink
    sudo ln -s /dev/null /home/y/var/cache/foo/foo.mdbm.protect

Using an environment variable to enable protection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

mdbm-3.27.0.69 added environment variable ``MDBM_PROTECT_PATH_REGEX`` that may be
used to define a POSIX Extended Regular Expression that is used in an unanchored
matched against the mdbm's ``realpath`` (resolved full pathname).  If there is a
match, the mdbm protect feature is enabled.

Although it's possible to specify an extremely permissive regular expression,
this is not recommended when the corresponding environment variable is set in a
system wide fashion (ex., a ``yinst`` variable).  However, if this is necessary,
using ``MDBM_PROTECT_PATH_REGEX`` enables the protect feature for any mdbm
by using a wildcard match on the first character in the mdbm's realpath.

Instead of extremely permissive regex using ".", a better approach would be to
target the MDBMs that you want to enable this feature.  For example, use::

    MDBM_PROTECT_PATH_REGEX=.*Maple.*

or::

    MDBM_PROTECT_PATH_REGEX=Maple

to enable the protect feature for MDBMs with "Maple" anywhere in its realpath.

.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: Aake BerkeleyDB CREAT DBSIZE EFAULT FNV LOS MROW NOLOCK PAGESIZE
   LocalWords: PThreads RDWR REGEX RW SIGBUS SIGSEGV TODO YLock addr alloc
   LocalWords: cdbdump cdbmake const dbh dbname dir dptr dsize dup'd dups
   LocalWords: emacsen errno faq firstkey fn func init iter kvpair kvpairs ky
   LocalWords: libhash ln lockfiles md mdbm mdbm's mmutex ndbm nextkey noval num
   LocalWords: oversized pagecount pagesize plock pre pthreads punlock rdist
   LocalWords: realpath regex ret rsync sdbm setspillsize sha struct sudo
   LocalWords: trylock ubig uint wildcard xm ysys

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
