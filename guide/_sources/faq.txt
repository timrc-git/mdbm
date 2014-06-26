.. $Id$
   $URL$

.. _faq:

Frequently Asked Questions
==========================

This is a list of FAQs about MDBM.
To suggest new entries, send mail to the mdbm-users group.


.. _faq-general:

General
-------

**What is an MDBM?**

    MDBM is a fast in-memory hash-based key-value store.

**What are the most common use cases for MDBM?**

    MDBM is commonly used for caching and storing static data for quick access.

**How do I get started?**

    - Build the package 'make' and install it 'make install'

**What are the different language bindings available for MDBM?**

    - Bindings are included for C/C++, and perl.


.. _faq-general-problems:

General Problems
----------------

**How do I report a problem with MDBM?**

  See :ref:`getting-help`

**Why can't I create a 2G MDBM on a 32-bit system?**

    In order to use a 2G MDBM, you'll have to lower the data-size limit in your
    process (man limits) from the default (512MB) to something lower.

**I have a corrupted mdbm, what can I do?**

    Often, MDBMs get corrupted due to an application not handling locking correctly.
    If you are doing reads and writes, you must open that MDBM with locking (default
    is exclusive locking).  You must hold the lock for the duration that you
    are referencing a record (for all operations, including reads, writes, and
    deletes).  For example, when doing a read, you would typically:

      #. ``mdbm_lock``
      #. ``mdbm_fetch``
      #. copy out the data pointed to by the returned fetched datum
      #. ``mdbm_unlock``
      #. access your copied-out data

    Another common way for MDBMs to become corrupt is for an application to
    mistakenly write (via a bad pointer) into the MDBM's mapped space.  There is an
    MDBM *protect* feature for debugging purposes to catch these problems.
    See :ref:`mdbm-data-store-protection` for more information.

    Try using ``mdbm_check`` to identify the extent of the damage.  If there is
    major corruption, ``mdbm_check``  will abort.

    Once an MDBM is corrupt, most of the time it's not possible to tell how it
    got corrupted.  You need to catch it in the act of being corrupted.

.. _faq-file-system:

File System
-----------

**Will MDBM perform well over NFS?**

    No.  MDBM over NFS **MUST BE AVOIDED** at all costs.

    It does not perform well at all.  MDBM uses mmap(), and although the NFS
    driver does support the mmap() operation, it gets converted to regular block
    fetches and updates, so the performance sucks.

**What's the worst-case-scenario if I never call mdbm_close nor mdbm_sync (I just exit)? Can the database become corrupted? Can individual records become corrupted?**

    If you never call ``mdbm_sync`` and don't pass either ``MDBM_O_ASYNC`` or
    ``MDBM_O_FSYNC`` to ``mdbm_open``, the data will never be sync'd to disk (unless
    the system runs low on physical memory and starts swapping).  If you reboot
    the system and open the database, it will be empty.

**Is the whole file mapped to memory at any given time, or is there some sort of page fault/swapping that goes on?**

    The whole thing is mapped when you open the database, but individual pages
    are faulted in when they're touched.

**How often (if ever) is the backing file sync'd to disk?**

    Never, by default.  If you want sync'ing, you either have to use ``mdbm_sync``
    to manually sync or specify ``MDBM_O_ASYNC`` when opening to enable background
    sync'ing by the kernel syncing process.

**There's a flag for a memory-only database without a backing file.  Is that correct?**

    There is a private internal flag which is not part of the public API to
    signify a memory-only MDBM.  To create a memory-only MDBM, specify ``NULL``
    for the ``mdbm_open`` *file* argument.

    Memory only MDBMs must be initialized as a fixed-sized MDBM because there is
    no way to handle dynamic-growth size changes across processes.  This means
    that you must specify a *presize* to ``mdbm_open``.  You must also
    use ``mdbm_limit_size_v3`` with *max_page* equivalent to the *presize*.

**Why is my MDBM file modified time not being updated after I store something?**

    Simply, mod time does not get updated on mmap'd writes.  MDBM writes are
    simply writing to memory; they are not directly doing file-based operations
    which would affect a file's modified time.

    For a high-performance store (ex., when you want to do 50K-100K writes/sec),
    it wouldn't be reasonable to update the modified time for each write.

**Under what circumstances can a process or system crash cause loss of data in an MDBM?**

    A system crash will lose data unless something has synced the MDBM.  By
    default, nothing does that.

**If mdbm_sync is called, am I guaranteed that the MDBM will be corruption-free if there is a subsequent crash, even if it's missing some updates subsequent to the sync?**

    That's tricky.  If the MDBM page size doesn't match the OS page size, then
    it's possible that VM pressure might cause only part of of a database page
    to get synced to disk.  That would corrupt that page.

    ``mdbm_sync`` itself isn't foolproof either because it doesn't lock the MDBM
    and does a background sync.  ``mdbm_fsync`` is better for integrity in that it
    locks the database and uses a synchronous fsync.  The downside, of course,
    is that the database is locked until all the dirty pages are flushed.

**What kind of overhead would I expect from using MDBM_O_ASYNC?**

    I infer that the system sync process, which flushes data to disk every 30
    seconds, would also write the mmap-ed changed pages to disk every 30
    seconds, so the worst case performance would be the time it takes to write
    the data, amortized over the time.

**But, does the sync process lock the pages (this could be important if we're doing very high data rates on the MDBM - for example 100Ks/sec)?**

    Yes, the sync locks the pages, so if you touch a page while it's being
    flushed, you'll block.  I haven't looked at this closely in a while, but I
    also recall that a sync results in a page fault when you touch a page for
    the first time after it's been synced.  It's a quick fault (no disk access),
    but it still hurts a bit.

**Does mdbm_close do an implicit flush to disk?**

    ``mdbm_close`` only syncs if the MDBM was opened with ``MDBM_O_FSYNC``.
    ``mdbm_close`` itself won't cause any flushing.

**Does the MDBM file on the disk have the latest updates to the key-value pairs?**

    In general, unless you use ``mdbm_sync``, or use ``mdbm_open`` with
    ``MDBM_O_FSYNC`` (or ``O_FSYNC`` in earlier MDBM versions), your data will
    probably not be written to disk by ``mdbm_close``.

    On FreeBSD, the mmap'd file that holds the MDBM is not sync'd to the
    physical disk unless ``mdbm_sync`` is used or during a normal system shutdown
    when all dirty file data gets sync'd to disk.

    On RHEL, modifications to the mmap'd file are background-sync'd to disk
    after 30 seconds for files that are on a normal file-system mount.  However,
    MDBMs that are hosted on a tmpfs file-system are not sync'd (and are also
    not preserved across a system reboot).

**Can I copy an MDBM file from one machine to another?**

    Normally this is a bad idea, it is recommended to use ``mdbm_export`` at the
    source machine to obtain a portable file and then perform an ``mdbm_import`` on
    this file to get the MDBM on the destination machine.  The ``mdbm_copy`` command
    is also available.  Neither ``mdbm_export`` nor ``mdbm_copy`` guarantee data
    consistency, since calls to mdbm_store that store related data can occur mid-copy.


.. _faq-sizes-and-limitations:

Sizes and limitations
---------------------

**My MDBM says it is 4G in size, will I need more RAM?**

    MDBM is a sparse file when large object mode is enabled.  Use ``mdbm_stat`` to
    view the actual allocated size of the database (and Large Object Store).

**Why has my MDBM dynamically grown to be huge?  I don't have nearly that much data.**

    You have a data-sensitive problem.  If you are using duplicate keys, or
    you have a pathological dataset, some of your pages are filling up too
    soon.  When there is no more room on a page, an MDBM will grow to a
    maximum limit.  When your MDBM can no longer grow, an attempted store to
    an full page will return an error.

    There are only few knobs to turn, in priority order:

      #. Enable large objects (only settable at create time) if you have a
         small percentage (<5%) of objects that are significantly bigger than the others.

         - The v2 implementation will create a 4GB file size because large
           objects are stored at a 4GB file offset and below.  It's a sparse
           file so only the necessary pages on disk are used for storing data.

      #. Increase your page size (only settable at create time)

         - This might decrease performance because many more keys
           might need to be compared on a page to determine whether your lookup key
           exists.  If there are few keys/page this won't be significant.  If there
           are many (100+) keys/page it might be noticeable.  If you have a lot of
           lookups where the key doesn't exist, this hurts performance because it
           has to compare every key on the page.

      #. Try another hash function (only settable at create time)

         - If your key is a string, try the Jenkins hash function
         - If you key is binary, try CRC32, SHA1, MD5 (probably in that order, YMMV)

      #. Use ``mdbm_open`` and ``mdbm_limit_size`` to set the initial and maximum MDBM
         size when the file is created.

         - This will create a flatter internal btree which might help
           distribute your data more uniformly. This might help you reduce that
           number of nearly full pages where a store operation would fail due to
           lack of space (this is your real problem as opposed to a large
           sparse MDBM).  A consequence is that the actual number of pages
           used on disk might be higher, but that's probably a good trade-off.

      #. If you really don't have a good idea of the final size of your MDBM
         (as needed in the previous option), use ``mdbm_open`` with an
         initialize size with your best guess.

    If option 1 does not work, then you might need a combination of the other.

    MDBM is a hashed key-value store, so changing your hash function or page size changes
    how your data is distributed between MDBM pages.  If the hash function you chose
    happens to parcel out too many keys into a single page, that page will split and
    MDBM's file size will double.  If you keep adding data that happens to hit the
    same page, the MDBM will keep splitting and file size will keep growing and growing.

    Use ``mdbm_stat`` to look at your histogram data.  You want to avoid having
    many pages that are nearly full when your MDBM close to its maximum size.


**How do I control the amount of memory available to mmap?**

    In FreeBSD this can be controlled by using the kernel variable
    vm.max_proc_mmap, though it's usually not necessary to tune this.  32-bit
    applications on FreeBSD trade-off space for malloc against space for mmap
    according to the data segment size limit.  This is controlled at the kernel
    level using the kern.maxdsiz loader variable (FreeBSD 4) or
    compat.ia32.maxdsiz sysctl (FreeBSD 6/7).  In addition, the process rlimit
    for data segment size can be used to lower the data segment size limit (and
    therefore make more room for mmaps).

**How do I determine how my MDBM is mapped into memory?**

    MDBM v3 mmaps an entire MDBM file into memory.  Simply mapping an MDBM does
    not make it memory resident.  Although a file's size on disk might be quite
    large, the sparse file structure will only bring pages containing data into
    memory when they are referenced (ex., fetch, store, or delete operation).
    The ``mdbm_preload`` routine may be used to make an MDBM memory resident.

    On RHEL, you can review a process' mapped regions and associated files via
    ``cat /proc/``\ *pid*\ ``/smaps``.

**I'm using the MDBM within PHP that runs within yapache, given that each yapache child runs as a process, will each process mmap the MDBM separately?**

    Usually, the individual processes will map the MDBM separately (because the
    MDBM is opened after the child has been forked from the parent), but they
    will all be sharing the same physical RAM mapping for that file.

**Can I use MDBMs in two or more machines in a cluster mode by connecting them?**

    MDBM can't do this out of the box.  Explore YDBM or DISC-GDS.


.. _faq-iteration:

Iteration
---------

**How do I initialize an MDBM iterator?**

    The ``MDBM_ITER_INIT()`` initializes an iterator.

**While iterating across an entire database, am I guaranteed to see all key-values present in the database when the iteration starts, if deletes occur during the iteration?  What if inserts or overwrites occur during iteration?**

    Deleting items will not affect iteration, assuming you only delete items
    you've already iterated over.

    If you lock the database; and begin an iteration, you will see all
    key-values.  Deletion of some key-values will not interfere with this, as
    long as you remove a key-value you've already iterated over.

    If you started an iteration; and removed a key you knew was in the database
    but hadn't iterated over yet; the iteration would *not* return the (now
    deleted) key-value pair, even though it was in the database when the
    iteration began.

    Overwriting depends on what you're doing.  If you're just fetching the value
    pointer and rewriting in-place, that's safe.  If you're replacing the value
    with a different size, that may cause garbage collection, which may cause
    your iterator to miss records.

    Inserting records may also trigger garbage collection, which may cause your
    iterator to miss records.


.. _faq-locking:

Locking
-------

**Do I need to use locking if I'm only doing read access and using mdbm_replace?**

    If you have a read-only MDBM (there are no store/delete operations) in a
    single-threaded application, you do not need to lock.  This is because the
    access operations are smart enough to check for replacement and to acquire
    an internal lock.

    However, if you use ``mdbm_replace`` in a multi-threaded application, you do
    need to lock around fetches.  A future enhancement will remove this locking
    requirement for multi-threaded applications.

**When should I use mdbm_lock?**

    When two or more processes are reading and writing to the same MDBM.
    ``mdbm_lock`` is used by a process reading or writing to obtain exclusive
    access.

**There doesn't appear to be a distinction between read locks and write locks. Is that correct?**

    For exclusive locks, that's correct.

**Is there any mechanism for allowing multiple readers and one writer (MROW) that doesn't have the readers block each other?**

    MDBM V3 has shared locks (sometimes called read-write locks).

**Are the lock requests FIFO?**

   No, locks are scheduled according to process priority.

**Why doesn't mdbm_fetch automatically lock?**

``mdbm_fetch`` doesn't lock so that an application can take greater control over
locking, and  the corresponding performance in a few ways:

    - ``mdbm_fetch`` doesn't copy-out the data.  An application could lock, fetch,
      look at that pointer's data contents, and unlock.  In some situations,
      this can be much faster than lock, fetch, copy-out, unlock, and look at returned
      contents.

    - If you are willing trade off latency for higher throughput: locking, doing
      multiple fetches (copy-out or not), and unlock, you could achieve higher
      throughput.  This approach is application and data dependent.

    - If you have a master record and dependent records (specified in that master
      record), your app might require that accesses to the master record and the
      dependent records be done in a single locked context.  Otherwise, dependent
      records could be deleted or be modified, which could be incompatible with
      the master record.

``mdbm_fetch_str`` locks, does a copy-out of the value, and unlocks. This,
however, is only for string data.  ``mdbm_fetch_buf`` also locks while copying
into the provided buffer.

**With MDBM V4, I'm getting the following error message: multiple different lockfiles exist**

If you're seeing the following error message when opening an MDBM:

    mdbm_lock.cc:68 YourFile.mdbm: multiple different lockfiles exist! : No such file or directory
    ERROR (2 No such file or directory) in mdbm_open_inner() mdbm.c:3817

Then this is what is likely happening: someone has opened YourFile.mdbm using a 32 bit process
and you are using 64-bit, or vice versa.
Make sure any tools you are using match your executables (bin vs bin64).


.. _faq-performance:

Performance
-----------

**If we have many little structures to store (possibly smaller than 64 bytes, keyed by registered user), how should we tune for that? (page-size?)**

    Many little structures work best.  It's bigger structures that create
    problems.  You should try different page sizes to see what performs better.
    8K or 16K are probably good starting points.

**Are there are guidelines for tuning MDBM, or is it more of trial and error?**

    It's mostly trial-and-error, but try to use the smallest page size that will
    fit the dataset without causing page overflows (a page overflow happens when
    a key to be inserted hashes to a page that's already full and the database
    can't be split because it's already too big).

    Use larger page sizes when key+value size is larger, smaller page sizes when
    key+value size is smaller.  Larger page sizes are slower because effectively
    the hash buckets take longer to locate a specific key.  In V3, however, this
    was significantly speeded up.

    Also, if you know you don't have duplicate keys (or don't care if they get
    inserted), you can avoid the lookup that occurs on insert by use the
    ``MDBM_INSERT_DUP`` flags.  That'll speed things up even more.

    There is a new ``mdbm_config`` tool that will help
    you select MDBM configuration parameters for your dataset.

**What should be the ratio between main memory size and MDBM size (in order to maintain its performance)?**

    MDBM expects all of its data pages to be in physical memory.  MDBM databases
    grow in power's of 2, and not all the pages mapped necessarily have data on
    them.  The ``mdbm_stat`` utility can analyze a database and show the various
    efficiencies (how full the pages are, how many non-empty pages there are).

**Why is building an offline MDBM slow, and my resulting file is highly fragmented?**

    If you are building offline and you have a known maximum size of the MDBM:
        - Create the MDBM with the initial size set to the final size
        - Use ``mdbm_limit_size_v3`` to ensure that MDBM doesn't split in the future
        - Make sure that your physical memory is larger than the resulting MDBM

    Setting the initial size to the final size will avoid MDBM splits, which
    also avoids the latency incurred during the split.  The resulting MDBM
    directory will also have fewer levels (enabling faster lookups).

    If the resulting MDBM is highly fragmented, you probably have highly a
    fragmented disk.  Either use a non-fragmented disk, or use a ramdisk to
    build the MDBM, then copy the MDBM out of the ramdisk.

**How can I speed up fetches in my read-only web application?**

    Frequently, web applications open an MDBM on a per-user-request basis.  This
    is a very bad idea because each open can take several hundred (or more)
    microseconds.  The best thing that you can do to improve your performance is
    to open the MDBM once at an application level, not at a per-request level.
    For a single (unlocked) memory-resident lookup, it should take ~4
    microseconds on standard hardware.

    Not only is an open call comparatively slow to a fetch, but concurrent calls
    to ``mdbm_open`` are single-threaded when it creates the locks for the first
    time (very slow) and then it maps the shared MDBM into process address
    space.  After initial lock creation, subsequent ``mdbm_open`` calls will also be
    single-threaded as it creates state in each new handle and maps the shared
    MDBM into process address space.

    If there are *no* writes taking place on the MDBM, and you are *not* using
    ``mdbm_replace``, you can disable all locking overhead by specifying
    ``MDBM_OPEN_NOLOCK`` in your ``mdbm_open`` flags.  This avoids creating the mutexes
    used for locking during ``mdbm_open``.

    Fetching a non-existent key is slower than fetching a key that exists.
    Non-existent keys require checking all keys on a page before determining
    that a key does not exist.  In this regard, MDBM V3 format files are faster
    than MDBM V2 due to some saved key hash data.

    The number of key comparisons will influence your fetch time.  Large pages
    typically have more keys/data, thus have a slower lookup time.  Using a
    smaller page size can get better performance.

    Don't use large objects, they are a little slower to reference.


.. _faq-data-access-and-management:

Data Access and Management
--------------------------

**Does MDBM automatically do garbage collection?**

    No.  MDBM doesn't do this for you.  Data-specific garbage collection can be
    implemented using the "shake" function that is registered by using
    ``mdbm_limit_size_v3``.

**What is the upper limit size for MDBM, before its performance degrades?**

    Even at maximum size, the design of MDBM requires only two database pages to
    be accessed for any single fetch.  If your system's RAM available to MDBMs
    cannot contain your "working set" of MDBM data, your performance will degrade.

**How much extra empty storage does an MDBM require?**

    It depends on the hash collision rate of the keys.  The better the hashed
    key distribution is, the less likely a leaf page is to be filled and require
    a premature database split.

**Is it possible to shrink an MDBM without rebuilding the entire database?**

    It's sometimes possible that ``mdbm_compress`` will be able to shrink the
    database because it rebalances the tree.

**How does mdbm_store with flag MDBM_MODIFY work?**

    When using ``mdbm_store`` with flag ``MDBM_MODIFY`` to change an existing record,
    what happens if the new record is of the same size as the original record?
    What happens if the new record is of a different size than the original?

    If the new record is of the same size as the original, the data is replaced
    but the location in memory stays the same.  If the new record is of a
    different size (larger or smaller) than the original, the original record is
    deleted and a new one is inserted, so the location in memory may change.

**When do I use mdbm_popen vs. mdbm_open in PHP?**

    Usage might depend on the frequency of access.  For example a PHP script
    running in yapache context that accesses the MDBM during all/most of the
    requests, then it should do a ``mdbm_popen``.

**Can I use mdbm_fetch without taking out a lock, to just check for key existence?**

    The short answer is that you must always take out a lock for a fetch
    operation if there are concurrent writes (store or delete operations).

    Here is the reasoning on why you must lock around ``mdbm_fetch`` if there are
    concurrent writes.

        - An unlocked ``mdbm_fetch`` cannot guarantee a coherent use of a key's
          fields for offset and size.  An intervening write might produce a
          mismatched offset and/or size for a key.
        - Depending on your hardware architecture for memory byte-ordering of
          read/write access and atomicity of access, reading a field (ex., offset)
          as it's being written could generate an undefined result.  The
          operations that write key meta-data information are not atomic
          operations.  For x86 architectures, this might not be an issue under
          some situations (ex., 2 or 4-byte writes with aligned access).  This
          issue requires additional investigation.  A further consideration is
          that the underlying access for ``mdbm_fetch`` is doing a 3-byte read.

    If you have concurrent writes, and you do not lock an ``mdbm_fetch``, the
    following consequences are possible, but (highly) unlikely:

        - False-positives -- ``mdbm_fetch`` could indicate that a key is present
          when it is not
        - False-negatives -- ``mdbm_fetch`` could indicate that a key is not present
          when it is
        - SEGV -- if a key resolves to an off-page address, and that page has not
          been mapped, it is an access violation.  Some related issues:

            - If you do get a crash, it will be very difficult to determine the
              overall reason for the crash.  An invalid address access will be
              obvious, but there will be insufficient information to develop a
              scenario to explain the crash.  The bad access generated by
              ``mdbm_fetch`` is transient, and not deterministically reproducible.
            - If a crash happens while an ``mdbm_store`` operation is taking place,
              the meta-data on the page could become corrupted.  An ``mdbm_store``
              can result in shuffling the meta-data on the page (to make room
              for the store) which could result in a partial update.


.. _faq-miscellaneous:

Miscellaneous
-------------

**Is MDBM thread-safe?**

    MDBM V2 is not thread-safe in any context.  There are known problems with
    ``mdbm_replace``, as well as other unqualified issues.

    MDBM V3 is thread-safe if used in a specific way:
      - Only 1 thread may use an MDBM handle at a time.
      - If an application needs concurrent MDBM access, then additional MDBM handles
        are required

    It is up to the app to decide how to ensure that a handle may be used by
    only 1 thread at a time.  Handles contains state, and you cannot use a
    handle concurrently across threads even if you are only doing fetches
    (reads).  An mdbm handle is a context object, and as is frequently done in
    reentrant programming, you pass a context object to a reentrant routine for
    that routine to read/write thread-specific state information so that it does
    not to use external (global) state.

    If you have a multi-threaded app, you will probably want to use dup handles
    to avoid remapping the same MDBM multiple times (use mdbm_dup_handle to
    create a new handle instead of mdbm_open).

    There are various approaches for using MDBM handles in a threaded context.
    For high-performance applications, it's recommended to use thread-local
    storage (TLS) containing an MDBM handle.  Alternatively, it would be
    possible to create a pool of MDBM handles, but that would require a lock to
    acquire a handle, which might have an unwanted impact on latency.

**Can I open an MDBM, then use it in forked child processes?**

    No, you cannot expect MDBM handles (MDBM \*) to be valid across fork() calls.
    You will have to call mdbm_open() in the child process after forking that child process.
    The MDBM handle contains data items such as lock counts that cannot be expected to
    remain consistent when copied to another process's memory space.

**How do I know which processes are using a particular MDBM?**

    lsof | grep *yourMdbmFile*


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===

   LocalWords: CRC DUP FSYNC MROW NOLOCK SHA TLS VM basedir
   LocalWords: YourFile btree buf cxx dev dup emacsen faq fsync grep init iter
   LocalWords: kern lockfile lockfiles lsof malloc maxdsiz mdbm mdbm's mdbms
   LocalWords: mdbmv mmap mmap'd mmaps mmutex nfs perl pid pmxmap pmxtools popen
   LocalWords: presize proc procs ramdisk rlimit sec smaps speeded stat str
   LocalWords: sync'd sync'ing sysctl tmp tmpfs vm yapache ydbm yjava
   LocalWords: yourMdbmFile yphp yroot ysys

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
