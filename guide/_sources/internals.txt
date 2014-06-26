.. $Id$
   $URL$

.. _internals:

Internal MDBM Information
=========================

**MDBM V3 File Format**

    The information below should only be used to understand the workings of MDBM!!  However,
    please do not use the information provided below to access data in MDBM Files, as it may 
    change in the future.  Compatibility with the data format below is NOT guaranteed,
    so please use only APIs and tools that were provided by the MDBM team.

    An MDBM file that uses the V3 file format always contains 2 or more pages.

    The MDBM file is broken up into "chunks", with each chunk being 1 or more contiguous
    whole physical pages.
    A page has a fixed size, and is different than a chunk.  A chunk is a logical contiguous
    collection of one or more pages, that is treated internally as though it is one logical page.
    In most cases, a chunk contains only one page, but a large object chunk or an oversized
    chunk will most likely be more than one page long.

    Each chunk starts with a mdbm_page_t structure (including the initial one).
    Each chunk contains a size (p_num_pages), and an offset to the previous chunk 
    (p_prev_num_pages). Together, these form a doubly linked list.

    The p_type field of mdbm_page_t near the start of each chunk identifies the chunk type, 
    which is one of MDBM_PTYPE_DIR, MDBM_PTYPE_DATA, MDBM_PTYPE_LOB, or MDBM_PTYPE_FREE.

    The first chunk (type DIR) contains the file header, directory bit vector, and 
    page-table.  Successive chunks include normal/data pages, oversized pages, large-object pages,
    and free-pages.

    Free-pages (type FREE) contain a "next" pointer (p_next_free), and the first page is 
    referenced by h_first_free, forming a singly-linked-list. Newly freed pages should 
    be inserted into the free list sorted by address, and adjacent free pages should be 
    coalesced into a single chunk.

    The directory is a linear array of bits, used to convert hash values of keys
    into a "logical page number".  These bits internally represent a trie, which tells
    MDBM how to find the logical page number where a particular data item is stored using
    a hash of the data item's key.  A logical page number determines the index into the page
    table, which is a linear array of integers that point to actual physical pages in the db.
    This provides a level of indirection that allows physical pages to be changed 
    (moved/expanded/defragged/rearranged) very quickly.

    Data pages (type DATA) contain a series of entry meta-data structures (mdbm_entry_t) 
    at the beginning (lowest VMA) of the page, and the actual key and value data grows 
    down from the end (highest VMA) of the page(s).  Oversized data chunk may span multiple 
    contiguous physical pages.  An oversized chunk has mdbm_page_t.p_num_pages of more than 1.

    The mdbm_entry_t structure contains the length and part of the hash for the key to enable
    fast scanning of a page. Deleted entries are signaled by a key length of 0, to allow
    for fast deletion, at the cost of possible defragmentation later.

    Large-object (type LOB) chunks contain db entries that exceed the capacity of a single 
    data page.  Because we return found entries to users as a pointer+length pair, we 
    cannot fragment these entries, so they are stored externally to the data page to 
    which they logically belong. The l_pagenum entry points back to the logical page 
    the large-object belongs to.

**File Format Notes**

    Some DBs may contain an extra, unmerged free-page at the end of the DB, which
    is not listed in the free-list.  The existence of h_num_pages (which gives the last
    physical page in the DB) and h_last_chunk (which gives the last used physical chunk
    in the db) creates some ambiguity, and the various check_xxx() functions, for example
    check_db_dir(), stop at h_last_chunk. It is currently unclear if this ambiguity is
    by design, or accidental.

**MDBM File Handle Notes**

    The MDBM file handle (MDBM \*) contains a lock count, which allows for very fast checking
    of whether an MDBM is locked by your process or thread.  This also means that MDBM handles
    cannot be shared among processes or threads.  It is also necessary because pthreads 
    does not provide a way to get the lock-count (how many times a lock has been locked).
    With a multi-process/single-thread use case, each process should first fork, then mdbm_open()
    the MDBM.  With a multi-threaded use case, one thread (e.g. by using pthread_once), should
    create a handle pool using mdbm_dup_handle(), and then allow other threads to acquire a
    handle, use it, and release it.

    With MDBM V4, partitioned locking and shared locking have a different
    path to the file used to open the ysys multi-mutex, so MDBM is better able to enforce the
    "one locking type per MDBM" rule, meaning that you should never mix shared and partitioned
    locking.


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: emacsen mdbm mdbm's mv pre

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
