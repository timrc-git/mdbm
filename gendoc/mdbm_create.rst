.. $Id$
   $URL$

.. _mdbm_create:

mdbm_create
===========

SYNOPSIS
--------

mdbm_create [-23LNz] [-a *record-alignment*] [-c *cache-mode*] [-d *mbytes*] [-h *hash-function*] [-l *spill-size*] [-m *int*] [-p *bytes*] [-s *mbytes*] *mdbm*

DESCRIPTION
-----------

``mdbm_create`` creates a new MDBM according to the specified options.

OPTIONS
-------

-2  Create v2 format mdbm.
-3  [Default] Create v3 format mdbm.
-a record-alignment
    ================  ================
    Record Alignment  Description
    ================  ================
    8                 1-byte alignment
    16                2 byte alignment
    32                3 byte alignment
    64                4 byte alignment
    ================  ================
-c cache-mode
    ==========  ================================
    Cache-Mode  Description
    ==========  ================================
    1           LFU: least frequently used
    2           LRU: least recently used
    3           GDSF: greedy dual size frequency
    ==========  ================================

    The default is not to enable cache mode.
-D mbytes
    Specify initial/maximum size of db (default: no size limit).
-d mbytes
    Specify initial size of db (default: minimum).
    Suffix k/m/g may be used to override default of m.
-h hash-function
    Create DB using hash function *hash-function*.

    =============  ===============
    hash-function  Description
    =============  ===============
    0              CRC-32
    1              EJB
    2              PHONG
    3              OZ
    4              TOREK
    5              FNV32 [Default]
    6              STL
    7              MD5
    8              SHA-1
    9              Jenkins
    10             Hsieh SuperFast
    =============  ===============

    hash-function for MDBM V2 are 0-9 only.
-L  Enable large-object mode
-l spill-size
    Set the number of spill size bytes for data to be put on large-object heap.
    Large objects must be enabled with the -L option for this option to be
    applicable.

    The default is 75% of the page size.
-m int
    File permissions (default: 0666).
-p bytes
    Specify size of main db (remainder is large-object/overflow space).
    Suffix k/m/g may be used to override default of m.
-s mbytes
    Specify size of main db (remainder is large-object/overflow space).
    Suffix k/m/g may be used to override default of m.
    mbytes must evaluate to be a power-of-2 number of pages.
-z
    Truncate db if it already exists.

Do not use both options -d and -D on a command line.  If you use both, the last value
will be used for the mbytes value.  If -D is present, it will set the maximum db size.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

Create an MDBM with an initial size of 2GB:

  mdbm_create -p 2g /tmp/bar.mdbm

SEE ALSO
--------

mdbm_check(1), mdbm_compare(1), mdbm_copy(1), mdbm_create(1),
mdbm_digest(1), mdbm_dump(1), mdbm_export(1), mdbm_fetch(1), mdbm_import(1),
mdbm_purge(1), mdbm_replace(1), mdbm_restore(1), mdbm_save(1), mdbm_stat(1),
mdbm_sync(1), mdbm_trunc(1)

CONTACT
-------

mdbm-users <mdbm-users@yahoo-inc.com>


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: CRC EJB FNV GDSF Hsieh LFU LNz LRU MD OZ PHONG SHA STL
   LocalWords: SuperFast TOREK emacsen mbytes mdbm trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
