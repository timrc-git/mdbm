.. $Id$
   $URL$

.. _mdbm_import:

mdbm_import
===========

SYNOPSIS
--------

mdbm_import [-23cDfhlTZ] [-i *infile*] [-s *hash-function*] [-S *storeflag*] [-p *pgsize*] [-d *dbsize*] [-L *lock-mode*] [-y *pgcnt*] [-z *spillsize*] *outfile.mdbm*

DESCRIPTION
-----------

``mdbm_import`` creates an MDBM from data files in db_dump or cdb_dump
format.  For more information on these formats, see mdbm_export(1).

OPTIONS
-------

-2  Create v2 format mdbm.
-3  Create v3 format mdbm.
-c  Import cdb_dump format (default db_dump format).
-d dbsize
    Create DB with initial dbsize *dbsize*.
-D  Delete keys with zero-length values.
-f  Fast mode (don't lock db while reading).
-h  Show help message.
-i infile
    Read from *infile* instead of STDIN.
-l  Open DB with large object support.
-L lock-mode

    =========  ===========
    lock-mode  Description
    =========  ===========
    exclusive  Exclusive access
    partition  Per-Partition access
    shared     Multiple readers, one writer access
    =========  ===========
-p pgsize
    Create DB with page size *pgsize*.
    Suffix k/m/g may be used to override the default of bytes.
-s hash-function
    Create DB using hash function *hash-function*.

    =============  ===============
    hash-function  Description
    =============  ===============
    0              CRC-32
    1              EJB
    2              PHONG
    3              OZ
    4              TOREK
    5              FNV32
    6              STL
    7              MD5
    8              SHA-1
    9              Jenkins
    10             Hsieh SuperFast
    =============  ===============

    hash-function for MDBM V2 are 0-9 only.
-S storeflag
    Use *storeflag* when storing values via mdbm_store.
    Numeric values for *storeflag*:

    ===========   ===============  ==========================================================
    Flag          Option           Description
    ===========   ===============  ==========================================================
    0             MDBM_INSERT      Stores will fail if there is an existing key
    1 [Default]   MDBM_REPLACE     Stores will replace an existing entry, or create a new one
    2             MDBM_INSERT_DUP  Stores will add duplicate entries for an existing key
    3             MDBM_MODIFY      Stores will fail if an existing key does not exist
    ===========   ===============  ==========================================================
-T  Input has no db_dump header
-y size
    Create DB, and set the large object spill size to *size*.
-z size
    Create DB, and set the large object limit size to *size*.
-Z  Truncate existing DB before importing.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_import -c -i /tmp/foo.data /tmp/newdb.mdbm

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
   LocalWords: CRC DUP EJB FNV Hsieh Jenkins MD OZ PHONG SHA STDIN STL SuperFast
   LocalWords: TOREK cDfhlTZ cdb dbsize emacsen infile mdbm outfile pgcnt pgsize
   LocalWords: spillsize storeflag trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
