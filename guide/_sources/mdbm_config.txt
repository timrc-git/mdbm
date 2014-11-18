.. $Id$
   $URL$

.. _mdbm_config:

mdbm_config
===========

This utility is part of the mdbm_tools package.

SYNOPSIS
--------

.. The following long line is required for formatting purposes.

mdbm_config [-cDhlv] [-a *analysislevel*] [-d dbsize] [-f *coefficientvalue*] [-i *infile*] [-L *lockmode*] [-n *objcount:objcount*] [-o *opcount*] [-p *pagesize*] [-r *value*] [-s *hash-function*] [-t *targetcapacity*] [-w *percentwrites*] *outfile.mdbm*

DESCRIPTION
-----------

``mdbm_config`` analyzes and benchmarks the data, then sets up an MDBM using
data files in db_dump or cdb_dump format.  This tool must be used with MDBM V3
or later.


OPTIONS
-------

-a analysislevel
    =====  ===========
    Level  Description
    =====  ===========
    1      Quick
    2      Medium
    3      Complete [Default level]
    =====  ===========
-c  Import using cdb_dump format (default is db_dump format or MDBM format for
    files with the .mdbm extension).
-d dbsize
    Create DB with initial MDBM size *dbsize*.
-D  Delete keys with zero-length values retrieved from import files.
-f coefficientvalue
    Set the spill-page coefficient value to *coefficientvalue*.
-h  Show help message.
-i infile
    Read from *infile* instead of stdin.  Files with a .mdbm extension are assumed to be MDBMs.
-l  Assume large object support is required for this MDBM setup.
-L lockmode
    Set locking mode to be one of exclusive, partition, or shared.
-n objcount
   (or -n objcount:objcount).
    If only 1 object count is provided, set the upper target for the number of
    objects per page (default is 100).  If 2 colon-separated object counts are
    provided, set the upper and lower target to the number of objects per page.
    The lower target's default is 75% of the upper target of objects per page.
-o opcount
    The total number of read and write operations to perform when benchmarking.
-p pagesize
    Create DB with pagesize *pagesize*.
    Page size will be rounded to system page size.
    Suffix k/m/g may be used to override the default of bytes.
-r coefficientvalue
    Set the ratio of object over-the-bound (-n option) penalty to capacity utilization penalty.
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
-t targetcapacity
    Set the target MDBM capacity utilization rate (1-75%).
    Default capacity utilization rate is 50%.
-v  Generate verbose output.
-w percentwrites
    When performing benchmarks, specify what floating point percentage of
    accesses are writes.  The default if zero.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_config -c -i /tmp/foo.data /tmp/newdb.mdbm

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
   LocalWords: CRC EJB FNV Hsieh Jenkins MD OZ PHONG SHA STL SuperFast TOREK
   LocalWords: analysislevel cDhlv cdb coefficientvalue dbsize emacsen infile
   LocalWords: lockmode mdbm objcount opcount outfile pagesize percentwrites
   LocalWords: stdin targetcapacity trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
