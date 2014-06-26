.. $Id$
   $URL$

.. _mdbm_big_data_builder:

mdbm_big_data_builder.pl
========================

SYNOPSIS
--------

mdbm_big_data_builder.pl [-c] [-d *dbsize*] [--delete-intermediate-files] [-D] [-h|--help] [--input-directory *directory*] [--input-prefix *prefix*] [-l] [-mdbm *name*] [-n|--num-buckets *bucket-count*] [--nodelete-intermediate-files] [--output-directory *directory*] [-p *pagesize*] [-s|--hash-function *hash-function*] [-S *store-flag*] [-y *maxpages*] [-z *spillsize*] *input-files* ...

DESCRIPTION
-----------

``mdbm_big_data_builder.pl`` should be used for speeding up the build for a
bigger-than-physical-memory MDBM.  ``mdbm_big_data_builder.pl`` takes specified
source data files and builds a V3 MDBM.  Multiple input files may be specified
for creating a new V3 database. A n existing V3 database may be used to add more
data to it.  An mdbm is built in a virtual-memory restricted manner.  Only a
contiguous range of virtual memory is constructed at a time to avoid
paging/swapping.

OPTIONS
-------

-c  This is the cdb format flag, input data files will be cdb format.
-d dbsize
    Used by *mdbm_import* to presize the database. Initializing an mdbm to the
    expected final size will avoid dynamically growing that mdbm, and reduce
    build time.
--delete-intermediate-files
    Delete the buckets when done. Default is to keep them in the output directory.
    This is a negatable option (i.e., *--nodelete-intermediate-files*).
-D  Delete keys flag, used by *mdbm_import*.
-h, --help
    Shows usage of the script then exits.
--input-directory directory
    The directory containing input data files in CDB or db_dump format.
--input-prefix prefix
    This is the prefix of name of input data files, used to identify what
    files are the source input data files.
-l  This is the large object support flag, used by *mdbm_import*.
--mdbm name
    This is the name of the mdbm file to be created or if it already exists,
    have more data added to it.
--nodelete-intermediate-files  Do not delete the buckets from the output directory when done. This is the default.
-n, --num-buckets bucket-count
    Number of buckets to use when splitting the source input data
--output-directory directory
    This is a REQUIRED parameter. It specifies the directory used to output
    the bucket files and new database.
-p pagesize
    This option is used by *mdbm_import* when creating a new mdbm.
-s, --hash-function hash-function
    This is a REQUIRED parameter. The valid values can be either a number
    or the associated name (case is ignored) as follows:

    =============  =======
    hash-function  Name
    =============  =======
    0              CRC32
    1              EJB
    2              PHONG
    3              OZ
    4              TOREK
    5              FNV32
    6              STL
    7              MD5
    8              SHA1
    9              Jenkins
    10             Hsieh
    =============  =======

-S store-flag
    This option is used by *mdbm_import*.

    ===========   ===============  ==========================================================
    Flag          Option           Description
    ===========   ===============  ==========================================================
    0             MDBM_INSERT      Stores will fail if there is an existing key
    1 [Default]   MDBM_REPLACE     Stores will replace an existing entry, or create a new one
    2             MDBM_INSERT_DUP  Stores will add duplicate entries for an existing key
    3             MDBM_MODIFY      Stores will fail if an existing key does not exist
    ===========   ===============  ==========================================================
-y maxpages
    Used by *mdbm_import*
-z spillsize
    Used by *mdbm_import*

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

Create a new V3 DB and load it with the data from *cdb_dump* format source files
in the input directory */tmp/cdb*::

  mdbm_big_data_builder.pl -id /tmp/cdb -if cdb -s 5 -d 2 -S 2 -mdbm saturn.mdbm -od /tmp/saturnoutput -c -nb 10

Load an existing V3 DB with data form *db_dump* format source files::

  mdbm_big_data_builder.pl -id /tmp/db_dump -mdbm saturn.mdbm -od /tmp/saturnoutput -ip bigdata -s 5 dbdumpdata1 dbdumpdata2

SEE ALSO
--------

mdbm_check(1), mdbm_dump(1), mdbm_fetch(1),
mdbm_replace(1), mdbm_restore(1), mdbm_save(1), mdbm_stat(1),
mdbm_sync(1), mdbm_trunc(1), mdbm_export(1), mdbm_import(1)

CONTACT
-------

mdbm-users <mdbm-users@yahoo-inc.com>


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: CRC EJB FNV Hsieh Jenkins MD OZ PHONG SHA STL TOREK bigdata cdb
   LocalWords: dbdumpdata dbsize emacsen ip maxpages mdbm nb nodelete num od
   LocalWords: pagesize presize saturn spillsize trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
