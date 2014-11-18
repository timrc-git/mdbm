.. $Id$
   $URL$

.. _mdbm_export_splitter:

mdbm_export_splitter
====================

This utility is part of the mdbm_tools package.

SYNOPSIS
--------

mdbm_export_splitter [-o *output-directory*] [-i *input-directory*] [-p *source-file-prefix*] [-c] [-b *bucket-count*] [--hash-function *hash-function*] *source files* ...

Source files must be provided in the specified input directory, on the command
line, or both.

DESCRIPTION
-----------

``mdbm_export_splitter`` reads the source input files and outputs their contents
dispersed in the created bucket files according to a per-key hash code.
The source input files must be in *cdb_dump* or *db_dump* format.
All source files must be in same format, either all *cdb_dump* or all *db_dump*.
The resulting bucket files will contain the data in the same format as the source files.

OPTIONS
-------

-b bucket-count
    This specifies the number of buckets in which to distribute record data.
-c  The source input files will be in CDB format
--hash-function hash-function
    MDBM specific numeric code representing the hash method.
    The numeric code or the associated name may be specified.
    Following are the possible values for *hash-code* number or their associated name:

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
-i input-directory
    The input directory *input-directory* where the source files that contain the
    records in *cdb_dump* or *db_dump* format.
-o output-directory
    The output directory *output-directory* where the bucket files will be written.
-p source-file-prefix
    The prefix used to identify source data files in an input directory.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_export_splitter -b 15 --hash-function 5 -p dd db_dump_records
  mdbm_export_splitter -b 20 --hash-function 5 -p cdb -o /tmp/cdb -c cdb_records

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
   LocalWords: CRC EJB FNV Hsieh Jenkins MD OZ PHONG SHA STL TOREK cdb emacsen
   LocalWords: mdbm trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
