.. $Id$
   $URL$

.. _mdbm_export:

mdbm_export
===========

SYNOPSIS
--------

mdbm_export [-hfc] [-o *outfile*] *infile.mdbm*

DESCRIPTION
-----------

``mdbm_export`` exports an MDBM into a human-readable format file.

By default, the output file is in Berkley DB's db_dump format:

  http://www.sleepycat.com/docs/utility/db_dump.html

By using the *-c* flag, this format may be changed to cdb format:

  http://cr.yp.to/cdb/cdbmake.html

OPTIONS
-------

-h  Shows help message
-f  Fast mode (don't lock db while reading). Please use -L none
-L lockmode
     Locking mode - any, none, exclusive, partition, or shared
-c  Export cdb_dump format (default db_dump format)
-o outfile
     Write output to this file, instead of STDOUT.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

  mdbm_export -c -o /tmp/foo.data /tmp/foo.mdbm

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
   LocalWords: STDOUT cdb emacsen hfc infile mdbm outfile trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
