.. $Id$
   $URL$

.. _mdbm_save:

mdbm_save
=========

SYNOPSIS
--------

mdbm_save [-chL] [-C *level*] *mdbm* *datafile*

DESCRIPTION
-----------

``mdbm_save`` dumps an MDBM to a file, optionally compressing the
file when completed.

NOTE: This feature is currently V2 only.

OPTIONS
-------

-h  Show help message
-c  Compress the b-tree before exporting data.
-L  Open with no locking.
-C level
    Specify compression level for output file.
    This corresponds to gzip levels 1-9.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_save -c -C 9 /tmp/foo.mdbm /tmp/datafile.txt

SEE ALSO
--------

mdbm_check(1), mdbm_compare(1), mdbm_compress(1), mdbm_copy(1), mdbm_create(1),
mdbm_digest(1), mdbm_dump(1), mdbm_export(1), mdbm_fetch(1), mdbm_import(1),
mdbm_purge(1), mdbm_replace(1), mdbm_restore(1), mdbm_save(1), mdbm_stat(1),
mdbm_sync(1), mdbm_trunc(1)

CONTACT
-------

mdbm-users <mdbm-users@yahoo-inc.com>

.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: chL emacsen gzip mdbm trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
