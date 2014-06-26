.. $Id$
   $URL$

.. _mdbm_restore:

mdbm_restore
============

SYNOPSIS
--------

mdbm_restore [-hL] *mdbm* *datafile*

DESCRIPTION
-----------

``mdbm_restore`` recreates an MDBM from the data stored in *datafile*.
This datafile is created by *mdbm_save*.

NOTE: This feature is currently V2 only.

OPTIONS
-------

-h  Show help message
-L  Open with no locking.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_restore /tmp/bar.mdbm /tmp/datafile.txt

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
   LocalWords: emacsen hL mdbm trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
