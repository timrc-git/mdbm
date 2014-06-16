.. $Id$
   $URL$

.. _mdbm_sync:

mdbm_sync
=========

SYNOPSIS
--------

mdbm_sync [-hL] *mdbm*

DESCRIPTION
-----------

``mdbm_sync`` forces all dirty pages of an MDBM to be synced to disk.
By default, pages are sent to disk only when the system runs low on
physical memory and begins to swap.  (Unless mdbm_open() was called
with either the O_ASYNC or O_FSYNC option.)

OPTIONS
-------

-h  Show help message
-L  Open with no locking.

RETURN VALUE
------------

Returns 0 upon success, 1 upon failure.

EXAMPLES
--------

::

  mdbm_sync /tmp/bar.mdbm

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
   LocalWords: FSYNC emacsen hL mdbm trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
