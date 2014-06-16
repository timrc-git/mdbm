.. $Id$
   $URL$

.. _mdbm_copy:

mdbm_copy
=========

SYNOPSIS
--------

mdbm_copy [-hlL] *source-mdbm* *destination-mdbm*

DESCRIPTION
-----------

``mdbm_copy`` copies an MDBM.

The locking options affect how data is locked during a copy.  If there are no
MDBM writers (in other processes) for the duration of a copy, an unlocked copy
may be used.  By default (no locking options specified), the internal header
data structures are locked while they are copied, and data pages are
individually locked for copying.  We therefore recommend that applications that
store cross-page "semantically linked" records in an MDBM cannot use mdbm_copy
simultaneously, as only some of such data items may get copied by mdbm_copy.


OPTIONS
-------

-h  Show help message
-l  Lock the entire database for the duration of the copy
-L  Open with no locking

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_copy /tmp/bar.mdbm
  mdbm_copy -l /tmp/bar.mdbm
  mdbm_copy -L /tmp/bar.mdbm

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
   LocalWords: emacsen mdbm trunc
   LocalWords: hlL

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
