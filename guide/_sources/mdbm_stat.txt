.. $Id$
   $URL$

.. _mdbm_stat:

mdbm_stat
=========

SYNOPSIS
--------

mdbm_stat [-hHLu] [-i keyword] [-w *windowsize*] *mdbm*

DESCRIPTION
-----------

``mdbm_stat`` shows various usage statistics about an MDBM.

OPTIONS
-------

-h  Show help message.
-H  Show MDBM file header information.
-i freepg
   Show total number of free pages and free-list pages, or
-i ecount
   Print the number of entries
-L  Do not lock database during check.
-o  Show additional information on oversized pages
-u  Show utilization statistics.
-w windowsize
   Specifies the window size in bytes (use k/m/g to use a different unit).

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_stat /tmp/bar.mdbm

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
   LocalWords: emacsen hHLu mdbm trunc windowsize

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
