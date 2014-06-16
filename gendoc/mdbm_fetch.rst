.. $Id$
   $URL$

.. _mdbm_fetch:

mdbm_fetch
==========

SYNOPSIS
--------

mdbm_fetch [-hLPv] [-w *windowsize*] *mdbm* *key*

DESCRIPTION
-----------

``mdbm_fetch`` fetches all occurrences of *key* from the specified
MDBM.

If values were inserted into the MDBM with the MDBM_INSERT_DUP flag
set, more than one value may exist.  These values are displayed in
random order.

OPTIONS
-------

-h  Show help message
-L  Open with no locking.
-p  Print page number.
-P  Use partitioned locking.
-v  Show verbose information.
-w windowsize
    Specify the size of the window through which the data should be accessed.

RETURN VALUE
------------

Returns 0 upon success, 1 upon failure.

EXAMPLES
--------

::

  mdbm_fetch /tmp/bar.mdbm mykey

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
   LocalWords: DUP emacsen hLPv mdbm mykey trunc windowsize

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
