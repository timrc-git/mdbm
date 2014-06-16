.. $Id$
   $URL$

.. _mdbm_purge:

mdbm_purge
==========

SYNOPSIS
--------

mdbm_purge [-L] [-w *windowsize*] *mdbm*

DESCRIPTION
-----------

``mdbm_purge`` purges the specified MDBM.  The existing file allocation and
configuration is kept.  All of the keys are logically deleted.

OPTIONS
-------

-h  Show help message
-L  Open with no locking.
-w windowsize
    Specify the size of the window through which the data should be accessed.
    Suffix k/m/g may be used to override default of b.

RETURN VALUE
------------

Returns 0 upon success, 1 upon failure.

EXAMPLES
--------

  mdbm_purge /tmp/foo.mdbm
  mdbm_purge -L -w 1024 /tmp/bar.mdbm

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
   LocalWords: emacsen fhLs mbytes mdbm presize purge trunc windowsize

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:

