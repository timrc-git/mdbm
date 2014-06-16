.. $Id$
   $URL$

.. _mdbm_dump:

mdbm_dump
=========

SYNOPSIS
--------

mdbm_dump [-CcdfkLPv] [-p *pagenum*] [-w *windowsize*] *mdbm*

DESCRIPTION
-----------

``mdbm_dump`` dumps the contents of an MDBM to STDOUT.

OPTIONS
-------

-C  Show cache info.
-c  Show chunks.
-d  Include deleted entries.
-f  Show free chunks only.
-k  Include key data.
-L  Do not lock MDBM.
-p pagenum
    Dump specified page.
-P  Dump page headers only.
-v  Include value data.
-w windowsize
    Specify the window size to use when accessing the MDBM.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_dump /tmp/bar.mdbm

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
   LocalWords: CcdfkLPv STDOUT emacsen mdbm pagenum trunc windowsize

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
