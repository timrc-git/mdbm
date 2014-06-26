.. $Id$
   $URL$

.. _mdbm_compare:

mdbm_compare
============

SYNOPSIS
--------

mdbm_compare [-kLvmM] [-f *file*] [F *format*] [-w *size*] *db_filename1* *db_filename2*

DESCRIPTION
-----------

``mdbm_compare`` compares 2 MDBMs and shows the differences between them.  Key
differences, value differences, or record differences may be shown.

OPTIONS
-------

-k
    Print key data.
-L
    Do not lock database.
-v
    Print value data.
-f file
    Specify an output file (defaults to STDOUT).
-F format
    Specify alternate output format (currently only cdb_dump format).
-m
    Missing.
    Only show entries missing from the second DB.
-M
    Not missing. Only show entries that exist (but differ) in both databases.
    NOTE: -m and -M are mutually exclusive.
-w size
    Use windowed mode with specified window size

RETURN VALUE
------------

* -1, error
*  0, no differences
*  1, differences founds

EXAMPLES
--------

::

  mdbm_compare -kv /tmp/foo.mdbm /tmp/bar.mdbm

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
   LocalWords: STDOUT cdb emacsen kLvmM kv mdbm trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
