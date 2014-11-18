.. $Id$
   $URL$

.. _mdbm_check:

mdbm_check
==========

SYNOPSIS
--------

mdbm_check [-hLV] [-d *level*] [-p *pagenum*] [-v *verbosity*] [-w *windowsize*] [-X *version*] *mdbm*

DESCRIPTION
-----------

``mdbm_check`` walks through the entire MDBM and performs a number of consistency checks.

OPTIONS
-------

-h    Display help message.
-L    Do not lock database during check.
-V    Display MDBM file version.  Requires the -v option, too.
-d check_level

      =====  ===========
      Level  Description
      =====  ===========
      0      Check data pages only
      1      Check header and chunks
      2      Check directory pages, header and chunks
      3      Check data pages and large objects.
      =====  ===========

-p pagenum   Check the specified page.
-v verbosity

      =====  ===========
      Level  Description
      =====  ===========
      0      Minimal information
      1      More information.
      =====  ===========

-w windowsize  Specifies the window size in bytes (use k/m/g to use a different unit).
-X version     Verify that the MDBM version is the one specified by version.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_check /tmp/bar.mdbm

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
   LocalWords: emacsen hLV mdbm pagenum trunc windowsize

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
