.. $Id$
   $URL$

.. _mdbm_trunc:

mdbm_trunc
==========

SYNOPSIS
--------

mdbm_trunc [-fhLs] [-D *mbytes*] [-p *presize*] [-S *mbytes*] [-w *windowsize*] *mdbm*

DESCRIPTION
-----------

``mdbm_trunc`` truncates the specified MDBM to a single, empty page.
This removes all data from the DB.

Truncating an mdbm does not preserve any mdbm configuration attributes.
You must use the available options to explicitly set the attributes that you need.

Currently, there is no way to preserve large objects.

If the -D option is not specified, the size limit is reset to 0.
This allows that mdbm to split to it's maximum allowed size.

OPTIONS
-------

-D pages
    Limit the size of the resulting mdbm to the specified number of megabytes
    (or use suffix k/m/g to override the default unit of megabytes).
-f  Force the truncation to occur.  By default, the user is prompted to
    confirm the truncation.
-h  Show help message
-L  Open with no locking.
-p presize
    Specify initial size (presize) the db in units of bytes
    (or use suffix k/m/g to override the default unit of bytes).
-S mbytes
    Specify size of the main db (remainder is large-object/overflow space).
-s  Sync DB on exit.
-w windowsize
    Specify the size of the window through which the data should be accessed.

RETURN VALUE
------------

Returns 0 upon success, 1 upon failure.

EXAMPLES
--------

  mdbm_trunc -f /tmp/foo.mdbm
  mdbm_trunc /tmp/bar.mdbm

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
   LocalWords: emacsen fhLs mbytes mdbm presize trunc windowsize

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:

