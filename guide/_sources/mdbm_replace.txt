.. $Id$
   $URL$

.. _mdbm_replace:

mdbm_replace
============

SYNOPSIS
--------

mdbm_replace [--cache *existing-cache-db*] [--clearcache|--refetch|--preload] *existing-mdbm* *new-mdbm*

DESCRIPTION
-----------

``mdbm_replace`` replaces an existing MDBM with a new MDBM.  Upon
replacement, the original MDBM is updated to notify all users that a
new MDBM exists; causing all existing MDBM users to re-open the (now
new) MDBM.  Essentially, the new mdbm is renamed to the old mdbm, and
the old mdbm is reopened.

If an associated MDBM cache db is specified, that cache's contents will be
updated so that they do not conflict with the contents of the backing store.

Please note also that the existing and new MDBM's must reside on the same
file-system.

If the preload option is specified, mdbm_replace will attempt to preload the
new MDBM into memory by using mdbm_preload() prior to performing the replace.
This will use up to 2 times the amount of RAM for a brief period because
preloading means that both MDBMs are briefly occupying RAM at the same time.
The old MDBM will continue to be mapped until all running processes have
performed an MDBM operation, causing them to map in the new MDBM.

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_replace /home/y/var/dbs/db.mdbm /tmp/new_db.mdbm

  mdbm_replace --cache /home/y/var/dbs/cachedb.mdbm /home/y/var/dbs/db.mdbm new_db.mdbm

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

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:


