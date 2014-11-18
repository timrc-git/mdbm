.. $Id$
   $URL$

.. _mdbm_delete_lockfiles:

mdbm_delete_lockfiles
=====================

SYNOPSIS
--------

mdbm_delete_lockfiles [-h] *mdbm*

DESCRIPTION
-----------

``mdbm_delete_lockfiles`` deletes all the lockfiles of a specified MDBM.
It requires the fully qualified path to an MDBM.
USE THIS UTILITY WITH EXTREME CAUTION!  Use only after you are completely done
with the MDBM and no other processes are using it.  Otherwise, the MDBM will
likely become corrupted.

OPTIONS
-------

-h  Show help message

RETURN VALUE
------------

Returns 0 upon success, 1 upon failure.

EXAMPLES
--------

  mdbm_delete_lockfiles /tmp/foo.mdbm

SEE ALSO
--------

mdbm_check(1), mdbm_compare(1), mdbm_copy(1), mdbm_create(1),
mdbm_digest(1), mdbm_dump(1), mdbm_export(1), mdbm_fetch(1), mdbm_import(1),
mdbm_purge(1), mdbm_replace(1), mdbm_restore(1), mdbm_save(1), mdbm_stat(1),
mdbm_sync(1), mdbm_trunc(1), mdbm_purge(1)

CONTACT
-------

mdbm-users <mdbm-users@yahoo-inc.com>

.. End of documentation
