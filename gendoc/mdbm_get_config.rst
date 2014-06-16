.. $Id$
   $URL$

.. _mdbm_get_config:

mdbm_get_config
===============

SYNOPSIS
--------

mdbm_get_config [-o *outfile*] *infile.mdbm*

DESCRIPTION
-----------

``mdbm_get_config`` reads the configuration from an MDBM file and outputs
it in JSON format to STDOUT, unless -o option is specified.

OPTIONS
-------

-o outfile
    Output JSON format to specified *outfile* instead of STDOUT

RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

EXAMPLES
--------

::

  mdbm_get_config -o /tmp/mydb.json /tmp/mydb.mdbm

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
   LocalWords: STDOUT emacsen infile mdbm outfile trunc

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
