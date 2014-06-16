.. $Id$
   $URL$

.. _restrictions:

Restrictions
============

**Using mv on open MDBMs is not supported**

    Using ``mv`` is not supported on open MDBMs because the lock files are based
    on an mdbm's path at the time of open.  Apps that had an mdbm and its lock
    files open before a ``mv`` should continue to work.  However, an app that
    opens the post-``mv`` mdbm will use a different set of lock files based on
    the new mdbm's path. You'd have 2 sets of lock files (pre-``mv`` and
    post-``mv``) and end up with uncoordinated access.

**Using mixed mode on RHEL6 is not supported**

    You have to work either with all native RHEL6 packages, or use all RHEL4 packages.
    For native RHEL6, please use MDBM V4.  For RHEL4 and 4-on-6, please use MDBM V3.
    To start native RHEL6 development, use a clean yroot without any RHEL4 packages
    (yinst restore 1 --empty restore; yinst clean -inactive -full -yes), use
    yinst 7.136.6217 or higher, and yinst set root.os_restriction=rhel-6.x


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: emacsen mdbm mdbm's mv pre

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
