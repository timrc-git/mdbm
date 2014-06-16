.. $Id$
   $URL$

.. _getting-help:

Getting Help
============

For non-production issues, you can get help from these resources:

* report the issue to the mdbm-users mailing list
* Create a Bug ticket

   1. Provide configuration information:

      * *Highly recommended*: Attach output from ``mdbm_environment.sh``.
        This script gathers environment and MDBM information.
        This script has been tested only on RHEL.
      * Alternatively, manually provide:
          * Platform information, including version: RHEL4/5/6, FreeBSD4/6
             * ``uname -a``
             * ``cat /etc/redhat-release`` for RHEL
          * Output from:
             * ``mdbm_stat -H`` *mdbm-file*
             * ``mdbm_check`` *mdbm-file*
          * Provide ``mdbm_stat`` output for your mdbm
            (use an attachment, and do not remove any information)

   2. In your ticket's description provide application information:

      * If you have a crash, provide a gdb backtrace from ``backtrace full``
      * What kind of MDBM locking are you using?
          - *none*
          - *exclusive* (database-level locking)
          - *partition*, sometimes mistakenly called "page" locking
          - *shared*, sometimes called "read-write" locking, and new in MDBM v3
      * Do multiple processes have your MDBM open?  If so, have you tried
        stopping all processes and starting them to see if the problem goes
        away?
      * Is your application the only process/thread that has your MDBM open?
         * If you have a multi-threaded application, only 1 thread may use an MDBM
           handle at time (i.e., a single operation context such as lock, fetch, unlock)
      * Is the ``mdbm_replace`` utility being used by your application?  If it's
        being used, and you are experiencing MDBM corruption, does the
        corruption result soon after an MDBM is replaced?
      * What MDBM operation(s) were you doing when the fault occurred?
        The most common ones are:

        ================  ==============  ===============  ===============  ==============
        Fetch             Store           Delete           First            Next
        ================  ==============  ===============  ===============  ==============
        mdbm_fetch        mdbm_store      mdbm_delete      mdbm_first       mdbm_next
        mdbm_fetch_buf    mdbm_store_r    mdbm_delete_r    mdbm_first_r     mdbm_next_r
        mdbm_fetch_r      mdbm_store_str  mdbm_delete_str  mdbm_firstkey    mdbm_nextkey
        mdbm_fetch_dup_r                                   mdbm_firstkey_r  mdbm_nextkey_r
        mdbm_fetch_str
        ================  ==============  ===============  ===============  ==============

         * Provide your source for the failing MDBM operation, and its parameters
         * Provide your ``mdbm_open`` source and corresponding parameter values
      * Do you have a copy of your MDBM available for inspection?
      * Is your problem reproducible?  If so, how easy/hard is it to reproduce?
      * If problem resulted in a core dump, can you provide the stack trace?

.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: backtrace buf emacsen firstkey gdb kern mdbm nextkey str
   LocalWords: uname ycore ysys

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
