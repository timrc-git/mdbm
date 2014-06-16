.. $Id$
   $URL$

.. _performance_page:

Performance
===========

.. _threaded-performance:

Multi-threaded MDBM Use and Performance
---------------------------------------

Multi-threaded applications need to use one MDBM handle per thread.

Dup'ed handles pull the per-thread info into the dup-ed handle, and share the memory-map, so
using ``mdbm_dup_handle()`` should improve performance over using ``mdbm_open`` in each thread.
The DB lock does double-duty as guardian of the MDBM (memory-map and backing file)
and of the per-thread-info.
This is great for reducing latency under low-contention, as MDBM only has to acquire one lock.

MDBM maintains a lock count and other information in the handle, which is the reason behind
the one-thread-per-handle rule, and it is needed because YLock doesn't have a way to get
the lock nesting count.

The other reason why MDBM maintains a lock count is performance. If the count is nonzero,
then the lock is held and MDBM can go off and do other things without a relatively expensive
lock call because acquiring a lock that's already held is still slower than an "if" statement.

For performance reasons, MDBM does not use the process ID and thread ID to see
who holds the lock because getting a the thread-ID is expensive.  We decided not to use this
approach because we determined that every operation that retrieves the lock count would need to
get the thread ID, and multi-process (but single thread) MDBM usage would take an unacceptable
performance hit.

.. _stats-callback-performance:

MDBM Performance Statistics collection
--------------------------------------

To capture performance statistics we've recently introduced the an API: ``mdbm_set_stats_func``
that uses a user supplied callback function to capture counts of fetch, store, or delete
operations and also optionally the time, in microseconds, that it takes to perform that operation.

The overhead of calling an empty callback is less than 1%, and we were trying to make sure that
the internal mechanism we use to provide time delta information to the callback has an
overhead that is 5% or less of an MDBM fetch or store. 

The first step we took was to measure the overhead of timed operations, which then pass
the data to the callback.

Performance Analysis of MDBM Timed Operations Using a Callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We measured the overhead introduced by measuring the time, as a percent of an MDBM fetch or store.

The measurement of the overhead of the MDBM performance statistics callback
API produced the following results with MDBM V3.
With MDBM V3, the MDBM code used ``ysys_get_usec`` to measure the time it takes to perform a
fetch or store, with the following results:

    ================  ========  =========
    Data Size         Store     Fetch
    ================  ========  =========
    32 bytes          9.5%      10%
    100 bytes         8.5%      9.5%
    1 Kbytes          6.1%      6.8%
    2 Kbytes          6.2%      6.5%
    ================  ========  =========

Later we measured the overhead for MDBM V4.  MDBM V4 uses the Linux API
``clock_gettime(CLOCK_MONOTONIC)`` to measure how many microseconds it takes to perform a fetch,
store or delete.  We also measured MDBM V4's overhead when using different APIs, too, in order
to find a way to reduce the overhead.  We compared that to using the CPU TimeStamp Counters (TSC),
using assembly code, with and without getting the CPU ID.
For a 100 byte fetch or store, the overhead was:

    =====================================  ========
    API                                    Overhead
    =====================================  ========
    ``clock_gettime(CLOCK_MONOTONIC)``     11%
    Reading the TSC                        3%
    Reading the TSC + get the CPU ID       20%
    ``clock_gettime(PROCESS_CPUTIME_ID)``  80%
    =====================================  ========

.. _improving-stat-performance:

Using TSC to improve performance 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Given the above results, we decided to modify MDBM to use the CPU TSC to improve the performance
of statistics monitoring time measurement.  Using the TSC is done with assembly code and is very
Intel (and possibly AMD) CPU-specific.  We added an API, ``mdbm_set_stat_time_func``, to allow
us to change the time measurement mechanism.  We added a wrapper function, ``tsc_get_usec``
that reads the TSC, and every 2 seconds uses ``gettimeofday`` to calibrate the time.

The code is not designed to provide perfectly accurate results in a setup where multiple threads
access TSC-based timer data simultaneously.  The wrapper therefore maintains a high water mark of
the TSC, in order to minimize the error introduced by multiple threads accessing TSC-based timers
at the same time.

The overhead produced, over several runs, for a 1024 byte operation, by ``tsc_get_usec`` is:

    ========  =========
    Store     Fetch
    ========  =========
    3.97%     4.86%
    3.42%     4.02%
    3.53%     4.39%
    3.79%     5.09%
    2.76%     4.06%
    4.16%     4.44%
    4.05%     4.65%
    3.64%     4.41%
    4.18%     4.79%
    3.98%     3.58%
    3.72%     5.08%
    3.41%     3.67%
    3.45%     4.22%
    4.12%     4.00%
    ========  =========

The average of the above runs, for fetches and stores is:

    ========  =========
    Average   Average
    Store     Fetch
    ========  =========
    3.73%     4.37%
    ========  =========

Since using the CPU's TSC took less than the 5% performance requirement, we decided to
add an API that allows us to switch to using the TSC from the normal mechanism of time collection,
which is ``ysys_get_usec`` for MDBM V3 and ``clock_gettime(MONOTONIC)`` for MDBM V4.
To use the TSC, we need to call ``mdbm_set_stat_time_func(MDBM_CLOCK_TIME_TSC)``.


.. _mdbm_preload-performance:

Performance Comparison of mdbm_preload
--------------------------------------

We have compared the performance of iterating through a half-full MDBM of various sizes.
Using MDBM V4 on RHEL6, we compared iteration using mdbm_first and mdbm_next against using
mdbm_preload() prior to iterating, and we've seen the following performance improvment:

    =========  ===========
    MDBM Size  preload
               improvement
    =========  ===========
    256MB      61%
    1GB        63.3%
    2GB        55.8%
    4GB        70.4%
    8GB        66.1%
    =========  ===========

Using MDBM V3 on RHEL4, we've seen the following performance improvement:

    =========  ===========
    MDBM Size  preload
               improvement
    =========  ===========
    256MB      38.7%
    1GB        43.6%
    2GB        43.1%
    4GB        56.3%
    8GB        54.4%
    =========  ===========

.. End of documentation
