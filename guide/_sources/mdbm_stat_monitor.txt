.. $Id$
   $URL$

.. _mdbm_stat_monitor:
.. _ mdbm_stat_monitor.pl:

mdbm_stat_monitor.pl
====================

SYNOPSIS
--------

mdbm_stat_monitor.pl [-dh] -s *ymon-scoreboard-name* *JSON-file* ...

DESCRIPTION
-----------

The ``mdbm_stat_monitor.pl`` script uses :ref:`mdbm_stat` to extract MDBM structure
and data characteristics for one or more MDBMs.

Metric values are emitted to a ymon scoreboard.  These Metrics are emitted
according to various ordered dimensions defined by A JSON file.  If the the
metrics for multiple MDBMs are being emitted to the same scoreboard, all MDBMs
must use that same setup for dimensions.  For example, if one JSON file
specifies dataset-name, partition number, and replication number dimensions,
then all JSON files must the same dimensions, and in the same order.

Applications should use a cron job to publish these metrics on a regular
interval to assist in MDBM capacity planning.

A JSON file contains:

    - An MDBM path
    - Whether or not to lock the MDBM when using mdbm_stat
    - A set of ordered dimensions (minimum of 1 dimension, *dataset name*, is required).
        - dataset name
        - partition number
        - replica number
    - (optional) A page utilization percentage for it to be considered full.
      This used is computing metrics for MDBM fullness.

A sample JSON file::

  {
    "mdbmPath" : "foo.mdbm",
    "lockMdbm" : false,
    "fullPercent" : 85,
    "dimensions" : [{"dataset" : "somedataset"}, {"partition" : 3}, {"replica" : 0}]
  }

OPTIONS
-------

-d, --debug
    Display debug information.

-h, --help
    Shows this help message.

-s, --scoreboard ymon-scoreboard-name
    ymon scoreboards reside in /home/y/var/scoreboards/.
    Only the basename is used from the ymon-scoreboard parameter.

*JSON-file*
    JSON format files describing ymon metric dimensions for an MDBM.
    Use multiple JSON files if there are multiple MDBMs.
    If stats are emitted for multiple MDBMs, they must all use the same dimensions.
    The JSON "mdbmPath" and "dimensions" settings are required.
    The JSON "dimensions" setting must specify at least 1 dimension.


RETURN VALUE
------------

Returns 0 upon success, non-zero upon failure.

OUTPUT
------

Metric values are emitted to a *ymon-scoreboard-name*.

In the following table, *normal* pages are pages that are *not* used for large
objects or oversized pages.

+-----------------+--------+-------------+---------------------------------------------------------+
| Metric Name     | YMon   | Aggregation | Description                                             |
|                 | Metric |             |                                                         |
+=================+========+=============+=========================================================+
| MstatBigPgFull  | stat   | last        | A very rough measure for the number of LOOP pages near  |
|                 |        |             | full.  This count reflects the number of rows (not the  |
|                 |        |             | number of pages) in the histogram data that are greater |
|                 |        |             | than or equal to the JSON "fullPercent" parameter       |
|                 |        |             | (default is 85%).  The "max" value in the histogram's   |
|                 |        |             | "Bytes" column is used calculate this percentage.       |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatFrPg       | stat   | last        | Free pages count                                        |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatKeyAvg     | stat   | last        | Key average bytes                                       |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatKeyMax     | stat   | last        | Key maximum bytes                                       |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatKeyMin     | stat   | last        | Key minimum bytes                                       |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatKvAvg      | stat   | last        | Key+Value average bytes                                 |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatKvMax      | stat   | last        | Key+Value maximum bytes                                 |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatKvMin      | stat   | last        | Key+Value minimum bytes                                 |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatLobAvg     | stat   | last        | Large Object average bytes                              |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatLobEnt     | stat   | last        | Large Object Store store entry count                    |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatLobMax     | stat   | last        | Large Object maximum bytes                              |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatLobMin     | stat   | last        | Large Object minimum bytes                              |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatLobSz      | stat   | last        | Large Object store entry bytes                          |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatNormEnt    | stat   | last        | Normal store entry count                                |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatNormPgFull | stat   | last        | A very rough indicator for the number of normal         |
|                 |        |             | (non-LOOP) pages near full.  This count reflects the    |
|                 |        |             | number of rows (not the number of pages) in the         |
|                 |        |             | histogram data that are greater than or equal to        |
|                 |        |             | the JSON "fullPercent" parameter (default is 85%).      |
|                 |        |             | The "max" value in the histogram's "Bytes" column is    |
|                 |        |             | used calculate this percentage.                         |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatNormSz     | stat   | last        | Normal store entry bytes                                |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatOvhSz      | stat   | last        | Overhead bytes                                          |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatPgEntAvg   | stat   | last        | Entries per page average count                          |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatPgEntMax   | stat   | last        | Entries per page maximum count                          |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatPgEntMin   | stat   | last        | Entries per page minimum count                          |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatTime       | stat   | last        | Elapsed seconds to stat an MDBM                         |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatTotEnt     | stat   | last        | Total entry count                                       |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatTotPg      | stat   | last        | Total number of pages                                   |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatTotSz      | stat   | last        | Total bytes                                             |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatTotUtil    | stat   | last        | Total bytes utilization, percentage                     |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatValAvg     | stat   | last        | Value average bytes                                     |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatValMax     | stat   | last        | Value maximum bytes                                     |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatValMin     | stat   | last        | Value minimum bytes                                     |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatUsedPg     | stat   | last        | Used pages count                                        |
+-----------------+--------+-------------+---------------------------------------------------------+
| MstatUnusedSz   | stat   | last        | Unused bytes                                            |
+-----------------+--------+-------------+---------------------------------------------------------+


Unlike other metrics, MstatNormPgFull and MstatBigPgFull are not simply
extracted from stat output.  Instead, all of histogram data is evaluated to find
the various minimum free byte points.  That section of the histogram and the
configuration input (ex., percent of page is used) is used to determine the
number of pages approaching fullness.

EXAMPLES
--------

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
   LocalWords: JSON MstatBigPgFull MstatFrPg MstatKeyAvg MstatKeyMax MstatKeyMin
   LocalWords: MstatKvAvg MstatKvMax MstatKvMin MstatLobAvg MstatLobEnt
   LocalWords: MstatLobMax MstatLobMin MstatLobSz MstatNormEnt MstatNormPgFull
   LocalWords: MstatNormSz MstatOvhSz MstatPgEntAvg MstatPgEntMax MstatPgEntMin
   LocalWords: MstatTime MstatTotEnt MstatTotPg MstatTotSz MstatTotUtil
   LocalWords: MstatUnusedSz MstatUsedPg MstatValAvg MstatValMax MstatValMin
   LocalWords: YMon basename cron dh emacsen fullPercent lockMdbm mdbm mdbmPath
   LocalWords: oversized somedataset trunc ymon

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
