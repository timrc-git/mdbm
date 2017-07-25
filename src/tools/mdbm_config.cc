/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/param.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "mdbm.h"
#include "mdbm_util.h"

#include "bench_data_utils.hh"
#include "bench_existing.cc"

// Need to be able to import data using mdbm_import, so include it:
#define main imp_main_wrapper
#include "mdbm_import.cc"
#undef main

// ** Definitions

#define PROG "mdbm_config"

static const int INITIAL_PAGESZ = 65536;
static uint SYS_PAGESIZE;    // system page size
static uint CONFIG_MAXPAGE = 1 << 24;   // 16MB (subtract system page size later)
static const int MBYT = 1024 * 1024;
static const uint ENTRY_OVERHEAD  =   8;     // MDBM per-entry overhead is 8 bytes
static const uint INITIAL_SIZE_MDBM = 2000;  // Default to big MDBM to allow for fast growth

// Spillsize is configurable, but assume it is a constant 75% for now
static const double MDBM_SPILLSIZE = 75.0;
// Assign higher penalty to pages at 72% or higher
static const double ALMOST_SPILLING = MDBM_SPILLSIZE - 3.0;
// Multiply "penalty score" by ALMOST_SPILLING_PENALTY (2.0*0.6) for usage above 72%
static const double ALMOST_SPILL_PENALTY = 0.6;
// Multiply "penalty score" by SPILL_PENALTY (2.0) for usage above 75%
static double SpillPenaltyCoefficient = 2.0;

// Default for multiplying the overbound penalty by this ratio, then adding the capacity penalty
static double OverboundToCapacityRatio = 10.0;

static int HashFuncs[] = { MDBM_HASH_CRC32, MDBM_HASH_EJB, MDBM_HASH_PHONG, MDBM_HASH_OZ,
                           MDBM_HASH_TOREK, MDBM_HASH_FNV, MDBM_HASH_STL, MDBM_HASH_MD5,
                           MDBM_HASH_SHA_1, MDBM_HASH_JENKINS, MDBM_HASH_HSIEH };
static uint HashNum = sizeof(HashFuncs) / sizeof(int);

static bool PrintVerbose = false;
// Number of total operations (reads+writes) to perform when benchmarking
static uint OpCount = 200000;

#define ROUND_TO_SYSPAGE(value)  ((((value) + SYS_PAGESIZE - 1) / SYS_PAGESIZE) * SYS_PAGESIZE)

using namespace std;

typedef pair<uint32_t, int> PagesizeAndHash;

typedef struct {
    uint64_t normal;
    uint64_t lob;
    uint64_t total;
    uint64_t overhead;
    uint64_t unused;
    uint64_t dbsize;

    uint32_t pagesize;
    int      hashfunc;
    string   hashname;
    uint32_t totalpages;
    uint32_t usedpages;
    uint32_t freepages;

    uint32_t totalChunks;
    uint32_t normalChunks;
    uint32_t oversizedChunks;
    uint32_t largeObjChunks;

    uint64_t entryCount;
    uint64_t largeEntryCount;

    uint32_t minkv;
    uint32_t meankv;
    uint32_t maxkv;
    uint32_t minval;
    uint32_t meanval;
    uint32_t maxval;
    uint32_t minkey;
    uint32_t meankey;
    uint32_t maxkey;
    uint32_t minlob;
    uint32_t meanlob;
    uint32_t maxlob;
    uint32_t minpgentries;
    uint32_t meanpgentries;
    uint32_t maxpgentries;
    uint32_t maxPageUsedSpace;
    uint32_t overUpperObjectCount;   // Count # of objects above upper object bound

    uint32_t bucketNumPages[MDBM_STAT_BUCKETS];
    uint32_t bucketNumEntries[MDBM_STAT_BUCKETS];
    uint32_t bucketMinSize[MDBM_STAT_BUCKETS];
    uint32_t bucketByteSum[MDBM_STAT_BUCKETS];
    uint32_t bucketMaxSize[MDBM_STAT_BUCKETS];
    uint32_t bucketMinFree[MDBM_STAT_BUCKETS];
    uint32_t bucketSumFree[MDBM_STAT_BUCKETS];
    uint32_t bucketMaxFree[MDBM_STAT_BUCKETS];

} MdbmConfigStats;

static MdbmConfigStats notFound;
static map<uint, MdbmConfigStats>  statsMap;
static map<uint, double>  scoreMap;   // The "penalty" score

static const char *SETUP_BASE_DIR = "/tmp/mdbm/config/";

// ** Code

inline static void
createTmp()
{
    string cmd("mkdir -p ");
    cmd += SETUP_BASE_DIR;

    system(cmd.c_str());
}


static void
setupUsage()
{
    fprintf(stderr, "\
usage: " PROG " [options] outfile.mdbm\n\
  -a level        Analysis level: 1=quick,2=medium,3=complete (default)\n\
  -c              Input is in cdbdump format (default db_dump)\n\
  -d dbsize       Create DB with initial <dbsize> DB size.\n\
                  Suffix g may be used to override default of m.\n\
  -D              Delete keys with zero-length values\n\
  -f value        Set the spill-page penalty coefficient\n\
  -h              Help\n\
  -i infile       Read from <infile> instead of stdin\n\
  -l              Create MDBM with large object support\n\
  -L lockmode     Specify the type of locking to use for benchmarking:\n\
                  exclusive  -  Exclusive locking (default)\n\
                  partition  -  Partition locking (requires a fixed size MDBM)\n\
                  shared     -  Shared locking\n\
                  nolock     -  Do not lock MDBM when benchmarking\n\
  -n objcount     Upper target on the average number of objects per page (default is 50).\n\
       or\n\
  -n #obj:#obj    Upper and Lower target on the average number of objects per page\n\
                  (default is 75%% of upper bound: 37 objects if -n option is not specified).\n\
  -o opcount      Total number of read and write operations to use when benchmarking\n\
                  (default is 200,000)\n\
  -p pgsize       Create DB with page size specified by this option.\n\
                  Suffix k/m/g may be used to override the default of bytes.\n\
  -r value        Set the ratio of the overbound-penalty to capacity-penalty coefficients\n\
  -s hash         Create DB with <hash> hash function\n\
         hash: CRC | EJB | PHONG | OZ | TOREK | FNV | STL | MD5 | SHA1 | JENKINS | HSIEH\n\
  -t targetcap    Target MDBM capacity utilization rate (default: 50%%, range: 1%% - 75%%)\n\
  -v              Verbose\n\
  -w <n.n%%>      Specifies floating-poing percentage of accesses that are writes\n\
");
}


// Returns true if succeeded in copying a file, false if not
inline static bool
copyfile(const string &fromfile, const string &tofile)
{
    string cmd("cp ");
    cmd += fromfile + string(" ") + tofile;
    int ret = system(cmd.c_str());
    if (ret != 0) {
        cerr << "Unable to copy from " << fromfile << " to file: " << tofile << endl;
        return false;
    }
    return true;
}

inline static uint
toIdx(uint32_t pagesize, int hashfunc)
{
    return (pagesize << 4) + hashfunc;
}

inline static void
storeStats(const MdbmConfigStats &stats)
{
    statsMap[toIdx(stats.pagesize, stats.hashfunc)] = stats;
}

static const MdbmConfigStats &getStats(uint pagesize, int hashfunc)
{
    uint idx = toIdx(pagesize, hashfunc);
    map<uint, MdbmConfigStats>::const_iterator it = statsMap.find(idx);

    if (it == statsMap.end()) {
        return notFound;
    }
    return it->second;
}

inline static void
setScore(uint32_t pagesize, int hashfunc, double score)
{
    scoreMap[toIdx(pagesize, hashfunc)] = score;
}

static double
getScore(uint32_t pagesize, int hashfunc)
{
    uint idx = toIdx(pagesize, hashfunc);
    map<uint, double>::const_iterator it = scoreMap.find(idx);

    if (it == scoreMap.end()) {
        return -1.0;
    }
    return it->second;
}

inline static string
toStr(int64_t val)
{
    stringstream s;

    s << val;
    return s.str();
}

inline static string
inK(uint val)
{
    return toStr(val / 1024) + "K";
}

inline static MDBM *openMdbmAndError(const char *filename, int flags, int mode,
                                     int pagesize, int dbsize, const string &complaint)
{
    MDBM *ret = mdbm_open(filename, flags, mode, pagesize, dbsize);
    if (ret == NULL) {
        cerr << "Cannot open MDBM: " << filename << " Cannot " << complaint << endl;
    }

    return ret;
}

static double
computeCapacityScore(const MdbmConfigStats &stat, uint targetCapacity)
{
    // Compute the "almost spill" (72%) coefficient.  Should be 1 or greater
    double almostSpillCoeff = ALMOST_SPILL_PENALTY, score = 0.0;
    if (ALMOST_SPILL_PENALTY * SpillPenaltyCoefficient < 1.0) {
        almostSpillCoeff = 1.0;
    }

    uint pageBytesTarget = targetCapacity * stat.pagesize / 100;
    uint32_t bucketSize = stat.maxPageUsedSpace / MDBM_STAT_BUCKETS;
    for (int i=0; i < MDBM_STAT_BUCKETS; ++i) {
        uint32_t numPages = stat.bucketNumPages[i];
        if (numPages == 0) {
            continue;
        }
        uint32_t bottom = bucketSize * i;
        uint32_t top = bucketSize * (i+1);
        // do not penalize pages around the target
        if ((pageBytesTarget >= bottom) && (pageBytesTarget < top)) {
            continue;
        }
        // Average capacity in percent of the bucket's midpoint (50 is 100% / 2)
        double avgCapacity = (double(top + bottom)) * 50.0 / stat.pagesize;
        double penalty = fabs(avgCapacity - targetCapacity) * numPages;
        if (avgCapacity > MDBM_SPILLSIZE) {
            penalty *= SpillPenaltyCoefficient;
        } else if (avgCapacity > ALMOST_SPILLING) {
            penalty *= almostSpillCoeff;
        }
        score += (penalty / 100.0);   // Normalize back from percent to a fraction
    }

    return score;
}

static void
storeScore(uint32_t pgsz, int hash, uint targetCapacity, ostream &outto)
{
    const MdbmConfigStats &newstat ( getStats(pgsz, hash) );
    double capacityScore = computeCapacityScore(newstat, targetCapacity);
    outto << "CapacityScore: " << capacityScore << endl;
    setScore(pgsz, hash, capacityScore);
}

static int
pageIterFunc(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
{
    pair<uint32_t, uint32_t> *dataPtr = (pair<uint32_t, uint32_t> *)user;

    uint32_t upperBound = dataPtr->first;
    uint32_t entryCount = info->i_page.page_active_entries;

    if (entryCount > upperBound) {
        dataPtr->second += (entryCount - upperBound);
    }
    return 0;
}

static double
getOverUpperLimitScore(const MdbmConfigStats &stats)
{
    double tmp = 0.0;
    if (stats.entryCount) {
        tmp = ((double) stats.overUpperObjectCount) / stats.entryCount;
    }
    return tmp;
}

static string
getHashname(int id)
{
    switch (id) {
        case 0:
            return "CRC";
            break;
        case 1:
            return "EJB";
            break;
        case 2:
            return "PHONG";
            break;
        case 3:
            return "OZ";
            break;
        case 4:
            return "TOREK";
            break;
        case 5:
            return "FNV";
            break;
        case 6:
            return "STL";
            break;
        case 7:
            return "MD5";
            break;
        case 8:
            return "SHA1";
            break;
        case 9:
            return "JENKINS";
            break;
        case 10:
            return "HSIEH";
            break;
        default:
            return "Unknown";
        }
    return "Unknown";
}

static bool
collectStatistics(const char *fname, MdbmConfigStats *stat, uint objsUpperBound = 0)
{
    int flags = MDBM_O_RDONLY | MDBM_OPEN_NOLOCK;
    int statflags = MDBM_STAT_NOLOCK;

    MDBM *db = openMdbmAndError(fname, flags, 0, 0, 0, "get Statistics");
    if (db == NULL) {
        return false;
    }

    mdbm_db_info_t dbinfo;
    mdbm_stat_info_t dbstats;
    if (mdbm_get_db_stats(db,&dbinfo,&dbstats,statflags) < 0) {
        cerr << "mdbm_get_db_stats failed to get statistics" << endl;
        return false;
    }

    stat->normal = dbstats.sum_key_bytes+dbstats.sum_normal_val_bytes;
    stat->lob = dbstats.sum_lob_val_bytes;
    stat->total = stat->normal + stat->lob;
    stat->overhead = dbstats.sum_overhead_bytes;
    stat->dbsize = (uint64_t)dbinfo.db_page_size *dbinfo.db_num_pages;
    stat->unused = stat->dbsize - stat->total - stat->overhead;

    stat->pagesize = dbinfo.db_page_size;
    stat->hashfunc = dbinfo.db_hash_func;
    stat->hashname = dbinfo.db_hash_funcname;
    if (stat->hashname.substr(0,4) == "hash") {
        stat->hashname = getHashname(stat->hashfunc);
    }
    stat->totalpages = dbinfo.db_num_pages;
    stat->usedpages = dbinfo.db_num_pages - dbstats.num_free_pages;
    stat->freepages = dbstats.num_free_pages;

    stat->totalChunks = dbstats.num_active_pages;   // normalpages + oversize + lobpages
    stat->normalChunks = dbstats.num_normal_pages;
    stat->oversizedChunks = dbstats.num_oversized_pages;
    stat->largeObjChunks = dbstats.num_lob_pages;

    stat->entryCount = dbstats.num_active_entries;
    stat->largeEntryCount = dbstats.num_active_lob_entries;

    // Min, Mean, and Max of both regular and Lobjs
    stat->minkv = dbstats.min_entry_bytes;
    stat->meankv = (uint32_t)
                    (dbstats.num_active_entries ? stat->total/dbstats.num_active_entries : 0);
    stat->maxkv = dbstats.max_entry_bytes;  // Maximum size of both regular and LOB

    stat->minkey = dbstats.min_key_bytes;
    stat->meankey =  (uint32_t)(dbstats.num_active_entries
                          ? dbstats.sum_key_bytes/dbstats.num_active_entries : 0);
    stat->maxkey = dbstats.max_key_bytes;

    // Min, Mean, and Max of regular objects (not including large)
    stat->minval = dbstats.min_val_bytes;
    stat->meanval = (uint32_t)(dbstats.num_active_entries
                         ? dbstats.sum_normal_val_bytes/dbstats.num_active_entries : 0);
    stat->maxval = dbstats.max_val_bytes;

    stat->minlob = dbstats.min_lob_bytes;
    stat->meanlob = (uint32_t)(dbstats.num_active_lob_entries
                         ? dbstats.sum_lob_val_bytes/dbstats.num_active_lob_entries : 0);
    stat->maxlob = dbstats.max_lob_bytes;

    stat->minpgentries = dbstats.min_page_entries;
    stat->meanpgentries = (uint32_t)(dbstats.num_active_entries / dbinfo.db_dir_width);
    stat->maxpgentries = dbstats.max_page_entries;

    stat->maxPageUsedSpace = dbstats.max_page_used_space;

    // Store the stats buckets
    for (int i = 0; i < MDBM_STAT_BUCKETS; i++) {
        mdbm_bucket_stat_t* buckt = dbstats.buckets+i;
        stat->bucketNumPages[i] = buckt->num_pages;
        stat->bucketNumEntries[i] = buckt->sum_entries;
        stat->bucketMinSize[i] = buckt->min_bytes;
        stat->bucketByteSum[i] = buckt->sum_bytes;
        stat->bucketMaxSize[i] = buckt->max_bytes,
        stat->bucketMinFree[i] = buckt->min_free_bytes,
        stat->bucketSumFree[i] = buckt->sum_free_bytes;
        stat->bucketMaxFree[i] = buckt->max_free_bytes;
    }

    // Go through pages, and store the count of objects above the upper bound
    if (objsUpperBound != 0) {
        pair<uint32_t, uint32_t> iterateArgs;
        iterateArgs.first = objsUpperBound;
        iterateArgs.second = 0;
        if (mdbm_iterate(db, -1, pageIterFunc, statflags, &iterateArgs) < 0) {
            cerr << "mdbm_iterate failed" << endl;
            return false;
        }
        stat->overUpperObjectCount = iterateArgs.second;
    }

    mdbm_close(db);
    return true;
}

// Can the largest object fit in 3/4 Will return false if you definitely do not need large objects.
static bool
needLargeObj(const MdbmConfigStats &stats, uint pagesz)
{
    if ((MAX(stats.maxval,stats.maxlob) + ENTRY_OVERHEAD) > (pagesz / 2 + pagesz / 4))
        return true;

    return false;
}

static double
percentLargeObj(const MdbmConfigStats &stats)
{
    return ((double) stats.largeEntryCount) / (stats.entryCount + stats.largeEntryCount) * 100.0;
}

static void
printArray(const uint32_t array[], const char *name, ostream &outto)
{
    outto << name << "(" << MDBM_STAT_BUCKETS << "): ";
    for (int i = 0; i < MDBM_STAT_BUCKETS-1; ++i) {
        outto << array[i] << ", ";
    }
    outto << array[MDBM_STAT_BUCKETS-1] << endl;
}

static void
logMdbmStats(const MdbmConfigStats &stats, ostream &outto, const char *MDBM_name,
             bool largeObjects = true)
{
    outto << MDBM_name << " Statistics --" << endl;

    outto << "Page size: " << inK(stats.pagesize) << endl;
    outto << "Hash function: " << stats.hashname << " (" << stats.hashfunc << ")" << endl;
    outto << "Large Objects: " << largeObjects << endl;

    outto << "EntryCount: " << stats.entryCount << endl;
    outto << "LargeEntryCount: " << stats.largeEntryCount << endl;

    outto << "NumNormalChunk: " << stats.normalChunks << endl;
    outto << "NumOversizedChunk: " << stats.oversizedChunks << endl;
    outto << "NumLargeObjChunk: " << stats.largeObjChunks << endl;

    outto << "MinObjSize: " << stats.minkv << endl;
    outto << "MeanObjSize: " << stats.meankv << endl;
    outto << "MaxObjSize: " << stats.maxkv << endl;

    outto << "MinKeySize: " << stats.minkey << endl;
    outto << "MeanKeySize: " << stats.meankey << endl;
    outto << "MaxKey Size: " << stats.maxkey << endl;

    outto << "MinValSize: " << stats.minval << endl;
    outto << "MeanValSize: " << stats.meanval << endl;
    outto << "MaxValSize: " << stats.maxval << endl;

    outto << "MaxPageUsedSpace: " << stats.maxPageUsedSpace << endl;

    printArray(stats.bucketNumPages, "BucketsPageCount", outto);
    printArray(stats.bucketNumEntries, "BucketsEntryCount", outto);
    printArray(stats.bucketByteSum, "BucketsByteSum", outto);

    outto << "OverUpperBound: " << stats.overUpperObjectCount << endl;

    outto << "OverUpperBoundRatio: " << getOverUpperLimitScore(stats) << endl;

#if 0
    // Compute "over the upper bound" objects per page
    uint32_t smallObjChunks = stats.normalChunks + stats.oversizedChunks;
    if (smallObjChunks) {
        outto << "OverUpperPerPage: "
              << ((double) stats.overUpperObjectCount) / smallObjChunks << endl;
    }
#endif
}

static void
getMdbmStatsPageSizes(const MdbmConfigStats &stats, ostream &outto, const char *MDBM_name,
                      uint32_t &minPageSize, uint32_t &maxPageSize, uint32_t targetCapacity,
                      uint32_t lowerBoundObjs, uint32_t upperBoundObjs, bool pagesizeSet)

{
    logMdbmStats(stats, outto, MDBM_name);
    storeStats(stats);
    storeScore(stats.pagesize, stats.hashfunc, targetCapacity, outto);
    uint64_t tmp = lowerBoundObjs * 100 / targetCapacity * (stats.meankv + ENTRY_OVERHEAD);

    if (tmp <= CONFIG_MAXPAGE) {
        minPageSize = ROUND_TO_SYSPAGE(tmp);
    } else if (!pagesizeSet) {
        minPageSize = CONFIG_MAXPAGE;
        cout << "** Minimum Page Size (" << inK(ROUND_TO_SYSPAGE(tmp))
             << ") is too high, using " << inK(CONFIG_MAXPAGE) << "." << endl << flush;
        if (PrintVerbose) {
            cout << "This happened because you requested " << lowerBoundObjs
                 << " objects per page, and the average object's size is " << stats.meankv
                 << " and your requested capacity is " << targetCapacity << "%."
                 << endl << flush;
        }
    }
    outto << "========" << endl;
    outto << "TargetMinPageSize: " << inK(minPageSize) << endl;

    tmp = upperBoundObjs * 100 / targetCapacity * (stats.meankv + ENTRY_OVERHEAD);

    if (tmp <= CONFIG_MAXPAGE) {
        maxPageSize = ROUND_TO_SYSPAGE(tmp);
    } else if (!pagesizeSet) {
        maxPageSize = CONFIG_MAXPAGE;
        cout << "** Maximum Page Size (" << inK(ROUND_TO_SYSPAGE(tmp))
             << ") is too high, using " << inK(CONFIG_MAXPAGE) << "." << endl << flush;
        if (PrintVerbose) {
            cout << "This happened because you requested " << upperBoundObjs
                 << " objects per page, and the average object's size is " << stats.meankv
                 << " and your requested capacity is " << targetCapacity << "%."
                 << endl << flush;
        }
    }
    outto << "TargetMaxPageSize: " << inK(maxPageSize) << endl;
    outto << "========" << endl;
}

// Build MDBM using data in basedb.
//  Get hash from basedb if not provided in newHash. presize is in MBytes.
static string
buildMdbm(MDBM *basedb, uint32_t pagesize, const MdbmConfigStats &baseStats, ostream &outto,
          uint objUpperBound, int newHash = -1, bool getstats = true, uint presize = 0)
{
    int curHash;

    if (newHash != -1) {
        curHash = newHash;
    } else {
        curHash = mdbm_get_hash(basedb);
    }
    string fname(SETUP_BASE_DIR);
    fname += string("setupdbpg") + inK(pagesize) + "_" + toStr(curHash) + ".mdbm";

    unlink(fname.c_str());
    bool largeobj = false;
    int flags = MDBM_O_RDWR|MDBM_O_CREAT|MDBM_OPEN_NOLOCK|MDBM_DBSIZE_MB;
    if (getstats) {
        flags |= MDBM_O_FSYNC;
    }
    if (needLargeObj(baseStats, pagesize)) {
        flags |= MDBM_LARGE_OBJECTS;
        largeobj = true;
    }

    if (presize == 0) {
        presize = (mdbm_get_size(basedb) / MBYT);
    }

    MDBM *newdb = openMdbmAndError(fname.c_str(), flags, 0644, pagesize, presize, "Get hash");

    if (!newdb || !mdbm_sethash(newdb, curHash)) {
        cerr << "Invalid hash function id " << curHash << endl;
        return "";
    }

    MDBM_ITER iter;
    kvpair kv = mdbm_first_r(basedb, &iter);

    while (kv.key.dsize != 0) {
        mdbm_store(newdb, kv.key, kv.val, MDBM_REPLACE);
        kv = mdbm_next_r(basedb, &iter);
    }

    mdbm_close(newdb);

    if (!getstats) {
        if (PrintVerbose) {
            outto << "Building MDBM " << fname << " with hash " << getHashname(newHash)
                  << endl << flush;
        }
        return fname;
    }

    MdbmConfigStats stat;
    if (collectStatistics(fname.c_str(), &stat, objUpperBound)) {
        logMdbmStats(stat, outto, fname.c_str(), largeobj);
        storeStats(stat);
    }

    return fname;
}

// Erase oldfile, overwrite oldfile with newfile
static void
overwriteWithNew(const string &newfname, string oldname)
{
    if (newfname.empty()) {
        cerr << "Cannot overwrite because no new file exists" << endl;
        return;
    }
    unlink(oldname.c_str());
    string cmd = string("mv -f ") + newfname + " " + oldname;
    system(cmd.c_str());
}


static bool
decideLargeObj(const MdbmConfigStats &stats, uint lowerPagesz, uint upperPagesz,
               uint objUpperBound, uint targetCapacity, string inname,
               uint &bestPageSize, string &filname, ostream &outto)
{
    MDBM *db = openMdbmAndError(inname.c_str(), MDBM_O_RDONLY | MDBM_OPEN_NOLOCK, 0444, 0, 0,
                                "LOS search");
    if (db == NULL) {
         return 1;
    }

    int hash = stats.hashfunc;
    if (!needLargeObj(stats, lowerPagesz)) {
        bestPageSize = lowerPagesz;
        filname = buildMdbm(db, bestPageSize, stats, outto, objUpperBound);
        storeScore(bestPageSize, hash, targetCapacity, outto);
        return false;
    }

    // Perform binary search for lower bound, starting from lowest page size to the highest,
    // trying to find an MDBM with 3% or less Large Objects.  Check for 5% at the end.

    MdbmConfigStats curstat;
    bool found = false;
    double largeObjPercent;
    uint32_t low = lowerPagesz / SYS_PAGESIZE, high = upperPagesz / SYS_PAGESIZE;
    uint32_t mid = low + high / 2;    // g++ complains w/o this, since while is optional
    while (low <= high) {
        mid = (low + high) / 2;
        if (!filname.empty()) {
            unlink(filname.c_str());
        }
        uint32_t midpgsz = mid * SYS_PAGESIZE;
        filname = buildMdbm(db, midpgsz, stats, outto, objUpperBound);
        if (filname.empty()) {
            mdbm_close(db);
            return true;
        }

        curstat = getStats(midpgsz , hash);
        storeScore(curstat.pagesize, hash, targetCapacity, outto);
        largeObjPercent = percentLargeObj(curstat);

        if (largeObjPercent <= 3.0) {
            found = true;
            break;
        } else {
            low = mid + 1;
        }
    }

    mdbm_close(db);

    if (found) {
        bestPageSize = mid * SYS_PAGESIZE;
        if (PrintVerbose) {
          cout << "Recommended page size is " << bestPageSize << "." << endl << flush;
        }
        return needLargeObj(curstat, bestPageSize);
    }

    largeObjPercent = percentLargeObj(curstat);
    bestPageSize = upperPagesz;
    if (largeObjPercent > 5.0) {
        cout << "Even at the suggested page size of " << inK(bestPageSize) << ", "
             << roundl(largeObjPercent) << "% of objects are large objects "
             << "and that is above the recommended 5%." << endl << flush;
        return true;
    }

    return needLargeObj(curstat, bestPageSize);
}


// Finding the "lowest" N.  Algorithm expects "N" to be small.

static vector<PagesizeAndHash>
findLowestN(uint num, const vector<int> &hashes, uint32_t startPageSize, uint32_t endPageSize,
            ostream &outto)
{
    multimap<double, PagesizeAndHash> values;
    vector<int>::const_iterator it;
    uint32_t pgsz;

    for (it = hashes.begin(); it != hashes.end(); ++it) {
        int curhash = *it;
        for (pgsz = startPageSize; pgsz <= endPageSize; pgsz += SYS_PAGESIZE) {
            const MdbmConfigStats &curstat ( getStats(pgsz, curhash) );
            if (!curstat.hashname.empty()) {
                double score = getScore(pgsz, curhash);  // Capacity penalty score
                if (score < -0.01) {
                    cerr << "Cannot find Capacity Penalty for page size " << inK(pgsz)
                         << " Hash " << getHashname(curhash) << endl << flush;
                    continue;
                }
                outto << "Pagesize " << inK(pgsz) << " Hash "
                      << getHashname(curhash) << " CapPenalty " << score;
                // Now Add overbound penalty times the ratio
                score += (getOverUpperLimitScore(curstat) * OverboundToCapacityRatio);
                outto << " OverPenalty " << getOverUpperLimitScore(curstat)
                      << " Total " << score << endl;
                pair<double, PagesizeAndHash> val;
                val.first = score;
                val.second = PagesizeAndHash(pgsz, curhash);
                values.insert(val);
            }
        }
    }

    bool manyHashes = (hashes.size() != 1);
    vector<PagesizeAndHash> ret;
    // We are trying to return results with unique hash functions, so we should
    // run through twice if many hashes were supplied, only once if just one
    set<int> hashesSeen;

    multimap<double, PagesizeAndHash>::iterator retit = values.begin();
    for(uint i = 0; (i < num) && (retit != values.end()); ++retit) {
        int curhash = (retit->second).second;
        // For many hashes, pick one that wasn't seen before
        if (hashesSeen.find(curhash) == hashesSeen.end()) {
            ret.push_back(retit->second);
            ++i;
            if (manyHashes) {  // For one hash, keep foundHashes empty
                hashesSeen.insert(curhash);
            }
        }
    }

    if (manyHashes) {   // Run through again if there when "-h hash" was not provided
        set<PagesizeAndHash> curResults;
        set<PagesizeAndHash>::iterator it = curResults.begin();
        copy(ret.begin(), ret.end(), inserter(curResults, it));
        retit = values.begin();
        for(uint i = ret.size(); (i < num) && (retit != values.end()); ++retit) {
            if (curResults.find(retit->second) == curResults.end()) {
                ret.push_back(retit->second);
                ++i;
                if (PrintVerbose) {
                    uint32_t pagsz = retit->second.first;
                    int hsh = retit->second.second;
                    outto << "Second look: Adding pagesize " << inK(pagsz) << " Hash= "
                          << getHashname(hsh) << endl;
                }
            }
        }
    }

    return ret;
}

static bool
importFile(uint optPagesize, int optHashfnid, uint64_t optDbsize, char *optInfile,
           bool optCdbdump, bool optDeleteZero, const char *outfile)
{
    const char *impArgs[20];
    int curArg=0;
    impArgs[curArg++] = "mdbm_import";
    impArgs[curArg++] = "-p";
    string pagesz (toStr(optPagesize));
    impArgs[curArg++] = const_cast<char *>(pagesz.c_str());
    impArgs[curArg++] = "-l";   // Initial pass w/ large obj
    impArgs[curArg++] = "-3";   // Force V3 for old MDBM libs
    impArgs[curArg++] = "-s";
    string hashname(toStr(optHashfnid));
    impArgs[curArg++] = const_cast<char *>(hashname.c_str());
    impArgs[curArg++] = "-d";
    string dbsize = toStr(INITIAL_SIZE_MDBM) + string("m");
    optDbsize /= MBYT;
    if (optDbsize) {
        dbsize = toStr(optDbsize) + string("m");
    }
    impArgs[curArg++] = const_cast<char *>(dbsize.c_str());
    if (optInfile) {  // Set up mdbm_import's args
        impArgs[curArg++] = "-i";
        impArgs[curArg++] = optInfile;
    }
    if (optCdbdump) {
        impArgs[curArg++] = "-c";
    }
    if (optDeleteZero) {
        impArgs[curArg++] = "-D";
    }
    impArgs[curArg++] = (char *) outfile;
    impArgs[curArg] = NULL; // Last arg

    reset_getopt();
    int errcode = imp_main_wrapper(curArg, (char **)impArgs);

    if (errcode) {
        cerr << "Unable to import data, error code " << errcode << endl << flush;
        return false;
    }
    return true;
}

static string
printedResult(uint32_t pgsz, int hashcode, bool largeObj)
{
    string prt ("Page Size ");
    string largeObjSetting = (largeObj ? "" : " No");
    prt += inK(pgsz) + string(", Hash ") + getHashname(hashcode) + string(",");
    prt += largeObjSetting + string(" Large Objects");

    return prt;
}

static double
benchmarkFile(const string &filename, double writefrac, int lockmode)
{
    string benchoutput = filename + string(".benchout");
    benchmarkExisting(filename.c_str(), writefrac, lockmode, benchoutput.c_str(), OpCount,
                      PrintVerbose);

    FILE *fp = fopen(benchoutput.c_str(), "r");

    if (fp == NULL) {
        cerr << "Unable to open benchmark output file " << benchoutput << endl << flush;
        return -1.0;
    }

    const int LINESZ = 512;
    char line[LINESZ];
    uint32_t fetchespersec = 0, writespersec = 0;
    double ret = -1.0;
    while (fgets(line, LINESZ, fp) != NULL) {
        char *found = strstr(line, "nproc  fetch/s");
        if (found) {
            char *tmp = fgets(line, LINESZ, fp);   // Get next line
            if (tmp == NULL) {
                cerr << "Could not find last line with benchmark results" << endl << flush;
                break;
            }
            stringstream st(tmp);
            st >> fetchespersec;   // throw out first number
            st >> fetchespersec;
            double skip;
            st >> skip;
            st >> writespersec;
            // Compute weighted average
            ret = ((double) fetchespersec * (1.0 - (writefrac / 100))) +
                  ((double) writespersec * writefrac / 100);
            break;
        }
    }
    fclose(fp);
    unlink(benchoutput.c_str());
    return ret;
}

static string
runBenchmarks(const vector<string> &filenames, double writefrac, int lockmode, string &bestFile,
              const char *outfile, const MdbmConfigStats &stats,
              const vector<PagesizeAndHash> &topInfo, ostream &outstrm)
{
    double best = -1.0;
    vector<double> resultVec;

    for (uint i = 0; i < filenames.size(); ++i) {
        string fname(filenames[i]);
        double result = benchmarkFile(fname, writefrac, lockmode);
        resultVec.push_back(result);
        if (result < 0.0) {
            cerr << "Error benchmarking file " << fname << endl << flush;
        } else if (result > best) {
            best = result;
            bestFile = fname;
        }
    }

    ostringstream outpt;
    long bestresult = lround(best);
    for (uint i = 0; i < filenames.size(); ++i) {  // Find hash and pagesize
        uint32_t pgsz = topInfo[i].first;
        int hashcode = topInfo[i].second;
        outstrm  << printedResult(pgsz, hashcode, needLargeObj(stats, pgsz))
                 << ", benchmarked at " << resultVec[i] << " TPS";
        if (bestFile == filenames[i]) {
            outstrm << " (** BEST **)";
            outpt << "Best performing setup stored in " << outfile << " with "
                  << printedResult(pgsz, hashcode, needLargeObj(stats, pgsz))
                  << ", benchmarked at " << bestresult << " TPS" << endl;

        }
        outstrm << endl;
    }

    return outpt.str();
}

void
printCmdLine(ostream &outstrm, char **argv)
{
    outstrm << "Running: ";

    while (*argv != NULL) {
        outstrm << string(*argv) << " ";
        ++argv;
    }
    outstrm << endl;
}


void
printStage1(uint optimalPageSize, bool decidedLarge, ostream &outstrm)
{
    string needlarge;
    if (!decidedLarge) {
        needlarge = " not";
    }
    outstrm << "1) Optimizing large objects to 3% or less (first pass through data) suggested "
               "a page size of " << inK(optimalPageSize) << "," << endl
            << "   and Large Object Support is" << needlarge << " required." << endl;
}

void
printStage2(const vector<PagesizeAndHash> &best, ostream &outstrm, uint optimalPageSize,
            uint upperPageSize, const MdbmConfigStats &stats)
{
    outstrm << "2) After analyzing hashes and page sizes of " << inK(optimalPageSize) << " - "
           << inK(upperPageSize) << " the top results are: " << endl;
    for (uint i = 0; i < best.size(); ++i) {   // Printing results of 2nd stage of analysis
        uint32_t pagsz = best[i].first;
        int hsh = best[i].second;
        outstrm << "Result # " << i + 1 << " - "
                << printedResult(pagsz, hsh, needLargeObj(stats, pagsz)) << endl;
    }
}

int
main(int argc, char** argv)
{
    int c, tmpval, analysisLevel = 3;
    bool errflag = false;
    bool opthelp = false;
    bool optCdbdump = false;
    char *optInfile = NULL;
    uint optPagesize = INITIAL_PAGESZ;
    int optHashfnid = MDBM_DEFAULT_HASH;
    bool optDeleteZero = false;
    bool optLargeObj = false;
    bool pagesizeSet = false;
    bool hashSet = false;
    uint64_t optDbsize = 0;
    uint objsPerPageUpperBound = 50;
    uint objsPerPageLowerBound = 37;  // 75% of 50
    bool objsPerPageSet = false;   // true if upper or lower bounds are set
    uint targetCapacity = 50;  // Default target capacity utilization
    int bestCount = 4;   // How many of the "best candidates" to try (for analysis level 3)
    double writefrac = 0.0;
    int locking = 1;
    double coeff, ratio;
    char *colonp;

    SYS_PAGESIZE = sysconf(_SC_PAGESIZE);
    CONFIG_MAXPAGE -= SYS_PAGESIZE;   // 16MB minus system_page_size

    while ((c = getopt(argc, argv, "a:b:cd:Df:hi:lL:n:o:p:r:s:t:vw:")) != EOF) {
        switch (c) {
        case 'a':
            tmpval = atoi(optarg);
            if ((tmpval < 1) || (tmpval > 3)) {
                cerr << "Invalid analysis level " << optarg << endl << flush;
                errflag = true;
                break;
            }
            analysisLevel = tmpval;
            break;
        case 'b':
            tmpval = atoi(optarg);
            if ((tmpval < 1) || (tmpval > 20)) {
                cerr << "Invalid best candidate number " << optarg << endl << flush;
                errflag = true;
                break;
            }
            bestCount = tmpval;
            break;
        case 'c':
            optCdbdump = true;
            break;
        case 'd':
            optDbsize = mdbm_util_get_size(optarg, MBYT);
            break;
        case 'D':
            optDeleteZero = 1;
            break;
        case 'f':
            coeff = atof(optarg);
            if (coeff == 0.0) {
                cerr << "Invalid Page Spill coefficient, ignoring " << optarg << endl << flush;
            } else {
                SpillPenaltyCoefficient = coeff;
                cout << "Using Spill coefficient " << SpillPenaltyCoefficient << "." << endl << flush;
            }
            break;
        case 'h':
            opthelp = true;
            break;
        case 'i':
            optInfile = optarg;
            break;
        case 'l':
            optLargeObj = true;
            break;
        case 'L':
            if (strcasecmp(optarg, "exclusive") == 0) {
                locking = MDBM_LOCK;
            } else if (strcasecmp(optarg, "partition") == 0) {
                locking = MDBM_PLOCK;
            } else if (strcasecmp(optarg, "shared") == 0) {
                locking = MDBM_RWLOCK;
            } else if (strcasecmp(optarg, "nolock") == 0) {
                locking = MDBM_NO_LOCK;
            } else {
                fprintf(stderr, "Invalid locking argument %s, ignoring\n", optarg);
            }
            break;
        case 'n':
            // Format: UpperBound:LowerBound.
            // UpperBound is first to make LowerBound optional.

            // Upper bound
            tmpval = atoi(optarg);
            if (tmpval) {
                if (tmpval < 0 || tmpval > 1000) {
                    cerr << "Invalid upper object bound: " << optarg
                         << ", Ignoring and leaving at 50 objects" << endl << flush;
                    break;
                }
                objsPerPageUpperBound = tmpval;
                objsPerPageSet = true;
            }
            // Colon delimiter
            colonp = strchr(optarg, ':');
            if (colonp == NULL) {
                objsPerPageLowerBound = objsPerPageUpperBound * 3 / 4;
                cout << "Using " << objsPerPageLowerBound << " as lower bound on objects per page."
                     << endl << flush;
                break;
            }
            tmpval = atoi(colonp + 1);
            // Lower bound, after the colon
            if (tmpval) {
                if (tmpval < 0 || tmpval > 900) {
                    cerr << "Invalid lower object bound: " << optarg
                         << ", Ignoring and leaving at " << objsPerPageLowerBound << " objects"
                         << endl << flush;
                    break;
                }
                objsPerPageLowerBound = tmpval;
                objsPerPageSet = true;
            }
            break;
        case 'o':
            tmpval = atoi(optarg);
            if (tmpval) {
                OpCount = tmpval;
            }
            break;
        case 'p':
            optPagesize = mdbm_util_get_size(optarg, 1);
            if ((optPagesize < SYS_PAGESIZE) ||
                (optPagesize > CONFIG_MAXPAGE)) {
                cerr << PROG << ": invalid page size - " << optarg << endl << flush;
                errflag = true;
            }
            pagesizeSet = true;
            break;
        case 'r':
            ratio = atof(optarg);
            if (ratio == 0.0) {
                cerr << "Invalid Overbound-to-Capacity coefficient ratio, ignoring "
                     << optarg << endl << flush;
            } else {
                OverboundToCapacityRatio = ratio;
                cout << "Using Overbound-to-Capacity coefficient ratio of "
                     << OverboundToCapacityRatio << "." << endl << flush;
            }
            break;
        case 's':
            if (optarg == NULL) {
                break;
            } else if (strcasecmp(optarg, "CRC") == 0) {
                optHashfnid = MDBM_HASH_CRC32;
            } else if (strcasecmp(optarg, "EJB") == 0) {
                optHashfnid = MDBM_HASH_EJB;
            } else if (strcasecmp(optarg, "PHONG") == 0) {
                optHashfnid = MDBM_HASH_PHONG;
            } else if (strcasecmp(optarg, "OZ") == 0) {
                optHashfnid = MDBM_HASH_OZ;
            } else if (strcasecmp(optarg, "TOREK") == 0) {
                optHashfnid = MDBM_HASH_TOREK;
            } else if (strcasecmp(optarg, "FNV") == 0) {
                optHashfnid = MDBM_HASH_FNV;
            } else if (strcasecmp(optarg, "STL") == 0) {
                optHashfnid = MDBM_HASH_STL;
            } else if (strcasecmp(optarg, "MD5") == 0) {
                optHashfnid = MDBM_HASH_MD5;
            } else if (strcasecmp(optarg, "SHA1") == 0) {
                optHashfnid = MDBM_HASH_SHA_1;
            } else if (strcasecmp(optarg, "JENKINS") == 0) {
                optHashfnid = MDBM_HASH_JENKINS;
            } else if (strcasecmp(optarg, "HSIEH") == 0) {
                optHashfnid = MDBM_HASH_HSIEH;
            }
            hashSet = true;
            break;
        case 't':
            tmpval = atoi(optarg);
            if (tmpval) {
                if (tmpval < 1 || tmpval > 90) {
                    cerr << "Invalid target capacity utilization: " << optarg
                         << ", (good: 1..90), ignoring and leaving at 50%" << endl << flush;
                    break;
                }
                targetCapacity = tmpval;
            }
        case 'v':
            PrintVerbose = true;
            break;
        case 'w':
            writefrac = atof(optarg);
            break;
        default:
            errflag = true;
        }
    }  // while (c=getopt...)

    if (opthelp) {
        setupUsage();
        return 0;
    }

    if (errflag || ((argc - optind) != 1)) {
        setupUsage();
        return 1;
    }
    const char *outfile = argv[optind];
    unlink(outfile);
    createTmp();

    if (objsPerPageUpperBound < objsPerPageLowerBound) {
        cerr << "Error: upper target of objects/page is set to: " << objsPerPageUpperBound
             << ". But it is lower than lower target: " << objsPerPageLowerBound
             << ". Setting lower bound to upper target." << endl << flush;
        objsPerPageLowerBound = objsPerPageUpperBound;
    }
    if (PrintVerbose && objsPerPageSet) {
        cout << "Upper target on objects per page is " << objsPerPageUpperBound
             << ", and Lower target on objects per page is " << objsPerPageLowerBound << "."
             << endl << flush;
    }

    MdbmConfigStats stats;
    ostream *statout = new ofstream("/tmp/setupmdbminfo.log");
    printCmdLine(*statout, argv);
    string newfname;

    if (optInfile && !optCdbdump && strstr(optInfile, ".mdbm")) {
        cout << "Assuming that file " << optInfile 
             << " is an MDBM (so importing data is not necessary)." << endl << flush;

        MDBM *db = openMdbmAndError(optInfile, MDBM_O_RDONLY, 0444, 0, 0, "open input file");
        if (db == NULL) {
             return 1;
        }

        stats.maxval = CONFIG_MAXPAGE + CONFIG_MAXPAGE;   // To ensure LOS is enabled
        newfname = buildMdbm(db, optPagesize, stats, *statout, objsPerPageUpperBound,
                             optHashfnid, false, optDbsize ? (optDbsize/MBYT) : INITIAL_SIZE_MDBM);
        mdbm_close(db);
        overwriteWithNew(newfname, outfile);

    } else if (!importFile(optPagesize, optHashfnid, optDbsize, optInfile,
                    optCdbdump, optDeleteZero, outfile)) {
        return 1;
    }

    if (pagesizeSet && optLargeObj && hashSet) {
        cerr << PROG << " created MDBM file " << outfile
             << " but has nothing else to decide," << endl
             << "since you specified the hash function, page size and large objects."
             << endl << flush;
        return 0;
    }

    if (!collectStatistics(outfile, &stats, objsPerPageUpperBound)) {
        return 1;
    }

    uint32_t lowerPageSize=0, upperPageSize=0;
    getMdbmStatsPageSizes(stats, *statout, "Initial MDBM", lowerPageSize, upperPageSize,
               targetCapacity, objsPerPageLowerBound, objsPerPageUpperBound, pagesizeSet);


    if (pagesizeSet && hashSet) {   // Hash/Pagesz is set: only trying to see if LOS is needed
        string yesno = (needLargeObj(stats, optPagesize) ? string("") : string("do not "));
        cout << "Please " << yesno << "set option MDBM_LARGE_OBJECTS when accessing "
             << outfile << "." << endl << flush;
        delete statout;
        return 0;
    }

    if (pagesizeSet) {
        if (objsPerPageSet) {
            cout << "You set both the page size and lower/upper bound of objs/page." << endl
                 << "Using lower+upper bound and ignoring page size." << endl << flush;
        } else {
            lowerPageSize = upperPageSize = optPagesize;
        }
    }

    uint optimalPageSize=0;  // Optimal page size for large objs at 3% or less
    // Analysis stage1: Determine large object support for 3% or less, and initial page size
    bool decideLarge = decideLargeObj(stats, lowerPageSize, upperPageSize, objsPerPageUpperBound,
                           targetCapacity, outfile, optimalPageSize, newfname, *statout);
    printStage1(optimalPageSize, decideLarge, cout);

    if (analysisLevel == 1) {
        cout << "Your requested quick analysis has been completed. Searched page sizes of: "
             << inK(lowerPageSize) << " - " << inK(upperPageSize) << "." << endl << flush;
        overwriteWithNew(newfname, outfile);
        delete statout;
        return 0;
    }

    // Analysis level 2: iterate through the page sizes and hash functions
    unlink(newfname.c_str());   // Collected stats, now unlink
    MDBM *basedb = openMdbmAndError(outfile, MDBM_O_RDONLY | MDBM_OPEN_NOLOCK, 0444, 0, 0,
                                    "Open Base MDBM");
    if (basedb == NULL) {
        delete statout;
        return 1;
    }

    string resultname(outfile);    // Generate a file containing the analysis results
    resultname += string(".analysis_results");
    ostream *resultout = new ofstream(resultname.c_str());
    printCmdLine(*resultout, argv);
    printStage1(optimalPageSize, decideLarge, *resultout);

    vector<int> hashes;
    vector<int>::iterator it;
    if (hashSet) {
        hashes.push_back( optHashfnid );
    } else {
        it = hashes.begin();
        copy(HashFuncs, HashFuncs + HashNum, inserter(hashes, it));
    }

    // Iterate through all hashes, and page sizes starting from optimal and continuing up to max
    uint32_t pgsz;
    for (it = hashes.begin(); it != hashes.end(); ++it) {
        int curhash = *it;
        for (pgsz = optimalPageSize; pgsz <= upperPageSize; pgsz += SYS_PAGESIZE) {
            const MdbmConfigStats &curstat ( getStats(pgsz, curhash) );   // Find existing
            if (curstat.hashname.empty()) {
                newfname = buildMdbm(basedb, pgsz, stats, *statout, objsPerPageUpperBound, curhash);
                storeScore(pgsz, curhash, targetCapacity, *statout);
                unlink(newfname.c_str());
            }
        }
    }

    vector<PagesizeAndHash> best = findLowestN(bestCount, hashes,
                                               optimalPageSize, upperPageSize, *statout);
    if (best.size() == 0) {
        cerr << "Cannot find good results, existing." << endl << flush;
        return 1;
    }

    // Print to cout and results file
    printStage2(best, cout, optimalPageSize, upperPageSize, stats);
    printStage2(best, *resultout, optimalPageSize, upperPageSize, stats);

    // Create MDBM with top resulting pagesize and hash, to replace the initially created MDBM
    pgsz = best[0].first;
    int hashcode = best[0].second;
    bool largeObj = needLargeObj(stats, pgsz);
    newfname = buildMdbm(basedb, pgsz, stats, *statout, objsPerPageUpperBound, hashcode, false);
    mdbm_close(basedb);
    overwriteWithNew(newfname, outfile);

    if (analysisLevel == 2) {
        cout << "Creating MDBM using first setup of " << printedResult(pgsz, hashcode, largeObj)
             << "." << endl << flush;
        delete statout;
        delete resultout;
        return 0;
    }

    // Start of analysis level 3 - run benchmark on top picks

    string backupFile;
    if (writefrac) {
        backupFile = newfname;  // Must create a backup if writing data
        if (!copyfile(outfile, backupFile)) {
            cerr << "Unable to create a backup file, will not be performing writes" << endl;
            writefrac = 0.0;
        }
    }

    basedb = openMdbmAndError(outfile, MDBM_O_RDONLY | MDBM_OPEN_NOLOCK, 0444, 0, 0,
                              "Open first result");
    if (basedb == NULL) {
        delete statout;
        delete resultout;
        return 1;
    }

    vector<string> filenames;  // Create the "best picks list"
    filenames.push_back(outfile);   // Push first best ranked file
    stats = getStats(pgsz, hashcode);
    for (uint i = 1; i < best.size(); ++i) {  // Rebuild the 2nd-best, 3rd-best,... MDBMs
        pgsz = best[i].first;
        hashcode = best[i].second;
        filenames.push_back(buildMdbm(basedb, pgsz, stats, *statout,
                                      objsPerPageUpperBound, hashcode, false));
    }
    mdbm_close(basedb);

    string bestfile;
    string bestout = runBenchmarks(filenames, writefrac, locking, bestfile, outfile, stats,
                                   best, *resultout);

    if (writefrac > 0.0) {         // Recreate best file from backupFile if there were writes
        if (bestfile == outfile) {
            copyfile(backupFile, outfile);
        } else if ((basedb = openMdbmAndError(backupFile.c_str(), MDBM_O_RDONLY | MDBM_OPEN_NOLOCK,
                                              0444, 0, 0, "Open backup MDBM")) != NULL) {
            newfname = buildMdbm(basedb, pgsz, stats, *statout,
                                 objsPerPageUpperBound, hashcode, false);
            copyfile(newfname, outfile);
            mdbm_close(basedb);
        } else {
            delete statout;
            delete resultout;
            return 1;
        }
        unlink(backupFile.c_str());
    } else if (bestfile != outfile) {   // Else, replace "outfile" with bestfile if no writes
        copyfile(bestfile, outfile);
    }

    cout << "3) " << bestout << "." << endl;

    // Cleanup all temporary MDBMs
    for (uint i = 0; i < filenames.size(); ++i) {
        if (filenames[i] != outfile) {
            unlink(filenames[i].c_str());
        }
    }

    delete statout;
    delete resultout;
    return(0);
}

