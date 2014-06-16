// $Id$

// simple example code that illustrates how to use an MDBM as a (small) cache.

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/fcntl.h>

#include <iostream>
#include <sstream>
#include <string>

#include <mdbm.h>

using namespace std;

static bool Verbose = false;

// Single architecture (32 bits only or 64 bits only) - Tells MDBM to use pthread-locks
static bool SingleArch = false;
static bool AnyLockingType = false;      // Do we allow any locking type

static uint64_t MaxDbSize = 1024;

// You get about 8 100-byte records in each 1K, if page size is 1K
static unsigned int PageSize = 1024;
static size_t ValueSize = 100;
static size_t RecordCount = 8;

static uint64_t StartingKey = 0ULL;
static size_t   KeySize = sizeof(uint64_t);

/**
 * Define a default shake function that does not delete any data.
 */
static int
shakeFunc(MDBM *db,
      const datum *key,
      const datum *val,
      struct mdbm_shake_data_v3 *shakeInfo)
{
    return 0; // 0 means do not delete entry.
              // You can add logic that determines whether to delete page entries and return 1.
}

static MDBM*
openDb(unsigned int  pageSize = PageSize)
{
    string fileName = "/tmp/test_cache.mdbm";
    unlink(fileName.c_str());   // cleanup for testing purposes
    unsigned int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;   // 0664
    // All processes accessing one MDBM must use the same locking mode: in this case,
    // exclusive locking is being used because it is the default and no flags 
    // requesting partitioned or shared locking are being passed to mdbm_open
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC;

    // Should only set -P flag to enable Pthread locking if all processes accessing this MDBM
    // have been compiled as 32 bit processes only or 64 bit processes only
    if (SingleArch) {
        flags |= MDBM_SINGLE_ARCH;
    }

    // Should set the -a flag if you're not sure what is the correct type of locking to use
    // to open this MDBM.
    if (AnyLockingType) {
        flags |= MDBM_ANY_LOCKS;
    }

    MDBM *handle = mdbm_open(fileName.c_str(),
                             flags,
                             mode,
                             pageSize,
                             0);
    if (handle == NULL) {
        perror("mdbm_open failed");
        exit(1);
    };

    if (MaxDbSize > 0) {
        unsigned int pages = (unsigned int) (MaxDbSize / PageSize);
        int rc = mdbm_limit_size_v3(handle, pages, shakeFunc, NULL);
        if (rc) {
            ostringstream oss;
            oss << "mdbm_limit_size failed"
                << ", MaxDbSize=" << MaxDbSize
                << ", handle=" << handle
                << ", pages=" << pages
                << ", rc=" << rc;
            perror(oss.str().c_str());
            exit(1);
        }
    }
    return handle;
}

static void
syncDb(MDBM *handle)
{
    int rc = mdbm_fsync(handle);
    if (rc) {
        perror("mbdm_fsync failed");
        exit(1);
    }
}

static void
insertRecords(MDBM    *handle,
              size_t   count,
              uint64_t startingKey,
              size_t   valueSize)
{
    int rc;

    char key[KeySize];
    memset(key, 0, sizeof(key));
    datum k;
    k.dsize = sizeof(key);
    k.dptr = reinterpret_cast<char*>(&key);

    // Set up a datum for the record value
    char value[valueSize];
    memset(value, 0xAB, sizeof(value));  // arbitrary values
    datum v;
    v.dsize = sizeof(value);
    v.dptr = reinterpret_cast<char*>(&value);

    cout << "Upsert"
         << ", count=" << count
         << ", startingKey=" << startingKey
         << ", valueSize=" << valueSize
         << endl;
    unsigned long long totalRecordSize = 0;
    for (size_t i = 0; i < count; i++) {
        memcpy(key, &startingKey, sizeof(startingKey));
        if (Verbose) {
            cout << "size=" << v.dsize << endl;
        }
        if (mdbm_lock(handle) != 1) {
            ostringstream oss;
            oss << "mdbm_lock failed, errno= " << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        rc = mdbm_store(handle, k, v, MDBM_REPLACE);
        if (rc) {
            ostringstream oss;
            oss << "mdbm_store failed"
                << ", key=" << key
                << ", count=" << count
                << ", i=" << i
                << ", rc=" << rc
                << ", errno=" << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        if (mdbm_unlock(handle) != 1) {
            ostringstream oss;
            oss << "mdbm_unlock failed, errno= " << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        startingKey++;
    }
    syncDb(handle);
}

static void
fetchRecords( MDBM    *handle,
              size_t   count,
              uint64_t startingKey)
{
    char key[KeySize];
    memset(key, 0, sizeof(key));
    datum k, fetched;
    k.dsize = sizeof(key);
    k.dptr = reinterpret_cast<char*>(&key);

    cout << "Fetch"
         << ", count=" << count
         << ", startingKey=" << startingKey
         << endl;

    for (size_t i = 0; i < count; i++, startingKey++) {
        memcpy(key, &startingKey, sizeof(startingKey));
        if (mdbm_lock(handle) != 1) {
            ostringstream oss;
            oss << "mdbm_lock failed, errno= " << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        fetched = mdbm_fetch(handle, k);
        if (fetched.dptr == NULL) {   // Expecting to fetch successfully
            ostringstream oss;
            oss << "mdbm_fetch failed"
                << ", key=" << startingKey
                << ", count=" << count
                << ", i=" << i
                << ", errno=" << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        if (mdbm_unlock(handle) != 1) {
            ostringstream oss;
            oss << "mdbm_unlock failed, errno= " << errno;
            perror(oss.str().c_str());
            exit(1);
        }
    }

    syncDb(handle);
}

static void
deleteRecords(MDBM     *handle,
              size_t   count,
              uint64_t startingKey)
{
    int rc;

    char key[KeySize];
    memset(key, 0, sizeof(key));
    datum k;
    k.dsize = sizeof(key);
    k.dptr = reinterpret_cast<char*>(&key);

    cout << "Delete"
         << ", count=" << count
         << ", startingKey=" << startingKey
         << endl;

    for (size_t i = 0; i < count; i++) {
        memcpy(key, &startingKey, sizeof(startingKey));
        if (mdbm_lock(handle) != 1) {
            ostringstream oss;
            oss << "mdbm_lock failed, errno= " << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        rc = mdbm_delete(handle, k);
        if (rc) {
            ostringstream oss;
            oss << "mdbm_delete failed"
                << ", key=" << key
                << ", count=" << count
                << ", i=" << i
                << ", rc=" << rc
                << ", errno=" << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        if (mdbm_unlock(handle) != 1) {
            ostringstream oss;
            oss << "mdbm_unlock failed, errno= " << errno;
            perror(oss.str().c_str());
            exit(1);
        }
        startingKey++;
    }

    syncDb(handle);
}

static void
usage(const char *program)
{
    size_t length = strlen(program);
    char prog[length + 1];
    strcpy(prog, program);
    prog[length] = 0;
    const char* p = basename(prog);
    cout << endl
         << "usage: " << p << endl
         << "\t-a        Any locking type is allowed." << endl
         << "\t-c COUNT  Record count." << endl
         << "\t-i SIZE   Initial database size." << endl
         << "\t-k SIZE   Key Size." << endl
         << "\t-l        Enable large objects." << endl
         << "\t-m SIZE   Maximum database size." << endl
         << "\t-p SIZE   Page size (bytes)." << endl
         << "\t-P        Enable Pthreads locking(either from 32 or 64 bits applications)." << endl
         << "\t-r        Randomize size of records." << endl
         << "\t-s INT    Starting key number." << endl
         << "\t-v SIZE   Record value size." << endl
         << "\t-V        Verbose mode." << endl
         << endl;
    exit (1);
}

static void
parseOptions(int   argc,
             char *argv[])
{
    int c;
    opterr = 0;
    unsigned int val;

    while ((c = getopt (argc, argv, "ac:k:m:n:Pp:s:Vv:")) != -1) {
        switch (c) {
        case 'a':
            AnyLockingType = true;
            break;
        case 'c':
            RecordCount = strtoul(optarg, NULL, 10);
            break;
        case 'k':
            KeySize = strtoul(optarg, NULL, 0);
            break;
        case 'm':
            MaxDbSize = strtoul(optarg, NULL, 0);
            break;
        case 'P':
            SingleArch = true;
            break;
        case 'p':
            PageSize = strtoul(optarg, NULL, 0);
            break;
        case 's':
            StartingKey = strtoull(optarg, NULL, 10);
            break;
        case 'v':
            ValueSize = strtoul(optarg, NULL, 0);
            break;
        case 'V':
            Verbose = true;
            break;
        default:
            usage(argv[0]);
        }
    }

    if (Verbose)
        cout << "Input parameters:" << endl
             << "  PageSize=" << PageSize << endl
             << "  MaxDbSize=" << MaxDbSize << endl
             << "  KeySize=" << KeySize << endl
             << "  ValueSize=" << ValueSize << endl
             << "  RecordCount=" << RecordCount << endl
             << "  StartingKey=" << StartingKey << endl
             << "  Verbose=" << Verbose << endl
             << endl;
}

int
main(int   argc,
     char *argv[])
{
    parseOptions(argc, argv);
    srandom(1031);  // An arbitrary prime
    MDBM *mdbm = openDb(PageSize);

    // Set up the cache mode: cache_mode = LRU (Least Recently Used), which means that
    // the size-limited cache will track (in this case) the stores, and the sequential
    // fetches of fetchRecords() will start failing once insertRecords stores more items
    // that the cache can accommodate.
    if (mdbm_set_cachemode(mdbm, MDBM_CACHEMODE_LRU)) {
        cerr << "Unable to set cachemode" << endl;
        return 1;
    }

    insertRecords(mdbm, RecordCount, StartingKey, ValueSize);

    // For 1K pages, you can fit 8 records of 100bytes each per page.  So the following will work:
    // mdbm_cache -m 1024 -c 8 
    // mdbm_cache -m 2048 -c 16 
    // But the following will fail:
    // mdbm_cache -m 2048 -c 17 
    fetchRecords(mdbm, RecordCount, StartingKey);

    deleteRecords(mdbm, RecordCount, StartingKey);
    mdbm_close(mdbm);
}
