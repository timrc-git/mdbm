// $Id$

// Example code that performs store/fetch/delete on a cache+backingstore(B/S) MDBM
// Once cache+B/S is set up, you should access the cache only.
// Generally, the B/S is likely very large and other processes will likely need to access it
// using windowed mode

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
static bool RandomValueSize = false;

// Single architecture (32 bits only or 64 bits only) - Tells MDBM to use pthread-locks
static bool SingleArch = false;
static bool AnyLockingType = false;      // Do we allow any locking type

static unsigned int PageSize = 8192;
static unsigned int InitialDbSize = 16; // 16MB.
static uint64_t MaxDbSize = 4;          // Default maximum Cache size is 4MB

static bool   LargeObjects = false;
static size_t ValueSize = 100;
static size_t RecordCount = 500;

static uint64_t StartingKey = 0ULL;
static size_t   KeySize = sizeof(uint64_t);

uint64_t WindowSize = 0;   // You can use Windowed mode with cache+backingstore, on the B/S

MDBM *BsHandle = NULL;     // Maintain a handle to the B/S, in order to cleanly close it.

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
openDb(unsigned int  pageSize = PageSize,
       unsigned int  initialSize = InitialDbSize,
       bool          largeObjects = LargeObjects)
{
    string bsFileName = "/tmp/test_bs.mdbm";    // File Name of Backingstore MDBM
    unlink(bsFileName.c_str());   // Cleanup for testing purposes
    unsigned int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;   // 0664
    // All processes accessing one MDBM must use the same locking mode: in this case,
    // exclusive locking is being used because it is the default and no flags 
    // requesting partitioned or shared locking are being passed to mdbm_open
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC
                | MDBM_DBSIZE_MB    // Initial size specified in MBytes
                | (largeObjects ? MDBM_LARGE_OBJECTS : 0);

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

    BsHandle = mdbm_open(bsFileName.c_str(),
                             flags,
                             mode,
                             pageSize,
                             initialSize);
    if (BsHandle == NULL) {
        perror("mdbm_open failed");
        exit(1);
    };

    // Set up Windowed access to the B/S MDBM
    if (WindowSize) {
        WindowSize *= (1024 * 1024);   // Size in MBytes
        uint32_t sysPageSz = sysconf(_SC_PAGESIZE);
        if (PageSize % sysPageSz != 0) {
            cerr << "Windowed mode can only be set up when page size is a multiple of the"
                 << " System page size of " << sysPageSz << endl;
            exit(1);
        }
        // Here's how to compute the (minimum recommended) size of an MDBM window:
        // ((4 * max-largest-object-size) + (N * 8 * pagesize)), where N = number of CPU cores.
        int Ncores = 8;  // Use 8 cores for this example: can also  /proc/cpuinfo if needed
        uint64_t minWindowSize = (4 * ValueSize) + (Ncores * 8 * PageSize);
          // Round to the next multiple of system page size
        minWindowSize += (sysPageSz - 1);
        minWindowSize = (minWindowSize / sysPageSz) * sysPageSz;
        if (WindowSize < minWindowSize) {
            cout << "Increasing window size to " << minWindowSize
                 << " because requested " << WindowSize << " is too low" << endl;
            WindowSize = minWindowSize;
        }
        if (mdbm_set_window_size(BsHandle, WindowSize) != 0) {
            cerr << "Unable to set Windowed mode of size=" 
                 << WindowSize << endl;
            exit(1);
        }
    }

    string fileName = "/tmp/test_cache.mdbm";    // File Name of the cache MDBM
    unlink(fileName.c_str());   // Clean up cache

    // flags can be different for the cache and B/S, but we just happen to use the same
    // It makes sense of the cache to have the same initial size and maximum size, since
    // that way you're moving the work of splitting the MDBM to the maximum size into the
    // initialization, which makes sense if you're trying to improve cache performance.
    MDBM *handle = mdbm_open(fileName.c_str(), flags, mode, pageSize, MaxDbSize);

    // You should (almost) always specify a maximum size for a cache MDBM
    if (MaxDbSize > 0) {
        // Watch out for 32 bit integer overflow
        uint64_t bytes = MaxDbSize * 1024 * 1024;
        unsigned int pages = (unsigned int) (bytes / PageSize);
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

    if (mdbm_set_backingstore(handle, MDBM_BSOPS_MDBM, BsHandle, 0) != 0) {
        cerr << "Unable to set Backingstore" << endl;
        exit(1);
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
    memset(value, 0xAB, sizeof(value));
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
        if (RandomValueSize) {
            unsigned int size = random() % valueSize;
            if (size == 0)
                size++;
            totalRecordSize += size;
            if (Verbose)
                cout << "size=" << size << endl;
            v.dsize = size;
        } else if (Verbose) {
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
    if (RandomValueSize)
        cout << "Average record size=" << totalRecordSize / count << endl;

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

    for (size_t i = 0; i < count; i++) {
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
                << ", key=" << key
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
        startingKey++;
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
         << "\t-i SIZE   Initial database size of Backingstore (MBytes)." << endl
         << "\t-k SIZE   Key Size." << endl
         << "\t-l        Enable large objects." << endl
         << "\t-m SIZE   Maximum database size of cache (MBytes)." << endl
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
    unsigned int initialVal = 0;

    while ((c = getopt (argc, argv, "ac:i:k:lm:n:Pp:rs:Vv:w:")) != -1) {
        switch (c) {
        case 'a':
            AnyLockingType = true;
            break;
        case 'c':
            RecordCount = strtoul(optarg, NULL, 10);
            break;
        case 'i':
            initialVal = strtoul(optarg, NULL, 0);
            if (initialVal > 0) {
                InitialDbSize = initialVal;
            }
            break;
        case 'k':
            KeySize = strtoul(optarg, NULL, 0);
            break;
        case 'l':
            LargeObjects = true;
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
        case 'r':
            RandomValueSize = true;
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
        case 'w':
            WindowSize = strtoul(optarg, NULL, 0);
            break;

        default:
            usage(argv[0]);
        }
    }

    if (KeySize + ValueSize >= PageSize) {
        LargeObjects = true;
    }

    if (Verbose)
        cout << "Input parameters:" << endl
             << "  PageSize=" << PageSize << endl
             << "  Backingstore InitialDbSize(in MB)=" << InitialDbSize << endl
             << "  Cache MaxDbSize(in MB)=" << MaxDbSize << endl
             << "  LargeObjects=" << (LargeObjects ? "true" : "false") << endl
             << "  KeySize=" << KeySize << endl
             << "  ValueSize=" << ValueSize << endl
             << "  RecordCount=" << RecordCount << endl
             << "  RandomValueSize=" << (RandomValueSize ? "true" : "false") << endl
             << "  StartingKey=" << StartingKey << endl
             << "  Verbose=" << Verbose << endl
             << "  WindowSize(in MB)=" << WindowSize << endl
             << endl;
}

int
main(int   argc,
     char *argv[])
{
    parseOptions(argc, argv);
    srandom(2141);  // An arbitrary prime
    MDBM *mdbm = openDb(PageSize, InitialDbSize, LargeObjects);

    insertRecords(mdbm, RecordCount, StartingKey, ValueSize);
    fetchRecords(mdbm, RecordCount, StartingKey);
    deleteRecords(mdbm, RecordCount, StartingKey);
    mdbm_close(mdbm);
}
