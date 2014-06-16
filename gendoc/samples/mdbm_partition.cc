// $Id$

// example code that performs store/fetch/delete using partitioned locks

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
static uint64_t MaxDbSize = InitialDbSize ; // By default, limit size of MDBM to 16MB

static bool   LargeObjects = false;
static size_t ValueSize = 100;
static size_t RecordCount = 500;

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
openDb(unsigned int  pageSize = PageSize,
       unsigned int  initialSize = InitialDbSize,
       bool          largeObjects = LargeObjects)
{
    // All processes accessing an MDBM must use the same locking mode: partitioned locking
    string fileName = "/tmp/test_part.mdbm";
    unlink(fileName.c_str());  // Cleanup for testing purposes
    unsigned int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;   // 0664
    int flags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC
                | MDBM_DBSIZE_MB    // Initial size specified in MBytes
                | (largeObjects ? MDBM_LARGE_OBJECTS : 0) 
                | MDBM_PARTITIONED_LOCKS;

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
                             initialSize);
    if (handle == NULL) {
        perror("mdbm_open failed");
        exit(1);
    };

    if (MaxDbSize > 0) {
        if (MaxDbSize < InitialDbSize) {
            cerr << "ERROR: MaxDbSize < InitialDbSize"
                 << ", MaxDbSize=" << MaxDbSize
                 << ", InitialDbSize=" << InitialDbSize
                 << endl;
            exit(1);
        }

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
                << ", InitialDbSize=" << InitialDbSize
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
    memset(value, 0xAB, sizeof(value));  // arbitrary value
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
        if (mdbm_plock(handle, &k, 0) != 1) {
            ostringstream oss;
            oss << "mdbm_plock failed, errno= " << errno;
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
        if (mdbm_punlock(handle, &k, 0) != 1) {
            ostringstream oss;
            oss << "mdbm_punlock failed, errno= " << errno;
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
        if (mdbm_plock(handle, &k, 0) != 1) {
            ostringstream oss;
            oss << "mdbm_plock failed, errno= " << errno;
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
        if (mdbm_punlock(handle, &k, 0) != 1) {
            ostringstream oss;
            oss << "mdbm_punlock failed, errno= " << errno;
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
        if (mdbm_plock(handle, &k, 0) != 1) {
            ostringstream oss;
            oss << "mdbm_plock failed, errno= " << errno;
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
        if (mdbm_punlock(handle, &k, 0) != 1) {
            ostringstream oss;
            oss << "mdbm_punlock failed, errno= " << errno;
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
         << "\t-i SIZE   Initial database size (MBytes)." << endl
         << "\t-k SIZE   Key Size." << endl
         << "\t-l        Enable large objects." << endl
         << "\t-m SIZE   Maximum database size (MBytes)." << endl
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

    while ((c = getopt (argc, argv, "ac:i:k:lm:n:Pp:rs:Vv:")) != -1) {
        switch (c) {
        case 'a':
            AnyLockingType = true;
            break;
        case 'c':
            RecordCount = strtoul(optarg, NULL, 10);
            break;
        case 'i':
            val = strtoul(optarg, NULL, 0);
            if (val > 0) {
                MaxDbSize = InitialDbSize = val;
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
             << "  InitialDbSize(in MB)=" << InitialDbSize << endl
             << "  MaxDbSize(in MB)=" << MaxDbSize << endl
             << "  LargeObjects=" << (LargeObjects ? "true" : "false") << endl
             << "  KeySize=" << KeySize << endl
             << "  ValueSize=" << ValueSize << endl
             << "  RecordCount=" << RecordCount << endl
             << "  RandomValueSize=" << (RandomValueSize ? "true" : "false") << endl
             << "  StartingKey=" << StartingKey << endl
             << "  Verbose=" << Verbose << endl
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
