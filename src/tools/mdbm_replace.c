/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mdbm.h"

enum ExitErrors {
  ARG_ERROR     =1,
  REPLACE_ERROR =2,
  RENAME_ERROR  =3,
  STAT_ERROR    =4,
  OPEN_ERROR    =5,
  TRUNC_CACHE_ERROR =6,
  FETCH_KEYS_ERROR  =7,
  DELETE_KEY_ERROR  =8,
  REPLACE_KEY_ERROR =9,
  LOCK_ERROR =10
};


extern char *optarg;
extern int optind, opterr, optopt;

int LockingFlag = MDBM_PARTITIONED_LOCKS;
int TraceFlag   = 0;

static void
usage ()
{
    printf(
      "Usage: mdbm_replace [Options] <existing-mdbm-path> <new-mdbm-path>\n\n"
      "Options:\n"
      "--cache existing-mdbm-cache-db : a cache db used with a backing store db that will be replaced\n"
      "--clearcache : default, clear/remove all entries from the specified cache\n"
      "--refetch : for all entries in the specified cache, replace or delete from the new backing store db\n"
      "--preload : preload the new MDBM to get it to be memory resident prior to the replace\n"
      "--lock : specify the type of locking to use:\n"
      "         exclusive  -  Exclusive locking (default)\n"
      "         partition  -  Partition locking\n"
      "         shared     -  Shared locking\n"
      "\nExample: mdbm_replace /tmp/dbs/db.mdbm new_db.mdbm\n"
      "\nExample: mdbm_replace --cache /tmp/dbs/cachedb.mdbm /tmp/dbs/db.mdbm new_db.mdbm\n"
      "\nUpon replacement, the original MDBM is updated to notify all open handles that\n"
      "a new MDBM exists; causing all existing MDBM users to re-open the (now\n"
      "new) MDBM.  Essentially, the new mdbm is renamed to the old mdbm, and\n"
      "the old mdbm is reopened.\n\n"
      "If an associated MDBM cache db is specified, that cache's contents will be updated\n"
      "so that they do not conflict with the contents of the backing store.\n"
      "\nPlease note also that the existing and new MDBM's must reside on the same file-system.\n"
    );
    exit(ARG_ERROR);
}

int
clearCache(char *cacheDB)
{
    MDBM *dbh = mdbm_open(cacheDB, MDBM_O_RDWR|LockingFlag, 0644, 0, 0);
    if (dbh == NULL) {
        perror("mdbm_replace:clearCache: FAILED to open the cache db: ");
        return OPEN_ERROR;
    }
    mdbm_purge(dbh);
    mdbm_close(dbh);

    return 0;
}

/*
 * We must manipulate the 2 db's independently, rather than together as a 
 * cache+backingstore configuration (via mdbm_set_backingstore). 
 * This is necessary because we cannot delete an entry from the cache without
 * deleting it from the backing store too. There is a flag called MDBM_CACHE_ONLY,
 * but this could only be used with mdbm_store(), not mdbm_delete().
 *
 * Iterate the cache:
 *   - for each key, fetch the value from bsDB
 *   - if we got a value from the backing store, then replace the value in the cache
 *   - else delete the key from the cache
 */
int
refetchKeys(char *cacheDB, char *bsDB)
{
    int ret = 0;
    int traceDelCnt = 0;
    int traceRepCnt = 0;
    MDBM_ITER iter;
    datum dkey, dval;
    MDBM *cdbh, *bsdbh;

    cdbh = mdbm_open(cacheDB, MDBM_O_RDWR | LockingFlag, 0644, 0, 0);
    if (cdbh == NULL) {
        perror("mdbm_replace:refetchKeys: failed to open the cache db: ");
        return OPEN_ERROR;
    }

    bsdbh = mdbm_open(bsDB, MDBM_O_RDWR | LockingFlag, 0644, 0, 0);
    if (bsdbh == NULL) {
        perror("mdbm_replace:refetchKeys: failed to open the backing store db: ");
        mdbm_close(cdbh);
        return OPEN_ERROR;
    }

    /* user requested exclusive locking */
    if (LockingFlag == 0 && mdbm_lock(cdbh) == -1) {
        perror("mdbm_replace:refetchKeys: failed to exclusive lock the cache db: ");
        mdbm_close(cdbh);
        mdbm_close(bsdbh);
        return LOCK_ERROR;
    }

    dkey = mdbm_firstkey_r(cdbh, &iter);
    if (dkey.dsize == 0) {
        fprintf(stderr, "mdbm_replace:refetchKeys: FAILED to fetch keys from cache=%s\n", cacheDB);
        return FETCH_KEYS_ERROR;
    }

    /* within the loop, we will lock the backing store db by key, 
       process the associated entry in the cache db,
       then unlock the backing store db by key */
    while (dkey.dsize > 0) {
        if (mdbm_lock_smart(bsdbh, &dkey, MDBM_O_RDONLY) == -1) {
            perror("mdbm_replace:refetchKeys: failed to lock the backing store db: ");
            ret = LOCK_ERROR;
            break;
        }

        dval = mdbm_fetch(bsdbh, dkey);

        if (dval.dsize == 0) {
            if (mdbm_delete(cdbh, dkey) == -1) {
                fprintf(stderr,
                        "mdbm_replace:refetchKeys: FAILED to delete key from cache=%s\n", cacheDB);
                ret = DELETE_KEY_ERROR;
            } else {
                traceDelCnt++;
            }
        } else if (mdbm_store(cdbh, dkey, dval, MDBM_REPLACE) == -1) {
            fprintf(stderr,
                    "mdbm_replace:refetchKeys: FAILED to replace key in cache=%s\n", cacheDB);
            ret = REPLACE_KEY_ERROR;
        } else {
            traceRepCnt++;
        }

        if (mdbm_unlock_smart(bsdbh, &dkey, 0) == -1) {
            perror("mdbm_replace:refetchKeys: failed to unlock the backing store db: ");
            ret = LOCK_ERROR;
            break;
        }

        dkey = mdbm_nextkey_r(cdbh, &iter);
    }

    if (LockingFlag == 0 && mdbm_unlock(cdbh) == -1) {
        perror("mdbm_replace:refetchKeys: failed to exclusive unlock the cache db: ");
        ret = LOCK_ERROR;
    } else if (ret == LOCK_ERROR) {
        /* this means we broke out of the iteration loop early due to locking
           issue with backing store, so must perform an explicit unlock 
           on the cache now */
        if (mdbm_unlock_smart(cdbh, NULL, 0) == -1) {
            perror("mdbm_replace:refetchKeys: failed to unlock the cache db: ");
        }
    }
    mdbm_close(cdbh);
    mdbm_close(bsdbh);

    if (TraceFlag) {
        printf("TRACE:mdbm_replace:refetchKeys: deleted %d keys and replaced %d keys\n",
               traceDelCnt, traceRepCnt);
    }

    return ret;
}

int
main (int argc, char** argv)
{
    struct stat sb;
    int    opt, option_index;
    int    clearCacheFlag  = 0;
    int    refetchKeysFlag = 0;
    int    preloadFlag = 0;
    char  *existingDB      = NULL;
    char  *newDB           = NULL;
    char  *cacheDB         = NULL;
    char  *lockType        = NULL;
    MDBM  *preloaded       = NULL;

    static struct option long_options[] = {
        {"cache",      1, 0, 1},
        {"clearcache", 0, 0, 2},
        {"refetch",    0, 0, 3},
        {"lock",       1, 0, 4},
        {"trace",      0, 0, 5},
        {"preload",    0, 0, 6},
        {0, 0, 0, 0}
    };
    for (opt = getopt_long(argc, argv, "", long_options, &option_index);
         opt != -1;
         opt = getopt_long(argc, argv, "", long_options, &option_index))
    {
        switch (opt) {
            case 1:
                if (refetchKeysFlag == 0) {
                    clearCacheFlag = 1;
                }
                cacheDB = optarg;
                break;
            case 2:
                clearCacheFlag  = 1;
                refetchKeysFlag = 0;
                break;
            case 3:
                clearCacheFlag  = 0;
                refetchKeysFlag = 1;
                break;
            case 4:
                lockType = optarg;
                break;
            case 5:
                TraceFlag = 1;
                break;
            case 6:
                preloadFlag = 1;
                break;
            default:
                fprintf(stderr, "Invalid option specified: %s\n", optarg);
                usage();
        }
    }

    if (optind < argc) {
        existingDB = argv[optind++];
    } else {
        fprintf(stderr, "Required option <existing-mdbm-path> is missing.\n");
        usage();
    }
    if (optind < argc) {
        newDB = argv[optind++];
    } else {
        fprintf(stderr, "Required option <new-mdbm-path> is missing.\n");
        usage();
    }

    if (optind < argc) {
        fprintf(stderr, "Unexpected arguments found on the command line.\n");
        usage();
    }

    if (lockType != NULL) {
        if (strcasecmp(lockType, "exclusive") == 0) {
            LockingFlag = 0;
        } else if (strcasecmp(lockType, "partition") == 0) {
            LockingFlag = MDBM_PARTITIONED_LOCKS;
        } else if (strcasecmp(lockType, "shared") == 0) {
            LockingFlag = MDBM_RW_LOCKS;
        } else {
            fprintf(stderr, "Invalid locking argument %s\n", lockType);
            exit(ARG_ERROR);
        }
    }

    if (cacheDB == NULL && (clearCacheFlag || refetchKeysFlag)) {
        fprintf(stderr, "Invalid cache flag(clearcache=%d, refetch=%d) specified although no cache"
                        " was specified\n", clearCacheFlag, refetchKeysFlag);
        exit(ARG_ERROR);
    }

    if (TraceFlag) {
        printf("TRACE:mdbm_replace: Using existing db name=%s and new db name=%s"
               " Locking flag to open db with=%x\n", existingDB, newDB, LockingFlag);
    }

    /* If the destination file doesn't exist; merely move the new
     * one into place.
     */
    if ((stat(existingDB, &sb)) == -1) {
        if (errno == ENOENT) {
            /* Destination file does not exist.  Attempt rename instead. */
            if (rename( newDB, existingDB )) {
                /* Error! */
                perror("mdbm_replace: attempt to rename failed: ");
                return RENAME_ERROR;
            }
            /* Success */
            return 0;
        } else {
            /* Unknown stat failure */
            perror("mdbm_replace: unable to stat dest file: ");
            return STAT_ERROR;
        }
    }

    if (preloadFlag == 1) {
        preloaded = mdbm_open(newDB, MDBM_O_RDONLY,0,0,0);
        if (!preloaded) {
            perror("mdbm_replace: cannot be opened for preloading: ");
        } else if (mdbm_preload(preloaded) != 0) {
            perror("mdbm_replace: failed to preload: ");
        }
    }

    /* ASSERT:  Dest file was stat'able.  Replace it. */
    if (mdbm_replace_file(existingDB, newDB) == -1) {
        perror("mdbm_replace: DB replacement failed: ");
        return REPLACE_ERROR;
    }

    /* if there is a cache, now we can process it to get it in-sync with the bs */
    if (clearCacheFlag) {
        return clearCache(cacheDB);
    } else if (refetchKeysFlag) {
        return refetchKeys(cacheDB, existingDB);
    }

    if (preloadFlag && preloaded) {
        mdbm_close(preloaded);
    }

    return 0;
}
