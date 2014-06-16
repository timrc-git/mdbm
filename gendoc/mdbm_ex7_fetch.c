/* Fetch all values for a given key.  Assumes more than on value was
 * inserted for a given key, via mbm_store() and the MDBM_INSERT_DUP.
 */

#include <assert.h>
#include <strings.h>
#include <yax/osutil.h>
#include <sys/fcntl.h>
#include "mdbm.h"

int main(int argc, char **argv)
{
    MDBM* db;
    char fn[MAXPATHLEN];

    MDBM_ITER iter;
    datum key, val;

    strlcpy(fn,yax_getroot(),sizeof(fn));
    strlcat(fn,"/tmp/example.mdbm",sizeof(fn));
    db = mdbm_open(fn,MDBM_O_RDWR,0666,0,0);
    if (!db) {
        perror("Unable to open database");
        exit(2);
    }

#define COMMON_KEY "mykey\0"
    key.dptr = COMMON_KEY;
    key.dsize = strlen(COMMON_KEY);

    MDBM_ITER_INIT( &iter );

    /* Loop through DB, looking at records with the same key.
     */
    while (mdbm_fetch_dup_r( db, &key, &val, &iter ) == 0) {
        printf("Found another: %.*s\n", val.dsize, val.dptr);
    }
    printf("End of db reached.\n");
    mdbm_close(db);
    exit(1);
}
