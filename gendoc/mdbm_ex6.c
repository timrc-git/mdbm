/* Use iterators to walk through MDBM.
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
    datum key;

    strlcpy(fn,yax_getroot(),sizeof(fn));
    strlcat(fn,"/tmp/example.mdbm",sizeof(fn));
    db = mdbm_open(fn,MDBM_O_RDWR,0666,0,0);
    if (!db) {
        perror("Unable to open database");
        exit(2);
    }

    /* Start at the beginning */
    key = mdbm_firstkey_r( db, &iter );
    if (key.dsize == 0) {
        printf("Database was empty!\n");
        mdbm_close(db);
        exit(3);
    }

    while (! (key.dsize == 0)) {
        printf("Key [%.*s]\n", key.dsize, key.dptr);

        key = mdbm_nextkey_r( db, &iter );
    }
    printf("End of db reached.\n");
    mdbm_close(db);
    exit(1);
}
