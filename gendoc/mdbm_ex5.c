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
    kvpair kv;

    snprintf(fn,sizeof(fn),"%s%s",yax_getroot(),"/tmp/example.mdbm");
    db = mdbm_open(fn,MDBM_O_RDWR,0666,0,0);
    if (!db) {
        perror("Unable to open database");
        exit(2);
    }

    /* Start at the beginning */
    kv = mdbm_first_r( db, &iter );
    if ((kv.key.dsize == 0) && (kv.val.dsize == 0)) {
          printf("Database was empty!\n");
          mdbm_close(db);
          exit(3);
    }

    while (! ((kv.key.dsize == 0) && (kv.val.dsize == 0))) {
        printf("Key [%.*s] Val [%.*s]\n",
               kv.key.dsize, kv.key.dptr,
               kv.val.dsize, kv.val.dptr);

        kv = mdbm_next_r( db, &iter );
    }
    printf("End of db reached.\n");
    mdbm_close(db);
    exit(1);
}
