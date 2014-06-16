#include <assert.h>
#include <string.h>
#include <yax/osutil.h>
#include <sys/fcntl.h>
#include "mdbm.h"

int main(int argc, char **argv)
{
    MDBM* db;
    char fn[MAXPATHLEN];
    kvpair kv;

    snprintf(fn,sizeof(fn),"%s%s",yax_getroot(),"/tmp/example.mdbm");
    db = mdbm_open(fn,MDBM_O_RDONLY,0666,0,0);
        if (!db) {
            perror("Unable to open database");
            exit(2);
        }

    mdbm_lock(db);
    kv = mdbm_first(db);
    while (kv.key.dptr) {
        printf("%.*s %.*s\n",
               kv.key.dsize,(char*)kv.key.dptr,
               kv.val.dsize,(char*)kv.val.dptr);
        kv = mdbm_next(db);
    }
    mdbm_unlock(db);
    mdbm_close(db);
    return 0;
}
