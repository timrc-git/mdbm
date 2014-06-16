#include <assert.h>
#include <string.h>
#include <yax/osutil.h>
#include <sys/fcntl.h>
#include "mdbm.h"

int main(int argc, char **argv)
{
    char buf[2048];
    MDBM* db;
    char fn[MAXPATHLEN];
    int notfound;

    snprintf(fn,sizeof(fn),"%s%s",yax_getroot(),"/tmp/example.mdbm");
    db = mdbm_open(fn,MDBM_O_RDWR,0666,0,0);
    if (!db) {
        perror("Unable to open database");
        exit(2);
    }
    notfound = 0;
    while (fgets(buf,sizeof(buf),stdin)) {
        datum key, val;
        static char DELETED[] = "deleted";
        int len = strlen(buf);
        int ret;

        if (buf[len-1] == '\n') {
            buf[--len] = 0;
        }
        key.dptr = buf;
        key.dsize = len;
        val.dptr = DELETED;
        val.dsize = sizeof(DELETED)-1;
        mdbm_lock(db);
        ret = mdbm_store(db,key,val,MDBM_REPLACE);
        mdbm_unlock(db);
        if (ret) {
            perror("Database store failed");
        }
    }
    mdbm_close(db);
    return 0;
}
