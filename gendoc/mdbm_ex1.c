#include <string.h>
#include <yax/osutil.h>
#include <sys/fcntl.h>
#include "mdbm.h"

int main(int argc, char **argv)
{
    MDBM* db;
    char fn[MAXPATHLEN];
    char buf[2048];
    int ndups;

    snprintf(fn,sizeof(fn),"%s%s",yax_getroot(),"/tmp/example.mdbm");
    db = mdbm_open(fn,MDBM_O_RDWR|MDBM_O_CREAT,0666,0,0);
    if (!db) {
        perror("Unable to create database");
        exit(2);
    }
    ndups = 0;
    while (fgets(buf,sizeof(buf),stdin)) {
        int len = strlen(buf);
        char* s;
        if (buf[len-1] == '\n') {
            buf[--len] = 0;
        }
        s = strchr(buf,':');
        if (s) {
            datum key, val;
            int ret;
            key.dptr = buf;
            key.dsize = s-buf;
            val.dptr = s+1;
            val.dsize = len-key.dsize-1;
            mdbm_lock(db);
            ret = mdbm_store(db,key,val,MDBM_INSERT);
            mdbm_unlock(db);
            if (ret == 1) {
                ndups++;
            } else if (ret == -1) {
                perror("Database store failed");
            }
        }
    }
    mdbm_sync(db);      /* optional flush to disk */
    mdbm_close(db);
    return 0;
}
