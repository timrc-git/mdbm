#include <assert.h>
#include <string.h>
#include <yax/osutil.h>
#include <sys/fcntl.h>
#include "mdbm.h"

struct user_rec {
    int u_count;
};

int main(int argc, char **argv)
{
    MDBM* db;
    char fn[MAXPATHLEN];
    char buf[2048];
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
        int len = strlen(buf);
        if (buf[len-1] == '\n') {
            buf[--len] = 0;
        }
        key.dptr = buf;
        key.dsize = len;
        mdbm_lock(db);
        val = mdbm_fetch(db,key);
        if (val.dptr) {
            struct user_rec* recp;
            assert(val.dsize == sizeof(*recp));
            recp = (struct user_rec*)val.dptr;
            if (--recp->u_count == 0) {
                mdbm_delete(db,key);
            }
        } else {
            notfound++;
        }
        mdbm_unlock(db);
    }
    mdbm_close(db);
    return 0;
}
