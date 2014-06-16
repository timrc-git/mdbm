/*
 * Generate a db with a bunch of different entires, all sharing the
 * same key.
 */

#include <strings.h>
#include <yax/osutil.h>
#include <sys/fcntl.h>
#include "mdbm.h"

int main(int argc, char **argv)
{
    MDBM* db;
    char fn[MAXPATHLEN];
    char buf[2048];
    int num;
    datum key, val;
    int ret;

    strlcpy(fn,yax_getroot(),sizeof(fn));
    strlcat(fn,"/tmp/example.mdbm",sizeof(fn));
    db = mdbm_open(fn,MDBM_O_RDWR|MDBM_O_CREAT,0666,0,0);
    if (!db) {
        perror("Unable to create database");
        exit(2);
    }
    
#define COMMON_KEY_ONE "mykey\0"
#define COMMON_KEY_TWO "fooba\0"

    /* Insert a bunch of different values */
    for (num=0; num<16; num++) {
        if (num%2) {
            key.dptr = COMMON_KEY_ONE;
        } else {
            key.dptr = COMMON_KEY_TWO;
        }
        key.dsize = strlen( key.dptr );

        sprintf(buf, "Value #%d", num);
        val.dptr = buf;
        val.dsize = strlen(buf);

        mdbm_lock(db);
        printf("Storing [%.*s] value [%.*s]\n",
               key.dsize, key.dptr, val.dsize, val.dptr);
        ret = mdbm_store(db, key, val, MDBM_INSERT_DUP);
        mdbm_unlock(db);
        if (ret == -1) {
            perror("Database store failed");
        }
    }
    mdbm_close(db);
}
