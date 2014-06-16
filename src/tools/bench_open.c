/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <string.h>

#include "mdbm.h"


uint64_t GetTime() {
  struct timeval now;
  uint64_t t=0;
  gettimeofday(&now, NULL);
  t = now.tv_sec*1000000;
  t += now.tv_usec;
  return t;
}

double GetFloatTime() {
  struct timeval now;
  gettimeofday(&now, NULL);
  return ((double)now.tv_sec) + ((double)now.tv_usec)/1000000.0;
}

#define dbName "/tmp/open_test.mdbm"

int
main (int argc, char** argv)
{
    /*int REPS = 1*1000*1000;*/
    int REPS = 100*1000;
    int lockModes[] = {
       MDBM_OPEN_NOLOCK,
       0, /* EXCLUSIVE*/
       MDBM_RW_LOCKS,
       MDBM_PARTITIONED_LOCKS
    };
    const char* lockModeNames[] = {
      "NONE",
      "EXCLUSIVE",
      "SHARED",
      "PARTITIONED"
    };

    const char* lockName = "/tmp/.mlock-named" dbName "._int_";
    MDBM *db;
    int oflags = 0;
    const char* testName;
    int i, j, l;
    uint64_t openSum = 0;
    double start, end;
    uint64_t opStart, opEnd;

    /*  timing various open operations...*/
    for (l=0; l<4; ++l) { /* foreach NOLOCK, EXCLUSIVE, SHARED, PARTITIONED*/
      oflags = lockModes[l];
      for (j=0; j<4; ++j) {
        openSum = 0;
        switch (j) {
          case 0: testName = "reopen"; break;
          case 1: testName = "new-db"; break;    /* delete mdbm on each cycle*/
          case 2: testName = "new-lock"; break;  /* delete lockfile on each cycle*/
          case 3: testName = "new-both"; break;  /* delete both on each cycle*/
        };
        /*uint64_t start = GetTime();*/
        start = GetFloatTime();
        /*fprintf(stderr, "Test:%s:%s\t started at %f for %d reps\n", */
        /*    testName, lockModeNames[l], start, REPS);*/
        for (i=0; i<REPS; ++i) {
          opStart = GetTime();
          db = mdbm_open(dbName, MDBM_O_RDWR|MDBM_O_CREAT|oflags, 0644, 0, 0);
          if (db == NULL) {
              perror(argv[optind]);
              exit(2);
          }
          opEnd = GetTime();
          openSum += opEnd - opStart;
          mdbm_close(db);
          if (j&1) {
            unlink(dbName);
          }
          if (j&2) {
            unlink(lockName);
          }
        }
        /*uint64_t end = GetTime();*/
        end = GetFloatTime();
        /*fprintf(stderr, "Test:%s:%s\t ended at %f for %d reps\n", */
        /*    testName, lockModeNames[l], end, REPS);*/
        fprintf(stderr, "Test:%s:%s\t elapsed % 7.3f (open = %7.3f) for %d reps\n", 
            testName, lockModeNames[l], end-start, ((double)openSum)/1000000.0, REPS);
      }
    }


    return 0;
}
