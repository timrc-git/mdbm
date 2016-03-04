/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <sys/fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#include "mdbm.h"
#include "mdbm_internal.h"
#include "mdbm_stats.h"


static void usage (void) {
    printf(
"Usage: mdbm_lock [options] <db filename>\n"
"Options:\n"
"       -d              Dump locking data (default)\n"
/*"     -e              Take an exclusive lock\n" */
"       -h              Display this help\n"
/*"     -p <index>      Take a partition/shared lock\n" */
"       -s <seconds>    Run for the specified time\n"
"       -r              Reset the locks. USE WITH CARE!\n"
);
    exit(1);
}

extern void print_lock_state_inner(struct mdbm_locks* locks);

extern struct mdbm_locks* open_locks_inner(const char* dbname, int flags, int do_lock, int* need_check);



int main (int argc, char** argv) {
    struct mdbm_locks* locks;
    uint64_t t0, tend;
    int flags = 0;

    int opt;
    int dump = 0;
    int excl = 0;
    int part = 0;
    int reset = 0;
    int secs = 0;
    int interval = 1;
    int need_check = 0;
    const char* dbname = NULL;
    char pathname[MAXPATHLEN+1];

    /*mdbm_log(LOG_ERR, "This is a log message!!!\n"); */

    while ((opt = getopt(argc,argv,"dehp:s:r")) != -1) {
        switch (opt) {
            case 'd': dump = 1; break;
            /*case 'e': excl = 1; break; */
            case 'h': usage(); break;
            /*case 'p': part = atoi(optarg); break; */
            case 's': secs = atoi(optarg); break;
            case 'r': reset = 1; break;
            default:
                fprintf(stderr, "Unexpected argument:'%c'\n", opt);
                exit(1);
        }
    }

    if (optind+1 != argc) {
        usage();
    }

    dbname=argv[optind];
    if (!realpath(dbname,pathname)) {
      fprintf(stderr,"Unable to resolve real path for mdbm: %s\n",dbname);
      exit(2);
    }
    dbname = pathname;

    /* TODO enforce pthreads-only at a higher level */
    locks=open_locks_inner(dbname, flags|MDBM_ANY_LOCKS, 0, &need_check);
    if (!locks) {
      perror(dbname);
      exit(2);
    }

    if (!(excl || part || reset)) {
      dump = 1;
    }

    if (need_check) {
      fprintf(stderr, "Warning: previous lock owner died! \nYou should run mdbm_check\n");
    }
    
    if (dump) {
      fprintf(stderr, "============= printing state ===============\n");
      print_lock_state_inner(locks);
    }

    if (excl) {
      /* TODO get excl lock */
    }

    if (part) {
      /* TODO get part lock */
    }


    t0 = get_time_usec();
    tend = t0 + (uint64_t)secs*1000000;

    while (t0 < tend) {
        uint64_t t, tnext;

        tnext = t0 + interval*1000000;
        t = get_time_usec();
        if (t < tnext) {
            struct timespec tv;

            t = tnext - t;
            tv.tv_sec = t / 1000000;
            tv.tv_nsec = (t % 1000000) * 1000;
            nanosleep(&tv,NULL);
        }

        t0 = get_time_usec();
        if (dump) {
          print_lock_state_inner(locks);
        }
    }

    if (part) {
      /* TODO release part lock */
    }

    if (excl) {
      /* TODO release excl lock */
    }

    if (reset) {
      /*if (do_lock_reset(argv[optind], flags)) { */
      if (mdbm_lock_reset(argv[optind], flags)) {
          perror(argv[optind]);
          exit(2);
      }
    }
    
    /* TODO, need close-locks-inner, or to more propperly abstract the locks struct... */

    return 0;
}

