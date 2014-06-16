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

static void
usage (int err)
{
    printf("\
Usage: mdbm_rstats [options] <db filename>\n\
Options:\n\
        -e              Show error statistics\n\
        -h              Display this help\n\
        -H <lines>      Specify frequency of column headers\n\
        -l              Show locking statistics\n\
        -L              Log mode (no pause on inactivity)\n\
        -p              Show page windowing statistics\n\
        -s <seconds>    Run for specified number of seconds, then exit\n\
        -t              Show timing histograms\n\
        -w <seconds>    Delay between reports\n");
    exit(err);
}


static void
print_thist (const char* what, mdbm_rstats_val_t* v)
{
    int i;
    int p = 0;
    for (i = 0; i < MDBM_RSTATS_THIST_MAX; i++) {
        if (v->thist[i]) {
            int us;
            if (i < 1*10) {
                us = (i+1)*10;
            } else if (i < 2*10) {
                us = (i-1*10+1)*100;
            } else if (i < 3*10) {
                us = (i-2*10+1)*1000;
            } else if (i < 4*10) {
                us = (i-3*10+1)*10000;
            } else if (i < 5*10) {
                us = (i-4*10+1)*100000;
            } else if (i < 6*10) {
                us = (i-5*10+1)*1000000;
            } else {
                us = -1;
            }
            if (!p) {
                printf("%-8s",what);
                p = 1;
            }
            if (us < 0) {
                printf(" >10s=%llu",(unsigned long long)v->thist[i]);
            } else if (us < 1000) {
                printf(" %.3f=%llu",(double)us/1000,(unsigned long long)v->thist[i]);
            } else {
                printf(" %d=%llu",us/1000,(unsigned long long)v->thist[i]);
            }
        }
    }
    if (p) {
        putchar('\n');
    }
}


int
main (int argc, char** argv)
{
    struct mdbm_rstats_mem* m;
    mdbm_rstats_t* s;
    mdbm_rstats_t s0, d;
    uint64_t t0, tend;
    int opt;
    int interval = 1;
    int header_count = 24;
    int hcount = 0;
    int errors = 0;
    int idle = 0;
    int locks = 0;
    int pages = 0;
    int times = 0;
    int log = 0;
    int secs = 999999;

    while ((opt = getopt(argc,argv,"ehH:lLps:tw:")) != -1) {
        switch (opt) {
        case 'e':
            errors = 1;
            break;

        case 'h':
            usage(0);
            break;

        case 'H':
            header_count = atoi(optarg);
            break;

        case 'l':
            locks = 1;
            break;

        case 'L':
            log = 1;
            break;

        case 'p':
            pages = 1;
            break;

        case 's':
            secs = atoi(optarg);
            break;

        case 't':
            times = 1;
            break;

        case 'w':
            interval = atoi(optarg);
            break;
        default:
            usage(1);
        }
    }

    if (optind+1 != argc) {
        usage(1);
    }

    memset(&s0,0,sizeof(s0));

    if (mdbm_open_rstats(argv[optind],O_RDONLY,&m,&s) < 0) {
        perror(argv[optind]);
        exit(2);
    }

    mdbm_diff_rstats(&s0,s,&d,&s0);
    t0 = get_time_usec();
    tend = t0 + (uint64_t)secs*1000000;

    while (t0 < tend) {
        uint64_t t, tnext, dt;

        tnext = t0 + interval*1000000;
        t = get_time_usec();
        if (t < tnext) {
            struct timespec tv;

            t = tnext - t;
            tv.tv_sec = t / 1000000;
            tv.tv_nsec = (t % 1000000) * 1000;
            nanosleep(&tv,NULL);
        }

        t = get_time_usec();
        mdbm_diff_rstats(&s0,s,&d,&s0);
        dt = t - t0;

        if (!log) {
            if (!d.fetch.num && !d.store.num && !d.remove.num
                && !d.fetch.num_error && !d.store.num_error && !d.remove.num_error)
            {
                if (++idle >= 10) {
                    if (idle == 10) {
                        printf("[paused; no activity]\n");
                    }
                    continue;
                }
            } else {
                if (idle > 10) {
                    printf("[resumed after %d seconds]\n",(idle-10) * interval);
                }
                idle = 0;
            }
        }

        if (header_count > 0 && --hcount < 0) {
            printf("\
   fetch  cache     miss    ---- latency (msec) --- |    store  latency  cache  c_evict  latency  cache  c_store  latency |   delete  latency%s%s%s\n\
    /sec  miss%%     /sec    total      hit     miss |     /sec   (msec) evict%%     /sec   (msec) store%%     /sec   (msec) |     /sec   (msec)%s%s%s\n\
",
                   errors ? "|    -----  errors/sec  -----" : "",
                   locks ? " |  fetch lock      store lock      remove lock   " : "",
                   pages ? " |  getpage  cache     miss     miss" : "",
                   errors ? " |    fetch    store   remove" : "",
                   locks ? " |  wait%     msec  wait%     msec  wait%     usec" : "",
                   pages ? " |     /sec  miss%     /sec     msec" : "");
            hcount = header_count;
        }

        printf("%8u %6.2f %8u %8.3f %8.3f %8.3f | %8u %8.3f %6.2f %8u %8.3f %6.2f %8u %8.3f | %8u %8.3f",
               (uint32_t)(d.fetch.num * 1000000 / dt),
               d.fetch.num ? ((double)d.fetch_uncached.num * 100 / d.fetch.num) : 0,
               (uint32_t)(d.fetch_uncached.num * 1000000 / dt),
               d.fetch.num
               ? (double)d.fetch.sum_usec / d.fetch.num / 1000
               : 0,
               d.fetch.num
               ? (double)(d.fetch.sum_usec-d.fetch_uncached.sum_usec)
               / (d.fetch.num-d.fetch_uncached.num) / 1000
               : 0,
               d.fetch_uncached.num
               ? (double)d.fetch_uncached.sum_usec / d.fetch_uncached.num / 1000
               : 0,
               (uint32_t)(d.store.num * 1000000 / dt),
               d.store.num
               ? (double)d.store.sum_usec / d.store.num / 1000
               : 0,
               d.fetch.num ? ((double)d.cache_evict.num * 100 / d.fetch.num) : 0,
               (uint32_t)(d.cache_evict.num * 1000000 / dt),
               d.cache_evict.num
               ? (double)d.cache_evict.sum_usec / d.cache_evict.num / 1000
               : 0,
               (d.version == MDBM_RSTATS_VERSION)
               ? ((d.store.num+d.fetch.num)
                  ? ((double)d.cache_store.num * 100 / (d.store.num+d.fetch.num))
                  : 0)
               : 0,
               (d.version == MDBM_RSTATS_VERSION)
               ? (uint32_t)(d.cache_store.num * 1000000 / dt)
               : 0,
               (d.version == MDBM_RSTATS_VERSION)
               ? (d.cache_store.num
                  ? (double)d.cache_store.sum_usec / d.cache_store.num / 1000
                  : 0)
               : 0,
               (uint32_t)(d.remove.num * 1000000 / dt),
               d.remove.num
               ? (double)d.remove.sum_usec / d.remove.num / 1000
               : 0);
        if (errors) {
            printf(" | %8u %8u %8u",
                   (uint32_t)(d.fetch.num_error * 1000000 / dt),
                   (uint32_t)(d.store.num_error * 1000000 / dt),
                   (uint32_t)(d.remove.num_error * 1000000 / dt));
        }
        if (locks) {
            printf(" | %6.2f %8.3f %6.2f %8.3f %6.2f %8.3f",
                   d.fetch.num_lock_wait
                   ? (double)d.fetch.num_lock_wait * 100 / d.fetch.num
                   : 0,
                   d.fetch.num_lock_wait
                   ? (double)d.fetch.sum_lock_wait_usec / d.fetch.num_lock_wait / 1000
                   : 0,
                   d.store.num_lock_wait
                   ? (double)d.store.num_lock_wait * 100 / d.store.num
                   : 0,
                   d.store.num_lock_wait
                   ? (double)d.store.sum_lock_wait_usec / d.store.num_lock_wait / 1000
                   : 0,
                   d.remove.num_lock_wait
                   ? (double)d.remove.num_lock_wait * 100 / d.remove.num
                   : 0,
                   d.remove.num_lock_wait
                   ? (double)d.remove.sum_lock_wait_usec / d.remove.num_lock_wait / 1000
                   : 0);
        }
        if (pages) {
            printf(" | %8u %6.2f %8u %8.3f",
                   (uint32_t)(d.getpage.num * 1000000 / dt),
                   d.getpage.num ? ((double)d.getpage_uncached.num * 100 / d.getpage.num) : 0,
                   (uint32_t)(d.getpage_uncached.num * 1000000 / dt),
                   d.getpage_uncached.num
                   ? (double)d.getpage_uncached.sum_usec / d.getpage_uncached.num / 1000
                   : 0);
        }
        putchar('\n');
        if (times) {
            print_thist("fetch",&d.fetch);
            print_thist("f-miss",&d.fetch_uncached);
            print_thist("store",&d.store);
            print_thist("remove",&d.remove);
            print_thist("getpage",&d.getpage);
            print_thist("gp-miss",&d.getpage_uncached);
        }
        t0 = t;
    }

    return 0;
}
