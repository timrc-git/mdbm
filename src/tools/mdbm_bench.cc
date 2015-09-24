/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <sys/types.h>
#include <sys/fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <sys/mman.h>
#include <ctype.h>
#include <inttypes.h>

#include "mdbm.h"
#include "mdbm_stats.h"
#include "mdbm_util.h"
#include "mdbm_internal.h"


#include <pthread.h>

#undef MDBM_USE_OPENSSL_RAND
#ifdef MDBM_USE_OPENSSL_RAND
#include <openssl/rand.h>
#endif
#include <openssl/sha.h>
#include <openssl/md5.h>

typedef pthread_mutex_t mutex_t;
typedef pthread_cond_t cond_t;
#define mutex_lock     pthread_mutex_lock
#define mutex_unlock   pthread_mutex_unlock
#define cond_broadcast pthread_cond_broadcast
#define cond_wait      pthread_cond_wait
#define mutex_init     init_pthread_mutex
#define cond_init      init_pthread_cond

#include "bench_existing.cc"

int init_pthread_mutex(mutex_t *mutex)  {
  int ret = -1;
  pthread_mutexattr_t attr;
  if (pthread_mutexattr_init(&attr)) {
    perror("pthread_mutexattr_init ");
    return ret;
  }
  if (0 != (ret=pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))) {
    perror("pthread_mutexattr_setpshared ");
    goto attr_destroy;
  }
  if (0 != (ret=pthread_mutex_init(mutex, &attr))) {
    perror("pthread_mutex_init ");
    goto attr_destroy;
  }
  ret = 0;
attr_destroy:
  pthread_mutexattr_destroy(&attr);
  return ret;
}

int init_pthread_cond(cond_t *cond) {
  int ret = -1;
  pthread_condattr_t attr;
  if (pthread_condattr_init(&attr)) {
    perror("pthread_condattr_init ");
    return ret;
  }
  if (0 != (ret=pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED))) {
    perror("pthread_condattr_setpshared ");
    goto attr_destroy;
  }
  if (0 != (ret=pthread_cond_init(cond, &attr))) {
    perror("pthread_mutex_init ");
    goto attr_destroy;
  }
  ret = 0;
attr_destroy:
  pthread_condattr_destroy(&attr);
  return ret;
}



extern int  benchmarkExisting(const char *filename, double percentWrites, int lockmode, 
                              const char *outputfile, uint optCount, int verbose);

typedef struct {
    uint64_t volatile tnow;
    uint64_t tstart;
    uint64_t tend;
    uint64_t tinterval;
    uint32_t nproc;
    uint32_t nstart;
    uint32_t nfinish;
    mutex_t lock;
    cond_t cv;
    struct {
        uint64_t num_reads;
        uint64_t num_writes;
        uint64_t num_deletes;
        uint64_t sum_read_latency;
        uint64_t sum_read_lock_latency;
        uint64_t sum_write_latency;
        uint64_t sum_write_lock_latency;
        uint64_t sum_delete_latency;
        uint64_t sum_delete_lock_latency;
    } proc[512];
} bench_stats_t;

#define STATSFILE       "/tmp/mdbm_bench.stats"
#define STATSSIZE       sizeof(bench_stats_t)

static char fn[MAXPATHLEN+1];
static char bsfn[MAXPATHLEN+1];
static uint32_t count = 2000000;
static uint32_t runcount = UINT_MAX;
static int nforks;
static int hash = MDBM_DEFAULT_HASH;
static const int MDBM_MAX_VALID_PAGESIZE = (MDBM_MAXPAGE - 64);

static uint64_t keysize = 4;
static int seq_keys = 0;
static uint32_t valsize = 32;
static int pagesize = 8192;
static uint64_t dbsize = 64*1024*1024;
static int align = 0;
static int large;
static int fetchmode;
static int updatemode;
static double writefrac;
static int verflag;
static int create = 1;
static int load = 1;
static int cachemode;
static int cache_load_thru;
static const char* bs;
static int rstats;
static int bs_rstats;
static uint64_t windowed;
static uint32_t working_set;
static uint32_t miss_target;
static int show_latency;
static int protect;
static int checkvals;
static uint64_t readval;
static int lockall;
static int do_dup;
static FILE *outfile = NULL;

mdbm_stats_t stats;

//enum {
//    MDBM_NO_LOCK,
//    MDBM_LOCK,
//    MDBM_PLOCK,
//    MDBM_RWLOCK
//};

static int lockmode = MDBM_LOCK;
static int lockarch = 0;

enum {
    MDBM_FETCH_BUF,
    MDBM_FETCH,
    MDBM_FETCH_READ,
    MDBM_UPDATE_INPLACE,
    MDBM_UPDATE_REPLACE,
    MDBM_UPDATE_DELETE_INSERT
};


enum {
    LOAD,
    ACCESS
};

/*
static int PRIMES[] = {
    1,
    2,      3,      5,      7,     11,     13,     17,     19,     23,     29,
    31,     37,     41,     43,     47,     53,     59,     61,     67,     71,
    73,     79,     83,     89,     97,    101,    103,    107,    109,    113,
    127,    131,    137,    139,    149,    151,    157,    163,    167,    173,
    179,    181,    191,    193,    197,    199,    211,    223,    227,    229,
    233,    239,    241,    251,    257,    263,    269,    271,    277,    281,
    283,    293,    307,    311,    313,    317,    331,    337,    347,    349,
    353,    359,    367,    373,    379,    383,    389,    397,    401,    409,
    419,    421,    431,    433,    439,    443,    449,    457,    461,    463,
    467,    479,    487,    491,    499,    503,    509,    521,    523,    541,
    547,    557,    563,    569,    571,    577,    587,    593,    599,    601,
    607,    613,    617,    619,    631,    641,    643,    647,    653,    659,
    661,    673,    677,    683,    691,    701,    709,    719,    727,    733,
    739,    743,    751,    757,    761,    769,    773,    787,    797,    809,
    811,    821,    823,    827,    829,    839,    853,    857,    859,    863,
    877,    881,    883,    887,    907,    911,    919,    929,    937,    941,
    947,    953,    967,    971,    977,    983,    991,    997,   1009,   1013,
    1019,   1021,   1031,   1033,   1039,   1049,   1051,   1061,   1063,   1069,
    1087,   1091,   1093,   1097,   1103,   1109,   1117,   1123,   1129,   1151,
    1153,   1163,   1171,   1181,   1187,   1193,   1201,   1213,   1217,   1223,
    1229,   1231,   1237,   1249,   1259,   1277,   1279,   1283,   1289,   1291,
    1297,   1301,   1303,   1307,   1319,   1321,   1327,   1361,   1367,   1373,
    1381,   1399,   1409,   1423,   1427,   1429,   1433,   1439,   1447,   1451,
    1453,   1459,   1471,   1481,   1483,   1487,   1489,   1493,   1499,   1511,
    1523,   1531,   1543,   1549,   1553,   1559,   1567,   1571,   1579,   1583,
    1597,   1601,   1607,   1609,   1613,   1619,   1621,   1627,   1637,   1657,
    1663,   1667,   1669,   1693,   1697,   1699,   1709,   1721,   1723,   1733,
    1741,   1747,   1753,   1759,   1777,   1783,   1787,   1789,   1801,   1811,
    1823,   1831,   1847,   1861,   1867,   1871,   1873,   1877,   1879,   1889,
    1901,   1907,   1913,   1931,   1933,   1949,   1951,   1973,   1979,   1987,
    1993,   1997,   1999,   2003,   2011,   2017,   2027,   2029,   2039,   2053,
    2063,   2069,   2081,   2083,   2087,   2089,   2099,   2111,   2113,   2129,
    2131,   2137,   2141,   2143,   2153,   2161,   2179,   2203,   2207,   2213,
    2221,   2237,   2239,   2243,   2251,   2267,   2269,   2273,   2281,   2287,
    2293,   2297,   2309,   2311,   2333,   2339,   2341,   2347,   2351,   2357,
    2371,   2377,   2381,   2383,   2389,   2393,   2399,   2411,   2417,   2423,
    2437,   2441,   2447,   2459,   2467,   2473,   2477,   2503,   2521,   2531,
    2539,   2543,   2549,   2551,   2557,   2579,   2591,   2593,   2609,   2617,
    2621,   2633,   2647,   2657,   2659,   2663,   2671,   2677,   2683,   2687,
    2689,   2693,   2699,   2707,   2711,   2713,   2719,   2729,   2731,   2741,
    2749,   2753,   2767,   2777,   2789,   2791,   2797,   2801,   2803,   2819,
    2833,   2837,   2843,   2851,   2857,   2861,   2879,   2887,   2897,   2903,
    2909,   2917,   2927,   2939,   2953,   2957,   2963,   2969,   2971,   2999,
    3001,   3011,   3019,   3023,   3037,   3041,   3049,   3061,   3067,   3079,
    3083,   3089,   3109,   3119,   3121,   3137,   3163,   3167,   3169,   3181,
    3187,   3191,   3203,   3209,   3217,   3221,   3229,   3251,   3253,   3257,
    3259,   3271,   3299,   3301,   3307,   3313,   3319,   3323,   3329,   3331,
    3343,   3347,   3359,   3361,   3371,   3373,   3389,   3391,   3407,   3413,
    3433,   3449,   3457,   3461,   3463,   3467,   3469,   3491,   3499,   3511,
    3517,   3527,   3529,   3533,   3539,   3541,   3547,   3557,   3559,   3571,
    3581,   3583,   3593,   3607,   3613,   3617,   3623,   3631,   3637,   3643,
    3659,   3671,   3673,   3677,   3691,   3697,   3701,   3709,   3719,   3727,
    3733,   3739,   3761,   3767,   3769,   3779,   3793,   3797,   3803,   3821,
    3823,   3833,   3847,   3851,   3853,   3863,   3877,   3881,   3889,   3907,
    3911,   3917,   3919,   3923,   3929,   3931,   3943,   3947,   3967,   3989,
    4001,   4003,   4007,   4013,   4019,   4021,   4027,   4049,   4051,   4057,
    4073,   4079,   4091,   4093,   4099,   4111,   4127,   4129,   4133,   4139,
    4153,   4157,   4159,   4177,   4201,   4211,   4217,   4219,   4229,   4231,
    4241,   4243,   4253,   4259,   4261,   4271,   4273,   4283,   4289,   4297,
    4327,   4337,   4339,   4349,   4357,   4363,   4373,   4391,   4397,   4409,
    4421,   4423,   4441,   4447,   4451,   4457,   4463,   4481,   4483,   4493,
    4507,   4513,   4517,   4519,   4523,   4547,   4549,   4561,   4567,   4583,
    4591,   4597,   4603,   4621,   4637,   4639,   4643,   4649,   4651,   4657,
    4663,   4673,   4679,   4691,   4703,   4721,   4723,   4729,   4733,   4751,
    4759,   4783,   4787,   4789,   4793,   4799,   4801,   4813,   4817,   4831,
    4861,   4871,   4877,   4889,   4903,   4909,   4919,   4931,   4933,   4937
};
*/

void* malloc_or_die_str(size_t sz, const char* file, int line) {
  void* chunk = malloc(sz);
  if (!chunk) {
    fprintf(stderr, "malloc() failed %s:%d exiting...\n", __FILE__, __LINE__);
    exit(1);
  }
  return chunk;
}
#define malloc_or_die(sz) malloc_or_die_str(sz, __FILE__, __LINE__);

static void
set_random (uint32_t seed)
{
#ifdef MDBM_USE_OPENSSL_RAND
    RAND_seed(&seed,sizeof(seed));
#else
    srand48(seed);
#endif
}


static inline uint32_t
get_random ()
{
#ifdef MDBM_USE_OPENSSL_RAND
    uint32_t r;
    RAND_pseudo_bytes((unsigned char*)&r,sizeof(r));
    return r;
#else
    return (uint32_t)mrand48();
#endif
}


static void
get_random_bytes (char* buf, size_t bufsize)
{
#ifdef MDBM_USE_OPENSSL_RAND
    RAND_pseudo_bytes((unsigned char*)buf,bufsize);
#else
    while (bufsize >= sizeof(uint32_t)) {
        *((uint32_t*)buf) = (uint32_t)mrand48();
        buf += sizeof(uint32_t);
        bufsize -= sizeof(uint32_t);
    }
    if (bufsize > 0) {
        uint32_t r = (uint32_t)mrand48();
        memcpy(buf,&r,bufsize);
    }
#endif
}


static bench_stats_t *bench_stats;

struct mdbm_bench_config {
    int proc;
    int numprocs;
    int mode;
    char* gkeys;
    MDBM* db;
};

extern void post_open_callback(MDBM *db);

static MDBM*
open_db (int find_size_multiple)
{
    int flags;
    int dsz;
    MDBM* db;

    flags = protect ? MDBM_O_RDONLY : MDBM_O_RDWR;
    if (create) {
        flags |= (MDBM_O_CREAT|MDBM_O_TRUNC) | verflag;
    }
    flags |= lockarch;
    if (lockmode == MDBM_PLOCK) {
        flags |= MDBM_PARTITIONED_LOCKS;
    } else if (lockmode == MDBM_RWLOCK) {
        flags |= MDBM_RW_LOCKS;
    }
    if (large) {
        flags |= MDBM_LARGE_OBJECTS;
    }

    if (find_size_multiple) {
        dbsize = count * valsize * find_size_multiple;
    }

    if (dbsize < 1ULL<<31) {
        dsz = dbsize;
    } else {
        if (verflag & MDBM_CREATE_V3) {
            flags |= MDBM_DBSIZE_MB;
            dsz = dbsize / (1024*1024);
        } else {
            dsz = 0;
        }
    }
    if (windowed) {
        flags |= MDBM_OPEN_WINDOWED;
    }
    if (verflag & MDBM_CREATE_V3) {
        flags |= MDBM_NO_DIRTY;
    }

    db = mdbm_open(fn,flags,0666,pagesize,dsz);
    if (!db) {
        mdbm_logerror(LOG_ALERT,0,"mdbm_open(%s)",fn);
        return NULL;
    }
    if (create) {
        mdbm_sethash(db,hash);
        if (align) {
            mdbm_set_alignment(db,align);
        }
        if (dbsize) {
            int pages = (dbsize+pagesize-1) / pagesize;
            if (!dsz) {
                mdbm_pre_split(db,pages);
            }
            mdbm_limit_size_v3(db, pages, NULL, NULL);
        }
        if (cachemode) {
            mdbm_set_cachemode(db,cachemode);
        }
    }
    if (windowed) {
        mdbm_set_window_size(db,windowed);
    }
    if (rstats) {
        mdbm_init_rstats(db,(rstats > 1) ? MDBM_RSTATS_THIST : 0);
    }
    if (bs) {
        char* buf = strdup(bs);
        char* s;
        char* path = NULL;
        const mdbm_bsops_t* type = MDBM_BSOPS_FILE;
        int createflags = 0;
        int oflag = 0;
        int odirect = 0;
        int lockflags = 0;
        int winflags = 0;
        size_t win = 0;
        void *opt = NULL;
        char* p = NULL;
        uint64_t sz = 0;
        uint64_t psz = 0;
        while ((s = strtok_r(buf,",",&p)) != NULL) {
            buf = NULL;
            if (!strcmp(s,"direct")) {
                odirect = MDBM_O_DIRECT;
            } else if (!strncmp(s,"file=",5)) {
                type = MDBM_BSOPS_FILE;
                path = s+5;
            } else if (!strcmp(s,"lock")) {
                lockflags &= ~(MDBM_PARTITIONED_LOCKS|MDBM_RW_LOCKS|MDBM_OPEN_NOLOCK);
            } else if (!strncmp(s,"mdbm=",5)) {
                type = MDBM_BSOPS_MDBM;
                path = s+5;
            } else if (!strcmp(s,"nolock")) {
                lockflags &= ~(MDBM_PARTITIONED_LOCKS|MDBM_RW_LOCKS|MDBM_OPEN_NOLOCK);
                lockflags |= MDBM_OPEN_NOLOCK;
            } else if (!strncmp(s,"pagesize=",9)) {
                psz = mdbm_util_get_size(s+9,1);
            } else if (!strcmp(s,"plock")) {
                lockflags &= ~(MDBM_PARTITIONED_LOCKS|MDBM_RW_LOCKS|MDBM_OPEN_NOLOCK);
                lockflags |= MDBM_PARTITIONED_LOCKS;
            } else if (!strcmp(s,"ro")) {
                oflag = MDBM_O_RDONLY;
            } else if (!strcmp(s,"rw")) {
                oflag = MDBM_O_RDWR;
            } else if (!strcmp(s,"rwlock")) {
                lockflags &= ~(MDBM_PARTITIONED_LOCKS|MDBM_RW_LOCKS|MDBM_OPEN_NOLOCK);
                lockflags |= MDBM_RW_LOCKS;
            } else if (!strncmp(s,"win=",4)) {
                winflags |= MDBM_OPEN_WINDOWED;
                win = mdbm_util_get_size(s+4,1024*1024);
            } else if (!strcmp(s,"rstats")) {
                bs_rstats++;
            } else if (!strncmp(s,"size=",5)) {
                createflags = MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC|MDBM_DBSIZE_MB|MDBM_CREATE_V3;
                sz = mdbm_util_get_size(s+5,1);
            } else {
                fprintf(stderr,"mdbm_bench: Invalid backing-store option: %s\n",s);
            }
        }
        if (type == MDBM_BSOPS_MDBM) {
            MDBM* db;
            strcpy(bsfn,path);
            if ((db = mdbm_open(path,
                                createflags|oflag|odirect|lockflags|winflags,
                                0660,psz,sz/(1024*1024))) != NULL)
            {
                if (flags & MDBM_O_TRUNC) {
                    mdbm_sethash(db,hash);
                    if (align) {
                        mdbm_set_alignment(db,align);
                    }
                }
                if (win) {
                    mdbm_set_window_size(db,win*1024*1024);
                }
                if (bs_rstats) {
                    mdbm_init_rstats(db,(bs_rstats > 1) ? MDBM_RSTATS_THIST : 0);
                }
            }
            opt = db;
        } else if (type == MDBM_BSOPS_FILE) {
            opt = path;
        }
        if (mdbm_set_backingstore(db,type,opt,flags) < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm_set_backingstore");
        }
    }
    post_open_callback(db);
    return db;
}


static void
get_seq_key (int key, char* buf)
{
    static const char HEX[] = "0123456789abcdef";
    uint64_t off = 0;

    while (off < keysize) {
        unsigned int k = key;
        int i, len;

        len = keysize - off;
        if (len > 8) {
            len = 8;
        }
        for (i = len-1; i >= 0; --i) {
            buf[off+i] = HEX[k&0xf];
            k >>= 4;
        }
        off += len;
    }
}


static int
load_data(struct mdbm_bench_config* config, MDBM *possible_dup)
{
    char* val;
    char* keybuf;
    uint32_t i;
    uint64_t tstart, treport, tlast;
    uint32_t lastcount;
    char* gkeys = config->gkeys;
    MDBM *db = config->db;
    int filled = 1;  /* was the entire DB was filled with "count" entries */

    if (possible_dup != NULL) {
        db = possible_dup;
    }

    keybuf = (char*)malloc_or_die(keysize);
    val = (char*)malloc_or_die(valsize);

    tstart = tlast = get_time_usec();
    treport = tstart + 1000000;
    lastcount = 0;

    for (i = load ? 0 : count; i < count; i++) {
        datum k, v;
        int ret;
        MDBM_ITER iter;
        int storef;

        storef = seq_keys ? MDBM_REPLACE : MDBM_INSERT;
        if (!cache_load_thru) {
            storef |= MDBM_CACHE_MODIFY;
        }

        for (;;) {
            if (seq_keys) {
                get_seq_key(i,keybuf);
                k.dptr = keybuf;
            } else {
                k.dptr = gkeys + i*keysize;
            }
            k.dsize = keysize;
            v.dptr = val;
            v.dsize = valsize;
            *((uint32_t*)val) = i;
            if (valsize >= 2*sizeof(uint32_t)) {
                *((uint32_t*)(val + valsize - sizeof(uint32_t))) = -i;
            }
            if ((ret = mdbm_store_r(db,&k,&v,storef,&iter)) != 1) {
                break;
            }
            get_random_bytes(gkeys + i*keysize,keysize);
        }
        if (ret < 0) {
            mdbm_logerror(LOG_ERR,0,"mdbm_store during load (keys loaded = %u)",i);
            count = i;
            filled = 0;
            break;
        } else {
            uint64_t t = get_time_usec();
            if (get_time_usec() >= treport) {
                fprintf(outfile, "1s=%-10.2f total=%-10.2f\n",
                       (double)(i - lastcount) / (t - tlast) * 1000000,
                       (double)(i) / (t - tstart) * 1000000);
                treport += 1000000;
                tlast = t;
                lastcount = i;
            }
        }
    }

    free(keybuf);
    free(val);
    return filled;
}

static int
bench (struct mdbm_bench_config* config)
{
    char* val;
    char* keybuf;
    datum b;
    MDBM* db;

    int proc = config->proc;
    unsigned int numprocs = config->numprocs;
    int mode = config->mode;
    char* gkeys = config->gkeys;

    keybuf = (char*)malloc_or_die(keysize);

    val = (char*)malloc_or_die(valsize);
    b.dptr = val;
    b.dsize = valsize;

    db = config->db ? (do_dup ? mdbm_dup_handle(config->db,0) : config->db) : open_db(0);
    if (!db) {
        mdbm_log(LOG_ALERT,"error opening db");
    }

    if (mdbm_isowned(db)) {
        perror("bench() mdbm lock");
        mdbm_lock_dump(db);
        pause();
        abort();
    }

    {
        unsigned int seed = 0;
        uint32_t s = proc*numprocs;
        MD5_CTX c;
        const unsigned char digest[16]= "";

        MD5_Init(&c);
        MD5_Update(&c,&s,sizeof(s));
        MD5_Final((unsigned char*)digest,&c);
        memcpy(&seed, digest, sizeof(unsigned int));
        set_random(seed);
    }

    if (mode == LOAD) {
        load_data(config, db);
    } else {
        if (mutex_lock(&bench_stats->lock) < 0 && errno != EINPROGRESS) {
            perror("bench stats lock");
            pause();
            abort();
        }
        if (++bench_stats->nstart == numprocs) {
            bench_stats->tstart = bench_stats->tnow = get_time_usec();
            bench_stats->tend = bench_stats->tstart + bench_stats->tinterval;
            cond_broadcast(&bench_stats->cv);
        } else {
            while (bench_stats->nstart < numprocs) {
                cond_wait(&bench_stats->cv,&bench_stats->lock);
            }
        }
        mutex_unlock(&bench_stats->lock);
/*
        if (proc >= sizeof(PRIMES)/sizeof(PRIMES[0])) {
            abort();
        }
*/
        {
            uint32_t num_ops = 0;
            uint32_t num_reads = 0;
            uint32_t num_writes = 0;
            uint32_t num_deletes = 0;
            uint64_t sum_read_latency = 0;
            uint64_t sum_read_lock_latency = 0;
            uint64_t sum_write_latency = 0;
            uint64_t sum_write_lock_latency = 0;
            uint64_t sum_delete_latency = 0;
            uint64_t sum_delete_lock_latency = 0;

            while (num_ops < runcount && bench_stats->tnow < bench_stats->tend) {
                int j, c;

                c = (runcount - num_ops > count) ? count : runcount - num_ops;
                for (j = 0; j < c && bench_stats->tnow < bench_stats->tend; j++) {
                    datum k, v;
                    uint64_t t0 = 0;
                    uint64_t t1;
                    uint32_t r;
                    int key;
                    int op;
                    int is_write;
                    uint64_t latency = 0;
                    uint64_t delete_latency = 0;
                    uint64_t lock_latency = 0;

                    r = get_random();
                    if (miss_target > 0 && r > miss_target) {
                        key = get_random() % working_set;
                    } else {
                        key = (get_random() % (count - working_set)) + working_set;
                    }
                    if ((writefrac > 0) && (((double)(r % 9991) / 100) < writefrac)) {
                        op = updatemode;
                        is_write = 1;
                    } else {
                        op = fetchmode;
                        is_write = 0;
                    }

                    if (seq_keys) {
                        get_seq_key(key,keybuf);
                        k.dptr = keybuf;
                    } else {
                        k.dptr = gkeys + key*keysize;
                    }
                    k.dsize = keysize;
                    if (show_latency) {
                        t0 = get_time_usec();
                    }

                    if (op == MDBM_UPDATE_REPLACE || op == MDBM_UPDATE_DELETE_INSERT) {
                        v.dptr = val;
                        v.dsize = valsize;
                        *((uint32_t*)val) = key;
                        if (valsize >= 2*sizeof(uint32_t)) {
                            *((uint32_t*)(val + valsize - sizeof(uint32_t))) = -key;
                        }
                        if (op == MDBM_UPDATE_REPLACE) {
                            if (mdbm_store(db,k,v,MDBM_REPLACE) < 0) {
                                mdbm_logerror(LOG_ALERT,0,"mdbm_store");
                            }
                        } else {
                            if (show_latency) {
                                delete_latency = get_time_usec();
                            }
                            if (mdbm_delete(db,k) < 0) {
                                mdbm_logerror(LOG_ALERT,0,"mdbm_delete");
                            }
                            if (show_latency) {
                                delete_latency = get_time_usec() - delete_latency;
                            }
                            if (mdbm_store(db,k,v,MDBM_INSERT) < 0) {
                                mdbm_logerror(LOG_ALERT,0,"mdbm_store");
                            }
                        }
                    } else if (op == MDBM_FETCH_BUF) {
                        if ((mdbm_fetch_buf(db,&k,&v,&b,0)) < 0) {
                            mdbm_logerror(LOG_ERR,0,
                                          "mdbm_fetch_buf (index=%d key=%u)",
                                          j,key);
                            k.dptr = seq_keys ? keybuf : gkeys + key*keysize;
                            k.dsize = keysize;
                            if (mdbm_fetch_buf(db,&k,&v,&b,0) < 0) {
                                mdbm_logerror(LOG_ALERT,0,
                                              "(retry) mdbm_fetch_buf (index=%d key=%u)",
                                              j,key);
                            }
                        } else {
                            if (checkvals) {
                                if ((unsigned)v.dsize != valsize) {
                                    mdbm_log(LOG_ALERT,
                                             "value size check failure (key=%u)",key);
                                }
                                if (*((uint32_t*)v.dptr) != (uint32_t)key) {
                                    mdbm_log(LOG_ALERT,
                                             "value (l) check failure (key=%u)",key);
                                }
                                if (checkvals > 1) {
                                    /* FIXME XXX clang flagged this, and it looks broken */
                                    if (*((uint32_t*)(v.dptr + valsize - sizeof(uint32_t))) != (uint32_t)-key)
                                    {
                                        mdbm_log(LOG_ALERT,
                                                 "value (h) check failure (key=%u)",key);
                                    }
                                }
                            }
                        }
                    } else {
                        MDBM_ITER i;

                        if (lockmode == MDBM_LOCK) {
                            mdbm_lock(db);
                        } else if (lockmode == MDBM_PLOCK) {
                            mdbm_plock(db,&k,0);
                        } else if (lockmode == MDBM_RWLOCK) {
                            mdbm_lock_shared(db);
                        }
                        if (show_latency) {
                            t1 = get_time_usec();
                            if (t1 >= t0) {
                                lock_latency += t1 - t0;
                            } else {
                                mdbm_log(LOG_ERR,
                                         "time went backwards t0=%llu t1=%llu tnow=%llu",
                                         (unsigned long long)t0,(unsigned long long)t1,
                                         (unsigned long long)get_time_usec());
                            }
                        }
                        if (mdbm_fetch_r(db,&k,&v,&i) < 0) {
                            mdbm_logerror(LOG_EMERG,0,"mdbm_fetch_r");
                        }
                        if (checkvals) {
                            if ((unsigned)v.dsize != valsize) {
                                mdbm_log(LOG_ALERT,
                                         "value size check failure (key=%u)",key);
                            }
                            if (*((uint32_t*)v.dptr) != (unsigned)key) {
                                mdbm_log(LOG_ALERT,
                                         "value (l) check failure (key=%u)",key);
                            }
                            if (checkvals > 1) {
                                /* FIXME XXX clang flagged this, and it looks broken */
                                if (*((uint32_t*)(v.dptr + valsize - sizeof(uint32_t))) != (uint32_t)-key)
                                {
                                    mdbm_log(LOG_ALERT,
                                             "value (h) check failure (key=%u)",key);
                                }
                            }
                        }
                        if (op == MDBM_FETCH_READ) {
                            uint64_t* p;

                            p = (uint64_t*)v.dptr;
                            readval += p[0];
                            readval += p[1];
                            readval += p[2];
                            readval += p[3];
                        } else if (op == MDBM_UPDATE_INPLACE) {
                            *((uint32_t*)val) = key;
                            if (valsize >= 2*sizeof(uint32_t)) {
                                *((uint32_t*)(val + valsize - sizeof(uint32_t))) = -key;
                            }
                            memcpy(v.dptr,val,valsize);
                        }
                        if (lockmode != MDBM_NO_LOCK) {
                            mdbm_unlock(db);
                        }
                    }
                    if (show_latency) {
                        latency = get_time_usec() - t0;
                    }
                    ++num_ops;
                    if (is_write) {
                        ++num_writes;
                        if (op == MDBM_UPDATE_DELETE_INSERT) {
                            ++num_deletes;
                            latency -= delete_latency;
                            sum_delete_latency += delete_latency;
                        }
                        sum_write_latency += latency;
                        sum_write_lock_latency += lock_latency;
                    } else {
                        ++num_reads;
                        sum_read_latency += latency;
                        sum_read_lock_latency += lock_latency;
                    }
                    if (!proc) {
                        bench_stats->tnow = get_time_usec();
                    }
                }
            }
            bench_stats->proc[proc].num_reads = num_reads;
            bench_stats->proc[proc].num_writes = num_writes;
            bench_stats->proc[proc].num_deletes = num_deletes;
            bench_stats->proc[proc].sum_read_latency = sum_read_latency;
            bench_stats->proc[proc].sum_read_lock_latency = sum_read_lock_latency;
            bench_stats->proc[proc].sum_write_latency = sum_write_latency;
            bench_stats->proc[proc].sum_write_lock_latency = sum_write_lock_latency;
            bench_stats->proc[proc].sum_delete_latency = sum_delete_latency;
            bench_stats->proc[proc].sum_delete_lock_latency = sum_delete_lock_latency;
        }
    }

    if (mutex_lock(&bench_stats->lock) < 0 && errno != EINPROGRESS) {
        perror("bench stats lock");
        pause();
        abort();
    }
    if (++bench_stats->nfinish == numprocs) {
        cond_broadcast(&bench_stats->cv);
    }
    mutex_unlock(&bench_stats->lock);

    if (db != config->db) {
        mdbm_close(db);
    }

    free(val);
    free(keybuf);

    return 0;
}

static void 
do_mlockall()
{
#ifdef __linux__
    mlockall(MCL_FUTURE);
#endif
}

static void*
bench_thread (void* arg)
{
    struct mdbm_bench_config* c = (struct mdbm_bench_config*)arg;
    if (lockall) { do_mlockall(); }
    bench(c);
    return NULL;
}


static void
getrange (int* min, int* max, int* incr)
{
    if (sscanf(optarg,"%d:%d:%d",min,max,incr) == 3) {
        return;
    }
    if (sscanf(optarg,"%d:%d",min,max) == 2) {
        *incr = -1;
        return;
    }
    if (sscanf(optarg,"%d",min) == 1) {
        max = min;
        *incr = -1;
        return;
    }
    fprintf(outfile, "bad range: %s\n",optarg);
}


void
child_handler (int signum)
{
    int status;
    pid_t pid;
    while ((pid = waitpid(-1,&status,WNOHANG)) > 0) {
        if (WIFSIGNALED(status) || WEXITSTATUS(status)) {
            fprintf(outfile, "pid %d %s %d\n",pid,WIFSIGNALED(status) ? "died" : "exited",
                   WIFSIGNALED(status) ? WTERMSIG(status) : WEXITSTATUS(status));
            kill(0,SIGTERM);
        }
    }
}


static const int NPROCS[] = {1, 2, 3, 4, 5, 6, 8, 10, 12, 16, 20, 24, 28, 32, 48, 64, 96, 128, 192, 256, 384, 512, 0};
#define NNPROCS 22


static void
usage (void)
{
    printf("\
Usage: mdbm_bench [options]\n\
Options:\n\
        -0              Lock process in memory (use with caution)\n\
        -3              Create version 3 db (default)\n\
        -a <bytes>      Set record alignment (default is byte alignment)\n\
        -B <optstring>  Enable backing store\n\
                         direct         Use O_DIRECT when accessing backing-store files\n\
                         file=<path>    Enable file-based backing store at <path> (DEPRECATED)\n\
                         lock           Use mdbm normal (db) locking\n\
                         mdbm=<dbfile>  Enable mdbm-based backing store at <dbfile>\n\
                         nolock         Use no mdbm locking\n\
                         pagesize=<bytes> Set mdbm pagesize\n\
                         plock          Use mdbm partitioned locking\n\
                         rstats         Enable runtime stats on mdbm backing store\n\
                         ro             Open backing-store for read-only access\n\
                         rw             Open backing-store for read-write access\n\
                         rwlock         Use mdbm read-write locking\n\
                         size=<mb>      Create mdbm-based backing store of specified size\n\
                                          If you don't specify size, the db must already exist.\n\
                         win=<bytes>    Use mdbm window of specified size\n\
                         winmin=<bytes> Use mdbm window (min mode) of specified size\n\
        -b <bytes>      Enable windowed mode with specified window size.\n\
                        Suffix k/m/g may be used to override default of m.\n\
        -C <mode>       Set cache mode (1=lfu 2=lru 3=gdsf)\n\
        -c <count>      Number of records (default: 2000000)\n\
        -D <devname>    Query disk i/o stats for specified device\n\
        -d <mbytes> or  Size of db (default: 64 MB).\n\
              0     or  Unlimited (results will include costs of splitting the MDBM)\n\
            find        Find the correct size and set large objects.\n\
                        When providing a size, suffix k/m/g may be used to override default of m.\n\
        -e              Benchmark using existing data only (requires -F)\n\
        -F <filename>   Specify db filename\n\
        -f <mode>       Fetch mode (default: 0)\n\
                          0  mdbm_fetch_buf\n\
                          1  mdbm_lock, mdbm_fetch, mdbm_unlock\n\
                          2  mdbm_lock, mdbm_fetch, read first 4 qwords, mdbm_unlock\n\
        -g <count>      Run count\n\
        -h <hash>       Hash func (default: 5)\n\
                          0  CRC-32\n\
                          1  EJB\n\
                          2  PHONG\n\
                          3  OZ\n\
                          4  TOREK\n\
                          5  FNV32\n\
                          6  STL\n\
                          7  MD5\n\
                          8  SHA-1\n\
                          9  Jenkins\n\
                         10  SuperFast\n\
        -K              Sequential keys (instead of random keys)\n\
        -k <bytes>      Key size (default: 4).\n\
                        Suffix k/m/g may be used to override default of bytes.\n\
        -L              Enable large object mode\n\
        -l <mode>       Locking mode (default: 1)\n\
                          0  No locking\n\
                          1  Exclusive locking\n\
                          2  Partitioned locking\n\
                          3  Shared (read/write) locking\n\
        -M <%%>         Target cache miss rate\n\
        -N <nice>       Process priority (default: (normal))\n\
        -n <min>:<max>  Number of concurrent processes (default: 1:1)\n\
        -O              Reopen db for each child process\n\
        -o <file>       Log results to specified file\n\
        -p <bytes>      Page size (default: 8192).\n\
                        Suffix k/m/g may be used to override default of bytes.\n\
                        A page size of zero means use PageSize of approx: (50 * SizeOfValue)\n\
        -R              Enable runtime stats gathering\n\
        -S              Pause for input between runs\n\
        -s <seconds>    Number of seconds to run test at each proc count\n\
        -T              Compute and display latency\n\
        -t              Use threads instead of procs (use twice to test mdbm_dup_handle)\n\
        -u <mode>       Update (write) mode (default: 0)\n\
                          0  Use store(REPLACE)\n\
                          1  Fetch and update in-place\n\
                          2  Delete then store(INSERT)\n\
        -V              Check fetched values\n\
        -v <bytes>      Size of value (default: 32).\n\
                        Suffix k/m/g may be used to override default of bytes.\n\
        -W <count>      Specifies working set for hit/miss targeting\n\
        -w <n.n%%>      Specifies floating-poing percentage of accesses that are writes\n\
        -X              Write-through cache when loading\n\
        -Y              Don't create db but load it (use existing db)\n\
        -y              Answer yes to prompts\n\
        -Z              Don't create or load db (use existing db and values created with -Y)\n\
        -z              Don't load db (create empty db but do not load)\n\
\n\
Examples:\n\
    mdbm_bench -c100 -y -K -F /tmp/foo-part.mdbm -l2 -n5:5 \n\
    mdbm_bench -3 -c 100 -y -K  -B mdbm=/tmp/bs.mdbm,size=100M -F /tmp/foo.mdbm \n\
");
    exit(1);
}


struct mdbm_disk_stats {
    uint64_t num_reads;
    uint64_t num_writes;
    uint64_t sum_io_msec;
    uint64_t sum_read_msec;
    uint64_t sum_write_msec;
};


static void
read_diskstats (const char* devname, struct mdbm_disk_stats* s)
{
#ifdef __linux__
    FILE* f;
    char buf[1024];
    int match = 0;

    f = fopen("/proc/diskstats","r");
    while (fgets(buf,sizeof(buf),f)) {
        int n = strlen(buf);
        char* s0;
        char* sptr;
        char* tok;
        int field;

        if (buf[n-1] == '\n') {
            buf[n-1] = 0;
            --n;
        }
        s0 = buf;
        field = -2;
        while ((tok = strtok_r(s0," \t",&sptr)) != NULL) {
            if (field == 0) {
                if (strcmp(tok,devname)) {
                    break;
                }
                match = 1;
            } else if (field == 1) {
                s->num_reads = strtoull(tok,NULL,0);
            } else if (field == 4) {
                s->sum_read_msec = strtoull(tok,NULL,0);
            } else if (field == 5) {
                s->num_writes = strtoull(tok,NULL,0);
            } else if (field == 8) {
                s->sum_write_msec = strtoull(tok,NULL,0);
            } else if (field == 10) {
                s->sum_io_msec = strtoull(tok,NULL,0);
                break;
            }
            s0 = NULL;
            ++field;
        }
        if (match) {
            break;
        }
    }
    fclose(f);
#endif
}


struct mdbm_bench_stats {
    int numprocs;
    uint64_t t;
    mdbm_rstats_t stats;
    mdbm_rstats_t bs_stats;
    struct mdbm_disk_stats disk_stats;
};


static void
print_stats (mdbm_rstats_t* s, uint64_t t)
{
    fprintf(outfile, " %8u %9.5f %8u %9.3f %8u %9.3f %8u %9.3f",
           (uint32_t)(s->fetch.num * 1000000 / t),
           s->fetch.num ? (double)s->fetch.sum_usec / s->fetch.num / 1000 : 0,
           (uint32_t)(s->store.num * 1000000 / t),
           s->store.num ? (double)s->store.sum_usec / s->store.num / 1000 : 0,
           (uint32_t)(s->remove.num * 1000000 / t),
           s->remove.num ? (double)s->remove.sum_usec / s->remove.num / 1000: 0,
           (uint32_t)(s->getpage_uncached.num * 1000000 / t),
           s->getpage_uncached.num
           ? (double)s->getpage_uncached.sum_usec / s->getpage_uncached.num / 1000: 0);
}


static const char* T[] = {
    ".01",".02",".03",".04",".05",".06",".07",".08",".09",
    ".1",".2",".3",".4",".5",".6",".7",".8",".9",
    "1","2","3","4","5","6","7","8","9",
    "10","20","30","40","50","60","70","80","90",
    "100","200","300","400","500","600","700","800","900",
    "1000","2000","3000","4000","5000","6000","7000","8000","9000","10000",
    "20000",NULL};


static void
print_times_header (int min)
{
    int i;
    fputs(" ms",outfile);
    for (i = min; T[i]; i++) {
        fputc(' ', outfile);
        fputs(T[i],outfile);
    }
    fputc('\n', outfile);
}


static void
print_times (mdbm_rstats_val_t* s, int min)
{
    int i;
    uint64_t sum = 0;
    for (i = min; i < MDBM_RSTATS_THIST_MAX; i++) {
        sum += s->thist[i];
    }
    for (i = min; i < MDBM_RSTATS_THIST_MAX; i++) {
        fprintf(outfile, " %.2f",sum ? (double)s->thist[i]*100/sum : 0);
    }
}

static void
set_pagesize()
{
    if (pagesize == 0) {
        int syspagesize = sysconf(_SC_PAGESIZE);
        pagesize = (((valsize * 50) + syspagesize) / syspagesize) * syspagesize;
        if (pagesize > MDBM_MAX_VALID_PAGESIZE) {
            pagesize = (MDBM_MAX_VALID_PAGESIZE / syspagesize) * syspagesize;
        }
        fprintf(outfile, "Using page size of %d\n", pagesize);
    } else if ((unsigned)(pagesize * 3 / 4) < valsize) {
        fprintf(stderr,
                "mdbm_bench: value size %d must be less than 3/4 of page size %d.\n",
                 valsize, pagesize);
        exit(1);
    }
}

#define BENCH_FIND_SIZE_MIN  2    /* Start from:  2 * count * valuesize */
#define BENCH_FIND_SIZE_MAX  5    /* continue until:  5 * count * valuesize */

static MDBM *
populate_db(int reopen, int find_size, char *gkeys)
{
    MDBM *db = NULL;
    int filled, cur_size = 0, end_size = 0;

    /* To find the MDBM size, look between BENCH_FIND_SIZE_MIN and BENCH_FIND_SIZE_MAX */
    if (find_size) {
        cur_size = BENCH_FIND_SIZE_MIN;
        end_size = BENCH_FIND_SIZE_MAX;
        if ((unsigned)pagesize < (10 * valsize)) {
            fprintf(outfile, "Page size of %d seems small compared to value size of %d\n",
                    pagesize, valsize);
        }
    }

    do {
        if (load || !reopen) {
            if (db != NULL) {
                mdbm_close(db);
                unlink(fn);
            }
            db = open_db(cur_size);
        }

        if (db == NULL) {
            fprintf(outfile,"mdbm_bench: Cannot open MDBM\n");
            exit(1);
        }

        if (load) {
            struct mdbm_bench_config c;
            c.proc = 0;
            c.numprocs = 0;
            c.mode = LOAD;
            c.gkeys = gkeys;
            c.db = db;
            if (cur_size) {
                fprintf(outfile, "Creating MDBM with capacity utilization of %d%% ... \n",
                        100/cur_size);
            } else {
                fprintf(outfile, "Loading ... \n");
            }
            filled = load_data(&c, NULL);
            if (!filled) {
                fprintf(outfile,"mdbm_bench: Failed to create %d entries, retrying\n", count);
            }
        }
    } while (!filled && (cur_size++ <= end_size));

    if (load) {
        fprintf(outfile, "Beginning test ...\n");
    }
    return db;
}


int
main (int argc, char** argv)
{
    int opt;
    int minprocs = 1;
    int maxprocs = 1;
    int incr = -1;
    mdbm_shmem_t* shm;
    int nice = 0;
    int seconds = 10;
    int init;
    int numprocs;
    char* gkeys = NULL;
    int thread = 0;
    char* ofile = NULL;
    int step = 0;
    mdbm_rstats_t* rs = NULL;
    mdbm_rstats_t* bs_rs = NULL;
    int yes = 0;
    struct mdbm_bench_stats stats[NNPROCS];
    int nstats = 0;
    const char* devname = NULL;
    int i;
    int reopen = 0;
    MDBM* db = NULL;
    pthread_t* threads = NULL;
    int existing = 0;
    uint64_t tmp;
    int find_size = 0;    /* Automatically find the right size MDBM to benchmark with */

#ifdef MDBM_CONFIG_DEFAULT_DBVER_V3
    verflag = MDBM_CREATE_V3;
#endif

    while ((opt = getopt(argc,argv,"03a:B:b:C:c:D:d:ef:F:h:Kk:l:LM:n:N:o:OPp:Rs:StTu:Vv:w:W:XYyzZ"))
           != -1)
    {
        switch (opt) {
        case '0':
            lockall = 1;
#ifndef __linux__
            fputs("mdbm_bench: WARNING -0 only supported on Linux\n",stderr);
#endif
            break;

        case '3':
            verflag = MDBM_CREATE_V3;
            break;

        case 'a':
            align = atoi(optarg);
            break;

        case 'B':
            bs = optarg;
            break;

        case 'b':
            windowed = mdbm_util_get_size(optarg,1024*1024);
            break;

        case 'C':
            cachemode = atoi(optarg);
            break;

        case 'c':
            count = atoi(optarg);
            break;

        case 'd':
            if (strcasecmp(optarg,"find") == 0) {
                find_size = 1;
                large = 1;
            } else {
                dbsize = mdbm_util_get_size(optarg,1024*1024);
            }
            break;

        case 'D':
            devname = optarg;
            break;

        case 'e':
            existing = 1;
            break;

        case 'F':
            strcpy(fn,optarg);
            break;

        case 'f':
            fetchmode = atoi(optarg);
            if (fetchmode < 0 || fetchmode > 2) {
                fprintf(stderr,"mdbm_bench: Invalid fetch mode: %s\n",optarg);
                exit(1);
            }
            break;

        case 'g':
            runcount = atoi(optarg);
            break;

        case 'h':
            hash = atoi(optarg);
            break;

        case 'k':
            keysize = mdbm_util_get_size(optarg,1);
            break;

        case 'K':
            seq_keys = 1;
            break;

        case 'l':
            lockmode = atoi(optarg);
            if (lockmode < 0 || lockmode > 3) {
                fprintf(stderr,"mdbm_bench: Invalid lock mode: %s\n",optarg);
                exit(1);
            }
            lockarch = MDBM_SINGLE_ARCH;
            break;

        case 'L':
            large = 1;
            break;

        case 'M':
            miss_target = atof(optarg) / 100 * UINT_MAX;
            break;

        case 'n':
            getrange(&minprocs,&maxprocs,&incr);
            break;

        case 'N':
            nice = atoi(optarg);
            break;

        case 'o':
            ofile = optarg;
            break;

        case 'O':
            reopen = 1;
            break;

        case 'P':
            protect = 1;
            break;

        case 'p':
            tmp = mdbm_util_get_size(optarg,1);
            if (tmp > (unsigned)MDBM_MAX_VALID_PAGESIZE) {
                fprintf(stderr,"mdbm_bench: Invalid page size: %s\n", optarg);
                exit(1);
            }
            pagesize = tmp;
            break;

        case 'R':
            rstats++;
            break;

        case 's':
            seconds = atoi(optarg);
            break;

        case 'S':
            step = 1;
            break;

        case 't':
            if (++thread == 2) {
                reopen = 0;
                do_dup = 1;
            } else {
                reopen = 1;
            }
            break;

        case 'T':
            show_latency = 1;
            break;

        case 'u':
            updatemode = atoi(optarg);
            if (updatemode < 0 || updatemode > 2) {
                fprintf(stderr,"mdbm_bench: Invalid update mode: %s\n",optarg);
                exit(1);
            }
            break;

        case 'V':
            checkvals = 1;
            break;

        case 'v':
            tmp = mdbm_util_get_size(optarg,1);
            if (tmp > INT_MAX) {
                fprintf(stderr,"mdbm_bench: Invalid value size: %s\n",optarg);
                exit(1);
            }
            valsize = tmp;
            break;

        case 'w':
            writefrac = atof(optarg);
            if (writefrac > 100) {
                fprintf(stderr,"mdbm_bench: Invalid write fraction: %s\n",optarg);
                exit(1);
            }
            break;

        case 'W':
            working_set = atoi(optarg);
            break;

        case 'X':
            cache_load_thru = 1;
            break;

        case 'Y':
            create = 0;
            load = 1;

        case 'y':
            yes = 1;
            break;

        case 'z':
            create = 1;
            load = 0;
            break;

        case 'Z':
            create = 0;
            load = 0;
            break;

        default:
            usage();
        }
    }

    if (updatemode == 0) {
        updatemode = MDBM_UPDATE_REPLACE;
    } else if (updatemode == 1) {
        updatemode = MDBM_UPDATE_INPLACE;
    } else if (updatemode == 2) {
        updatemode = MDBM_UPDATE_DELETE_INSERT;
    }

    if ((miss_target > 0) != (working_set > 0)) {
        fprintf(stderr,
                "mdbm_bench: -M (miss target) and -W (working set) must be specified together.\n");
        exit(1);
    }
    if (working_set > count) {
        fprintf(stderr,
                "mdbm_bench: -W (working set) must be less than or equal to -c (count).\n");
        exit(1);
    }

    if (!fn[0]) {
        strcpy(fn,"/tmp/mdbm_bench.mdbm");
    } else if (existing) {
        benchmarkExisting(fn, writefrac, lockmode, ofile, count, 0);
        return 0;
    }

    mdbm_lock_reset(fn,0);

    if (ofile == NULL) {
        outfile = stdout;
    } else if ((outfile = fopen(ofile, "a")) == NULL) {
        fprintf(stderr,
                "mdbm_bench: unable to open file %s for output - ignoring -o option.\n", ofile);
        outfile = stdout;
        ofile = NULL;
    } else if (mdbm_select_log_plugin("file") != 0)  {
        fprintf(stderr,
                "mdbm_bench: unable select file-based plugin.\n");
        outfile = stdout;
        ofile = NULL;
    } else if (mdbm_set_log_filename(ofile) != 0) {
        fprintf(stderr,
                "mdbm_bench: unable log into file %s.\n", ofile);
        outfile = stdout;
        ofile = NULL;
    }

    set_pagesize();

    if (checkvals) {
        if (valsize < sizeof(uint32_t)) {
            fprintf(stderr,"mdbm_bench: valsize (%d) too small for value checks (need %d)\n",
                    (int)valsize,(int)sizeof(uint32_t));
            exit(1);
        }
        if (valsize >= 2*sizeof(uint32_t)) {
            checkvals = 2;
        }
    }

    signal(SIGCHLD,child_handler);
    if (nice) {
        if (setpriority(PRIO_PROCESS,0,nice) < 0) {
            mdbm_logerror(LOG_ALERT,0,"setpriority");
        }
    }

    shm = mdbm_shmem_open(STATSFILE, 
            MDBM_SHMEM_RDWR|MDBM_SHMEM_CREATE|MDBM_SHMEM_TRUNC, STATSSIZE, &init);
    if (!shm) {
        mdbm_logerror(LOG_ALERT,0,"mdbm_shmem_open(%s)",STATSFILE);
        exit(1);
    }
    if (init) {
        memset(shm->base,0,STATSSIZE);
    }
    mdbm_shmem_init_complete(shm);
    bench_stats = (bench_stats_t*)shm->base;
    mutex_init(&bench_stats->lock);
    cond_init(&bench_stats->cv);

    fprintf(outfile, "Top-level pid %d\n",getpid());

    set_random(70194039);
    if (!seq_keys) {
        fprintf(outfile, "Generating keys ... ");
        gkeys = (char*)malloc_or_die(count * keysize);
        get_random_bytes(gkeys,count * keysize);
        {
            uint8_t md[SHA_DIGEST_LENGTH];
            int i;
            SHA1((const unsigned char *)gkeys, count*keysize, md);
            fprintf(outfile, " [OK] (");
            for (i = 0; i < SHA_DIGEST_LENGTH; ++i) {
                fprintf(outfile, "%02x",md[i]);
            }
            fprintf(outfile, ")\n");
        }
    }

    if (create || load) {
        if (!yes) {
            char buf[32];
            printf("Are you sure you want to %s db? ",create ? "create" : "load");
            fgets(buf,sizeof(buf),stdin);
            if (strcasecmp(buf,"y\n") && strcasecmp(buf,"yes\n")) {
                exit(1);
            }
        }
    }

    db = populate_db(reopen, find_size, gkeys);
    if (create && !load) {
        printf("Asked for create but not load... exiting.\n");
        exit(0);
    }
    create = 0;

    setbuf(outfile,NULL);

    if (rstats) {
       struct mdbm_rstats_mem* m;
       if (mdbm_open_rstats(fn,O_RDWR,&m,&rs) < 0) {
           mdbm_logerror(LOG_ALERT,0,"mdbm_open_rstats(%s)",fn);
       }
       if (bsfn[0]) {
           if (mdbm_open_rstats(bsfn,O_RDWR,&m,&bs_rs) < 0) {
               mdbm_logerror(LOG_ALERT,0,"mdbm_open_rstats(%s)",fn);
           }
       }
    }

    if (thread) {
        threads = (pthread_t*)malloc_or_die(maxprocs * sizeof(pthread_t));
    }

    for (numprocs = minprocs; numprocs <= maxprocs;) {
        int i;
        mdbm_rstats_t rs0, rs1;
        mdbm_rstats_t bs_rs0, bs_rs1;
        struct mdbm_disk_stats ds0, ds1;
        uint64_t t0 = 0;
        uint64_t t1 = 0;

        if (step) {
            char buf[32];
            fprintf(outfile, "ready to start nprocs=%d: ",numprocs);
            fgets(buf,sizeof(buf),stdin);
        } else {
            fprintf(outfile, "Running %d second test with %d %s(s) ...",seconds,
                   numprocs,thread ? "thread" : "proc");
        }

        bench_stats->nproc = numprocs;
        bench_stats->tinterval = (uint64_t)seconds*1000000;
        bench_stats->tstart = bench_stats->tnow = get_time_usec();
        bench_stats->tend = bench_stats->tstart + bench_stats->tinterval;

        if (minprocs == 0 && maxprocs == 0) {
            struct mdbm_bench_config c;
            c.proc = 0;
            c.numprocs = 1;
            c.gkeys = gkeys;
            c.mode = ACCESS;
            c.db = reopen ? NULL : db;
            if (rs) {
                rs0 = *rs;
            }
            if (bs_rs) {
                bs_rs0 = *bs_rs;
            }
            if (devname) {
                read_diskstats(devname,&ds0);
            }
            t0 = get_time_usec();
            if (lockall) { do_mlockall(); }
            fputc('\n',outfile);
            bench(&c);
            t1 = get_time_usec();
            if (rs) {
                rs1 = *rs;
            }
            if (bs_rs) {
                bs_rs1 = *bs_rs;
            }
            if (devname) {
                read_diskstats(devname,&ds1);
            }
        } else {
            bench_stats->nstart = bench_stats->nfinish = 0;
            for (i = 0; i < numprocs; i++) {
                struct mdbm_bench_config* c;
                c = (struct mdbm_bench_config*)malloc_or_die(sizeof(*c));
                c->proc = i;
                c->numprocs = numprocs;
                c->gkeys = gkeys;
                c->mode = ACCESS;
                c->db = reopen ? NULL : db;
                if (thread) {
                    int err;
                    pthread_t t;
                    if ((err = pthread_create(&t,NULL,bench_thread,c)) != 0) {
                        mdbm_logerror(LOG_ALERT,0,"pthread_create");
                    } else {
                        threads[nforks++] = t;
                    }
                } else {
                    pid_t pid;
                    if ((pid = fork()) < 0) {
                        mdbm_logerror(LOG_ALERT,0,"fork");
                    }
                    if (pid) {
                        nforks++;
                    } else {
                        fprintf(outfile, " %d",getpid());
                        if (lockall) { do_mlockall(); }
                        bench(c);
                        exit(0);
                    }
                }
            }
            if (mutex_lock(&bench_stats->lock) < 0 && errno != EINPROGRESS) {
                perror("bench stats lock");
                pause();
                abort();
            }
            while (bench_stats->nstart < (unsigned)numprocs) {
                cond_wait(&bench_stats->cv,&bench_stats->lock);
            }
            mutex_unlock(&bench_stats->lock);
            fprintf(outfile, " ...");

            if (rs) {
                i = 0;
                while (bench_stats->nfinish == 0) {
                    sleep(1);
                    if (++i == ((seconds > 10) ? 10 : 1)) {
                        rs0 = *rs;
                        t0 = get_time_usec();
                        if (bs_rs) {
                            bs_rs0 = *bs_rs;
                        }
                        if (devname) {
                            read_diskstats(devname,&ds0);
                        }
                    }
                }
                rs1 = *rs;
                if (bs_rs) {
                    bs_rs1 = *bs_rs;
                }
                if (devname) {
                    read_diskstats(devname,&ds1);
                }
                t1 = get_time_usec();
            } else {
                t0 = bench_stats->tstart;
                t1 = bench_stats->tend;
            }

            if (mutex_lock(&bench_stats->lock) < 0 && errno != EINPROGRESS) {
                perror("bench stats lock");
                pause();
                abort();
            }
            while (bench_stats->nfinish < (unsigned)numprocs) {
                cond_wait(&bench_stats->cv,&bench_stats->lock);
            }
            mutex_unlock(&bench_stats->lock);
        }
        if (devname) {
            stats[nstats].disk_stats.num_reads = ds1.num_reads - ds0.num_reads;
            stats[nstats].disk_stats.num_writes = ds1.num_writes - ds0.num_writes;
            stats[nstats].disk_stats.sum_io_msec = ds1.sum_io_msec - ds0.sum_io_msec;
            stats[nstats].disk_stats.sum_read_msec = ds1.sum_read_msec - ds0.sum_read_msec;
            stats[nstats].disk_stats.sum_write_msec = ds1.sum_write_msec - ds0.sum_write_msec;
        }
        if (rs) {
            mdbm_diff_rstats(&rs0,&rs1,&stats[nstats].stats,NULL);
            if (bs_rs) {
                mdbm_diff_rstats(&bs_rs0,&bs_rs1,&stats[nstats].bs_stats,NULL);
            }
        } else {
            memset(stats+nstats,0,sizeof(stats[nstats]));
            for (i = 0; i < numprocs; i++) {
                stats[nstats].stats.fetch.num += bench_stats->proc[i].num_reads;
                stats[nstats].stats.fetch.sum_usec += bench_stats->proc[i].sum_read_latency;
                stats[nstats].stats.fetch.sum_lock_wait_usec +=
                    bench_stats->proc[i].sum_read_lock_latency;
                stats[nstats].stats.store.num += bench_stats->proc[i].num_writes;
                stats[nstats].stats.store.sum_usec += bench_stats->proc[i].sum_write_latency;
                stats[nstats].stats.store.sum_lock_wait_usec +=
                    bench_stats->proc[i].sum_write_lock_latency;
                stats[nstats].stats.remove.num += bench_stats->proc[i].num_deletes;
                stats[nstats].stats.remove.sum_usec += bench_stats->proc[i].sum_delete_latency;
                stats[nstats].stats.remove.sum_lock_wait_usec +=
                    bench_stats->proc[i].sum_delete_lock_latency;
            }
        }
        stats[nstats].numprocs = numprocs;
        stats[nstats].t = t1 - t0;

        {
            uint64_t ops = (stats[nstats].stats.fetch.num
                            + stats[nstats].stats.store.num
                            + stats[nstats].stats.remove.num);
            fprintf(outfile, " (%lu)\n",(long unsigned int)(ops ? (ops * 1000000 / stats[nstats].t) : 0));
        }

        nstats++;

        if (thread) {
            for (i = 0; i < nforks; i++) {
                void* p;
                pthread_join(threads[i],&p);
            }
        } else {
            int status;
            while (!waitpid(-1,&status,0));
        }

        nforks = 0;

        if (incr > 0) {
            numprocs += incr;
        } else {
            for (i = 0; NPROCS[i]; i++) {
                if (NPROCS[i] > numprocs) {
                    numprocs = NPROCS[i];
                    break;
                }
            }
            if (!NPROCS[i]) {
                if (numprocs == minprocs) {
                    break;
                }
                numprocs = maxprocs;
            }
        }

    }

    fprintf(outfile, "\
nproc  fetch/s      msec  store/s      msec    del/s      msec  getpg/s      msec");
    if (rstats && bs) {
        fprintf(outfile, "\
 %%miss");
    }
    if (bs_rstats) {
        fprintf(outfile, "\
  fetch/s      msec  store/s      msec    del/s      msec  getpg/s      msec");
    }
    if (devname) {
        fprintf(outfile, "\
   read/s     msec  write/s     msec  %%busy");
    }
    fputc('\n', outfile);

    for (i = 0; i < nstats; i++) {
        uint64_t t = stats[i].t;
        fprintf(outfile, "  %3d",stats[i].numprocs);
        print_stats(&stats[i].stats,t);
        if (rstats && bs) {
            fprintf(outfile, " %6.2f",
                   stats[i].stats.fetch.num
                   ? (double)stats[i].stats.fetch_uncached.num*100/stats[i].stats.fetch.num
                   : 0);
        }
        if (bs_rstats) {
            print_stats(&stats[i].bs_stats,t);
        }
        if (devname) {
            fprintf(outfile, " %8u %8.3f %8u %8.3f %6.2f",
                   (uint32_t)(stats[i].disk_stats.num_reads * 1000000 / t),
                   stats[i].disk_stats.num_reads
                   ? (double)stats[i].disk_stats.sum_read_msec / stats[i].disk_stats.num_reads
                   : 0,
                   (uint32_t)(stats[i].disk_stats.num_writes * 1000000 / t),
                   stats[i].disk_stats.num_writes
                   ? (double)stats[i].disk_stats.sum_write_msec/stats[i].disk_stats.num_writes
                   : 0,
                   (double)stats[i].disk_stats.sum_io_msec * 100000 / t);
        }
        fputc('\n',outfile);
    }
    if (rstats > 1) {
        print_times_header(5);
        for (i = 0; i < nstats; i++) {
            fprintf(outfile, " %3d",stats[i].numprocs);
            print_times(&stats[i].stats.fetch,5);
            fputc('\n', outfile);
        }
    }
    if (bs_rstats > 1) {
        print_times_header(5);
        for (i = 0; i < nstats; i++) {
            fprintf(outfile, " %3d",stats[i].numprocs);
            print_times(&stats[i].bs_stats.getpage_uncached,5);
            fputc('\n', outfile);
        }
    }

    if (db) {
        mdbm_close(db);
    }
    if (gkeys) {
        free(gkeys);
    }
    if (shm) {
        mdbm_shmem_close(shm,0);
    }

    if (ofile) {
        fclose(outfile);
    }

    munlockall();
    return 0;
}
