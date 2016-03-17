/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef DONT_MULTI_INCLUDE_MDBM_UTIL_HH
#define DONT_MULTI_INCLUDE_MDBM_UTIL_HH

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>

#include <execinfo.h>
extern "C" {
  /* Obtain a backtrace and print it to stdout. */
  inline void print_trace(void) {
    void *array[10];
    size_t size = backtrace (array, 10);
    char **strings = backtrace_symbols (array, size);
    printf ("Obtained %d stack frames.\n", (int)size);
    for (size_t i = 0; i < size; i++) { fprintf (stderr, "  @-->%s\n", strings[i]); }
    free (strings);
  }


  // Intel (and later model AMD) Fetch Time-StampCounter
  // WARNING:  This value may be affected by speedstep and may vary randomly across cores.
  __inline__ uint64_t rdtsc(void) {
    uint32_t lo, hi;
    __asm__ __volatile__ (      // serialize
    "xorl %%eax,%%eax \n        cpuid"
    ::: "%rax", "%rbx", "%rcx", "%rdx");
    /* We cannot use "=A", since this would use %rax on x86_64 and return only the lower 32bits of the TSC */
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (uint64_t)hi << 32 | lo;
  }
}

#include <map>
#include <string>

class AccumTSCRecord {
public:
  volatile uint64_t total;
  volatile uint64_t count;
  std::string       label;

  AccumTSCRecord() : total(0), count(0), label("<unknown>") {}
};

class AccumTSC {
public:
  typedef std::map<std::string, AccumTSCRecord*> AccumulatorMap;
  AccumulatorMap records;
  AccumTSC() { }
  ~AccumTSC() {
    // G++ seems to be creating and destroying these with great abandon, even though
    // the macro that makes them declares them (function) static
    AccumulatorMap::iterator iter;
    for (iter = records.begin(); iter != records.end(); ++iter) {
      AccumTSCRecord* rec = iter->second;
      fprintf(stderr, "TSC pid:%d [%-29s] Time % 12lld for % 9lld avg:% 7lld\n", getpid(), rec->label.c_str(), (long long)rec->total, (long long)rec->count, rec->count ? ((long long)rec->total) / (long long)rec->count : 0);
    }
  }
  AccumTSCRecord* GetRecord(const char* label) {
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    AccumTSCRecord *rec = NULL;
    //fprintf(stderr, "Fetching accumulator for [%s]\n", label);
    pthread_mutex_lock(&mutex);
    AccumulatorMap::iterator iter = records.find(label);
    if (iter != records.end()) {
      rec = records[label];
    } else {
      rec = new AccumTSCRecord;
      rec->label = label;
      records[label] = rec;
    }
    pthread_mutex_unlock(&mutex);
    return rec;
  }
  static void Increment(AccumTSCRecord *rec, uint64_t time) {
    __sync_fetch_and_add(&rec->count, 1);
    __sync_fetch_and_add(&rec->total, time);
    //fprintf(stderr, "Time %lld for %lld [%s]\n", (long long)total, (long long)count, label);
  }
};

static AccumTSC Accumulators;

class AutoTSC {
public:
  uint64_t        start;
  const char     *label;
  AccumTSCRecord *rec; 
  AutoTSC(const char* lbl, AccumTSCRecord* rec) : rec(rec) {
    label = lbl;
    start = rdtsc();
  }
  ~AutoTSC() {
    uint64_t delta = rdtsc() - start;
    Accumulators.Increment(rec, delta);
    //fprintf(stderr, "Time %lld for [%s]\n", (long long)delta, label);
  }
};


// profiling via Intel TimeStampCounters, function-static and scoped-automatic variables
#if 0
#  define AUTO_TSC(desc)                                           \
     static AccumTSCRecord* record = Accumulators.GetRecord(desc); \
     AutoTSC TSCAuto(desc, record);
#else
#  define AUTO_TSC(desc)
#endif

// branch hinting macros
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

// cache prefetching macros
#define prefetch(x) __builtin_prefetch(x)


//#define CHECKPOINTV(msg, ...) fprintf(stderr, "%d %s:%d::%s() "msg"\n", getpid(), __FILE__, __LINE__, __PRETTY_FUNCTION__,__VA_ARGS__); 
#define CHECKPOINTV(msg, ...) 

//#define TRACE_LOCKS
#define PRINT_LOCK_TRACE(msg) {                                                      \
    char buf[512];                                                                   \
    snprintf(buf, 512,                                                               \
        "rec:%p pid:%d self:" OWNER_FMT " %s count:%u owner:" OWNER_FMT " idx:%d\n", \
        rec, getpid(), get_thread_id(), msg, rec->count,                             \
        rec->owner, index);                                                          \
    fprintf(stderr, buf);                                                            \
  }

#endif /* DONT_MULTI_INCLUDE_MDBM_UTIL_HH */
