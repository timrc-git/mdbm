/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef DONT_MULTI_INCLUDE_MDBM_ATOMIC_H
#define DONT_MULTI_INCLUDE_MDBM_ATOMIC_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <signal.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Atomically decrement 'var'. Returns old value. */
static inline uint32_t atomic_dec32u(uint32_t* var) {
  return __sync_fetch_and_sub(var, 1);
}
static inline int32_t atomic_dec32s(int32_t* var) {
  return __sync_fetch_and_sub(var, 1);
}
static inline sig_atomic_t atomic_decsa(sig_atomic_t* var) {
  return __sync_fetch_and_sub(var, 1);
}

/* Atomically increment 'var'. Returns old value. */
static inline uint32_t atomic_inc32u(uint32_t* var) {
  return __sync_fetch_and_add(var, 1);
}
static inline int32_t atomic_inc32s(int32_t* var) {
  return __sync_fetch_and_add(var, 1);
}
static inline sig_atomic_t atomic_incsa(sig_atomic_t* var) {
  return __sync_fetch_and_add(var, 1);
}


/* Atomically adjust 'var' by 'delta'. Returns old value. */
static inline uint32_t atomic_add32u(uint32_t* var, uint32_t delta) {
  return __sync_fetch_and_add(var, delta);
}
static inline int32_t atomic_add32s(int32_t* var, int32_t delta) {
  return __sync_fetch_and_add(var, delta);
}

/* Atomically replace 'var' by 'to' if it is 'from'. Returns old value. */
static inline uint32_t atomic_cas32u(uint32_t* var, uint32_t from, uint32_t to) {
  return __sync_val_compare_and_swap(var, from, to);
}
static inline int32_t atomic_cas32s(int32_t* var, int32_t from, int32_t to) {
  return __sync_val_compare_and_swap(var, from, to);
}

/* returns true if the swap completed. */
static inline int atomic_cmp_and_set_32_bool (volatile void *ptr, uint32_t oldval, uint32_t newval)
{
  return __sync_bool_compare_and_swap((uint32_t*)ptr, oldval, newval);
}


/* returns true if the swap completed. */
static inline int atomic_cmp_and_set_64_bool (volatile void *ptr, uint64_t oldval, uint64_t newval)
{
  return __sync_bool_compare_and_swap((uint32_t*)ptr, oldval, newval);
}



static inline void atomic_barrier() {
  __sync_synchronize();
}

static inline void atomic_read_barrier() {
#ifdef __x86_64__
    __asm__ __volatile__ ("lfence" : : : "memory");
#else
    __asm__ __volatile__ ("lock addl $0,0(%%esp)" : : : "memory");
#endif
}

static inline void atomic_pause() {
  __asm__ __volatile__ ("pause");
}


/* returns (linux-specific) thread-id (for single-thread processes it's just PID) */
static inline uint32_t gettid() {
  /* AUTO_TSC("gettid()"); */
#ifdef __linux__
  return syscall(SYS_gettid);
#else
  /* Horrible hack, but pthread_self is not unique across processes, */
  /* and some OS don't expose any other unique id. */
  /* xor-fold pthread_self() pointer down to 16-bits */
  uint32_t tid;
  uint64_t pself = (uint64_t)pthread_self();
  pself = pself ^ (pself >> 32);
  pself = pself ^ (pself >> 16);
  /* add in PID to help unique-ify */
  /* NOTE: tiger and later OSX have PID #s up to 10k. */
  tid = ((pself & 0xffff) << 16) + getpid();
  return tid;
#endif
}

#ifdef __cplusplus
}
#endif


#endif /* DONT_MULTI_INCLUDE_MDBM_ATOMIC_H */
