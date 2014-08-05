/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef DONT_MULTI_INCLUDE_STALL_SIGNALS_H
#define DONT_MULTI_INCLUDE_STALL_SIGNALS_H

/// Called by the user to begin delaying signals, 
/// (i.e. in a critical section to avoid shared memory being left in 
///  an inconsistent state.)
/// This function does one-time (normally) installation of a custom 
/// signal handler. It also caches the existing signal handlers, so
/// any user signal handlers MUST BE INSTALLED before it is first called.
/// This is done for performance reasons, as installing and removing handlers
/// involves a syscall which is relatively slow.
extern void hold_signals();

/// Called by the user to begin delivering signals again.
/// Any pending signals may be sent out before this call returns.
extern void resume_signals();

#endif /* DONT_MULTI_INCLUDE_STALL_SIGNALS_H */
