/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>

#include "atomic.h"

typedef void (*handler_t)(int);

struct signal_rec {
  int sigval;
  handler_t handler;
  sig_atomic_t count;
  sig_atomic_t installed;
};

/* The list of signals to be delayed */
/* Note: we intentionally don't delay things like SIGSEGV */
struct signal_rec delayed_signals[5] = {
  {SIGHUP,  NULL, 0, 0},
  {SIGINT,  NULL, 0, 0},
  {SIGQUIT, NULL, 0, 0},
  {SIGTERM, NULL, 0, 0},
  {SIGUSR1, NULL, 0, 0}
};
int sig_count = sizeof(delayed_signals)/sizeof(delayed_signals[0]);

sig_atomic_t handler_installed = 0;
sig_atomic_t delay_nest = 0;

/* Distributes a signal to the normal handler */
void pass_signal(int sig) {
  int i;
  for (i=0; i<sig_count; ++i) {
    if (sig == delayed_signals[i].sigval) {
      if (delayed_signals[i].handler == SIG_DFL) {
        /* default handler... temporarily disable ours and re-send (via kill), */
        /* as there is no portable way to invoke the default handler directly */
        signal(sig, SIG_DFL);
        delayed_signals[i].installed = 0;
        handler_installed = 0;
        kill(getpid(), sig); 
      } else if (delayed_signals[i].handler == SIG_IGN) {
        /* do nothing SIG_IGN == ignore */
      } else {
        /* user handler installed... invoke it directly */
        delayed_signals[i].handler(sig);
      }
    }
  }
}

/* our custom signal handler that conditionaly delays signals */
void delay_handler(int sig) {
  int i;
  if (delay_nest) {
    /* signals should be held... just make a note */
    for (i=0; i<sig_count; ++i) {
      if (sig == delayed_signals[i].sigval) {
        ++delayed_signals[i].count; 
      } 
    }
  } else {
    /* not holding signals now, pass it on directly */
    pass_signal(sig);
  }
}

/*
/// Called by the user to begin delaying signals, 
/// (i.e. in a critical section to avoid shared memory being left in 
///  an inconsistent state.)
/// This function does one-time (normally) installation of a custom 
/// signal handler. It also caches the existing signal handlers, so
/// any user signal handlers MUST BE INSTALLED before it is first called.
/// This is done for performance reasons, as installing and removing handlers
/// involves a syscall which is relatively slow.
/// TODO: add function to allow the user to explicitly udate handler cache...
*/
void hold_signals() {
  int i;
  struct sigaction oldact;

  atomic_incsa(&delay_nest);
  if (!handler_installed) {
    /* (re)install our delaying handler */
    /* this happens only the first time, or after a signal is delivered */
    /* to the system default handler (SIG_DFL) */
    for (i=0; i<sig_count; ++i) {
      if (!delayed_signals[i].installed) {
        /* store the old handler and install ours */
        int signum = delayed_signals[i].sigval;
        sigaction(signum, NULL, &oldact);
        delayed_signals[i].handler = oldact.sa_handler;
        signal(signum, delay_handler);
        delayed_signals[i].installed = 1;
      }
    }
    handler_installed = 1;
  }
}

/*
/// Called by the user to begin delivering signals again.
/// Any pending signals may be sent out before this call returns.
*/
void resume_signals() {
  int i;

  atomic_decsa(&delay_nest);
  /* send out any delayed signals */
  if (delay_nest == 0) {
    for (i=0; i<sig_count; ++i) {
      while (delayed_signals[i].count>0) {
        --delayed_signals[i].count; 
        pass_signal(delayed_signals[i].sigval);
      } 
    }
  }
}


