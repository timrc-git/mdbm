/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of General/Other features

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sched.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>

// for usage
#include <sys/time.h>
#include <sys/resource.h>

#include <string>
#include <iostream>
#include <sstream>
#include <vector>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"
#include "mdbm_internal.h"
#include "atomic.h"
#include "util.hh"
//#include "../lib/multi_lock_wrap.h"
#include "multi_lock.hh"
#include "mdbm_shmem.h"
extern "C" {
#include "stall_signals.h"
}

#include "TestBase.hh"

using namespace std;

class MdbmUnitTestSignals : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestSignals(int tFlags) : TestBase(tFlags, "MdbmUnitTestSignals") { testFlags = tFlags; }
    virtual ~MdbmUnitTestSignals() {}

    void setUp() {}       // Per-test initialization - applies to every test
    void tearDown() {}    // Per-test cleanup - applies to every test

    void initialSetup();

    void testSignals();

    void finalCleanup();

  private:
    int testFlags;
};

void
MdbmUnitTestSignals::initialSetup()
{
}


sig_atomic_t signal_count = 0;
void sig_handler(int signum)
{
   // printf("Caught signal %d\n",signum);
   ++signal_count;
}

void MdbmUnitTestSignals::testSignals() {
  TRACE_TEST_CASE(__func__)
  int sig = SIGHUP;
  int sig2 = SIGUSR1;

  signal_count = 0;
  // install signal handler
  signal(sig, sig_handler);
  signal(sig2, SIG_IGN);

  printf("Delaying signals (count:%d)\n", signal_count);
  hold_signals();

  // fire signal
  printf("Sending signal %d\n", sig);
  kill(getpid(), sig);
  kill(getpid(), sig2);
  sleep(1);
  CPPUNIT_ASSERT(0 == signal_count);

  printf("restoring signals \n");
  resume_signals();

  CPPUNIT_ASSERT(1 == signal_count);

  // restore default handler
  signal(SIGHUP, SIG_DFL);
}


void
MdbmUnitTestSignals::finalCleanup()
{
    TRACE_TEST_CASE(__func__);
    CleanupTmpDir();
    GetLogStream() << "Completed " << versionString << " General/Signals Tests." << endl<<flush;
}

/// MDBM V3 class

class MdbmUnitTestSignalsV3 : public MdbmUnitTestSignals
{
  CPPUNIT_TEST_SUITE(MdbmUnitTestSignalsV3);

    // must be first for signal handler setup...
    CPPUNIT_TEST(initialSetup);

    CPPUNIT_TEST(testSignals);

    CPPUNIT_TEST(finalCleanup);

  CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestSignalsV3() : MdbmUnitTestSignals(MDBM_CREATE_V3) { }
};


CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestSignalsV3);



