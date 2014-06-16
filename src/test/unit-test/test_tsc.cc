/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of Statistics features

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"
#include "mdbm_stats.h"
#include "TestBase.hh"

class MdbmUnitTestTSC : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestTSC() : TestBase(0, "MdbmUnitTestTSC") {
    }
    virtual ~MdbmUnitTestTSC() {}

    void setUp() {}       // Per-test initialization - applies to every test
    void tearDown() {}    // Per-test cleanup - applies to every test
    void initialSetup() {}
    void finalCleanup() {}

    void test_GetTscUsec();

    CPPUNIT_TEST_SUITE(MdbmUnitTestTSC);
    CPPUNIT_TEST(initialSetup);
// TODO add #ifndef DISABLE_TSC
      CPPUNIT_TEST(test_GetTscUsec);
    CPPUNIT_TEST(finalCleanup);
    CPPUNIT_TEST_SUITE_END();
};
CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestTSC);



extern "C" { uint64_t tsc_get_usec(void); }

void
MdbmUnitTestTSC::test_GetTscUsec()
{
#ifdef DISABLE_TSC
      fprintf(stderr,
          "\n\n\n"
          "************************************************************ \n"
          "* NOTE: !! TSC test skipped due to DISABLE_TSC !!          * \n"
          "************************************************************ \n"
          "* Comment the DISABLE_TSC line in the top level            * \n"
          "* Makefile.base to re-enable it.                           * \n"
          "************************************************************ \n"
      );
#else //DISABLE_TSC
    uint64_t usecs = 300000;
    uint64_t t0, delta;

    // give the timer a chance to stabilize...
    t0 = tsc_get_usec();
    sleep(1);
    t0 = tsc_get_usec();

    for (int i=0; i<3; ++i) {
      t0 = tsc_get_usec();
      usleep(usecs);
      delta = tsc_get_usec() - t0;
      fprintf(stderr, "---- delta = %llu (expected ~%llu)----\n", (unsigned long long)delta, (unsigned long long)usecs);
      fprintf(stderr, "---- observed/expected time ratio is %f\n", delta/(float)usecs);
      if (delta > (usecs*.8) && delta < (usecs*1.2)) {
        break;
      }
    }

    if (!(delta > (usecs*.8) && delta < (usecs*1.2))) {
      fprintf(stderr,
          "\n\n\n"
          "************************************************************ \n"
          "* WARNING: !! TSC is not stable on this platform !!        * \n"
          "************************************************************ \n"
          "* The Time Stamp Counter can be a high performance clock,  * \n"
          "* built-in on most modern Intel and AMD CPUs.              * \n"
          "* See http://en.wikipedia.org/wiki/Time_Stamp_Counter      * \n"
          "*                                                          * \n"
          "* Unfortunately, on some processors, it is only suitable   * \n"
          "* as a counter, not as a clock.                            * \n"
          "* This appears to be such a system.                        * \n"
          "*                                                          * \n"
          "* Please uncomment the DISABLE_TSC line in the top level   * \n"
          "* Makefile.base to use alternate clock functions.          * \n"
          "************************************************************ \n"
          "\n\n\n"
      );
    }
    CPPUNIT_ASSERT(delta > (usecs*.8) && delta < (usecs*1.2));
#endif //DISABLE_TSC
}
