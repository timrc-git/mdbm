/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// CppUnitTestRunner.cc
//
// Contains a main() that runs all unit tests registered
// in the same executable.  Register your
// CppUnit::TestFixture-derived class with macro:
// CPPUNIT_TEST_SUITE_REGISTRATION(MyClassXUnitTest)
//
// If you'd like XML output to a file, instead of just simple
// success/failure status sent to stdout, set environment variable
// "XUNIT_XML_FILE" to the path and filename to write to.

#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestCaller.h>
#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/XmlOutputter.h>
#include <cppunit/TextOutputter.h>
//#include <cppunit/extensions/TestFactoryRegistry.h>

#include <unistd.h>  // getopt()
#include <sstream>   // cout <<
#include <fstream>   // ofstream
#include <string>
#include <stdexcept> // exception, runtime_error
#include <memory>    // unique_ptr
#if __cplusplus <= 199711L
  #define unique_ptr auto_ptr
#endif

using namespace std;


//////////////////////////////////////////////////////////////
// Timing listener, prints out Test and TestSuite run-times
//////////////////////////////////////////////////////////////
#include <cppunit/TestListener.h>
#include <cppunit/TestResult.h>
#include <cppunit/Test.h>
#include <cppunit/TestFailure.h>

//#include <time.h>    // for clock()
#include <sys/time.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

struct timeval ZERO_TV = {0,0};

class TimingListener : public CppUnit::TestListener {
public:
    static float DiffTimevals(struct timeval start, struct timeval end) {
        float elapsed =  end.tv_sec-start.tv_sec +
                        (end.tv_usec-start.tv_usec)*.000001;
        return elapsed;
    }

    TimingListener(bool timeTests=true, bool timeSuites=true, float minTiming=0.0)
        : showTest(timeTests), showSuite(timeSuites), minTime(minTiming),
          allTests(0.0),
          testStart(ZERO_TV) , testEnd(ZERO_TV) {
    }

    // Called at the start of every Test
    virtual void startTest(CppUnit::Test *test) {
        if (!showTest) { return; }
        gettimeofday(&testStart, NULL);
        //fprintf(stderr, "Test [%s]: started @%d sec \n", test->getName().c_str(), testStart.tv_sec);
    }

    // Called at the end of every Test, even failed ones
    virtual void endTest(CppUnit::Test *test) {
        if (!showTest) { return; }
        string name = test->getName();
        gettimeofday(&testEnd, NULL);
        float elapsed = DiffTimevals(testStart, testEnd);
        if (elapsed > minTime) {
            fprintf(stderr, "Test [%s]: %6.4f sec elapsed \n", name.c_str(), elapsed);
        }
    }

    // Called at the start of every TestSuite
    // and first for the meta-suite "All Tests"
    virtual void startSuite(CppUnit::Test *suite) {
        if (!showSuite) { return; }
        gettimeofday(&suiteStart, NULL);
        //fprintf(stderr, "TestSuite [%s]: started @%d sec \n", suite->getName().c_str(), suiteStart.tv_sec);
    }
    // Called at the end of every TestSuite
    virtual void endSuite(CppUnit::Test *suite) {
        if (!showSuite) { return; }
        string name = suite->getName();
        if (name == "All Tests") {
            if (allTests > minTime) {
                fprintf(stderr, "TestSuite [%s]: %6.4f sec elapsed \n", name.c_str(), allTests);
            }
        } else {
          gettimeofday(&suiteEnd, NULL);
          float elapsed = DiffTimevals(suiteStart, suiteEnd);
          if (elapsed > minTime) {
              fprintf(stderr, "TestSuite [%s]: %6.4f sec elapsed \n", name.c_str(), elapsed);
          }
          allTests += elapsed;
        }
    }

private:
    bool showTest;
    bool showSuite;
    float minTime;
    float allTests;
    struct timeval testStart;
    struct timeval testEnd;
    struct timeval suiteStart;
    struct timeval suiteEnd;
};

//////////////////////////////////////////////////////////////
// Failure listener, counts tests-run and tests-failed
//////////////////////////////////////////////////////////////
class FailListener : public CppUnit::TestListener {
public:
    FailListener() : tests(0), fails(0), complete(0) {
    }
    // Called for every Test that fails
    virtual void addFailure(const CppUnit::TestFailure& failure) {
        CppUnit::Test* test = failure.failedTest();
        fprintf(stderr, "Test [%s]: FAILED! \n", test->getName().c_str());

        ++fails;
        //(void)failure;
    }
    virtual void startTest(CppUnit::Test *test)  {
        ++tests;
    }

    virtual void endTest(CppUnit::Test *test) {
        ++complete;
    }

public:
    uint64_t tests;
    uint64_t fails;
    uint64_t complete;
};


// Handy when writing results to an XML file.
// Also writes "text" (not XML) results to stdout.
class ComboOutputter : public CppUnit::Outputter
{
private:
    // Test run results will be available here.  (Don't delete,
    // this object does not own this object.)
    //CppUnit::TestResultCollector* pResults; // stack thinks this is unused.
    // XML file to write to.
    ofstream xmlFile;
    // Responsible for writing XML results to xmlFile.
    unique_ptr<CppUnit::XmlOutputter> xmlOutputterPtr;
    // Responsible for writing text results to stdout.
    unique_ptr<CppUnit::TextOutputter> textOutputterPtr;

// Construction
public:
    //
    // IN   pResults  Pointer to the results object that write()
    //      will need to read.  (This results object won't be
    //      filled in yet, at the time this constructor is called.)
    // IN   xmlFilename  Filename (may include a path) to write
    //      XML results to, when write() is called.
    //
    ComboOutputter(CppUnit::TestResultCollector* pResults, const string& xmlFilename)
    {
        if (xmlFilename.empty()) {
            throw runtime_error("ComboOutputter::ComboOutputter() - Empty xmlFilename.");
        }

        // Create XML output file.
        xmlFile.exceptions(ofstream::failbit | ofstream::badbit);
        xmlFile.open(xmlFilename.c_str(), ofstream::out | ofstream::trunc);

        // Create the "outputters" we'll need.
        xmlOutputterPtr.reset(new CppUnit::XmlOutputter(pResults, xmlFile));
        textOutputterPtr.reset(new CppUnit::TextOutputter(pResults, cout));
    }

    virtual ~ComboOutputter()
    {
        // Flush/close output file.
        try {
            xmlFile.close();
        } catch (...) {}
    }

// Public API.
public:
    // Called by Runner, after it is done running the tests.
    // Writes XML results to a file, and text results to stdout.
    virtual void write()
    {
        xmlOutputterPtr->write();
        textOutputterPtr->write();
    }
};

/**
 * Output a command-line "usage" message.
 * IN   cmd  Name of this executable.
 */
static void
usage(const string& cmd)
{
    cout
    << "Usage: " << cmd << " [-t [type]] [-T time] [-x filename]" << endl
    << "Runs all unit tests registered in this executable." << endl
    << endl
    << "Register your CppUnit::TestFixture-derived class" << endl
    << "'ExampleTest', with macro:" << endl
    << "CPPUNIT_TEST_SUITE_REGISTRATION(ExampleTest)" << endl
    << endl
    << "By default, a 'text' summary of test results is" << endl
    << "written to stdout/stderr." << endl
    << endl
    << "args:" << endl
    << endl
    << "-x filename  (optional)" << endl
    << "  If specified, test results are written to the specified" << endl
    << "  filename (may include a directory path) in an XML format." << endl
    << "  (Fails, if directory path does not exist.)" << endl
    << endl
    << "-t [type]  (optional)" << endl
    << "  If specified, test timings are also printed." << endl
    << "  'type' (optional) is one of:" << endl
    << "    \"all\"    - prints suite timings and individual tests (default)" << endl
    << "    \"notest\" - prints suite timings, but not individual tests" << endl
    << "    \"nosuite\" - prints individual test timings, but not suites" << endl
    << endl
    << "-t time  (optional)" << endl
    << "  If specified, only test timings longer than 'time' seconds are printed." << endl
    << endl
    ;
}

/**
 * Runs all unit tests registered in this executable.
 * See usage().
 */
int main(int argc, char** argv)
{
    try {
        // If not empty, write results as XML, to this file.
        string xmlFilename;
        bool timeTests = false;
        bool timeSuites = false;
        float minTiming  = 0.0;

        // For each command-line option,
        int ch;
        while ((ch = getopt(argc, argv, "x:t:T:")) != -1)
        {
            switch (ch)
            {
            // XML results file
            case 'x': {
                xmlFilename = optarg;
                break;
            }
            //
            case 't': {
                timeTests=true;
                timeSuites=true;
                if (optarg) {
                    if (!strcmp("all", optarg)) {
                    } else if (!strcmp("notest", optarg)) {
                        timeTests = false;
                    } else if (!strcmp("nosuite", optarg)) {
                        timeSuites = false;
                    } else {
                        cout << "Unknown timing option \"" << optarg <<  "\"." << endl;
                        usage(argv[0]);
                        return 1;
                    }
                }
                break;
            }
            case 'T': {
                minTiming = (float)strtod(optarg, NULL);
                break;
            }
            // Unknown switch
            case '?': default: {
                cout << "Unknown switch." << endl;
                usage(argv[0]);
                return 1;
            }
            }
        }

        // Find all tests linked into this executable.
        CppUnit::TestFactoryRegistry &registry
            = CppUnit::TestFactoryRegistry::getRegistry();

        // Create a runner that will run all of these tests.
        CppUnit::TextUi::TestRunner runner;
        runner.addTest(registry.makeTest());

        // informs test-listener about testresults
        //CppUnit::TestResult testresult;
        TimingListener timingListener(timeTests, timeSuites, minTiming);
        FailListener failListener;
        if (timeTests || timeSuites) {
          //testresult.addListener(&timingListener);
          //testresult.addListener(&failListener);
          runner.eventManager().addListener(&timingListener);
          runner.eventManager().addListener(&failListener);
        }

        // If writing results to XML file,
        ofstream xmlFile;
        if (!xmlFilename.empty())
        {
            // Tell runner it will write test output to an XML file,
            // as well as text output to stdout.
            runner.setOutputter(
                new ComboOutputter(&(runner.result()), xmlFilename));

            // Report that output is headed to XML file.
            cout << "Writing unit test results to XML file ("
                 << xmlFilename << ")." << endl;
        }

        bool success;
        // Run the tests.
        //if (timeTests || timeSuites) {
        //  runner.run(testresult);
        //  success = (failListener.fails == 0);
        //} else {
          success = runner.run();
        //}

        // Return success/failure to shell.
        return ((success) ? 0 : 1);
    }
    catch (exception& e)
    {
        cout << "Failure due to: " << e.what() << endl;
        return 1;
    }
}

////
