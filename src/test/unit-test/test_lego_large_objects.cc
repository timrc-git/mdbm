/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  File: test_lego_large_objects.cc
//  Purpose: [BUG 5999799] Cannot store large objects into a dynamic-growth mdbm

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sstream>

// For much better gdb debugging, define `DEBUG_PROGRAM 1' to build without CPPUNIT.
// The Makefile builds with CppUnitTestRunnerLocal.o, so you will need to
// manually link without that object.
#define DEBUG_PROGRAM 0

#if DEBUG_PROGRAM
#else
#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

#include <mdbm.h>
#include <mdbm_internal.h>
#include "TestBase.hh"

#if DEBUG_PROGRAM

#undef CPPUNIT_TEST_SUITE
#define CPPUNIT_TEST_SUITE(suiteName) ;

#undef CPPUNIT_TEST
#define CPPUNIT_TEST(testName) ;

#undef CPPUNIT_TEST_SUITE_END
#define CPPUNIT_TEST_SUITE_END() ;

#undef CPPUNIT_TEST_SUITE_REGISTRATION
#define CPPUNIT_TEST_SUITE_REGISTRATION(ignore) ;

#undef CPPUNIT_ASSERT
#define CPPUNIT_ASSERT assert

#undef CPPUNIT_ASSERT_EQUAL
#define CPPUNIT_ASSERT_EQUAL(expected, actual) assert((expected) == (actual))

#undef CPPUNIT_ASSERT_NO_THROW
#define CPPUNIT_ASSERT_NO_THROW(expression) \
{ \
    try { \
        expression ; \
    } \
    catch (...) { \
        std::cerr << "Caught exception at " \
                 << __FILE__ << ":" << __LINE__; \
        exit(1); \
    } \
}

#endif // End DEBUG_PROGRAM


class MdbmLegoLargeObjectsUnitTest
#if DEBUG_PROGRAM
    : public TestBase
#else
    : public CppUnit::TestFixture,
      public TestBase
#endif
{
    CPPUNIT_TEST_SUITE(MdbmLegoLargeObjectsUnitTest);

    CPPUNIT_TEST(testMdbmLargeObjects);
    CPPUNIT_TEST(testMdbmLargeAndSmallObjects);

    CPPUNIT_TEST_SUITE_END();

  public:
    MdbmLegoLargeObjectsUnitTest()
        : TestBase(MDBM_CREATE_V3, "MdbmLegoLargeObjectsUnitTest")
        {
        }

    // Per-test initialization - applies to every test
    void
    setUp();

    // Per-test cleanup - applies to every test
    void
    tearDown();

    void testMdbmLargeObjects();
    void testMdbmLargeAndSmallObjects();

  private:
    bool
    fileExists(const std::string& path);

    bool
    deleteFileIfExists(const std::string& path);

    void
    createNewMdbm(const std::string& path, int auxFlags = 0, int pageSize = pageSizeDefault_);

    void
    testMdbmLargeObjectsPageSize(int pageSize, bool smallObjects);

  private:
    static const int pageSizeDefault_ = 8192;
    static const int modeDefault_ = 0666;
    static const int dbSizeDefault_ = 0;
    static const int outerIterations_ = 100;
    static const int smallObjectIterations_ = 100;

    bool verbose_;
    std::string basePath_;
    std::string mdbm_;
};

CPPUNIT_TEST_SUITE_REGISTRATION(MdbmLegoLargeObjectsUnitTest);

void
MdbmLegoLargeObjectsUnitTest::setUp()
{
    TRACE_TEST_CASE(__func__);

    verbose_ = false; // False to inhibit per-key messages.
    //SetAutoClean(false); // DEBUG: set to fall so keep mdbm for running mdbm_stat.

    CPPUNIT_ASSERT_EQUAL(0, GetTmpDir(basePath_));
    //basePath_ = "/tmp"; // DEBUG: Use a temporary static path.
    mdbm_ = basePath_ + std::string("/test_lego_large_objects.mdbm");

    TRACE_LOG("setUp"
              << ", mdbm_=" << mdbm_
              << ", logLevel=" << logLevel);

    // Delete the test mdbm before each test.
    deleteFileIfExists(mdbm_);
}

void
MdbmLegoLargeObjectsUnitTest::tearDown()
{
}

bool
MdbmLegoLargeObjectsUnitTest::fileExists(const std::string& path)
{
    struct stat buf;
    bool exists = (stat(path.c_str(), &buf) == 0
                   ? true
                   : false);
    std::cout << "path=" << path << ", exists=" << exists << std::endl << std::flush;
    return exists;
}

bool
MdbmLegoLargeObjectsUnitTest::deleteFileIfExists(const std::string& path)
{
    bool deleted;

    if (fileExists(path)) {
        CPPUNIT_ASSERT_EQUAL(0, unlink(path.c_str()));
        deleted = true;
    } else {
        deleted = false;
    }

    return deleted;
}

void
MdbmLegoLargeObjectsUnitTest::createNewMdbm(const std::string& path, int auxFlags, int pageSize)
{
    deleteFileIfExists(path);
    int flags = (MDBM_O_CREAT |
                 MDBM_O_TRUNC |
                 MDBM_O_RDWR  |
                 MDBM_O_FSYNC |
                 auxFlags);
    MDBM *mdbm;
    CPPUNIT_ASSERT((mdbm = EnsureNamedMdbm(path, flags, modeDefault_, pageSize, dbSizeDefault_))
                   != NULL);
    mdbm_close(mdbm);
    CPPUNIT_ASSERT(fileExists(path));
}

void
MdbmLegoLargeObjectsUnitTest::testMdbmLargeObjectsPageSize(int pageSize, bool smallObjects)
{
    TRACE_TEST_CASE(__func__);

    char bigValueString[69291+1];
    memset(bigValueString, '.', sizeof(bigValueString));
    bigValueString[sizeof(bigValueString)-1] = 0;

    datum bigValue;
    bigValue.dsize = strlen(bigValueString);
    bigValue.dptr = bigValueString;

    createNewMdbm(mdbm_, MDBM_CREATE_V3 | MDBM_LARGE_OBJECTS, pageSize);
    MDBM* db = NULL;
    CPPUNIT_ASSERT((db = mdbm_open(mdbm_.c_str(), MDBM_O_RDWR, 0660, 0, 0)) != NULL);

    //DEBUG: Manually test how pre-splitting affects large-object allocation.
    //CPPUNIT_ASSERT_EQUAL(0, mdbm_pre_split(db, 10000));

    for (int i = 0; i < outerIterations_; i++) {
        if (smallObjects) {
            std::string spacing;
            for (int j = 0; j < smallObjectIterations_; j++) {
                // Write a small value record and verify it.
                std::ostringstream oss;
                oss << "SMALL_STRING_" << i << "_" << j;
                std::string smallKeyString = oss.str();
                if (verbose_) {
                    std::cout << spacing << smallKeyString << std::flush;
                    if (j == 0) {
                        spacing = " ";
                    }
                }

                datum smallKey;
                smallKey.dptr = const_cast<char*>(smallKeyString.c_str());
                CPPUNIT_ASSERT(smallKey.dptr != NULL);
                smallKey.dsize = smallKeyString.size();
                CPPUNIT_ASSERT(smallKey.dsize > 0);

                oss.str(""); // Reset stream.
                oss << j;
                std::string smallValueString = oss.str();

                datum smallValue;
                smallValue.dptr =  const_cast<char*>(smallValueString.c_str());
                CPPUNIT_ASSERT(smallValue.dptr != NULL);
                smallValue.dsize = smallValueString.size();
                CPPUNIT_ASSERT(smallValue.dsize > 0);

                CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, smallKey, smallValue, MDBM_REPLACE));

                datum smallFetch;
                smallFetch.dptr = NULL;
                smallFetch.dsize = 0;
                smallFetch = mdbm_fetch(db, smallKey);
                CPPUNIT_ASSERT(smallFetch.dptr != NULL);
                CPPUNIT_ASSERT_EQUAL(smallValue.dsize, smallFetch.dsize);
            }
        }

        // Write a big value and verify it.
        std::ostringstream oss;
        oss << "BIG_STRING_" << i;
        std::string bigKeyString = oss.str();

        datum bigKey;
        bigKey.dptr = const_cast<char*>(bigKeyString.c_str());
        CPPUNIT_ASSERT(bigKey.dptr != NULL);
        bigKey.dsize = bigKeyString.size();
        CPPUNIT_ASSERT(bigKey.dsize > 0);

        if (verbose_) {
            std::cout << std::endl
                      << bigKeyString
                      << "[" << bigKey.dsize << ", " << bigValue.dsize << "]"
                      << std::endl << std::flush;
        }

        CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, bigKey, bigValue, MDBM_REPLACE));

        datum bigFetch;
        bigFetch.dptr = NULL;
        bigFetch.dsize = 0;
        bigFetch = mdbm_fetch(db, bigKey);
        CPPUNIT_ASSERT(bigFetch.dptr != NULL);
        CPPUNIT_ASSERT_EQUAL(bigValue.dsize, bigFetch.dsize);
    }

    mdbm_close(db);
}

void
MdbmLegoLargeObjectsUnitTest::testMdbmLargeObjects()
{
    // Large object only tests for various page sizes.
    testMdbmLargeObjectsPageSize(pageSizeDefault_, false);
    testMdbmLargeObjectsPageSize(8096, false); // Rounds up to 8128 page size.
    testMdbmLargeObjectsPageSize(8196, false); // Rounds up to 8256 page size.
}

void
MdbmLegoLargeObjectsUnitTest::testMdbmLargeAndSmallObjects()
{
    // Large object + small object tests for various page sizes.
    testMdbmLargeObjectsPageSize(pageSizeDefault_, true);
    testMdbmLargeObjectsPageSize(8096, true); // Rounds up to 8128 page size.
    testMdbmLargeObjectsPageSize(8196, true); // Rounds up to 8256 page size.
}

#if DEBUG_PROGRAM
int
main(int argc, char* argv[])
{
    MdbmLegoLargeObjectsUnitTest t;
    t.setUp();
    t.testMdbmLargeObjects();
    return 0;
}
#endif
