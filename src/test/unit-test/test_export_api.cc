/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

//  Purpose: Unit tests of DBdump/CDBdump API that write records to file or add records to buffer

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <iostream>
#include <sstream>
#include <string>
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
#include "TestBase.hh"

// Include mdbm_import and redefine main() and usage() to perform unit tests w/o using fork+exec

#define main import_main_wrapper
#define usage import_usage_wrapper
#include "../../tools/mdbm_import.cc"
#undef main
#undef usage

extern int print_write_err(const char *filename);
extern int print_read_format_error(char badchar, int linenum, const char *filename);

using namespace std;

class MdbmUnitTestDump : public CppUnit::TestFixture, public TestBase
{
  public:
    MdbmUnitTestDump(int tFlags) : TestBase(tFlags, "MdbmUnitTestDump") { testFlags = tFlags;}
    virtual ~MdbmUnitTestDump() {}

    void setUp() {}       // Per-test initialization - applies to every test
    void tearDown() {}    // Per-test cleanup - applies to every test

    void initialSetup();
    void test_DbDumpNeg();
    void test_DbDumpBinary();
    void test_DbDumpText();

    void test_CDbDumpNeg();
    void test_CDbDumpBinary();
    void test_CDbDumpText();

    void test_DbDumpRecordsText();
    void test_DbDumpRecordsBinary();

    void test_CDbDumpRecordsText();
    void test_CDbDumpRecordsBinary();

    void test_ImportDbDumpBinary();
    void test_ImportDbDumpHeaderBinary();
    void test_ImportCdbBinary();
    void test_ImportDbDumpText();
    void test_ImportDbDumpHeaderText();
    void test_ImportCdbText();

    void finalCleanup();

    // Helper methods

    string writeToFile(MDBM *db, bool cdbFormat, bool header = false);
    void findDiffs(MDBM *db1, MDBM *db2);
    void exportImportCompare(const string &prefix, bool cdbFormat, bool useMemBuffer = false,
                             MDBM *mdbm = NULL, const string &mdbmName = "");
    MDBM *createTextMdbm(const string &prefix, string *filename = NULL, int keyCount = 30);
    string writeAllRecords(MDBM *db, bool cdbFormat);

    void testImportAPI(const string &prefix, bool cdbFormat, bool binary, bool header = false);

  private:

    int testFlags;
};

void
MdbmUnitTestDump::initialSetup()
{
}

void
MdbmUnitTestDump::test_DbDumpNeg()
{
    string prefix = string("DbDumpNeg");
    TRACE_TEST_CASE(__func__)

    kvpair kv;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_dbdump_to_file(kv, NULL));
}

void
MdbmUnitTestDump::test_CDbDumpNeg()
{
    string prefix = string("CDbDumpNeg");
    TRACE_TEST_CASE(__func__)

    kvpair kv;
    CPPUNIT_ASSERT_EQUAL(-1, mdbm_cdbdump_to_file(kv, NULL));
}

void
MdbmUnitTestDump::test_DbDumpBinary()
{
    string prefix = string("DbDumpBinary");
    TRACE_TEST_CASE(__func__)

    exportImportCompare(prefix, false);
}

void
MdbmUnitTestDump::test_CDbDumpBinary()
{
    string prefix = string("CDbDumpBinary");
    TRACE_TEST_CASE(__func__)

    exportImportCompare(prefix, true);
}


void
MdbmUnitTestDump::test_DbDumpText()
{
    string prefix = string("DbDumpText");
    TRACE_TEST_CASE(__func__)

    string filename;
    MDBM *mdbm = createTextMdbm(prefix, &filename);
    exportImportCompare(prefix, false, false, mdbm, filename);
}


void
MdbmUnitTestDump::test_CDbDumpText()
{
    string prefix = string("CDbDumpText");
    TRACE_TEST_CASE(__func__)

    string filename;
    MDBM *mdbm = createTextMdbm(prefix, &filename);
    exportImportCompare(prefix, true, false, mdbm, filename);
}


void
MdbmUnitTestDump::test_DbDumpRecordsText()
{
    string prefix = string("DbDumpRecordsText");
    TRACE_TEST_CASE(__func__)

    string filename;
    MDBM *mdbm = createTextMdbm(prefix, &filename);
    exportImportCompare(prefix, false, true, mdbm, filename);
}


void
MdbmUnitTestDump::test_CDbDumpRecordsText()
{
    string prefix = string("CDbDumpRecordsText");
    TRACE_TEST_CASE(__func__)

    string filename;
    MDBM *mdbm = createTextMdbm(prefix, &filename);
    exportImportCompare(prefix, true, true, mdbm, filename);
}

void
MdbmUnitTestDump::test_DbDumpRecordsBinary()
{
    string prefix = string("DbDumpRecordsBinary");
    TRACE_TEST_CASE(__func__)

    exportImportCompare(prefix, false, true);
}


void
MdbmUnitTestDump::test_CDbDumpRecordsBinary()
{
    string prefix = string("CDbDumpRecordsBinary");
    TRACE_TEST_CASE(__func__)

    exportImportCompare(prefix, true, true);
}

void
MdbmUnitTestDump::test_ImportDbDumpBinary()
{
    string prefix = string("ImportDbDumpBinary");
    TRACE_TEST_CASE(__func__)

    testImportAPI(prefix, false, true);

    // Maintain function coverage
    CPPUNIT_ASSERT_EQUAL(-2, print_write_err("testfile"));
    CPPUNIT_ASSERT_EQUAL(-1, print_read_format_error('1', 2, "testfile"));
}

void
MdbmUnitTestDump::test_ImportDbDumpHeaderBinary()
{
    string prefix = string("ImportDbDumpHeaderBinary");
    TRACE_TEST_CASE(__func__)

    testImportAPI(prefix, false, true, true);
}

void
MdbmUnitTestDump::test_ImportCdbBinary()
{
    string prefix = string("ImportCdbBinary");
    TRACE_TEST_CASE(__func__)

    testImportAPI(prefix, true, true);
}

void
MdbmUnitTestDump::test_ImportDbDumpText()
{
    string prefix = string("ImportDbDumpText");
    TRACE_TEST_CASE(__func__)

    testImportAPI(prefix, false, false);
}

void
MdbmUnitTestDump::test_ImportDbDumpHeaderText()
{
    string prefix = string("ImportDbDumpHeaderText");
    TRACE_TEST_CASE(__func__)

    testImportAPI(prefix, false, false, true);
}

void
MdbmUnitTestDump::test_ImportCdbText()
{
    string prefix = string("ImportCdbText");
    TRACE_TEST_CASE(__func__)

    testImportAPI(prefix, true, false);
}

void
MdbmUnitTestDump::finalCleanup()
{
    TRACE_TEST_CASE(__func__);
    CleanupTmpDir();
    GetLogStream() << "Completed " << versionString << " DB/CDB Dump API Tests." << endl<<flush;
}

/// ------------------------------------------------------------------------------
///
/// Helper methods
///
/// ------------------------------------------------------------------------------

void
MdbmUnitTestDump::exportImportCompare(const string &prefix, bool cdbFormat, bool useMemBuffer,
                                      MDBM *mdbm, const string &mdbmName)
{
    string fname;
    MDBM *db1;

    if (mdbm == NULL) {
        db1 = GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | versionFlag, 0666, 0, 0, &fname);
        InsertData(db1, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT,
                   true, DEFAULT_ENTRY_COUNT);
    } else {
        db1 = mdbm;
        fname = mdbmName;
    }

    string datafile;
    if (useMemBuffer) {
        datafile = writeAllRecords(db1, cdbFormat);
    } else {
        datafile = writeToFile(db1, cdbFormat);
    }

    string outfile = GetTmpName("imp_out");

    const char *arg1 = "-T";
    if (cdbFormat) {
        arg1 = "-c";
    }

    const char* args[] = { "mdbm_import", arg1, "-i", datafile.c_str(), outfile.c_str(), NULL };
    reset_getopt();
    int ret = import_main_wrapper(sizeof(args)/sizeof(args[0])-1, (char**)args);
    CPPUNIT_ASSERT_EQUAL(0, ret);

    MdbmHolder db2 = mdbm_open(outfile.c_str(), MDBM_O_RDONLY, 0644, 0 , 0);
    CPPUNIT_ASSERT(NULL != (MDBM *) db2);
    findDiffs(db1, db2);

    mdbm_close(db1);
    unlink(datafile.c_str());
}

string
MdbmUnitTestDump::writeToFile(MDBM *db, bool cdbFormat, bool header)
{
    const char *outfile = "/tmp/data.dump";
    FILE *fp = fopen(outfile, "w");
    CPPUNIT_ASSERT(fp != NULL);

    if (header) {
        CPPUNIT_ASSERT_EQUAL(0, mdbm_dbdump_export_header(db, fp));
    }

    kvpair kv;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    for (kv=mdbm_first_r(db, &iter); kv.key.dptr != NULL; kv=mdbm_next_r(db, &iter)) {
        if (cdbFormat) {
            CPPUNIT_ASSERT_EQUAL(0, mdbm_cdbdump_to_file(kv, fp));
        } else {
            CPPUNIT_ASSERT_EQUAL(0, mdbm_dbdump_to_file(kv, fp));
        }
    }

    int ret;
    if (cdbFormat) {
        ret = mdbm_cdbdump_trailer_and_close(fp);
    } else {
        ret = mdbm_dbdump_trailer_and_close(fp);
    }

    CPPUNIT_ASSERT_EQUAL(0, ret);
    return outfile;
}

void
MdbmUnitTestDump::findDiffs(MDBM *db1, MDBM *db2)
{
    kvpair kv;
    datum dta;
    MDBM_ITER iter;
    string msg;

    MDBM_ITER_INIT(&iter);
    kv = mdbm_first_r(db1, &iter);
    while (kv.key.dptr != NULL) {
        dta = mdbm_fetch(db2, kv.key);
        msg = string("Could not find key ") + kv.key.dptr;
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), dta.dptr != NULL);
        msg = string("Value size of key ") + kv.key.dptr + " of " + ToStr(kv.val.dsize)
              + string(" is different than fetched: ") + ToStr(dta.dsize);
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), kv.val.dsize == dta.dsize);
        msg = string("Data for key ") + kv.key.dptr + string(" is not the same in both MDBMs");
        CPPUNIT_ASSERT_MESSAGE(msg.c_str(), memcmp(kv.val.dptr, dta.dptr, kv.val.dsize) == 0);
        kv = mdbm_next_r(db1, &iter);
    }
}


MDBM *MdbmUnitTestDump::createTextMdbm(const string &prefix, string *filename, int keyCount)
{
    int flags = versionFlag | MDBM_O_CREAT | MDBM_O_RDWR;
    MDBM *mdbm = EnsureTmpMdbm(prefix, flags, 0644, sysconf(_SC_PAGESIZE), 0, filename);
    string key;
    for (int i = 0; i < keyCount; ++i) {
        datum val;
        datum ky = CreateTestValue("", (random() % (DEFAULT_PAGE_SIZE * 2)) + 1, val);
        ++val.dsize;  // store the null
        key = string("KeyPrefix") + ToStr(i + 100);
        ky.dptr = const_cast<char *> (key.c_str());
        ky.dsize = key.size() + 1;
        int ret = mdbm_store( mdbm, ky, val, MDBM_REPLACE);
        std::ostringstream oss;
        oss << "createTextMdbm: Cannot add key, key=" << key
            << ", errno= " << strerror(errno);
        CPPUNIT_ASSERT_MESSAGE(oss.str(), ret == 0);
    }
    return mdbm;
}

string
MdbmUnitTestDump::writeAllRecords(MDBM *db, bool cdbFormat)
{
    uint32_t offst = 0U, datasize = 0U, bufsize = 64U;  // allocate small buffer to test realloc
    char *buf = (char *) malloc(bufsize);
    CPPUNIT_ASSERT(buf != NULL);
    kvpair kv;
    MDBM_ITER iter;

    MDBM_ITER_INIT(&iter);
    for (kv=mdbm_first_r(db, &iter); kv.key.dptr != NULL; kv=mdbm_next_r(db, &iter)) {
        if (cdbFormat) {
            CPPUNIT_ASSERT_EQUAL(0, mdbm_cdbdump_add_record(kv, &datasize, &buf, &bufsize, offst));
        } else {
            CPPUNIT_ASSERT_EQUAL(0, mdbm_dbdump_add_record(kv, &datasize, &buf, &bufsize, offst));
        }
        offst = datasize;
    }

    const char *outfile = "/tmp/datarecs.dump";
    FILE *fp = fopen(outfile, "w");
    CPPUNIT_ASSERT(fp != NULL);
    CPPUNIT_ASSERT(fwrite(buf, datasize, 1, fp) > 0);
    int ret;
    if (cdbFormat) {
        ret = mdbm_cdbdump_trailer_and_close(fp);
    } else {
        ret = mdbm_dbdump_trailer_and_close(fp);
    }

    CPPUNIT_ASSERT_EQUAL(0, ret);
    free(buf);
    return outfile;
}

void
MdbmUnitTestDump::testImportAPI(const string &prefix, bool cdbFormat, bool binary, bool header)
{
    int pageSize1;
    MDBM *db1 = NULL;

    if (binary) {
        pageSize1 = 10240;
        db1 = GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR | versionFlag, 0666, pageSize1, 0);
        CPPUNIT_ASSERT(NULL != db1);
        InsertData(db1, DEFAULT_KEY_SIZE, DEFAULT_VAL_SIZE, DEFAULT_ENTRY_COUNT,
                   true, DEFAULT_ENTRY_COUNT * 2);
    } else {
        pageSize1 = MDBM_PAGESIZ;
        string filename;
        db1 = createTextMdbm(prefix, &filename);
    }

    string datafile;
    datafile = writeToFile(db1, cdbFormat, header);

    FILE *fp = fopen(datafile.c_str(), "r");
    CPPUNIT_ASSERT(NULL != fp);

    uint32_t linenum = 1;
    int pageSize = 0, pageCount = 0, large = 0;
    if (!cdbFormat && header) {
        CPPUNIT_ASSERT_EQUAL(0,
            mdbm_dbdump_import_header(fp, &pageSize, &pageCount, &large, &linenum));
        CPPUNIT_ASSERT_EQUAL(pageSize1, pageSize);
        CPPUNIT_ASSERT(0 < linenum);
        CPPUNIT_ASSERT(0 < pageCount);
    }

    MdbmHolder db2(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | versionFlag,
                                 0666, pageSize , 0));
    CPPUNIT_ASSERT(NULL != (MDBM *) db2);

    int ret;
    if (cdbFormat) {
        ret = mdbm_cdbdump_import(db2, fp, datafile.c_str(), MDBM_REPLACE);
    } else {
        ret = mdbm_dbdump_import(db2, fp, datafile.c_str(), MDBM_REPLACE, &linenum);
    }
    CPPUNIT_ASSERT_EQUAL(0, ret);
    fclose(fp);

    findDiffs(db1, db2);

    unlink(datafile.c_str());
    mdbm_close(db1);
}

/// MDBM V3 class

class MdbmUnitTestDumpV3 : public MdbmUnitTestDump
{
    CPPUNIT_TEST_SUITE(MdbmUnitTestDumpV3);

    CPPUNIT_TEST(initialSetup);

    CPPUNIT_TEST(test_DbDumpNeg);
    CPPUNIT_TEST(test_DbDumpBinary);
    CPPUNIT_TEST(test_DbDumpText);

    CPPUNIT_TEST(test_CDbDumpNeg);
    CPPUNIT_TEST(test_CDbDumpBinary);
    CPPUNIT_TEST(test_CDbDumpText);

    CPPUNIT_TEST(test_DbDumpRecordsText);
    CPPUNIT_TEST(test_DbDumpRecordsBinary);

    CPPUNIT_TEST(test_CDbDumpRecordsText);
    CPPUNIT_TEST(test_CDbDumpRecordsBinary);

    CPPUNIT_TEST(test_ImportDbDumpBinary);
    CPPUNIT_TEST(test_ImportDbDumpHeaderBinary);
    CPPUNIT_TEST(test_ImportCdbBinary);
    CPPUNIT_TEST(test_ImportDbDumpText);
    CPPUNIT_TEST(test_ImportDbDumpHeaderText);
    CPPUNIT_TEST(test_ImportCdbText);

    CPPUNIT_TEST(finalCleanup);

    CPPUNIT_TEST_SUITE_END();

  public:
    MdbmUnitTestDumpV3() : MdbmUnitTestDump(MDBM_CREATE_V3) { }
};

CPPUNIT_TEST_SUITE_REGISTRATION(MdbmUnitTestDumpV3);
