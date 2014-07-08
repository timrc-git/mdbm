/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#include <iostream>
#include <vector>
#include <algorithm>

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

/*
 *  bugzilla tickets:
 *  BZ ticket 5241421: V2: mdbm_set_alignment returns success for invalid input(-1)
 *  BZ ticket 5350542: V2: mdbm_set_alignment should fail after data has been set in the DB
*/
class AlignTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    AlignTestSuite(int vFlag) : TestBase(vFlag, "AlignTestSuite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    //
    void AllValidMasks();  // TC H-1
    void InvalidMasks();  // TC H-2
    void ResetValidMasks();  // TC H-H3
    void DefaultAlignPerPagesize();  // TC H-4
    void VerifyAlignAfterTrunc();  // TC H-5
    void SetAlignForNumType();  // TC H-6
    void SetAlignPerMultiDatatype();  // TC H-7
    void SetAlignErrorCases(); //TC H-8

private:
    int assertAlign(MDBM *dbh, int mask, const string &prefix, bool expectSuccess = true);
    void CheckAlign(const string& tcprefix, int mask);
    void CheckInvalidAlign(const string& tcprefix, int mask);
    void CheckDefaultAlign(const string& tcprefix, int pagesize, int numPages);

    static const int _StartMask; // for CheckDefaultAlign
    int _lastmask;  // keeps track of last mask that was set for CheckDefaultAlign
    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
    static int _ValidMasks[];
    static int _InvalidMasks[];
    static int _PageSizeSeries[];
    static int _NumPageSeries[];
};

//  0 - 8-bit alignment
//  1 - 16-bit alignment
//  3 - 32-bit alignment
//  7 - 64-bit alignment
int AlignTestSuite::_ValidMasks[] = { 0, 1, 3, 7 };
int AlignTestSuite::_InvalidMasks[] = { -1, 2, 4, 5, 6, 8 };

// using Max V2 pagesize = (64 * 1024)
int AlignTestSuite::_PageSizeSeries[] = { 128, 512, 1024, 4096, 8192, (64 * 1024) };
int AlignTestSuite::_NumPageSeries[] = { 1, 2, 3, 4, 5, 6, 7, 8 };
int AlignTestSuite::_setupCnt = 0;
const int AlignTestSuite::_StartMask = -2;

void AlignTestSuite::setUp()
{
    _lastmask = _StartMask;
    if (_setupCnt++ == 0)
        cout << endl << "DB Alignment Test Suite Beginning..." << endl << flush;
}

int AlignTestSuite::assertAlign(MDBM *dbh, int mask, const string &prefix, bool expectSuccess)
{
    int ret = mdbm_set_alignment(dbh, mask);

    stringstream alignerr;
    alignerr << prefix;
    if (expectSuccess) {
        alignerr << " Failed to set alignment to " << mask << endl;
        CPPUNIT_ASSERT_MESSAGE(alignerr.str(), (ret == 0));
    } else {
        alignerr << " Should have Failed to set alignment to " << mask << endl;
        CPPUNIT_ASSERT_MESSAGE(alignerr.str(), (ret == -1));
    }

    return ret;
}

void AlignTestSuite::CheckAlign(const string& tcprefix, int mask)
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << "CheckAlign: mask=" << mask;
    string prefix = premsg.str();

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2. Call mdbm_set_alignment: a
    mdbm_set_alignment(dbh, mask);

    // 3. Call mdbm_get_alignment: g
    int curmask = mdbm_get_alignment(dbh);

    // 4. Expected result: a == g
    stringstream ssmsg;
    ssmsg << prefix
          << "Expected mask=" << mask
          << " should equal: mdbm_get_mask=" << curmask
          << endl;

    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (mask == curmask));
}
void AlignTestSuite::CheckInvalidAlign(const string& tcprefix, int mask)
{
    stringstream premsg;
    premsg << SUITE_PREFIX() << tcprefix << "CheckInvalidAlign: mask=" << mask;
    string prefix = premsg.str();

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2. Call mdbm_get_alignment: d(default)
    int defmask = mdbm_get_alignment(dbh);

    // 3. Set errno = 0; Call mdbm_set_alignment: a
    errno = 0;
    int ret = mdbm_set_alignment(dbh, mask);
    int errnum = errno;

    // 4. Expected result: FAIL and errno is set
    stringstream ssmsg;
    ssmsg << prefix
          << "Expected mdbm_set_alignment to return=-1, but it Returned=" << ret
          << " Expected errno to be set to non-0 but errno=" << errnum
          << endl;
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (ret == -1 && errnum != 0));

    // 5. Call mdbm_get_alignment: g
    int curmask = mdbm_get_alignment(dbh);

    // 6. Expected result: g == d
    stringstream msg;
    msg << prefix
        << "Failed to get original mask. Expected mask=" << defmask
        << " but mdbm_get_alignment=" << curmask
        << endl;

    CPPUNIT_ASSERT_MESSAGE(msg.str(), (defmask == curmask));
}

void AlignTestSuite::CheckDefaultAlign(const string& tcprefix, int pagesize, int numPages)
{
    stringstream premsg;
    int presize = pagesize * numPages;
    premsg << SUITE_PREFIX() << tcprefix
           << "CheckDefaultAlign: pagesize=" << pagesize
           << " presize=" << presize;
    string prefix = premsg.str();

    // 1. Create new DB using page size = p, presize = z
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, pagesize, presize));

    // 2. Call mdbm_get_alignment: g
    int curmask = mdbm_get_alignment(dbh);

    // 3. Expected result: g == previous g returned
    if (_lastmask == _StartMask) { // 1st time thru
        _lastmask = curmask;
    } else { // subsequent masks will be checked
        stringstream msg;
        msg << prefix
            << "Default mask mismatch. Expected mask=" << _lastmask
            << " but mdbm_get_alignment=" << curmask
            << endl;

        CPPUNIT_ASSERT_MESSAGE(msg.str(), (curmask == _lastmask));
    }
}


void AlignTestSuite::AllValidMasks()   // TC H-1
{
    string tcprefix = "TC H-1: AllValidMasks: ";
    TRACE_TEST_CASE(tcprefix);

    int len = sizeof(_ValidMasks)/sizeof(int);
    for (int i=0; i<len; ++i) {
        CheckAlign(tcprefix, _ValidMasks[i]);
    }
}

void AlignTestSuite::InvalidMasks()  // TC H-2
{
    string tcprefix = "TC H-2: InvalidMasks: ";
    TRACE_TEST_CASE(tcprefix);

    int len = sizeof(_InvalidMasks)/sizeof(int);
    for (int i=0; i<len; ++i) {
        CheckInvalidAlign(tcprefix, _InvalidMasks[i]);
    }

}

void AlignTestSuite::ResetValidMasks()  // TC H-3
{
    string tcprefix = "TC H-3: ResetValidMasks: ";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // * For each valid alignment mask: a in 0, 1, 3, 7
    //       Call mdbm_set_alignment: a
    unsigned int cnt = 0;
    for (; cnt < sizeof(_ValidMasks) / sizeof(int); ++cnt) {
        mdbm_set_alignment(dbh, _ValidMasks[cnt]);
        int mask = mdbm_get_alignment(dbh);

        // verify mask we get is the mask we set
        //
        stringstream sgmsg;
        sgmsg << prefix
              << "After setting alignment Expected " << _ValidMasks[cnt]
              << " but Got " << mask
              << endl;

        CPPUNIT_ASSERT_MESSAGE(sgmsg.str(), (mask == _ValidMasks[cnt]));
    }

    // 1. Call mdbm_get_alignment: g
    int curmask = mdbm_get_alignment(dbh);

    // 2. Expected result: g == 7
    int lastMask = _ValidMasks[--cnt];
    stringstream msg;
    msg << prefix
        << "Failed to get final mask. Expected mask=" << lastMask
        << " but mdbm_get_alignment=" << curmask
        << endl;

    CPPUNIT_ASSERT_MESSAGE(msg.str(), (curmask == 7));

    // 3. store data: s = long long
    const char * keyname = "llong13";
    long long storenum = 13;
    int ret = store(dbh, keyname, storenum);


    // 4. fetch data: f = long long
    long long fetnum = -1;
    ret = fetch(dbh, keyname, fetnum);

    // 5. Expected result: f == s
    stringstream sstrm;
    sstrm << prefix
          << "Using mask=" << curmask
          << ": Expected stored val=" << storenum
          << " equal to fetched val=" << fetnum
          << endl;
    CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (ret != -1 && fetnum == storenum));
}

void AlignTestSuite::DefaultAlignPerPagesize()  // TC H-4
{
    string tcprefix = "TC H-4: DefaultAlignPerPagesize: ";
    TRACE_TEST_CASE(tcprefix);

    // Verify Default alignment of new DB's for different page sizes is consistent
    int plen = sizeof(_PageSizeSeries)/sizeof(int);
    int nlen = sizeof(_NumPageSeries)/sizeof(int);
    for (int p=0; p<plen; ++p) {
      for (int n=0; n<nlen; ++n) {
          CheckDefaultAlign(tcprefix, _PageSizeSeries[p], _NumPageSeries[n]);
      }
    }

}

void AlignTestSuite::VerifyAlignAfterTrunc()  // TC H-5
{
    string tcprefix = "TC H-5: VerifyAlignAfterTrunc: ";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2. Call mdbm_set_alignment (non default): a = 7
    int align = 7;
    assertAlign(dbh, align, prefix);

    // 3. Call mdbm_store data
    const char * key = "tc5ll";
    long long num = 17;
    store(dbh, key, num, prefix.c_str());

    // 4. Call mdbm_truncate
    // docs say this will lose the existing config info
    mdbm_truncate(dbh);

    // 5. Call mdbm_get_alignment: g
    int curmask = mdbm_get_alignment(dbh);

    // 6. Expected result: g != a
    stringstream sstrm;
    sstrm << prefix;
    sstrm << "Expected non-default alignment=" << align
          << " to be reset to default after truncation" << endl;
    CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (curmask != align));
}

void AlignTestSuite::SetAlignForNumType()  // TC H-6
{
    string tcprefix = "TC H-6: SetAlignForNumType: ";
    TRACE_TEST_CASE(tcprefix);
    string prefix = SUITE_PREFIX() + tcprefix;

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2.  Call mdbm_set_alignment: a = 3
    int curmask = 3;  // this means 32 bit alignment
    assertAlign(dbh, curmask, prefix);

    // 3. store series of int's k in 3, 5, 7, 9, 11
    //      * Call mdbm_store: k
    // NOTE: using floats rather than int
    //
    float numSeries[]   = {  3.11, 5.12,   7.13,  9.14,  11.15 };
    const char * keys[] = { "311", "512", "713", "914", "1115" };
    for (unsigned int cnt = 0; cnt < sizeof(numSeries) / sizeof(int); ++cnt) {
        store(dbh, keys[cnt], numSeries[cnt], prefix.c_str());
    }

    // 1. fetch the int's and use directly in expressions
    //      * Call mdbm_fetch: f
    //      * cast data pointer to (int ) in an expression, ex: (int)datum.dptr
    //      * Expected result: success
    // NOTE: using floats rather than int
    for (unsigned int cnt = 0; cnt < sizeof(numSeries) / sizeof(int); ++cnt) {
        float num;
        int ret = fetch(dbh, keys[cnt], num);

        stringstream sstrm;
        sstrm << prefix
              << "Using mask=" << curmask
              << ": Expected stored val=" << numSeries[cnt]
              << " equal to fetched val=" << num
              << endl;

        CPPUNIT_ASSERT_MESSAGE(sstrm.str(), (ret != -1 && num == numSeries[cnt]));
    }
}


//  In this test we will fetch data using the alignment it was stored in.
//  For storing/fetching strings, we set alignment to 0.
//  For storing/fetching int's, we set alignment to 2.
void AlignTestSuite::SetAlignPerMultiDatatype()  // TC H-7
{
    string tcprefix = "TC H-7: SetAlignPerMultiDatatype: ";
    string prefix = SUITE_PREFIX() + tcprefix;

    // Mix and Match: Per data type alignment: each data type uses individual alignment

    // 1. Call mdbm_open; create DB with defaults
    MdbmHolder dbh(EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag));

    // 2. Call mdbm_set_alignment: 0
    int curmask = 0;  // byte alignment
    int ret = assertAlign(dbh, curmask, prefix);
    (void)ret; // unused, make compiler happy

    // 3. Call mdbm_store: string, key="str1"
    const char * keystr1  = "tc7str1";
    const char * str1 = "str1 on duty";
    store(dbh, keystr1, str1, prefix.c_str());

    // 4. Call mdbm_set_alignment: 2
    curmask = 7;  // 64 bit alignment
    ret = assertAlign(dbh, curmask, prefix, false);

    // 5. Call mdbm_store: integer, key="int1"
    const char * keynum1  = "tc7num1";
    const long long num1 = 101;
    store(dbh, keynum1, num1, prefix.c_str());

    // 6. Call mdbm_store: integer, key="int2"
    const char * keynum2  = "tc7num2";
    const long long num2 = 202;
    store(dbh, keynum2, num2, prefix.c_str());

    // 7. Call mdbm_set_alignment: 0
    curmask = 0;  // byte alignment
    ret = assertAlign(dbh, curmask, prefix, false);

    // 8. Call mdbm_store: string, key="str2"
    const char * keystr2  = "tc7str2";
    const char * str2 = "str2 on duty";
    store(dbh, keystr2, str2, prefix.c_str());

    // 1. Call mdbm_fetch: string, key="str1"
    // 2. Expected result: FAIL
    char * str = 0;
    ret = fetch(dbh, keystr1, str, prefix.c_str());


    // 3. Call mdbm_fetch: string, key="str2"
    // 4. Expected result: FAIL
    str = 0;
    ret = fetch(dbh, keystr2, str, prefix.c_str());


    // 5. Call mdbm_set_alignment: 2
    curmask = 7;  // 64 bit alignment
    ret = assertAlign(dbh, curmask, prefix, false);

    // 6. Call mdbm_fetch: int, key="int1"
    // 7. Expected result: FAIL
    long long num = -1;
    ret = fetch(dbh, keynum1, num, prefix.c_str());


    // 8. Call mdbm_fetch: int, key="int2"
    // 9. Expected result: FAIL
    num = -1;
    ret = fetch(dbh, keynum2, num, prefix.c_str());
}

// Error cases in mdbm_set_alignment()
void AlignTestSuite::SetAlignErrorCases()  // TC H-8
{
    string tcprefix = "TC H-8: SetAlignErrorCases: ";
    string prefix = SUITE_PREFIX() + tcprefix;
    int mask = 0;
    string fname;

    MdbmHolder mdbm = EnsureTmpMdbm(prefix, MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag, 0644, DEFAULT_PAGE_SIZE, 0, &fname);
    MdbmHolder mdbmnew = mdbm_open(fname.c_str(), MDBM_O_RDONLY | versionFlag, 0444, 0, 0);
    CPPUNIT_ASSERT((MDBM *) mdbm != NULL);

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_set_alignment(mdbmnew, mask));
}

class AlignTestSuiteV3 : public AlignTestSuite
{
    CPPUNIT_TEST_SUITE(AlignTestSuiteV3);
    CPPUNIT_TEST(AllValidMasks);  // TC H-1
    CPPUNIT_TEST(InvalidMasks);  // TC H-2
    CPPUNIT_TEST(ResetValidMasks);  // TC H-H3
    CPPUNIT_TEST(DefaultAlignPerPagesize);  // TC H-4
    CPPUNIT_TEST(VerifyAlignAfterTrunc);  // TC H-5
    CPPUNIT_TEST(SetAlignForNumType);  // TC H-6
    CPPUNIT_TEST(SetAlignPerMultiDatatype);  // TC H-7
    CPPUNIT_TEST(SetAlignErrorCases);  // TC H-8
    CPPUNIT_TEST_SUITE_END();

public:
    AlignTestSuiteV3() : AlignTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(AlignTestSuiteV3);


