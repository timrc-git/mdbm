/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// Unit Tests for mdbm_limit_size mdbm_limit_size_new mdbm_limit_size_v3

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

#include <iostream>
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
//#include "configstoryutil.hh"
#include "TestBase.hh"

using namespace std;

typedef int (*shaker_funcp_t)(struct mdbm *, datum, datum, void *);

/*
    bugzilla tickets:
    BZ ticket 5262467: V2: mdbm_limit_size neither reports a warning nore returns error upon num pages=UINT_MAX
    BZ ticket 5354822: v2: mdbm_limit_size: no effect unless presized during the mdbm_open
    BZ ticket 5354923: v2: mdbm_limit_size: returns success upon invalid number of pages

*/

class LimitSizeTestSuite : public CppUnit::TestFixture, public TestBase
{
public:
    LimitSizeTestSuite(int vFlag) : TestBase(vFlag, "LimitSizeTestSuite") {}
    void setUp();
    void tearDown() { }

    // unit tests in this suite
    //
    void ResetNumPagesBiggerV3_lsv3();  // TC D-1
    void PremadeResetNumPagesBiggerV3_lsv3();  // TC D-2
    void ResetNumPagesSmallerV3_lsv3();  // TC D-3
    void PremadeResetNumPagesSmallerV3_lsv3();  // TC D-4
    void MaxNumPagesUIntV3_lsv3();  // TC D-5
    void MaxNumPagesV3_lsv3();  // TC D-6
    void ChangeShakeNeedNfreeNV3_lsv3();  // TC D-7
    void NoShakeNeedNV3_lsv3();  // TC D-8
    void ShakeNeedNfreeLessNV3_lsv3();  // TC D-9
    void ShakeNeedNfreeTwiceV3_lsv3();  // TC D-10
    void ShakeNeedNfreeThriceV3_lsv3();  // TC D-11

    void TestLegacyFail();

    void PreSplitLimitSize();  // TC D-12

private:

    // for limit size function objects
    struct LimitSizeFO
    {
        virtual ~LimitSizeFO() { }
        LimitSizeFO() {}
        virtual int operator() (MDBM *dbh, mdbm_ubig_t numPages) = 0;
    };
    struct LimitSizeV3FO : public LimitSizeFO
    {
        LimitSizeV3FO(mdbm_shake_func_v3 shaker, const string& shakev3data="") :
             _shakerv3(shaker), _shakedata(shakev3data) {}

        int operator() (MDBM *dbh, mdbm_ubig_t numPages) {
            return mdbm_limit_size_v3(dbh, numPages, _shakerv3, (void*)&_shakedata);
        }

        mdbm_shake_func_v3 _shakerv3;
        const string  _shakedata;
    };

    void resetNumPagesBigger(LimitSizeFO & lsfo, const string& prefix);
    void resetNumPagesSmaller(LimitSizeFO & lsfo, const string& tcprefix);
    void premadeResetNumPagesSmaller(LimitSizeFO & lsfo, const string& tcprefix);
    void maxNumPagesUInt(LimitSizeFO & lsfo, const string& tcprefix);
    void maxNumPages(LimitSizeFO & lsfo, const string& tcprefix);
    void premadeResetNumPagesBigger(LimitSizeFO & lsfo, const string& tcprefix);
    void changeShakeNeedNfree(LimitSizeFO & lsfo1, LimitSizeFO & lsfo2, const string& tcprefix);
    void noShakeNeedN(LimitSizeFO & lsfo, const string& tcprefix);
    void shakeNeedNfreeLess(LimitSizeFO & lsfo, const string& tcprefix);
    void shakeNeedNfreeTwice(LimitSizeFO & lsfo, const string& tcprefix);
    void shakeNeedNfreeThrice(LimitSizeFO & lsfo, const string& tcprefix);

    static int _setupCnt; // CPPUNIT creates instance per CPPUNIT_TEST
};


int LimitSizeTestSuite::_setupCnt = 0;

// mdbm_limit_size shake functions
int noOpShaker_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 *);
int shakePass1_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 *);
int shakePass2_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 *);
int shakePass3_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 *);


void LimitSizeTestSuite::setUp()
{
    //_cfgStory = new ConfigStoryUtil("setlimtestv2", "setlimtestv3", "Set Limit Size Test Suite: ");

    if (_setupCnt++ == 0) {
        cout << endl << "Set Limit Size Test Suite Beginning..." << endl << flush;
    }
}


void LimitSizeTestSuite::resetNumPagesBigger(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-1
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    // Call mdbm_open; Create new DB using page size = p
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags));

    // 2. 1st call to mdbm_limit_size uses number of pages = n; 2nd call uses number of pages > n
    int numpages = 2;
    int ret = (lsfo)(dbh, numpages);
    stringstream lsfoss1;
    lsfoss1 << prefix << " set limit Numberpages=" << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss1.str(), (ret == 0));

    numpages = 5;
    ret = (lsfo)(dbh, numpages);
    stringstream lsfoss2;
    lsfoss2 << prefix << " set limit Numberpages=" << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss2.str(), (ret == 0));

    // 3. Expected result: return 0 = success

    // 4. Call mdbm_get_page_size, mdbm_get_limit_size to calculate the number of pages (size limit of DB / page size)
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t limsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limsize / pgsize;

    // 5. Expected result: number pages set == calculated number pages
    stringstream expectss;
    expectss << prefix
             << "Expected Calculated number of pages=" << calcnumpages
             << " to be == Set number of pages=" << numpages
             << endl;
    CPPUNIT_ASSERT_MESSAGE(expectss.str(), (calcnumpages >= numpages));
}

void LimitSizeTestSuite::ResetNumPagesBiggerV3_lsv3()  // TC D-1
{
    // Reset number of pages to bigger value. Perform using newly created DB.
    //
    string tcprefix = "TC D-1: ResetNumPagesBigger_lsv3: mdbm_limit_size_v3: ";
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    resetNumPagesBigger(lsfo, tcprefix);
}

void LimitSizeTestSuite::premadeResetNumPagesBigger(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-2
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));
    CPPUNIT_ASSERT(NULL != (MDBM*)dbh);

    // Call mdbm_get_page_size, mdbm_get_limit_size(or mdbm_get_size if get_limit_size returns 0) to calculate the number of pages (size limit of DB / page size)
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t limsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limsize / pgsize;
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;

    // Call mdbm_limit_size specifying the number of pages > calculated number of pages
    int numpages = premadeNumPages + 1;
    int ret = (lsfo)(dbh, numpages);
    stringstream lsfoss1;
    lsfoss1 << prefix << " Bigger limit from=" << premadeNumPages
            << " to Numberpages=" << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss1.str(), (ret == 0));

    // Expected result: newly calculated number pages set == calculated number pages
    stringstream expectss;
    expectss << prefix
             << "Expected Premade limit Number of pages=" << premadeNumPages
             << " and the calculated Number of pages=" << calcnumpages
             << " to be <= new Set limit Number of pages=" << numpages << endl;
    CPPUNIT_ASSERT_MESSAGE(expectss.str(), (premadeNumPages < numpages && calcnumpages <= numpages));
}
void LimitSizeTestSuite::PremadeResetNumPagesBiggerV3_lsv3()  // TC D-2
{
    string tcprefix = "TC D-2: PremadeResetNumPagesBiggerV3_lsv3: mdbm_limit_size_v3: ";
    // Reset number of pages to bigger value. Use premade DB.
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    premadeResetNumPagesBigger(lsfo, tcprefix);
}

void LimitSizeTestSuite::resetNumPagesSmaller(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-3
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags, 0644, 512));

    // 2. 1st call to mdbm_limit_size uses number pages = n; 2nd call uses number of pages < n
    int numpagesHI = 5;
    int ret = (lsfo)(dbh, numpagesHI);
    stringstream lsfoss1;
    lsfoss1 << prefix
            << "Set limit Numberpages=" << numpagesHI
            << ", ret=" << ret
            << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss1.str(), (ret == 0));

    // fill the DB with data before resetting smaller
    int numpagesLO = 3;
    int numrecs    = FillDb(dbh);
    ret = (lsfo)(dbh, numpagesLO);
    stringstream lsfoss2;
    lsfoss2 << prefix
            << " Reset limit from Numberpages=" << numpagesHI
            << " to smaller limit=" << numpagesLO
            << " Number of records inserted=" << numrecs
            << ", ret=" << ret
            << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss2.str(), (ret == -1));

    // 3. Expected result: return 0 = success

    // 4. Call mdbm_get_page_size, mdbm_get_limit_size to calculate the number of pages (size limit of DB / page size)
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t limsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limsize / pgsize;

    // 5. Expected result: number pages set == calculated number pages
    stringstream expectss;
    expectss << prefix
             << "Expected Calculated number of pages=" << calcnumpages
             << " to be == Set number of pages=" << numpagesHI
             << endl;
    CPPUNIT_ASSERT_MESSAGE(expectss.str(), (calcnumpages >= numpagesHI));

}

void LimitSizeTestSuite::ResetNumPagesSmallerV3_lsv3()  // TC D-3
{
    string tcprefix = "TC D-3: ResetNumPagesSmallerV3_lsv3: mdbm_limit_size_v3: ";
    // Create new DB. Call twice with different number of pages.
    // Reset number of pages to smaller value
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    resetNumPagesSmaller(lsfo, tcprefix);
}

void LimitSizeTestSuite::premadeResetNumPagesSmaller(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-4
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));

    // 2. Call mdbm_get_page_size, mdbm_get_limit_size(or mdbm_get_size if get_limit_size returns 0) to calculate the number of pages (size limit of DB / page size)
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t limsize = mdbm_get_limit_size(dbh);
    int calcnumpages = limsize / pgsize;
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;

    // 3. Call mdbm_limit_size specifying the number of pages < calculated number of pages
    int numpages = premadeNumPages - 1;
    int ret = (lsfo)(dbh, numpages);
    stringstream lsfoss1;
    lsfoss1 << prefix
            << " Expected Failure when Lowering limit from=" << premadeNumPages
            << " to Numberpages=" << numpages << " but retcode=" << ret
            << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss1.str(), (ret == -1));

    // 4. Expected result: newly calculated number pages == calculated number pages
    stringstream expectss;
    expectss << prefix
             << "Expected Premade limit Number of pages=" << premadeNumPages
             << " and the calculated Number of pages=" << calcnumpages
             << " to be > new Set limit Number of pages=" << numpages
             << endl;
    CPPUNIT_ASSERT_MESSAGE(expectss.str(), (premadeNumPages > numpages && calcnumpages > numpages));
}

void LimitSizeTestSuite::PremadeResetNumPagesSmallerV3_lsv3()  // TC D-4
{
    string tcprefix = "TC D-4: PremadeResetNumPagesSmallerV3_lsv3: mdbm_limit_size: ";
    // Use premade DB. Call twice with different number of pages.
    //  Reset number of pages to smaller value
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    premadeResetNumPagesSmaller(lsfo, tcprefix);
}

void LimitSizeTestSuite::maxNumPagesUInt(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-5
{
    // Specify maximum number of pages possible.
    // Test maximum limits by using largest unsigned int allowed (UINT_MAX).
    // System dependent: 32 bit allows 2 giB, 64 bit allows 128 TB
    // mdbm_limit_size uses a mdbm_ubig_t for max number of pages.
    // The mdbm_ubig_t is defined to be uint32_t; thus we will use UINT_MAX.

    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    // Call mdbm_open; Create new DB using page size = p
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags));

    // Call mdbm_get_limit_size, mdbm_get_size, mdbm_get_page_size
    int      pgsize  = mdbm_get_page_size(dbh);
    uint64_t limsize = mdbm_get_limit_size(dbh);
    mdbm_ubig_t calcnumpages = limsize / pgsize;

    // Call mdbm_limit_size with number of pages = UINT_MAX
    // Expect failure and a warning message output to terminal window
    stringstream lsfoss;
    lsfoss << prefix
           << "Calculated Number of pages=" << calcnumpages
           << " from getting Current limit size=" << limsize
           << ": Set Invalid MAX limit Numberpages=" << UINT_MAX
           << ": Expect this limit should fail but did not."
           << endl;
    string limsizemsg = lsfoss.str();
    lsfoss << endl;
    int ret = (lsfo)(dbh, UINT_MAX);

    CPPUNIT_ASSERT_MESSAGE(lsfoss.str(), (ret == -1));

    // Call mdbm_get_limit_size to see what the max number pages was changed to and calculate the max bytes by max_num_pages * page_size
    uint64_t newlimsize = mdbm_get_limit_size(dbh);
    stringstream lsfoss2;
    lsfoss2 << limsizemsg
            << " Expected limit size shouldnt change, new limit size=" << newlimsize
            << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss2.str(), (newlimsize == limsize));

    // Call getrlimit specifying RLIMIT_AS(max size of virtual memory address space) and call getrlimit with RLIMIT_FSIZE(max size of file that process may create)

    // Expected result: max_num_pages * page_size < RLIMIT_AS since mmap should
    // fail with ENOMEM upon exceeding that limit;
    // also max_num_pages * page_size < RLIMIT_FSIZE since attempts to extend a
    // file beyond this limit result in delivery of a SIGXFSZ signal
}

//  NOTES:
//  MDBM_MAX_SHIFT and is calced for arch: 64 bit and 32 bit giving the max'es of: 63 and 31
void LimitSizeTestSuite::MaxNumPagesUIntV3_lsv3()  // TC D-5
{
    // Specify maximum number of pages possible.
    // Test maximum limits by using largest unsigned int allowed (UINT_MAX).
    // System dependent: 32 bit allows 2 giB, 64 bit allows 128 TB
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    maxNumPagesUInt(lsfo, "TC D-5: MaxNumPagesUIntV3_lsv3: mdbm_limit_size_v3: ");
}

void LimitSizeTestSuite::maxNumPages(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-6
{
    // Specify maximum number of pages allowed.
    // Test maximum limits by using largest MDBM allowed.
    // v2: 2^31 max number of pages
    // v3: 2^24 max number of pages
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open and create DB with typical defaults
    // Call mdbm_open; Create new DB using page size = p
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openFlags));

    // Call mdbm_get_limit_size, mdbm_get_size, mdbm_get_page_size
    int pgsize = mdbm_get_page_size(dbh);

    uint32_t maxPages = GetMaxNumPages(versionFlag);
    uint64_t maxDBsize = GetMaxDBsize(versionFlag);
    // Call mdbm_limit_size with maximum number of pages < MDBM_NUMPAGES_MAX
    mdbm_ubig_t setNumPages = maxPages;
    int ret = (lsfo)(dbh, setNumPages);
    stringstream lsfoss;
    lsfoss << prefix << "Maximum Number of pages=" << maxPages
           << " Page size=" << pgsize << " Max DB size used=" << maxDBsize
           << ": Set Calculated Valid MAX limit Number pages=" << setNumPages
           << ": Expected this limit to succeed." << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss.str(), (ret == 0));

    // Call mdbm_get_limit_size to see what the max number pages was changed to and calculate the max bytes by max_num_pages * page_size
    uint64_t getLimSize = mdbm_get_limit_size(dbh);
    uint64_t calcMaxDBsize = uint64_t(setNumPages) * uint64_t(pgsize);

    stringstream newlimss;
    newlimss << prefix
             << " Page Size=" << pgsize
             << " Number Pages=" << setNumPages
             << ": Expect mdbm_get_limit_size(" << getLimSize
             << ") >= (NumberPages(" << setNumPages << ") * PageSize(" << pgsize
             << "))=" << calcMaxDBsize << endl;

    CPPUNIT_ASSERT_MESSAGE(newlimss.str(), (getLimSize >= calcMaxDBsize));

    // Call getrlimit specifying RLIMIT_AS(max size of virtual memory address space) and call getrlimit with RLIMIT_FSIZE(max size of file that process may create)
    // Expected result: max_num_pages * page_size < RLIMIT_AS since mmap should fail with ENOMEM upon exceeding that limit; also max_num_pages * page_size < RLIMIT_FSIZE since attempts to extend a file beyond this limit result in delivery of a SIGXFSZ signal
}

void LimitSizeTestSuite::MaxNumPagesV3_lsv3()  // TC D-6
{
#if 0
    // Specify maximum number of pages allowed. Test maximum limits by using MAXimum number of pages allowed (MDBM_NUMPAGES_MAX). System dependent: 32 bit allows 2 giB, 64 bit allows 128 TB
// FIX BZ ticket 5355294
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    maxNumPages(lsfo, MDBM_CREATE_V3, "TC D-6: MaxNumPagesV3_lsv3: mdbm_limit_size_v3: ");
#endif
}

void LimitSizeTestSuite::changeShakeNeedNfree(LimitSizeFO & lsfo1, LimitSizeFO & lsfo2, const string& tcprefix)  // TC D-7
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));

    // 1st call to mdbm_limit_size use no-op shake; 2nd call use shake that will return 1
    // Set shake function(shake_no_op): always returns 0
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;
    int ret = (lsfo1)(dbh, premadeNumPages);
    stringstream lsfoss1;
    lsfoss1 << prefix
            << " Unchanged limit used=" << premadeNumPages
            << " with no-op shake function"
            << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss1.str(), (ret == 0));

    // Call mdbm_store(MDBM_INSERT) with record size same as DB was setup with
    string keystr = GetDefaultKey(0);
    keystr.replace(0, 1, 1, '7' );
    //string pmval = GetDefaultValue(0);
    string valstr(DEFAULT_VAL_SIZE, '7');
    //ret = store(dbh, keystr.c_str(), valstr.c_str());
    ret = store(dbh, keystr, valstr);

    // Expected partial result: mdbm_store returns failure, errno = db_errno = ENOMEM
    stringstream stss;
    stss << prefix
         << " Store data(" << valstr.c_str()
         << ") should FAIL because no space freed: ";
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == -1));

    string foundval;
    ret = fetch(dbh, keystr, foundval);

    stringstream ffss;
    ffss << prefix
         << "Expected to FAIL to fetch the stored string(" << valstr.c_str()
         << ") of length=" << ret
         << " using key=" << keystr.c_str()
         << " from the DB."
         << endl;

    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret == -1) );

    // Set new shake function(shake_pass or shake_passv3) which is implemented to return 1 when finds single record with key="pass1"
    ret = (lsfo2)(dbh, premadeNumPages);
    stringstream lsfoss2;
    lsfoss2 << prefix
            << " Unchanged limit used=" << premadeNumPages
            << " with pass1 shake function" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss2.str(), (ret == 0));

    // Call mdbm_store(MDBM_INSERT) with value size=n(same size as value for "pass1" record value) and is a unique value
    ret = store(dbh, keystr, valstr);

    // Expected result: shake function frees space to allow storage of key-value pair, return 0 = success
    stringstream stss2;
    stss2 << prefix
          << " Store data(" << valstr.c_str()
          << ") of size=" << valstr.size()
          << " should Succeed because enough space freed: ";
    CPPUNIT_ASSERT_MESSAGE(stss2.str(), (ret == 0));

    // Call mdbm_fetch(use unique key same as was used in mdbm_store) and compare value to inserted record value
    foundval = "";
    ret = fetch(dbh, keystr, foundval);

    stringstream fss;
    fss << prefix
        << "Expected to fetch the stored string(" << valstr.c_str()
        << ") of length=" << ret
        << " using key=" << keystr.c_str()
        << " from the DB." << endl;

    CPPUNIT_ASSERT_MESSAGE(fss.str(), (ret == int(valstr.size()) && memcmp(valstr.data(), foundval.data(), ret) == 0));

    // Expected result: values are the same
}

void LimitSizeTestSuite::ChangeShakeNeedNfreeNV3_lsv3()  // TC D-7
{
    // Call twice with same number pages but different shake functions. 2nd call frees enough space using 1 key-value pair for store of value size=n. Border condition case: Need space=n; free space size=n
    LimitSizeV3FO lsfo1(noOpShaker_v3);
    LimitSizeV3FO lsfo2(shakePass1_v3);
    changeShakeNeedNfree(lsfo1, lsfo2, "TC D-7: ChangeShakeNeedNfreeNV3_lsv3: mdbm_limit_size_v3: ");
}

void LimitSizeTestSuite::noShakeNeedN(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-8
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));

    //  Call mdbm_store(MDBM_INSERT) with record size=n(same size as dummy fill values), unique key and value
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;
    int ret = (lsfo)(dbh, premadeNumPages);
    stringstream lsfoss1;
    lsfoss1 << prefix
            << " Unchanged limit used=" << premadeNumPages
            << " with NO shake function provided" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss1.str(), (ret == 0));

    string dummyval = GetDefaultValue(DEFAULT_ENTRY_COUNT+1);
    string keystr = GetDefaultKey(DEFAULT_ENTRY_COUNT+1);
    keystr += "D";
    ret = store(dbh, keystr, dummyval);

    // Expected result: return -1 = failure, errno = db_errno = ENOMEM
    stringstream stss;
    stss << prefix
         << " Store data(" << dummyval
         << ") using key(" << keystr.c_str()
         << ") should FAIL because there should be no space"
         << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == -1));

    // Call mdbm_fetch(use unique key same as was used in mdbm_store)
    char* foundval = 0;
    ret = fetch(dbh, keystr, foundval);

    // Expected result: no value returned
    stringstream ffss;
    ffss << prefix
         << "Expected to FAIL to fetch the stored string(" << dummyval
         << ") of length=" << dummyval.size()
         << " using key=" << keystr.c_str()
         << " from the DB." << endl;

    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret == -1) );
}
void LimitSizeTestSuite::NoShakeNeedNV3_lsv3()  // TC D-8
{
    // Need space=n; No shake function provided
    //LimitSizeV3FO lsfo(0, 0);
    LimitSizeV3FO lsfo(0);
    noShakeNeedN(lsfo, "TC D-8: NoShakeNeedNV3_lsv3: mdbm_limit_size_v3: ");
}

void LimitSizeTestSuite::shakeNeedNfreeLess(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-9
{
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));

    //  Shake function(shake_pass): only returns 1 on record size < n (key="pass1")
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;
    int ret = (lsfo)(dbh, premadeNumPages);
    stringstream lsfoss;
    lsfoss << prefix
           << " Unchanged limit used=" << premadeNumPages
           << " with pass1 shake function" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss.str(), (ret == 0));

    // Call mdbm_store(MDBM_INSERT) with record size=n, unique value
    string keystr = GetDefaultKey(0);
    keystr.replace(0, 1, 1, '9' );
    string valstr = GetDefaultValue(0);
    valstr += " and a whole lot more for test D-9 to return failure";
    ret = store(dbh, keystr, valstr);

    // Expected result: mdbm_store returns -1 = failure, errno = db_errno = ENOMEM
    stringstream stss;
    stss << prefix
         << " Store data(" << valstr.c_str()
         << ") should FAIL because not enough space freed: ";
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == -1));

    // Call mdbm_fetch(use unique key same as was used in mdbm_store)
    char * foundval = 0;
    ret = fetch(dbh, keystr, foundval);

    // Expected result: no value returned
    stringstream ffss;
    ffss << prefix
         << "Expected to FAIL to fetch the stored string(" << valstr.c_str()
         << ") but got return value length=" << ret
         << " using key=" << keystr.c_str()
         << " from the DB." << endl;

    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret == -1) );
}
void LimitSizeTestSuite::ShakeNeedNfreeLessNV3_lsv3()  // TC D-9
{
    // Need space=n; shake function frees space < n;
    LimitSizeV3FO lsfo(shakePass1_v3);
    shakeNeedNfreeLess(lsfo, "TC D-9: ShakeNeedNfreeLessNV3_lsv3: mdbm_limit_size_v3: ");
}

void LimitSizeTestSuite::shakeNeedNfreeTwice(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-10
{
    // Shake function(shake_pass): will require 2 "passes" to delete enough space
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));

    // Shake function(shake_pass): will require 2 "passes" to delete enough space
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;
    int ret = (lsfo)(dbh, premadeNumPages);
    stringstream lsfoss;
    lsfoss << prefix
           << " Unchanged limit used=" << premadeNumPages
           << " with pass1 shake function" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss.str(), (ret == 0));

    // Call mdbm_store(MDBM_INSERT) with record size=n, unique value
    string keystr = GetDefaultKey(0);
    keystr.replace(0, 1, 1, '0');
    string valstr = GetDefaultValue(0);
    valstr += GetDefaultValue(1);
    ret = store(dbh, keystr, valstr);

    // Expected result: mdbm_store returns 0 = success
    stringstream stss;
    stss << prefix
         << " Store data(" << valstr.c_str()
         << ") of size=" << valstr.size()
         << " should Succeed because enough space freed: ";
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));

    // Call mdbm_fetch(use unique key same as was used in mdbm_store) and compare value to inserted record value
    string foundval;
    ret = fetch(dbh, keystr, foundval);

    // Expected result: values are the same
    stringstream ffss;
    ffss << prefix
         << "Expected to Succeed to fetch the stored string(" << valstr.c_str()
         << ") but FAILED using key=" << keystr.c_str()
         << " from the DB." << endl;

    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret == int(valstr.size()) && memcmp(valstr.data(), foundval.data(), ret) == 0));
}
void LimitSizeTestSuite::ShakeNeedNfreeTwiceV3_lsv3()  // TC D-10
{
    // Free enough space using 2 key-value pairs for store of size=n. Need space=n; free space size = x where x < n on key-value pair 1; free space size = y where y <=n on key-value pair 2;
    LimitSizeV3FO lsfo(shakePass2_v3);
    shakeNeedNfreeTwice(lsfo, "TC D-10: ShakeNeedNfreeTwiceV3_lsv3: mdbm_limit_size_v3: ");
}

// Free enough space using 3 key-value pairs for store of size=n. Need space=n; free space size < n with key-value pairs 1 and 2; free space size >= n on key-value pair 3; where x + y < n and x + y + z >= n; all other recs of size < n
void LimitSizeTestSuite::shakeNeedNfreeThrice(LimitSizeFO & lsfo, const string& tcprefix)  // TC D-11
{
    // Shake function(shake_pass): will require 2 "passes" to delete enough space
    string prefix = SUITE_PREFIX() + tcprefix;
    TRACE_TEST_CASE(tcprefix);

    // 1. Setup DB: Open the premade DB
    MdbmHolder dbh(GetTmpFilledMdbm(prefix, MDBM_O_RDWR|versionFlag));

    // Shake function(shake_pass): will require 3 passes to delete enough space
    int premadeNumPages =  DEFAULT_DB_SIZE / DEFAULT_PAGE_SIZE;
    int ret = (lsfo)(dbh, premadeNumPages);
    stringstream lsfoss;
    lsfoss << prefix
           << " Unchanged limit used=" << premadeNumPages
           << " with pass1 shake function" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsfoss.str(), (ret == 0));

    // Call mdbm_store(MDBM_INSERT) with record size=n, unique value
    string keystr = GetDefaultKey(0);
    keystr.replace(0, 1, 1, '0' );
    string valstr = GetDefaultValue(0);
    valstr += GetDefaultValue(1);
    valstr += GetDefaultValue(2);
    ret = store(dbh, keystr, valstr);

    // Expected result: mdbm_store returns 0 = success
    stringstream stss;
    stss << prefix
         << " Store data(" << valstr.c_str()
         << ") of size=" << valstr.size()
         << " should Succeed because enough space freed: ";
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));

    // Call mdbm_fetch(use unique key same as was used in mdbm_store) and compare value to inserted record value
    string foundval;
    ret = fetch(dbh, keystr, foundval);

    // Expected result: values are the same
    stringstream ffss;
    ffss << prefix
         << "Expected to Succeed to fetch the stored string(" << valstr.c_str()
         << ") but FAILED using key=" << keystr.c_str()
         << " from the DB." << endl;

    CPPUNIT_ASSERT_MESSAGE(ffss.str(), (ret == int(valstr.size()) && memcmp(valstr.data(), foundval.data(), ret) == 0));
}
void LimitSizeTestSuite::ShakeNeedNfreeThriceV3_lsv3()  // TC D-11
{
    // user-data pointer passed to mdbm_limit_size
    LimitSizeV3FO lsfo(shakePass3_v3, GetDefaultKey(2));
    shakeNeedNfreeThrice(lsfo, "TC D-10: ShakeNeedNfreeThriceV3_lsv3: mdbm_limit_size_new: ");
}

void LimitSizeTestSuite::PreSplitLimitSize()  // TC D-12
{
    int flags = versionFlag|MDBM_O_RDWR|MDBM_O_CREAT|MDBM_O_TRUNC;
    int pagesz = 4096;
    MdbmHolder dbh(GetTmpMdbm(flags, 0644, pagesz));

    stringstream prefix;
    prefix << SUITE_PREFIX() << "TC E-1: Limit size and Pre split: DB version=" << versionFlag << " Page size=" << pagesz << ": ";

    // limit size
    mdbm_ubig_t numpages = 1000;
    int ret;
    ret = mdbm_limit_size_v3(dbh, numpages, NULL, NULL);
    stringstream lsss;
    lsss << prefix.str() << " Set limit size=" << numpages << " pages" << endl;
    CPPUNIT_ASSERT_MESSAGE(lsss.str(), (ret == 0));

    // pre split
    ret = mdbm_pre_split(dbh, numpages);
    stringstream ppss;
    ppss << prefix.str() << " Set pre split=" << numpages << " pages" << endl;
    CPPUNIT_ASSERT_MESSAGE(ppss.str(), (ret == 0));

    // limit size smaller - negative test - should fail
    numpages /= 2;
    ret = mdbm_limit_size_v3(dbh, numpages, NULL, NULL);
    stringstream ls2ss;
    ls2ss << prefix.str() << " Reduce Limit size=" << numpages << " pages should fail" << endl;
    CPPUNIT_ASSERT_MESSAGE(ls2ss.str(), (ret == -1));
}

// mdbm_limit_size shake functions

// urgency is incremented with each full pass through the page: 0..2
int noOpShaker(MDBM *, datum key, datum val, void* urgency)
{
    return 0;
}
int
shakePass1(MDBM *, datum key, datum val, void* urgency)
{
    string pmkey = TestBase::GetDefaultKey(0);
    int len = pmkey.size();
    len = len > key.dsize ? key.dsize : len;
    int match = !memcmp(key.dptr, pmkey.data(), len);
// FIX    if (match) cout << endl << "FIX FIX FIX shakepass1 got match for key=" << pmkey << " and size freed=" << val.dsize << endl << flush;
    return match;
}
int
shakePass2(MDBM *, datum key, datum val, void* urgency)
{
    // look for first key
    if (shakePass1(0, key, val, urgency))
        return 1;

    // look for 2nd key
    string pmkey = TestBase::GetDefaultKey(1);
    int len = pmkey.size();
    len = len > key.dsize ? key.dsize : len;
    return !memcmp(key.dptr, pmkey.data(), len);
}
int
shakePass3(MDBM *, datum key, datum val, void* urgency)
{
    // look for 1st and 2nd key
    if (shakePass2(0, key, val, urgency)) {
        return 1;
    }

    // look for 3rd key
    string pmkey = TestBase::GetDefaultKey(2);
    int len = pmkey.size();
    len = len > key.dsize ? key.dsize : len;
    return !memcmp(key.dptr, pmkey.data(), len);
}

int
noOpShaker_new(MDBM *, const datum * key, const datum * val, struct mdbm_shake_data *)
{
    return 0;
}

// The v2 version is a completely different algorithm than v1.
// Implementor is forced to know internals of mdbm in order to implement this!
//
// When traversing the kvpairs in the mdbm_shake_data struct, a kvpair
// may be marked as "deleted" by setting the key size to 0.
int
shakekeyv3(kvpair * page_items, uint32_t page_num_items,
           const string& pmkey1, const string& pmkey2, void* user_data)
{
    int len = pmkey1.size();

    // traverse the page and mark the known entry(keyword="pass1") as deleted
    int foundcnt = 0;
    for (uint16_t cnt = 0; cnt < page_num_items; ++cnt) {
        kvpair * page_item = page_items+cnt;
        char * keyname = page_item->key.dptr;
//cout << endl << " FIX FIX pass1new: check key=" << keyname << endl << flush;
        int klen = len > page_item->key.dsize ? page_item->key.dsize : len;
        if (!memcmp(pmkey1.data(), keyname, klen)) {
//cout << endl << " FIX FIX pass1new: Found[" << cnt << "] key=" << keyname << " val isze=" << page_item->val.dsize << endl << flush;
            page_item->key.dsize = 0;
            foundcnt++;
        }
        if (pmkey2.size() && !memcmp(pmkey2.data(), keyname, klen)) {
            page_item->key.dsize = 0;
            foundcnt++;
        }
        if (user_data && !memcmp(((string*)user_data)->data(), keyname, klen)) {
            page_item->key.dsize = 0;
            foundcnt++;
        }
    }
    return foundcnt;
}

int
noOpShaker_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 *)
{
    return 0;
}
int
shakePass1_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 * sd)
{
    return shakekeyv3(sd->page_items, sd->page_num_items,
                      TestBase::GetDefaultKey(0),
                      "", 0);
}
int
shakePass2_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 * sd)
{
    return shakekeyv3(sd->page_items, sd->page_num_items,
                      TestBase::GetDefaultKey(0),
                      TestBase::GetDefaultKey(1),
                      0);
}

int
shakePass3_v3(MDBM *, const datum*, const datum*, struct mdbm_shake_data_v3 * sd)
{
    return shakekeyv3(sd->page_items, sd->page_num_items,
                      TestBase::GetDefaultKey(0),
                      TestBase::GetDefaultKey(1),
                      sd->user_data);
}

extern "C" {
struct mdbm;
int mdbm_limit_size(MDBM *db, mdbm_ubig_t a, int (*func)(struct mdbm *, datum k, datum v, void *x));
struct mdbm_shake_data;
typedef int (*mdbm_shake_func)(MDBM *, const datum*, const datum*, struct mdbm_shake_data *);
int mdbm_limit_size_new(MDBM *db, mdbm_ubig_t a, mdbm_shake_func shake, void *x);
}


void LimitSizeTestSuite::TestLegacyFail()
{
  //TRACE_TEST_CASE(__func__)
  //  MdbmHolder mdbm(GetTmpMdbm(versionFlag|MDBM_O_CREAT|MDBM_O_RDWR, 0644, 0));

  //pid_t pid = fork();
  //CPPUNIT_ASSERT(pid >= 0);
  //if (!pid) {
  //  // child, deprecated limit-size causes abort();
  //  mdbm_limit_size(mdbm, 8192, NULL);
  //  exit(0);
  //} else {
  //  // parent
  //  int status;
  //  CPPUNIT_ASSERT(pid == waitpid(pid, &status, 0));
  //  CPPUNIT_ASSERT(!WIFEXITED(status));
  //}

  //pid = fork();
  //CPPUNIT_ASSERT(pid >= 0);
  //if (!pid) {
  //  // child, deprecated limit-size causes abort();
  //  mdbm_limit_size_new(mdbm, 8192, NULL, NULL);
  //  exit(0);
  //} else {
  //  // parent
  //  int status;
  //  CPPUNIT_ASSERT(pid == waitpid(pid, &status, 0));
  //  CPPUNIT_ASSERT(!WIFEXITED(status));
  //}

}


class LimitSizeTestSuiteV3 : public LimitSizeTestSuite
{
    CPPUNIT_TEST_SUITE(LimitSizeTestSuiteV3);
      CPPUNIT_TEST(ResetNumPagesBiggerV3_lsv3);  // TC D-1
      CPPUNIT_TEST(PremadeResetNumPagesBiggerV3_lsv3);  // TC D-2
      CPPUNIT_TEST(ResetNumPagesSmallerV3_lsv3);  // TC D-3
      CPPUNIT_TEST(PremadeResetNumPagesSmallerV3_lsv3);  // TC D-4
      CPPUNIT_TEST(MaxNumPagesUIntV3_lsv3);  // TC D-5
      CPPUNIT_TEST(MaxNumPagesV3_lsv3);  // TC D-6
      CPPUNIT_TEST(ChangeShakeNeedNfreeNV3_lsv3);  // TC D-7
      CPPUNIT_TEST(NoShakeNeedNV3_lsv3);  // TC D-8
      CPPUNIT_TEST(ShakeNeedNfreeLessNV3_lsv3);  // TC D-9
      CPPUNIT_TEST(ShakeNeedNfreeTwiceV3_lsv3);  // TC D-10
      CPPUNIT_TEST(ShakeNeedNfreeThriceV3_lsv3);  // TC D-11

      CPPUNIT_TEST(PreSplitLimitSize);  // TC D-12
    CPPUNIT_TEST_SUITE_END();

public:
    LimitSizeTestSuiteV3() : LimitSizeTestSuite(MDBM_CREATE_V3) {}
};
CPPUNIT_TEST_SUITE_REGISTRATION(LimitSizeTestSuiteV3);


