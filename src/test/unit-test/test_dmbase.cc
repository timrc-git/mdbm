/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults
// FIX BZ 5469518 - v3: mdbm_chk_page: get an abort specifying page num=2 in a DB of 4 pages
// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults

#include <unistd.h>
#include <string.h>

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
#include "test_lockbase.hh"
#include "test_dmbase.hh"

int DataMgmtBaseTestSuite::_setupCnt = 0;
int DataMgmtBaseTestSuite::_PageSizeRange[] = { 256, 2048, 4096, 16384, 65536 };


void DataMgmtBaseTestSuite::setUp()
{
    if (_setupCnt++ == 0)
    {
        cout << endl << "DB Management Test Suite Beginning..." << endl << flush;
    }
}

string
DataMgmtBaseTestSuite::makeKeyName(int incr, const string &keyBaseName)
{
    stringstream dummykeyss;
    dummykeyss << keyBaseName;
    if (incr < 10) {
        dummykeyss << "00";
    } else if (incr < 100) {
        dummykeyss << "0";
    }

    dummykeyss << incr;

    string dkey = dummykeyss.str();
    return dkey;
}
// numPages : 0 means create with 1 page and store only one key/value pair
//          : > 0 means create with numPages and fill with key/value pairs
// value : null value allowed to be stored
int
DataMgmtBaseTestSuite::createAndAddData(MdbmHolder &dbh, int numPages, const string &keyBaseName, const char *value, int &defAlignVal, int &defHashID, int extraFlags = 0)
{
    if (numPages < 0) { // valid range is >= 0
        numPages = 0;
    }

    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | extraFlags;
    int dbret = dbh.Open(openFlags, 0644, 512, 512 * numPages);
    if (dbret == -1) {
        int errnum = errno;
        cout << "WARNING:(CDB): " << SUITE_PREFIX()
             << " MdbmHolder:Open FAILed with errno=" << errnum
             << " to open DB=" << (const char*)dbh
             << " with number of pages=" << numPages << endl << flush;

        return -1;
    }

    int numRecs = 1;
    if (numPages > 0) {
        dbret = mdbm_limit_size_v3(dbh, numPages, 0, NULL); // limit to numPages and no shake function
        numRecs = -1;
    }

    defAlignVal = mdbm_get_alignment(dbh);
    int alignVal = defAlignVal < MDBM_ALIGN_32_BITS ? MDBM_ALIGN_32_BITS : MDBM_ALIGN_16_BITS;
    int ret = mdbm_set_alignment(dbh, alignVal);
    if (ret == -1) {
        cout << "WARNING:(CDB): " << SUITE_PREFIX()
             << " mdbm_set_alignment FAILed. default mask=" << defAlignVal
             << " Tried to set mask=" << alignVal << endl << flush;
        return -1;
    }
    defHashID = mdbm_get_hash(dbh);
    int hashID = (defHashID + 1) % (MDBM_MAX_HASH + 1);
    ret = mdbm_sethash(dbh, hashID);
    if (ret != 1) {
        cout << "WARNING:(CDB): " << SUITE_PREFIX()
             << " mdbm_sethash FAILed. default hash ID=" << defHashID
             << " Tried to set hash ID=" << hashID << endl;
        return -1;
    }

    int cnt = 0;
    for (; numRecs != 0; --numRecs)
    {
        string dkey = makeKeyName(++cnt, keyBaseName);
        int len = value == 0 ? 0 : strlen(value) + 1;
        if (store(dbh, dkey.c_str(), const_cast<char*>(value), len, 0) == -1)
        {
            break;
        }
    }

    numRecs = numRecs < 0 ? (-1 - numRecs) : cnt;
    {
        stringstream trmsg;
        trmsg << "addData: numpages=" << numPages << " numRecs=" << numRecs;
        TRACE_TEST_CASE(trmsg.str())
    }

    return numRecs;
}

// return the size of the specified db file
// -1 upon error
off_t DataMgmtBaseTestSuite::getFileSize(const string &dbName)
{
    struct stat dbstat;
    memset(&dbstat, 0, sizeof(dbstat));
    int ret = stat(dbName.c_str(), &dbstat);
    if (ret == -1)
    {
        return ret;
    }

    return dbstat.st_size;
}

void
DataMgmtBaseTestSuite::EmptyDbCompressA1()
{
    // create empty DB
    string baseName = "dmtcA1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC A1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // get file size
    int b4compr_size = getFileSize(dbName);

    // compress
    mdbm_compress_tree(dbh);

    // stat to get file size
    int aftercompr_size = getFileSize(dbName);
    stringstream szss;
    szss << SUITE_PREFIX()
         << "TC A1: Empty DB: Compression Not Possible. File size before compression=" << b4compr_size
         << " should be same as after compression=" << aftercompr_size << endl;
    CPPUNIT_ASSERT_MESSAGE(szss.str(), (b4compr_size == aftercompr_size));
}
void
DataMgmtBaseTestSuite::PartFilledSinglePagedCompressA2()
{
    // create a partial filled DB
    string baseName = "dmtcA2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcA2key";
    string value       = "tc2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // get file size
    int b4compr_size = getFileSize(dbName);

    // compress
    mdbm_compress_tree(dbh);

    // stat to get file size
    int aftercompr_size = getFileSize(dbName);
    stringstream szss;
    szss << SUITE_PREFIX()
         << "TC A2: Partial Single Page DB: Compression Not Possible. File size before compression=" << b4compr_size
         << " should be same as after compression=" << aftercompr_size << endl;
    CPPUNIT_ASSERT_MESSAGE(szss.str(), (b4compr_size == aftercompr_size));
}
void
DataMgmtBaseTestSuite::FilledMultiPageCompressA3()
{
    // create a multi-page DB and fill with keys and values where all keys are the same
    // length, and all values are the same length
    string baseName = "dmtcA3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName  = "tcA3key";
    string value        = "tc3dummy";
    int defAlignVal;
    int defHashID;
    int    numRecsAdded = createAndAddData(dbh, 7, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A3: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // get file size
    int b4compr_size = getFileSize(dbName);

    // compress
    mdbm_compress_tree(dbh);

    // stat to get file size
    int aftercompr_size = getFileSize(dbName);
    stringstream szss;
    szss << SUITE_PREFIX()
         << "TC A3: Filled Multi Page DB: Compression Not Possible. File size before compression=" << b4compr_size
         << " should be same as after compression=" << aftercompr_size << endl;
    CPPUNIT_ASSERT_MESSAGE(szss.str(), (b4compr_size == aftercompr_size));
}

// RETURN number of entries deleted
int
DataMgmtBaseTestSuite::deleteAllEntriesOnPage(MDBM *dbh)
{
    // iterate to get a page number and collect all keys for the page
    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    if (key.dsize == 0)
    {
        return -1;  // DB is empty
    }
    vector<datum> keysToDelete;
    keysToDelete.push_back(key);

    mdbm_ubig_t pageNumToDel = mdbm_get_page(dbh, &key);

    // then iterate the collected keys and delete them from the DB
    while (key.dsize > 0)
    {
        key = mdbm_nextkey_r(dbh, &iter);
        mdbm_ubig_t pageNum = mdbm_get_page(dbh, &key);
        if (pageNum == pageNumToDel)
        {
            keysToDelete.push_back(key);
        }
    }

    for (size_t cnt = 0; cnt < keysToDelete.size(); ++cnt)
    {
        mdbm_delete(dbh, keysToDelete[cnt]);
    }

    return keysToDelete.size();
}
void
DataMgmtBaseTestSuite::FilledMultiPageLoopDeleteAndCompressA4()
{
    // create a multi-page DB and fill with keys and values where all keys are the same
    // length, and all values are the same length
    string baseName = "dmtcA4";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName  = "tcA4key";
    string value        = "tc4dummy";
    int defAlignVal;
    int defHashID;
    int    numRecsAdded = createAndAddData(dbh, 7, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A4: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // get file size
    int b4compr_size = getFileSize(dbName);
    {
        stringstream trmsg;
        trmsg << "TC A4: DB size before compression=" << b4compr_size;
        TRACE_TEST_CASE(trmsg.str())
    }

    // lets loop: delete, compress, get file size
    int compressCnt = 0;
    vector<int> pagesProcessed;
    int aftercompr_size = b4compr_size;
    stringstream szcmpss;
    szcmpss << SUITE_PREFIX()
            << "TC A4: Compress delivered bad result. Starting DB size=" << b4compr_size;
    while (aftercompr_size == b4compr_size)
    {
        // delete a page of data
        int pageNum = deleteAllEntriesOnPage(dbh);
        if (pageNum != -1)
        {
            pagesProcessed.push_back(pageNum);
        }
        else
        {
            // nothing left to delete
            break;
        }

        // compress and get file size
        mdbm_compress_tree(dbh);
        compressCnt++;

        aftercompr_size = getFileSize(dbName);
        if (aftercompr_size > b4compr_size)
        {
            szcmpss << " Current DB file size=" << aftercompr_size
                    << " should be smaller than or equal, not bigger"
                    << " than original DB file size after compressing " << compressCnt
                    << " times" << endl;

            CPPUNIT_ASSERT_MESSAGE(szcmpss.str(), (b4compr_size >= aftercompr_size));
        }

        {
            stringstream trmsg;
            trmsg << "TC A4: DB size after compression=" << aftercompr_size;
            TRACE_TEST_CASE(trmsg.str())
        }
    }

    szcmpss << " Current DB file size=" << aftercompr_size
            << " should be smaller than, not bigger"
            << " than original DB file size after compressing " << compressCnt
            << " times" << endl;
    CPPUNIT_ASSERT_MESSAGE(szcmpss.str(), (b4compr_size > aftercompr_size));
}

void
DataMgmtBaseTestSuite::FilledMultiPageAllNullValueEntriesA5()
{
    // create a multi-page DB and fill with keys and values where all keys are the same
    // length, and all values are the same length
    string baseName = "dmtcA5";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName  = "tcA5key";
    int defAlignVal;
    int defHashID;
    int    numRecsAdded = createAndAddData(dbh, 7, keyBaseName, 0, defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A5: FAILed to create a filled(with empty values) multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // get file size
    int b4compr_size = getFileSize(dbName);

    // compress
    mdbm_compress_tree(dbh);

    // stat to get file size
    int aftercompr_size = getFileSize(dbName);
    stringstream szss;
    szss << SUITE_PREFIX()
         << "TC A5: Filled Multi Page DB: Compression Should Not be Possible. File size before compression=" << b4compr_size
         << " should be same as after compression=" << aftercompr_size << endl;
    CPPUNIT_ASSERT_MESSAGE(szss.str(), (b4compr_size == aftercompr_size));

    {
        stringstream trmsg;
        trmsg << "TC A5: File size before compression=" << b4compr_size
              << " File size after compression=" << aftercompr_size;
        TRACE_TEST_CASE(trmsg.str())
    }
}
void
DataMgmtBaseTestSuite::FilledMultiPageMaxNumPagesA6()
{
    if (GetUnitTestLevelEnv() < RunAllTestCases)
    {
        return;
    }

    string baseName = "dmtcA6";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName   = "tcA6key";
    string value         = "tcA6dummy";
    mdbm_ubig_t numPages = 4194303;
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, numPages, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A6: FAILed to create a filled multi paged DB=" << dbName
         << " with number-of-pages=" << numPages
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // get file size
    off_t b4compr_size = getFileSize(dbName);

    // compress
    mdbm_compress_tree(dbh);

    // stat to get file size
    off_t aftercompr_size = getFileSize(dbName);
    stringstream szss;
    szss << SUITE_PREFIX()
         << "TC A6: Filled Multi Page DB: Compression may not be Possible. File size before compression=" << b4compr_size
         << " should be >= after compression=" << aftercompr_size
         << " Number of pages requested=" << numPages
         << " Number of entries added=" << numRecsAdded << endl;
    CPPUNIT_ASSERT_MESSAGE(szss.str(), (b4compr_size >= aftercompr_size));

    {
        stringstream trmsg;
        trmsg << "TC A6: File size before compression=" << b4compr_size
              << " File size after compression=" << aftercompr_size
              << " Number of pages requested=" << numPages
              << " Number of entries added=" << numRecsAdded;
        TRACE_TEST_CASE(trmsg.str())
    }
}
void
DataMgmtBaseTestSuite::MultiPageSizesNoLargeObjFlagA7()
{
    string tcprefix = "TC A7: Multi-page sizes, No large object flag: ";
    vector<int> pagerange(_PageSizeRange, _PageSizeRange + sizeof(_PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(versionFlag, pagerange, 32, true, tcprefix);
}
void
DataMgmtBaseTestSuite::MultiPageSizesLargeObjFlagA8()
{
    string tcprefix = "TC A8: Multi-page sizes, Large object flag: ";
    vector<int> pagerange(_PageSizeRange, _PageSizeRange + sizeof(_PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(versionFlag|MDBM_LARGE_OBJECTS, pagerange, 32, true, tcprefix);
}
void
DataMgmtBaseTestSuite::MultiPageSizesMemoryOnlyA9()
{
    string tcprefix = "TC A9: Multi-page sizes, Memory-Only: ";
    vector<int> pagerange(_PageSizeRange, _PageSizeRange + sizeof(_PageSizeRange) / sizeof(int));
    StoreFetchKnownValues(0, pagerange, 32, true, tcprefix);
}

// parse for the following:
// "Page size: %d\n"
// "Page count: %d\n"
// "Hash function: %s\n"
// "Lock owner pid: n/a (stat running w/o locking)\n" : if DB opened without locking
void
DataMgmtBaseTestSuite::parseHeaderOutput(stringstream &istrm, const string &prefix, int expectedNumPages, int expectedPageSize, const string &hashFunc, bool lockingEnabled, int pid)
{
    char buff[256];
    bool pageSizeFound = false;
    bool numPagesFound = false;
    bool hashFuncFound = false;
    bool lockOwnerFuncFound = false;
    if (lockingEnabled == true)
    {
        lockOwnerFuncFound = true;  // ignored
    }

    stringstream phss;
    phss << prefix;
    while (!istrm.eof())
    {
        memset(buff, 0, sizeof(buff));
        istrm.getline(buff, sizeof(buff), '\n');
        if (istrm.fail() || istrm.bad())
        {
            break;
        }

        string line = buff;
        stringstream pstrm(line);
        if (line.find("Page size: ") != string::npos)
        {
            string pagetoken, sizetoken;
            int pagesizetoken;
            pstrm >> pagetoken >> sizetoken >> pagesizetoken;
            if (pagesizetoken != expectedPageSize)
            {
                phss << "Parsed Page size=" << pagesizetoken
                     << " does not match expected page size=" << expectedPageSize << endl;
                CPPUNIT_ASSERT_MESSAGE(phss.str(), (pagesizetoken == expectedPageSize));
            }
            pageSizeFound = true;
        }
        else if (line.find("Page count: ") != string::npos)
        {
            string pagetoken, cnttoken;
            int pagecnttoken;
            pstrm >> pagetoken >> cnttoken >> pagecnttoken;
            if (pagecnttoken < expectedNumPages)
            {
                phss << "Parsed Page count=" << pagecnttoken
                     << " does not match expected number pages=" << expectedNumPages << endl;
                CPPUNIT_ASSERT_MESSAGE(phss.str(), (pagecnttoken >= expectedNumPages));
            }
            numPagesFound = true;
        }
        else if (line.find("Hash function: ") != string::npos)
        {
            string hashtoken, functoken, hashfunctoken;
            pstrm >> hashtoken >> functoken >> hashfunctoken;
            size_t pos = hashfunctoken.find(hashFunc);
            if (pos == string::npos)
            {
                phss << "Parsed Hash token=" << hashfunctoken
                     << " does not match expected Hash token=" << hashFunc << endl;
                CPPUNIT_ASSERT_MESSAGE(phss.str(), (pos != string::npos));
            }
            hashFuncFound = true;
        }
        else if (line.find("Lock owner pid: ") != string::npos)
        {
            if (lockingEnabled == false)
            {
                size_t pos = line.find("w/o locking");
                if (pos == string::npos)
                {
                    phss << "Parsed Lock owner no-lock token missing in line=" << line << endl;
                    CPPUNIT_ASSERT_MESSAGE(phss.str(), (pos != string::npos));
                }
                lockOwnerFuncFound = true;
            }
        }
    }

    phss << "Parsed output is missing a line for: ";
    if (pageSizeFound == false)
    {
        phss << "Page size MISSING!";
    }
    else if (numPagesFound == false)
    {
        phss << "Page count MISSING!";
    }
    else if (hashFuncFound == false)
    {
        phss << "Hash function MISSING!";
    }
    else if (lockOwnerFuncFound == false)
    {
        phss << "Lock owner MISSING!";
    }

    // wait on child to get exit status
    int status;
    waitpid(pid, &status, 0);

    phss << endl;
    CPPUNIT_ASSERT_MESSAGE(phss.str(), (pageSizeFound == true && numPagesFound == true && hashFuncFound == true && lockOwnerFuncFound == true));
}
void
DataMgmtBaseTestSuite::EmptyDbNonDefaultsStatB1()
{
    // create empty DB with non-default page size
    string baseName = "dmtcB1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize  = 1024;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC B1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // set hash function to non-default
    int hashID = MDBM_HASH_OZ; // "oz (sdbm)"
    string hashFunc = "oz";
    mdbm_sethash(dbh, hashID);

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int numPagesInDB = 1;
        parseHeaderOutput(strstrm, prefix, numPagesInDB, pageSize, hashFunc, true, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_header(dbh);
        exit(0);
    }
}
void
DataMgmtBaseTestSuite::MultiPageDbNonDefaultsStatB2()
{
    // fill a multi page DB
    string baseName = "dmtcB2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    int numPages = 3;
    string keyBaseName = "tcB2key";
    string value       = "tcB2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, numPages, keyBaseName, value.c_str(), defAlignVal, defHashID);

    string prefix = SUITE_PREFIX();
    prefix += "TC B2: ";
    stringstream cdss;
    cdss << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // set hash function to non-default
    int hashID = MDBM_HASH_OZ; // "oz (sdbm)"
    string hashFunc = "oz";
    mdbm_sethash(dbh, hashID);

    // setup pipes and fork child to do the mdbm_stat_header

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int pageSize = 512;
        parseHeaderOutput(strstrm, prefix, numPages, pageSize, hashFunc, true, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_header(dbh);
        exit(0);
    }
}
void
DataMgmtBaseTestSuite::OpenDbNOLOCKflagTheStatB3()
{
    // create empty DB with non-default page size
    string baseName = "dmtcB3";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_OPEN_NOLOCK | versionFlag;
    int pageSize  = 1024;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC B3: Open DB with MDBM_OPEN_NOLOCK flag Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // set hash function to non-default
    int hashID = MDBM_HASH_OZ; // "oz (sdbm)"
    string hashFunc = "oz";
    mdbm_sethash(dbh, hashID);

    // store a couple of entries in the DB
    string key = "tcB3key1";
    string val = "tcB3value1";
    store(dbh, key.c_str(), const_cast<char*>(key.c_str()), val.size()+1, 0);
    key = "tcB3key2";
    val = "tcB3value2";
    store(dbh, key.c_str(), const_cast<char*>(key.c_str()), val.size()+1, 0);

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int numPagesInDB = 1;
        parseHeaderOutput(strstrm, prefix, numPagesInDB, pageSize, hashFunc, false, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_header(dbh);
        exit(0);
    }
}

// Following are the kinds of output mdbm_stat_all_page produces:
// "Total mapped db size: %s\n"
// "Total number of entries: %ld\n"
// "ADDRESS SPACE page efficiency     : %3ld %%; (%ld/%ld)\n"
// "ADDRESS SPACE byte efficiency     : %3ld %%; (%s/%s)\n"
// "PHYSICAL MEM/DISK SPACE efficiency: %3ld %%;"
// "Average bytes per record: %d\n"
// "Maximum B-tree level:  %d\n"
// "Minimum B-tree level:  %d\n"
//
// We will parse for only those things we can easily verify, such as:
// "Total number of entries: %ld\n"
// "Average bytes per record: %d\n"
//
// if large objects added, then:
// "Large-object entries: %d\n"
// "Large-object average record size: %d\n" - NOTE: this is actually data size only
void
DataMgmtBaseTestSuite::parseStatAllPageOutput(stringstream &istrm, const string &prefix, const string &dbName, int expectedNumEntries, int avgBytesPerRecord, int numLargeObjEntries, int largeObjAvgDataSize, int pid)
{
    char buff[256];
    bool checkLargeObjs = false;
    stringstream phss;
    phss << prefix;
    while (!istrm.eof())
    {
        memset(buff, 0, sizeof(buff));
        istrm.getline(buff, sizeof(buff), '\n');
        if (istrm.fail() || istrm.bad())
        {
            break;
        }

        string line = buff;
        stringstream pstrm(line);

// Ignoring mapped db size parsing. Lots of processing for how it does output.
// It outputs the DB size in K, M, G, T, P, and S
// Also, large object handling needs to be accounted for....
//
// For a DB of 1 page and page size is 512 bytes, example output:
//   "Total mapped db size: 0.5K"
//
// For a DB created with page size of 512 bytes and that has only one single
// large object added, example output:
//  "Total mapped db size: 1M"
//
//        if (line.find("Total mapped db size: ") != string::npos)

        if (line.find("Total number of entries: ") != string::npos)
        {
            string totaltoken, numtoken, oftoken, entriestoken;
            off_t numEntries = -1;
            pstrm >> totaltoken >> numtoken >> oftoken >> entriestoken >> numEntries;

            if (numEntries != expectedNumEntries)
            {
                phss << "Parsed Number entries=" << numEntries
                     << " does not match expected Number of entries=" << expectedNumEntries << endl;
                CPPUNIT_ASSERT_MESSAGE(phss.str(), (numEntries == expectedNumEntries));
            }
        }
        else if (line.find("Average bytes per record: ") != string::npos)
        {
            string avgtoken, bytestoken, pertoken, rectoken;
            int avgBytes = -1;
            pstrm >> avgtoken >> bytestoken >> pertoken >> rectoken >> avgBytes;

            // NOTE: only check if there are no large objects else it doesnt
            // make sense - is it worth investigating?
            // For a DB created with page size of 512 bytes and that has only
            //  one single large object added, example output:
            // "Average bytes per record: 52"
            if (avgBytes < avgBytesPerRecord)
            {
                phss << "Parsed Average bytes per record=" << avgBytes
                     << " should be >= expected average bytes per record=" << avgBytesPerRecord << endl;
                if (numLargeObjEntries == 0)
                {
                    CPPUNIT_ASSERT_MESSAGE(phss.str(), (avgBytes >= avgBytesPerRecord));
                }
                else
                {
                    cout << "WARNING: " << phss.str() << flush;
                }
            }
        }
        else if (line.find("Large-object entries: ") != string::npos)
        {
            string lotoken, entriestoken;
            int numEntries = -1;
            pstrm >> lotoken >> entriestoken >> numEntries;

            if (numEntries != numLargeObjEntries)
            {
                phss << "Parsed Number of Large Object entries=" << numEntries
                     << " should be same as expected number of entries=" << numLargeObjEntries << endl;
                CPPUNIT_ASSERT_MESSAGE(phss.str(), (numEntries == numLargeObjEntries));
            }
            checkLargeObjs = true;
        }
        else if (line.find("Large-object average record size:") != string::npos)
        {
            // NOTE: this is actually the data size, not the record size as in
            // "Average bytes per record" parsed above.
            string lotoken, avgtoken, rectoken, sizetoken;
            int avgDataSize = -1;
            pstrm >> lotoken >> avgtoken >> rectoken >> sizetoken >> avgDataSize;

            if (avgDataSize != largeObjAvgDataSize)
            {
                phss << "Parsed Large Object Average size record=" << avgDataSize
                     << " should be same as expected average record size=" << largeObjAvgDataSize << endl;
                CPPUNIT_ASSERT_MESSAGE(phss.str(), (avgDataSize == largeObjAvgDataSize));
            }
            checkLargeObjs = true;
        }
    }

    if (numLargeObjEntries > 0 && checkLargeObjs == false)
    {
        phss << "Should have parsed lines for Large Object statistics"
             << " but they are missing or garbled?" << endl;
        CPPUNIT_ASSERT_MESSAGE(phss.str(), (numLargeObjEntries > 0 && checkLargeObjs));
    }

    // wait on child to get exit status
    int status;
    waitpid(pid, &status, 0);
}
void
DataMgmtBaseTestSuite::EmptyDbStatAllC1()
{
    // create empty DB with non-default page size
    string baseName = "dmtcC1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int pageSize  = 1024;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC C1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int expectedNumEntries = 0;
        int avgBytesPerRecord  = 0;
        parseStatAllPageOutput(strstrm, prefix, dbName, expectedNumEntries, avgBytesPerRecord, 0, 0, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_all_page(dbh);
        exit(0);
    }
}
void
DataMgmtBaseTestSuite::FilledSinglePagedDbStatAllC2()
{
    string baseName = "dmtcC2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcC2key";
    string value       = "tcC2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);

    string prefix = SUITE_PREFIX();
    prefix += "TC C2: ";
    stringstream cdss;
    cdss << prefix
         << "FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int avgBytesPerRecord = keyBaseName.size() + value.size();
        parseStatAllPageOutput(strstrm, prefix, dbName, numRecsAdded, avgBytesPerRecord, 0, 0, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_all_page(dbh);
        exit(0);
    }
}
void
DataMgmtBaseTestSuite::FilledMultiPageDbStatAllC3()
{
    string baseName = "dmtcC3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcC3key";
    string value       = "tcC3dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 4, keyBaseName, value.c_str(), defAlignVal, defHashID);

    string prefix = SUITE_PREFIX();
    prefix += "TC C3: ";
    stringstream cdss;
    cdss << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int avgBytesPerRecord = keyBaseName.size() + value.size();
        parseStatAllPageOutput(strstrm, prefix, dbName, numRecsAdded, avgBytesPerRecord, 0, 0, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_all_page(dbh);
        exit(0);
    }
}
void
DataMgmtBaseTestSuite::AddLargeObjectStatAllC4()
{
    string baseName = "dmtcC4";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_LARGE_OBJECTS | versionFlag;
    int pageSize  = 512;
    int dbret = dbh.Open(openflags, 0644, pageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC C4: Large Object DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // add a large object
    string lokey = "dmtcC4lokey";
    datum dkey;
    dkey.dptr  = (char*)lokey.c_str();
    dkey.dsize = lokey.size();
    string value(511, 'x');
    datum dval;
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size();
    mdbm_store(dbh, dkey, dval, MDBM_INSERT);

    // setup pipe to read output from the child
    LockBaseTestSuite::FD_RAII parReadFD, parSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(parReadFD, parSendFD);
    LockBaseTestSuite::FD_RAII childReadFD, childSendFD;
    LockBaseTestSuite::FD_RAII::makePipe(childReadFD, childSendFD);

    int   pid = fork();
    stringstream frkss;
    frkss << prefix
          << " Could not fork the child process - cannot continue this test" << endl;
    CPPUNIT_ASSERT_MESSAGE(frkss.str(), (pid != -1));

    if (pid)  // parent
    {
        // wait for output from the child then read it
        stringstream strstrm;
        string resp;
        string parprefix = prefix;
        parprefix += "Parent: ";
        while (LockBaseTestSuite::waitForResponse(childReadFD, resp, parprefix))
        {
            strstrm << resp;
            resp = "";
        }

        // parse childs output
        int numRecsAdded = 1;
        int avgBytesPerRecord = dkey.dsize + value.size();
        int numLargeObjEntries = 1;
        parseStatAllPageOutput(strstrm, prefix, dbName, numRecsAdded, avgBytesPerRecord, numLargeObjEntries, value.size(), pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        if (ret == -1)
        {
            cout << prefix
                 << "Child: dup2 FAILed, cannot complete the test. errno=" << errnum << endl << flush;
        }

        mdbm_stat_all_page(dbh);
        exit(0);
    }
}

void
DataMgmtBaseTestSuite::EmptyDbChkAllD1()
{
    // create empty DB
    string baseName = "dmtcD1";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC D1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // expect no errors from mdbm_chk_all_page
    errno = 0;
    int ret = mdbm_chk_all_page(dbh);

    stringstream cass;
    cass << prefix
         << "Should be no errors reported for an empty DB=" << dbName << endl;
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::PartFilledDbChkAllD2()
{
    string baseName = "dmtcD2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcD2key";
    string value       = "tcD2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // expect no errors from mdbm_chk_all_page
    errno = 0;
    int ret = mdbm_chk_all_page(dbh);

    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC D2: Should be no errors reported for a partial filled DB=" << dbName << endl
         << "Got errno=";
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FilledSinglePagedDbChkAllD3()
{
    string baseName = "dmtcD3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcD3key";
    string value       = "tcD3dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D3: FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // expect no errors from mdbm_chk_all_page
    errno = 0;
    int ret = mdbm_chk_all_page(dbh);

    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC D3: Should be no errors reported for a filled single page DB=" << dbName << endl
         << "Got errno=";
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FilledMultiPageDbChkAllD4()
{
    string baseName = "dmtcD4";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcD4key";
    string value       = "tcD4dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 3, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D4: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // expect no errors from mdbm_chk_all_page
    errno = 0;
    int ret = mdbm_chk_all_page(dbh);

    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC D4: Should be no errors reported for a filled multi page DB=" << dbName << endl
         << "Got errno=";
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::PartFilledMultiPageDbChkAllD5()
{
    string baseName = "dmtcD5";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcD5key";
    string value       = "tcD5dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 3, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D5: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // lets delete a few things to make it a partial multi page DB
    for (int cnt = 3; cnt < 6; ++cnt)
    {
        string key = makeKeyName((cnt * 7), keyBaseName);
        datum dkey;
        dkey.dptr  = const_cast<char*>(key.c_str());
        dkey.dsize = key.size();
        mdbm_delete(dbh, dkey);
    }

    // expect no errors from mdbm_chk_all_page
    errno = 0;
    int ret = mdbm_chk_all_page(dbh);

    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC D5: Should be no errors reported for a partial filled multi page DB=" << dbName << endl
         << "Got errno=";
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}

void
DataMgmtBaseTestSuite::EmptyDbChkPage0E1()
{
    // create empty DB
    string baseName = "dmtcE1";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openflags, 0644, 512, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC E1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // expect no errors from mdbm_chk_page
    errno = 0;
    int ret = mdbm_chk_page(dbh, 0);

    stringstream cass;
    cass << prefix
         << "Should be no errors reported for an empty DB=" << dbName << endl;
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}

void
DataMgmtBaseTestSuite::PartFilledDbChkPage0E2()
{
    string baseName = "dmtcE2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcE2key";
    string value       = "tcE2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // expect no errors from mdbm_chk_page
    errno = 0;
    int ret = mdbm_chk_page(dbh, 0);

    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC E2: Should be no errors reported for a partial filled DB=" << dbName << endl
         << "Got errno=";
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FilledSinglePagedDbChkPage0E3()
{
    string baseName = "dmtcE3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcE3key";
    string value       = "tcE3dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E3: FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // expect no errors from mdbm_chk_page
    errno = 0;
    int ret = mdbm_chk_page(dbh, 0);

    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC E3: Should be no errors reported for a filled single page DB=" << dbName << endl
         << "Got errno=";
    cass << errno << endl;
    CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FilledMultiPageDbChkAllPagesE4()
{
    string baseName = "dmtcE4";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcE4key";
    string value       = "tcE4dummy";
    int defAlignVal;
    int defHashID;
    int numPages = 3;
    int numRecsAdded = createAndAddData(dbh, numPages, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E4: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    uint64_t dbSize = mdbm_get_size(dbh);
    uint64_t npages = dbSize / 512;

    // expect no errors from mdbm_chk_page
    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC E4: Should be no errors reported for a filled multi page DB=" << dbName << endl;

    for (unsigned cnt = 0; cnt < npages; ++cnt) {
        //fprintf(stderr, "CHECKING PAGE %d of %d\n", cnt, mdbm_count_pages(dbh));
        int ret = mdbm_chk_page(dbh, cnt);
        if (ret == -1) {
            if (errno == EINVAL) {
              // not a real page, mdbm_count_pages() includes dir/free/etc
            } else {
              cass << "Got errno="; cass << errno << endl;
              cass << "Error returned for page=" << cnt << endl;
              CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
            }
        }
    }
}
void
DataMgmtBaseTestSuite::PartFilledMultiPageDbChkAllPagesE5()
{
// FIX BZ 5469518 - v3: mdbm_chk_page: get an abort specifying page num=2 in a DB of 4 pages
    string baseName = "dmtcE5";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcE5key";
    string value       = "tcE5dummy";
    int defAlignVal;
    int defHashID;
    int numPages = 3;
    int numRecsAdded = createAndAddData(dbh, numPages, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E5: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // lets delete a few things to make it a partial multi page DB
    for (int cnt = 3; cnt < 6; ++cnt) {
        string key = makeKeyName((cnt * 7), keyBaseName);
        datum dkey;
        dkey.dptr  = const_cast<char*>(key.c_str());
        dkey.dsize = key.size();
        mdbm_delete(dbh, dkey);
    }

    uint64_t dbSize = mdbm_get_size(dbh);
    uint64_t npages = dbSize / 512;

    // expect no errors from mdbm_chk_page
    stringstream cass;
    cass << SUITE_PREFIX()
         << "TC E5: Should be no errors reported for a partial filled multi page DB=" << dbName << endl;

    for (unsigned cnt = 0; cnt < npages; ++cnt) {
        errno = 0;
        int ret = mdbm_chk_page(dbh, cnt);
        if (ret == -1) {
            if (errno == EINVAL) {
              // not a real page, mdbm_count_pages() includes dir/free/etc
            } else {
              cass << "Got errno=";
              cass <<  errno << endl;
              cass << "Error returned for page=" << cnt << endl;
              CPPUNIT_ASSERT_MESSAGE(cass.str(), (ret == 0));
            }
        }
    }
}

// NOTE: number of pages will be doubled via mdbm_limit_size
void
//DataMgmtBaseTestSuite::verifyDefaultConfig(MdbmHolder &dbh, const string &prefix, bool truncateDB, int defAlignVal, int defHashID, shakernew_funcp_t shaker, int npagesLimit)
DataMgmtBaseTestSuite::verifyDefaultConfig(MdbmHolder &dbh, const string &prefix, bool truncateDB, int defAlignVal, int defHashID, int npagesLimit, shakernew_funcp_t shaker, void* shakerData)
{
    // lets set non-defaults for these config params: alignment, hashfunc, pagesize, limitsize
    int presetPageSize = mdbm_get_page_size(dbh);

    int alignVal = mdbm_get_alignment(dbh);

    int hashID = mdbm_get_hash(dbh);

    uint64_t startNumPagesLimit = mdbm_get_limit_size(dbh);
    startNumPagesLimit /= presetPageSize;

    uint64_t numPagesLimit = (npagesLimit == -1) ? 4 : npagesLimit;

    int ret = mdbm_limit_size_v3(dbh, numPagesLimit, shaker, shakerData);
fprintf(stderr, "SETTING LIMIT SIZE SHAKE KEY to %s\n", (char*)shakerData);
    stringstream limerr;
    limerr << prefix
           << " mdbm_limit_size FAILed. Starting limit number of pages=" << startNumPagesLimit
           << " Tried to set number of pages=" << numPagesLimit << endl;
    CPPUNIT_ASSERT_MESSAGE(limerr.str(), (ret == 0));

    {
        stringstream trmsg;
        trmsg << prefix << "Values before truncation: "
              << "Preset Page size=" << presetPageSize << endl
              << " : Default Alignment=" << defAlignVal
              << " Reset Alignment=" << alignVal << endl
              << " : Default Hash ID=" << defHashID
              << " Reset truncation Hash ID=" << hashID << endl
              << " : Starting Number pages limit=" << startNumPagesLimit
              << " Reset Number pages limit=" << numPagesLimit;
        TRACE_TEST_CASE(trmsg.str())
    }

    //fprintf(stderr, "------------ PRE-CLEAR setlimit:%lu gotlimit:%lu ----------------\n", 
    //    numPagesLimit, mdbm_get_limit_size(dbh));
    string trunc = "truncation";
    if (truncateDB) {
        //fprintf(stderr, "------------ TRUNCATE ----------------\n");
        mdbm_truncate(dbh);
    } else {
        //fprintf(stderr, "------------ PURGE ----------------\n");
        trunc = "purge";
        mdbm_purge(dbh);
    }

    // verify all data is gone
    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    stringstream itss;
    itss << prefix
         << "FAILed to " << trunc << " DB=" << (const char *)dbh
         << " of all data. Should be no data left!" << endl;
    CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize == 0));

    // now to retrieve all the config values again to verify they reverted to defaults
    int      postTruncPageSize      = mdbm_get_page_size(dbh);
    int      postTruncAlignVal      = mdbm_get_alignment(dbh);
    int      postTruncHashID        = mdbm_get_hash(dbh);
    uint64_t postTruncNumPagesLimit = mdbm_get_limit_size(dbh) / postTruncPageSize;
    stringstream cfgss;
    cfgss << prefix;
    if (truncateDB) {
        cfgss << " Verifying defaults reset after truncation: " << endl;
    } else {
        cfgss << " Verifying defaults NOT reset after purge: " << endl;
    }
    //fprintf(stderr, "------------ setlimit:%lu gotlimit:%lu ----------------\n", 
    //    numPagesLimit, mdbm_get_limit_size(dbh));

    cfgss << " : Preset Page size=" << presetPageSize
          << " : Post-" << trunc <<" Page size=" << postTruncPageSize << endl
          << " : Default Alignment=" << defAlignVal
          << " : Reset=" << alignVal
          << " : Post-" << trunc <<" Alignment=" << postTruncAlignVal << endl
          << " : Default Hash ID=" << defHashID
          << " : Reset=" << hashID
          << " : Post-" << trunc <<" Hash ID=" << postTruncHashID << endl
          << " : Starting Number pages limit=" << startNumPagesLimit
          << " : Reset=" << numPagesLimit
          << " : Post-" << trunc <<" Number pages limit=" << postTruncNumPagesLimit << endl;

    CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncPageSize == presetPageSize));

    if (truncateDB) {
        CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncAlignVal == defAlignVal));
        CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncHashID == defHashID));
        CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncNumPagesLimit == 0));
    } else { // purge
        CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncAlignVal == alignVal));
        CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncHashID == hashID));
        CPPUNIT_ASSERT_MESSAGE(cfgss.str(), (postTruncNumPagesLimit >= numPagesLimit));
    }
}
void
DataMgmtBaseTestSuite::EmptyDbNonDefaultsTruncF1()
{
// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults
    string baseName = "dmtcF1";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int presetPageSize  = MDBM_PAGESIZ / 2;  // divide the default pagesize in half
    int dbret = dbh.Open(openflags, 0644, presetPageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC F1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int defAlignVal = MDBM_ALIGN_8_BITS;
    int defHashID   = MDBM_DEFAULT_HASH;
    verifyDefaultConfig(dbh, prefix, true, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::PartFilledDbNonDefaultsTruncF2()
{
// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults
    string baseName = "dmtcF2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcF2key";
    string value       = "tcF2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC F2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC F2: ";
    verifyDefaultConfig(dbh, prefix, true, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::FilledSinglePagedDbNonDefaultsTruncF3()
{
// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults
    string baseName = "dmtcF3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcF3key";
    string value       = "tcF3dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC F3: FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC F3: ";
    verifyDefaultConfig(dbh, prefix, true, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::FilledMultiPageDbNonDefaultsTruncF4()
{
// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults
    string baseName = "dmtcF4";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcF4key";
    string value       = "tcF4dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 4, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC F4: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC F4: ";
    verifyDefaultConfig(dbh, prefix, true, defAlignVal, defHashID);
}

// RETURN 1 == match therefore delete the record, else 0
static int shakerTC5(struct mdbm *dbh, const datum *key, const datum *val, mdbm_shake_data_v3 *pagedata)
{
    int ret = 0;
    static const char* toMatch = NULL;
    if (pagedata) {
      toMatch = (const char*)pagedata->user_data;
    }
    string keytc5(key->dptr, key->dsize);
    if (!pagedata) { // just looking for match of the known key
        // look for key that contains toMatch
        if (keytc5.find(toMatch) != string::npos) {
            ret = 1;
        }
        if (ret && GetUnitTestLogLevelEnv() > LOG_NOTICE) {
            cout << "Shaker-tc5: found matching key=" << keytc5 << endl << flush;
        }
        //fprintf(stderr,  "Shaker-tc5: found matching key=%s\n", keytc5.c_str());
    } else { // delete key
        // find the key on the page and mark it as deleted
        for (uint16_t cnt = 0; cnt < pagedata->page_num_items; ++cnt) {
            kvpair *page_item = pagedata->page_items + cnt;

            keytc5.assign(page_item->key.dptr, page_item->key.dsize);

            if (keytc5.find(toMatch) != string::npos) {
                page_item->key.dsize = 0;
                ret = 1;
                //fprintf(stderr,  "Shaker-tc5: found matching (to delete) key=%s (vs %s)\n", 
                //    keytc5.c_str(), toMatch);
                break;
            }
        }
    }

    return ret;
}
//static int shakerTCF5(struct mdbm *dbh, const datum *key, const datum *val, mdbm_shake_data_v3 *pagedata)
//{
//    return shakerTC5(dbh, key, val, pagedata, "tcF5key");
//}
static int shakerTCG5(struct mdbm *dbh, const datum *key, const datum *val, mdbm_shake_data_v3 *pagedata)
{
    return shakerTC5(dbh, key, val, pagedata);
}
//void
//DataMgmtBaseTestSuite::FilledSinglePagedDbNonDefsAndShakeFuncTruncF5()
//{
//// FIX BZ 5447216 - v2: mdbm_truncate: does not reset configuration params to defaults
//    string baseName = "dmtcF5";
//    string dbName   = GetTmpName(baseName);
//    MdbmHolder dbh(dbName);
//    string keyBaseName = "tcF5key";
//    string value       = "tcF5dummy";
//    int defAlignVal;
//    int defHashID;
//    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);
//
//    stringstream cdss;
//    cdss << SUITE_PREFIX()
//         << "TC F5: FAILed to create a filled single paged DB=" << dbName
//         << endl;
//    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));
//
//    string prefix = SUITE_PREFIX();
//    prefix += "TC F5: ";
//    verifyDefaultConfig(dbh, prefix, true, defAlignVal, defHashID, shakerTCF5);
//
//    // now lets refill it with numRecsAdded * 2; try to go over limits
//    int addcnt = 0;
//    for (; addcnt < (numRecsAdded * 2) + 2; ++addcnt)
//    {
//        string dkey = makeKeyName(addcnt, keyBaseName);
//        int len = value.size() + 1;
//        if (store(dbh, dkey.c_str(), const_cast<char*>(value.c_str()), len, 0) == -1)
//        {
//            break;
//        }
//    }
//
//    // confirm the shaker was not used to remove a known key
//    // look for keys that end with "005" or "035"
//    MDBM_ITER iter;
//    datum key = mdbm_firstkey_r(dbh, &iter);
//    stringstream itss;
//    itss << SUITE_PREFIX()
//         << "TC F5: FAILed to refill DB=" << dbName
//         << endl;
//    CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize > 0));
//
//    bool existInDB = false;
//    datum valunused;
//    while (key.dsize > 0)
//    {
//        key = mdbm_nextkey_r(dbh, &iter);
//        int found = shakerTCF5(dbh, &key, &valunused, 0);
//        existInDB = existInDB ? existInDB : found == 1;
//    }
//    stringstream inss;
//    inss << SUITE_PREFIX()
//         << "TC F5: FAILed to insert key in DB=" << dbName
//         << " but it should have been able to." << endl;
//    CPPUNIT_ASSERT_MESSAGE(inss.str(), (existInDB));
//}
void
DataMgmtBaseTestSuite::FilledMultiPageDbFileSizeTruncF6()
{
    string baseName = "dmtcF6";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcF6key";
    string value       = "tcF6dummy";
    int numPages = 4;
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, numPages, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC F6: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC F6: ";

    // get size of the DB file before truncation
    int b4trunc_size = getFileSize(dbName);

    mdbm_truncate(dbh);

    // get after truncation file size
    int aftertrunc_size = getFileSize(dbName);
    stringstream szss;
    szss << SUITE_PREFIX()
         << "TC F6: Filled Multi Page DB: File size before truncation=" << b4trunc_size
         << " should be much bigger than after truncation=" << aftertrunc_size
         << " Number of pages before truncation=" << numPages
         << " Number of entries added before truncation=" << numRecsAdded << endl;
    CPPUNIT_ASSERT_MESSAGE(szss.str(), (b4trunc_size > aftertrunc_size));
}
void
DataMgmtBaseTestSuite::EmptyDbNotDefaultsPurgeG1()
{
    string baseName = "dmtcG1";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int presetPageSize  = MDBM_PAGESIZ / 2;  // divide the default pagesize in half
    int dbret = dbh.Open(openflags, 0644, presetPageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC G1: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    int defAlignVal = MDBM_ALIGN_8_BITS;
    int defHashID   = MDBM_DEFAULT_HASH;
    verifyDefaultConfig(dbh, prefix, false, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::PartFilledDbNotDefaultsPurgeG2()
{
    string baseName = "dmtcG2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcG2key";
    string value       = "tcG2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC G2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC G2: ";
    verifyDefaultConfig(dbh, prefix, false, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::FilledSinglePagedDbNotDefaultsPurgeG3()
{
    string baseName = "dmtcG3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcG3key";
    string value       = "tcG3dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC G3: FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC G3: ";
    verifyDefaultConfig(dbh, prefix, false, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::FilledMultiPagedDbNotDefaultsPurgeG4()
{
    string baseName = "dmtcG4";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcG4key";
    string value       = "tcG4dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 4, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC G4: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC G4: ";
    verifyDefaultConfig(dbh, prefix, false, defAlignVal, defHashID);
}
void
DataMgmtBaseTestSuite::FilledSinglePagedDbNonDefsAndShakeFuncPurgeG5()
{
    string baseName = "dmtcG5";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcG5key";
    string value       = "tcG5dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);
    uint64_t count_orig = mdbm_count_records(dbh);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC G5: FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "TC G5: ";
    shakernew_funcp_t shaker = shakerTCG5;
    string shakeKey = makeKeyName(1, keyBaseName);
    verifyDefaultConfig(dbh, prefix, false, defAlignVal, defHashID, 2, shaker, (void*)shakeKey.c_str());

    uint64_t size_prior = mdbm_get_size(dbh);
    uint64_t count_prior = mdbm_count_records(dbh);
    // now lets refill it with numRecsAdded * 5; try to go over limits
    // why 5? because verifyDefaultConfig will set the page limit to 2
    // (since the original limit set was 1)
    int refillAddCnt = 0;
    for (; refillAddCnt < (numRecsAdded * 20); ++refillAddCnt) {
        string dkey = makeKeyName(refillAddCnt, keyBaseName);
        int len = value.size() + 1;
        int ret = store(dbh, dkey.c_str(), const_cast<char*>(value.c_str()), len, 0);
        if (ret) {
          fprintf(stderr, "STORE returned %d for key %d\n", ret,refillAddCnt);
        }
        if (ret == -1) {
            break;
        }
    }
    uint64_t size_post = mdbm_get_size(dbh);
    uint64_t count_post = mdbm_count_records(dbh);

    fprintf(stderr, "MDBM SIZE went from %lu to %lu after %d entries (orig:%lu ent-prior:%lu post:%lu)\n", 
        size_prior, size_post, refillAddCnt, count_orig, count_prior, count_post);

    // Should only be able to add twice the number of entries as first time
    // since verifyDefaultConfig performs mdbm_limit_size with twice the number of pages
    stringstream adss;
    adss << SUITE_PREFIX()
         << "TC G5: FAILed to refill DB=" << dbName
         << " properly. Configuration information for number of pages may have been lost." << endl
         << " Since an mdbm_limit_size was done with the number of"
         << " allowed pages = 2, we should only be able to fill the DB with 2"
         << " times the number of entries using same keys and values." << endl
         << " Original number of records=" << numRecsAdded
         << " : Refill number of records=" << refillAddCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(adss.str(), (refillAddCnt >= ((numRecsAdded*2)-2) && (refillAddCnt < (numRecsAdded*5))));

    MDBM_ITER iter;
    datum key = mdbm_firstkey_r(dbh, &iter);
    stringstream itss;
    itss << SUITE_PREFIX()
         << "TC G5: FAILed to refill DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(itss.str(), (key.dsize > 0));

    // confirm the shaker was used to remove a known key
    bool existInDB = false;
    datum valunused;
    while (key.dsize > 0) {
        key = mdbm_nextkey_r(dbh, &iter);
        int found = shaker(dbh, &key, &valunused, 0);
        existInDB = existInDB ? existInDB : found == 1;
    }
    stringstream inss;
    inss << SUITE_PREFIX()
         << "TC G5: Found key inserted in DB=" << dbName
         << " but it should NOT have been " << endl
         << ": Original number of records=" << numRecsAdded
         << " : Refill number of records=" << refillAddCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(inss.str(), (existInDB == false));
}

// Find a full or partial match of the param in the given key.
// if found, then key should be deleted.
// RETURN: 1 == delete; 0 == retain
static int pruner(MDBM *dbh, datum key, datum val, void *param)
{
    const char *matcher = (const char *)param;
    string keytcH(key.dptr, key.dsize);
    if (keytcH.find(matcher) != string::npos)
    {
        return 1;
    }
    return 0;
}
void
DataMgmtBaseTestSuite::AddUniqueKeyPrunerMatchesH1()
{
    // fill a DB - keep one key as the prune-matcher
    string baseName = "dmtcH1";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcH1key";
    string value       = "tcH1dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 1, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC H1: FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // fetch the key from the DB to prove it is there
    string matcherkey = keyBaseName;
    matcherkey += "005";  // choose arbitrary known key
    string val;
    string fmsg = "TCH1: Fetch should have found the key=";
    fmsg += matcherkey;
    int ret = fetch(dbh, matcherkey, val, fmsg.c_str());
    (void)ret; // unused, make compiler happy

    // perform the prune with the specified prune-matching key
    mdbm_prune(dbh, pruner, const_cast<char*>(matcherkey.c_str()));

    // perform fetch again to prove it is no longer in the DB
    fmsg = "TC H1: Fetch should NOT have found the key=";
    fmsg += matcherkey;
    fmsg += " after mdbm_purge";
    ret = fetch(dbh, matcherkey, val, fmsg.c_str());
}
void
DataMgmtBaseTestSuite::AddDuplicateKeysPrunerMatchesH2()
{
    // add a bunch of records and add some duplicate records
    string baseName = "dmtcH2";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcH2key";
    string value       = "tcH2dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC H2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // now add some duplicates
    string dupKeyName = "tcH2dup001";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    datum dval;
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size();
    stringstream dupss;
    dupss << SUITE_PREFIX()
          << "TC H2: FAILed to insert a duplicate key=" << dupKeyName
          << " in the DB=" << dbName;
    int dupcnt;
    for (dupcnt = 0; dupcnt < 3; ++dupcnt)
    {
        int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP);
        if (ret != 0)
        {
            dupss << " Number of duplicates inserted=" << dupcnt << endl;
            CPPUNIT_ASSERT_MESSAGE(dupss.str(), (ret == 0));
        }
    }

    // now add some more non duplicate records
    for (int cnt = 50; cnt < 60; ++cnt)
    {
        string dkey = makeKeyName(cnt, keyBaseName);
        int len = value.size();
        if (store(dbh, dkey.c_str(), const_cast<char*>(value.c_str()), len, 0) == -1)
        {
            break;
        }
    }

    // prune the duplicate records with the specified prune-matching key
    mdbm_prune(dbh, pruner, const_cast<char*>(dupKeyName.c_str()));

    // verify these records are deleted from the DB
    datum fdata = mdbm_fetch(dbh, dkey);
    stringstream fdss;
    fdss << SUITE_PREFIX()
         << "TC H2: Fetched a duplicate key=" << dupKeyName
         << " from the DB=" << dbName
         << " but is should have been pruned!" << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize == 0));
}

// Find a full or partial match of the param in the given value.
// if found, then the record should be deleted.
// RETURN: 1 == delete; 0 == retain
static int prunevals(MDBM *dbh, datum key, datum val, void *param)
{
    const char *matcher = (const char*)param;
    string valtcH(val.dptr, val.dsize);
    if (valtcH.find(matcher) != string::npos)
    {
        return 1;
    }
    return 0;
}
void
DataMgmtBaseTestSuite::AddUniqueValuePrunerMatchesH3()
{
    // add a bunch of records and add some duplicate records
    string baseName = "dmtcH3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcH3key";
    string value       = "tcH3dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC H3: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // now add some more records all with unique values
    vector<string> valvec;
    vector<string> keyvec;
    for (int cnt = 30; cnt < 60; ++cnt)
    {
        string key = makeKeyName(cnt, keyBaseName);
        string val = key;
        val += "uq";
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
        if (cnt % 10 == 0)
        {
            valvec.push_back(val);
            keyvec.push_back(key);
        }
    }
    // HAVE: added 30 records, and kept track of 3 to verify pruning

    // prune the duplicate records with the specified prune-matching key
    stringstream fdss;
    fdss << SUITE_PREFIX()
         << "TC H3: Fetched a unique value=";
    for (size_t cnt = 0; cnt < valvec.size(); ++cnt)
    {
        mdbm_prune(dbh, prunevals, const_cast<char*>(valvec[cnt].c_str()));

        // verify these records are deleted from the DB
        datum dkey;
        dkey.dptr  = const_cast<char*>(keyvec[cnt].c_str());
        dkey.dsize = keyvec[cnt].size();
        datum fdata = mdbm_fetch(dbh, dkey);
        if (fdata.dsize > 0)
        {
            fdss << valvec[cnt]
                 << " with key=" << keyvec[cnt]
                 << " from the DB=" << dbName
                 << " but it should have been pruned by value!" << endl;
            CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize == 0));
        }
    }
}
void
DataMgmtBaseTestSuite::FilledSinglePageDbAddDupValPrunerMatchesH4()
{
    // fill the DB, make several records as duplicate values
    string baseName = "dmtcH4";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcH4key";
    string value       = "tcH4dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC H4: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    mdbm_limit_size_v3(dbh, 1, 0, NULL); // limit to 1 page and no shake function
    string matchval = "jupiter";
    int matchcnt = 0;
    for (int cnt = 50; cnt < 100; ++cnt)
    {
        string key = makeKeyName(cnt, keyBaseName);
        string val = key;
        val += "uq";
        if (cnt % 5 == 0)
        {
            val = matchval;
            ++matchcnt;
        }
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
    }
    // HAVE: filled DB with several records using same value (matchval)

    mdbm_prune(dbh, prunevals, const_cast<char*>(matchval.c_str()));

    // lets iterate the DB and look for that value
    MDBM_ITER iter;
    kvpair kv = mdbm_first_r(dbh, &iter);
    stringstream fkss;
    fkss << SUITE_PREFIX()
         << "TC H4: No data in the DB=" << dbName
         << " : Everything was pruned!"
         << " : Number of matching values set in DB=" << matchcnt << endl;
    CPPUNIT_ASSERT_MESSAGE(fkss.str(), (kv.key.dsize > 0));

    stringstream fss;
    fss << SUITE_PREFIX()
         << "TC H4: Found a matching value=" << matchval
         << " which should have been pruned from the DB=" << dbName << endl
         << " Number of matching values set in DB=" <<  matchcnt << endl;
    while (kv.key.dsize > 0 && kv.val.dsize > 0)
    {
        kv = mdbm_next_r(dbh, &iter);
        // lets check the value against matchval - assert if found a match
        string value(kv.val.dptr, kv.val.dsize);
        bool found = (value.find(matchval) != string::npos);
        CPPUNIT_ASSERT_MESSAGE(fss.str(), (found == false));
    }
}
void
DataMgmtBaseTestSuite::FilledMultiPageDbAddDupValPrunerMatchesH5()
{
    // just like H4 but with multiple pages rather than limited to 1
    string baseName = "dmtcH5";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcH5key";
    string value       = "tcH5dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 0, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC H5: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    mdbm_limit_size_v3(dbh, 4, 0, NULL); // limit to 4 pages and no shake function
    string matchval = "jupiter";
    int matchcnt = 0;
    for (int cnt = 100; cnt < 1000; ++cnt)
    {
        string key = makeKeyName(cnt, keyBaseName);
        string val = key;
        val += "uq";
        if (cnt % 5 == 0)
        {
            val = matchval;
            ++matchcnt;
        }
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
    }
    // HAVE: filled DB with several records using same value (matchval)

    mdbm_prune(dbh, prunevals, const_cast<char*>(matchval.c_str()));

    // lets iterate the DB and look for that value
    MDBM_ITER iter;
    kvpair kv = mdbm_first_r(dbh, &iter);
    stringstream fkss;
    fkss << SUITE_PREFIX()
         << "TC H5: No data in the DB=" << dbName
         << " : Everything was pruned!"
         << " : Number of matching values set in DB=" << matchcnt << endl;
    CPPUNIT_ASSERT_MESSAGE(fkss.str(), (kv.key.dsize > 0));

    stringstream fss;
    fss << SUITE_PREFIX()
         << "TC H5: Found a matching value=" << matchval
         << " which should have been pruned from the DB=" << dbName << endl
         << " Number of matching values set in DB=" <<  matchcnt << endl;
    while (kv.key.dsize > 0 && kv.val.dsize > 0)
    {
        kv = mdbm_next_r(dbh, &iter);
        // lets check the value against matchval - assert if found a match
        string value(kv.val.dptr, kv.val.dsize);
        bool found = (value.find(matchval) != string::npos);
        CPPUNIT_ASSERT_MESSAGE(fss.str(), (found == false));
    }
}
void
DataMgmtBaseTestSuite::FilledMultiPageDbPrunerDestroyAllH6()
{
    string baseName = "dmtcH6";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcH6key";
    string value       = "tcH6dummy";
    int defAlignVal;
    int defHashID;
    int numRecsAdded = createAndAddData(dbh, 4, keyBaseName, value.c_str(), defAlignVal, defHashID);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC H6: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    // lets prune the whole thing by matching the base name of the keys
    mdbm_prune(dbh, pruner, const_cast<char*>(keyBaseName.c_str()));

    // lets iterate the DB to determine it is empty
    MDBM_ITER iter;
    kvpair kv = mdbm_first_r(dbh, &iter);
    stringstream fkss;
    fkss << SUITE_PREFIX()
         << "TC H6: Data exists in the DB=" << dbName
         << " : NOT Everything was pruned!"
         << " : Number of matching values set in DB=" << numRecsAdded << endl;
    CPPUNIT_ASSERT_MESSAGE(fkss.str(), (kv.key.dsize == 0));
}

static int cleanvalofsize(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    (void)dkey; // make pedantic compiler happy
    if (!matcher || !matcher->user_data) {
        return 0;
    }

    int len = *(int*)matcher->user_data;
    if (len == dval->dsize) {
        if (quit) {
            *quit = 1;
        }
        return 1;
    }

    return 0;
}
static int valclean(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    (void)dkey; // make pedantic compiler happy
    (void)quit; // make pedantic compiler happy
    if (!matcher || !matcher->user_data) {
        return 0;
    }

    string val(dval->dptr, dval->dsize);
    if (val.find((const char*)matcher->user_data) != string::npos) {
        return 1;
    }

    return 0;
}

static int keyclean(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    if (!matcher || !matcher->user_data) {
        return 0;
    }

    string key;
    if (dkey->dptr && dkey->dsize) {
      key.assign(dkey->dptr, dkey->dsize);
    }
    //fprintf(stderr, "=*=*=* CHECKING KEY (%s) vs (%s) *=*=*=\n", key.c_str(), (char*)matcher->user_data);
    if (key.find((const char*)matcher->user_data) != string::npos) {
        //fprintf(stderr, "=*=*=* KEY MATCHED *=*=*=\n");
        return 1;
    }

    return 0;
}
static int keycleanandquit(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    int ret = keyclean(dbh, dkey, dval, matcher, quit);
    if (quit) {
        if (ret || !matcher || !matcher->user_data) {
            *quit = 1;
        }
    }

    return ret;
}
static int keycleananddontquit(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    int ret = keyclean(dbh, dkey, dval, matcher, quit);
    return ret;
}
static int cleananything(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    if (quit)
    {
        *quit = 0;
    }

    return 1;
}
static int cleanerthatasserts(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    // ohoh assert
    string failmsg = (const char*)matcher->user_data;
    failmsg += " This cleaner should NOT have been called";
    CPPUNIT_ASSERT_MESSAGE(failmsg, (false));
    return 1;
}
static int multikeyclean(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_clean_data *matcher, int *quit)
{
    static string nextkey; // we put this in a persistent structure below...
    int ret = keyclean(dbh, dkey, dval, matcher, quit);
    if (!ret)
    {
        nextkey = (const char*)matcher->user_data;
        char numchar = nextkey[nextkey.size() - 1];
        nextkey[nextkey.size() - 1] = ++numchar;
        matcher->user_data = (void*)nextkey.c_str();
        ret = keyclean(dbh, dkey, dval, matcher, quit);
    }
    if (quit)
    {
        if (ret || !matcher || !matcher->user_data)
        {
            *quit = 1;
        }
    }

    return ret;
}

int
DataMgmtBaseTestSuite::createCacheModeDB(string &prefix, MdbmHolder &dbh, string &keyBaseName, string &value, int limitNumPages, mdbm_clean_func cleanerfunc, void *cleanerdata)
{
    // create a DB
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbret == 0));

    // set cleaner function that cleans anything
    dbret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    if (cleanerfunc) {
        mdbm_set_cleanfunc(dbh, cleanerfunc, cleanerdata);
    }

    // limit size of the DB
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit number of pages and no shake function

    // Fill the DB - prevent infinite loop due to cache clean feature
    // We will do this by limiting the number of entries to number of pages * 50
    // Since we know we could never enter 50 entries to a DB of 1 page
    // It will require iteration afterwards to get the entry count
    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt) {
        string key = makeKeyName(cnt, keyBaseName);
        if (store(dbh, key.c_str(), const_cast<char*>(value.c_str()), value.size()+1, 0) == -1) {
            break;
        }
    }


    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    kvpair kv = mdbm_first_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; kv.key.dsize > 0 && kv.val.dsize > 0; ++entrycnt) {
        kv = mdbm_next_r(dbh, &iter);
    }

    return entrycnt;
}
void
DataMgmtBaseTestSuite::SetCleanerCleansAllThenLimitSizeDbI1()
{
    // create a DB
    string baseName    = "dmtcI1";
    string dbName      = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI1key";
    string value       = "tcI1dummy";
    string prefix      = "TC I1: ";
    int limitNumPages  = 1;
    int cnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages, cleananything, 0);

    uint64_t numPages = mdbm_get_limit_size(dbh) / 512;

    stringstream fdss;
    fdss << SUITE_PREFIX() << prefix
         << "Attempted to fill the DB with number of entries=" << cnt
         << " Although we limited the DB to num pages=" << limitNumPages
         << " The DB has number of pages=" << numPages
         << endl;
    CPPUNIT_ASSERT_MESSAGE(fdss.str(), (uint64_t(cnt) < (numPages*50) && numPages <= uint64_t(limitNumPages+1)));
}
void
DataMgmtBaseTestSuite::FillDbCleanerCleansNothingStoreDataI2()
{
    // fill a single page DB
    string baseName = "dmtcI2";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I2: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI2key";
    string value       = "tcI2dummy";
    int limitNumPages  = 1;
    int cnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (cnt > 0));

    // set a clean function that cant clean anything
    mdbm_clean_data cleandata;
    cleandata.user_data = 0;
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    // try and store some data 
    int ret = store(dbh, keyBaseName, keyBaseName);
    stringstream errss;
    errss << SUITE_PREFIX()
          << "TC I2: Store should pass, the DB is full and no Cleaner set, but cache mode drops entries"
           << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillDbCleanerCleanKeyQuitStoreSameSizedDataI3()
{
    string baseName = "dmtcI3";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I3: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI3key";
    string value       = "tcI3dummy";
    int limitNumPages  = 4;
    createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);

    // set a clean function that cleans a given key and quits
    mdbm_clean_data cleandata;
    // Note: key:164 was empirically found to map to the same page as key:7
    string keymatch = makeKeyName(164, keyBaseName);
    string storekey = makeKeyName(7, keyBaseName);
    cleandata.user_data = (void*)keymatch.c_str();
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    // try and store some data - expect it to succeed
    datum dkey;
    dkey.dptr  = const_cast<char*>(storekey.c_str());
    dkey.dsize = storekey.size();
    datum dval;
    value[0] = '7';
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size() + 1;
    int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    stringstream errss;
    errss << SUITE_PREFIX()
          << "TC I3: Store should succeed though the DB is full, Cleaner was set for key=" << keyBaseName
          << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));

    datum fdata = mdbm_fetch(dbh, dkey);
    if (fdata.dsize == 0) {
        stringstream fdss;
        fdss << SUITE_PREFIX()
             << " Store returned success with key=" << keymatch
             << " for the DB=" << dbName
             << " but fetch did not retrieve this key!" << endl;
        CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize > 0));
    }
}
void
DataMgmtBaseTestSuite::FillDbCleanerCleanKeyQuitStoreBiggerSizedDataI4()
{
    string baseName = "dmtcI4";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I4: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI4key";
    string value       = "tcI4dummy";
    int limitNumPages  = 4;
    createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);

    // set a clean function that cleans a given key and quits
    mdbm_clean_data cleandata;
    string keymatch = makeKeyName(7, keyBaseName);
    cleandata.user_data = (void*)keymatch.c_str();
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    //mdbm_dump_all_page(dbh);
    // try and store some data - expect it to fail
    datum dkey;
    dkey.dptr  = const_cast<char*>(keymatch.c_str());
    dkey.dsize = keymatch.size();
    datum dval;
    value[0] = '7';
    size_t origSize = value.size() + 1;
    value.append("a whole lot of extra data to make it way too large to fit in the page despite the old entry being cleaned up by the registered clean function");
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size() + 1;
    int ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    stringstream errss;
    errss << SUITE_PREFIX()
          << "TC I4: Store should pass. The DB is full and the Cleaner was set for key=" << keyBaseName
          << " which cointained a value of size=" << origSize
          << " But the new value for the same key is bigger=" << dval.dsize << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));

    datum fdata = mdbm_fetch(dbh, dkey);
    if (fdata.dsize == 0) {
        stringstream fdss;
        fdss << SUITE_PREFIX()
             << " Store returned success with key=" << keymatch
             << " for the DB=" << dbName
             << " but fetch did not retrieve this key!" << endl;
        CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize > 0));
    }
}
void
DataMgmtBaseTestSuite::CreateDbNODIRTYflagCleanerCleanKeyQuitStoreDataI5()
{
    // Create DB using MDBM_NO_DIRTY flag
    // This means that the clean function should not be called
    // We determine this by using a callback cleaner that will assert if called
    string baseName = "dmtcI5";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I5: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI5key";
    string value       = "tcI5dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_NO_DIRTY | versionFlag;
    int dbret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbret == 0));

    dbret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages  = 2;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit to 2 pages and no shake function

    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        string key = makeKeyName(cnt, keyBaseName);
        string val = key;
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; dkey.dsize > 0; ++entrycnt)
    {
        dkey = mdbm_nextkey_r(dbh, &iter);
    }

    // set clean function that will assert if called
    mdbm_clean_data cleandata;
    stringstream clss;
    clss << SUITE_PREFIX() << prefix
         << "Created DB=" << dbName
         << " with number of entries=" << cnt << endl
         << "Since DB was opened with MDBM_NO_DIRTY flag, the cleaner function should NOT be called" << endl;
    cleandata.user_data = (void*)clss.str().c_str();
    mdbm_set_cleanfunc(dbh, cleanerthatasserts, &cleandata);

    // store using a new key and value
    string key = keyBaseName;
    key += "aaa";
    string val = key;
    int ret = store(dbh, key.c_str(), const_cast<char*>(key.c_str()), val.size()+1, 0);

    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << " Expected the store to Succeed" << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillMultiPageDbUniqueKeysCleanKeyQuitStoreKeyI6()
{
    string baseName = "dmtcI6";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I6: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI6key";
    string value       = "tcI6dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int dbret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbret == 0));

    dbret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages  = 2;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit to 2 pages and no shake function

    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        string key = makeKeyName(cnt, keyBaseName);
        string val = key;
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; dkey.dsize > 0; ++entrycnt)
    {
        dkey = mdbm_nextkey_r(dbh, &iter);
    }

    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)keyBaseName.c_str();
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    // store using a new key and value
    string key = keyBaseName;
    key += "aaa";
    string val = key;
    int ret = store(dbh, key.c_str(), const_cast<char*>(key.c_str()), val.size()+1, 0);

    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << " Expected the store to Succeed" << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillMultiPageDbDuplicateKeysCleanKeyPageStoreDupKeysI7()
{
    string baseName = "dmtcI7";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I7: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI7key";
    string value       = "tcI7dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages  = 2;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit to 2 pages and no shake function

    string dupKeyName = "tcI7dup001";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    datum dval;
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size();
    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; itkey.dsize > 0; ++entrycnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)keyBaseName.c_str();
    mdbm_set_cleanfunc(dbh, keycleananddontquit, &cleandata);

    // store using a new key and value
    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << " Expected the store(duplicate key) to Succeed"
         << " for DB=" << dbName << endl
         << " containing number of entries=" << entrycnt << endl;
    for (int cnt = 0; cnt < 3; ++cnt)
    {
        ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP);

        CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
    }
}
void
DataMgmtBaseTestSuite::FillMultiPageDbDuplicateKeysCleanKeyPageInsertUniqueKeyI8()
{
    string baseName = "dmtcI8";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I8: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI8key";
    string value       = "tcI8dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages  = 2;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit to 2 pages and no shake function

    string dupKeyName = "tcI8dup001";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    datum dval;
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size();
    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; itkey.dsize > 0; ++entrycnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)keyBaseName.c_str();
    mdbm_set_cleanfunc(dbh, keycleananddontquit, &cleandata);

    // store using a new key and value
    string uniqueName = "tcI8uq999";
    dkey.dptr  = const_cast<char*>(uniqueName.c_str());
    dkey.dsize = uniqueName.size();
    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << " Expected the store(INSERT of unique key)=" << uniqueName
         << " to Succeed for DB=" << dbName << endl
         << " containing number of entries=" << entrycnt << endl;

   ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);

   CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillMultiPageDbDuplicateKeysCleanKeyPageStoreDupNewKeyI9()
{
    string baseName = "dmtcI9";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I9: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI9key";
    string value       = "tcI9dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages  = 2;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit to 2 pages and no shake function

    string dupKeyName = keyBaseName;
    dupKeyName += "dup";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    datum dval;
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size();
    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; itkey.dsize > 0; ++entrycnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)keyBaseName.c_str();
    mdbm_set_cleanfunc(dbh, keycleananddontquit, &cleandata);

    // store using a new key and value
    string newName = "tcI9newkey";
    dkey.dptr  = const_cast<char*>(newName.c_str());
    dkey.dsize = newName.size();
    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << " Expected the store(INSERT duplicate of new key)=" << newName
         << " to Succeed for DB=" << dbName << endl
         << " containing number of entries=" << entrycnt << endl;
    for (int cnt = 0; cnt < 3; ++cnt)
    {
        ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP);

        CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
    }
}

// This shaker wont actually free space, it will just report a match was
// found. That way, since not enough space was freed, the clean function
// should be called to try and free space.
// RETURN 1 == match, else 0
static int shakev3(MDBM *dbh, const datum *dkey, const datum *dval, struct mdbm_shake_data_v3 *pagedata)
{
    const char *keyname = (const char*)pagedata->user_data;
    int klen = strlen(keyname);
    // traverse the page and mark the entry matching keyname as deleted
    for (uint32_t cnt = 0; cnt < pagedata->page_num_items; ++cnt)
    {
        kvpair *page_item = pagedata->page_items + cnt;
        if (page_item->key.dsize < klen)
        {
            continue;
        }

        string key(page_item->key.dptr, page_item->key.dsize);
        if (key.find(keyname) != string::npos)
        {
            // force library to try the clean function since doesnt free space
            // page_item->key.dsize = 0;
            return 1;
        }
    }
    return 0;
}

void
DataMgmtBaseTestSuite::FillMultiPageDbLimitWithShakerSetCleanStoreBigDataI10()
{
    // create a DB and place some smaller data and some bigger data
    string baseName = "dmtcI10";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I10: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI10key";
    string value       = "tcI10dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit DB size and specify a shaker that only cleans small data
    int limitNumPages  = 2;
    string smallkey = "tcI10smallkey";
    mdbm_limit_size_v3(dbh, limitNumPages, shakev3, const_cast<char*>(smallkey.c_str()));

    string smallval = "small";
    string bigkey   = "tcI10bigkey";
    string bigval   = "biggysmallswasverybig";
    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        // alternate adding small and big values
        bool insertsmall = (cnt % 2 == 0);
        string key = makeKeyName(cnt, (insertsmall ? smallkey : bigkey));
        string val = (insertsmall ? smallval : bigval);
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
    }

    // set cleaner that cleans small data and quits
    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)smallkey.c_str();
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    // insert data item that is at least twice size of small data
    string newkey = keyBaseName;
    newkey += "2timesbigger";
    ret = store(dbh, newkey.c_str(), const_cast<char*>(bigval.c_str()), bigval.size()+1, 0);

    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << " Expected the store(INSERT new key)=" << newkey
         << " to Succeed for DB=" << dbName << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));
}

int dummyCleanFunc(MDBM *, const datum*, const datum*, struct mdbm_clean_data *, int* quit) {
  *quit = 0;
  return 1;
}

void
DataMgmtBaseTestSuite::FillDbDontSetCleanerFuncThenCleanAllJ1()
{
    string baseName = "dmtcJ1";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J1: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ1key";
    string value       = "tcJ1dummy";
    int limitNumPages  = 1;
    int cnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages, dummyCleanFunc);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled single paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (cnt > 0));

    // although we have set cache modes, we are not going to set a cleaner function
    int ret = mdbm_clean(dbh, -1, 0);

    stringstream clss;
    clss << SUITE_PREFIX() << prefix
         << "Filled DB with number of pages=" << limitNumPages
         << " and number of entries=" << cnt << endl
         << "Specified cleaning page number=-1 which means all pages but mdbm_clean FAILed" <<endl;
    CPPUNIT_ASSERT_MESSAGE(clss.str(), (ret != -1));
}


void
DataMgmtBaseTestSuite::FillDb45pagesCleanPage32J2()
{
    string baseName = "dmtcJ2";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J2: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ2key";
    string value       = "tcJ2dummy";
    int limitNumPages  = 64;
    int cnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages, dummyCleanFunc);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (cnt > 0));

    {
      int cnt = limitNumPages * 50;
      int maxCnt = cnt*2;
      for (; cnt < maxCnt; ++cnt) {
          string key = makeKeyName(cnt, keyBaseName);
          if (store(dbh, key.c_str(), const_cast<char*>(value.c_str()), value.size()+1, 0) == -1) {
              break;
          }
      }
    }

    // although we have set cache modes, we are not going to set a cleaner function
    int ret = mdbm_clean(dbh, 32, 0);

    stringstream clss;
    clss << SUITE_PREFIX() << prefix
         << "Filled DB with number of pages=" << limitNumPages
         << " and number of entries=" << cnt << endl
         << "Specified cleaning page number=32 should be valid page number but mdbm_clean FAILed" <<endl;
    CPPUNIT_ASSERT_MESSAGE(clss.str(), (ret != -1));
}
void
DataMgmtBaseTestSuite::EmptyDbCleanerCleansAnyCleanPage0J3()
{
    // create empty DB
    string baseName = "dmtcJ3";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int presetPageSize  = MDBM_PAGESIZ / 2;  // divide the default pagesize in half
    int dbret = dbh.Open(openflags, 0644, presetPageSize, 0);
    string prefix = SUITE_PREFIX();
    prefix += "TC J3: Empty DB Test: ";
    CPPUNIT_ASSERT_MESSAGE(prefix, (dbret != -1));

    // set cleaner that cleans anything
    dbret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);
    mdbm_set_cleanfunc(dbh, cleananything, 0);

    // specify page 0
    int ret = mdbm_clean(dbh, 0, 0);
    stringstream clss;
    clss << prefix
         << "Created an empty DB=" << dbName << endl
         << "Specified cleaning page number=0. Should return but mdbm_clean returned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(clss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::MultiPageDbCleanerCleansAnyThenLoopAndCleanJ4()
{
    // fill DB - number pages = 5
    string baseName = "dmtcJ4";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J4: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ4key";
    string value       = "tcJ4dummy";
    int limitNumPages  = 5;
    int entrycnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << " with number of pages=" << limitNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (entrycnt > 0));

    // set cleaner that cleans anything
    mdbm_set_cleanfunc(dbh, cleananything, 0);

    // loop thru each page and clean it
    int sumNumCleaned = 0;
    for (int pageNum = 0; pageNum <= limitNumPages; ++pageNum)
    {
        int ret = mdbm_clean(dbh, pageNum, 0);
        if (ret > 0)
        {
            sumNumCleaned += ret;
        }
    }

    stringstream clss;
    clss << SUITE_PREFIX() << prefix
         << "FAILed to clean all entries in DB=" << dbName << endl
         << "Which was limited to number of pages=" << limitNumPages << endl
         << "It contained number of entries=" << entrycnt
         << " but mdbm_clean cleaned number of entries=" << sumNumCleaned << endl;

    CPPUNIT_ASSERT_MESSAGE(clss.str(), (sumNumCleaned == entrycnt));
}
void
DataMgmtBaseTestSuite::FillDbCleanerCleansAnyCleanAllRefillDbJ5()
{
    // fill DB - number pages = 5
    string baseName = "dmtcJ5";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J5: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ5key";
    string value       = "tcJ5dummy";
    int limitNumPages  = 5;
    int entrycnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << " with number of pages=" << limitNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (entrycnt > 0));

    // set cleaner that cleans anything
    mdbm_set_cleanfunc(dbh, cleananything, 0);

    int ret = mdbm_clean(dbh, -1, 0);

    stringstream clss;
    clss << SUITE_PREFIX() << prefix
         << "FAILed to clean all entries in DB=" << dbName << endl
         << "Which was limited to number of pages=" << limitNumPages << endl
         << "It contained number of entries=" << entrycnt
         << " but mdbm_clean cleaned number of entries=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(clss.str(), (ret == entrycnt));

    // now refill
    string newkey = keyBaseName;
    newkey[0]     = 'N';
    string newval = value;
    newval[0]     = 'N';
    int cnt;
    int maxCnt = entrycnt + 10;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        string key = makeKeyName(cnt, newkey);
        if (store(dbh, key.c_str(), const_cast<char*>(newval.c_str()), newval.size()+1, 0) == -1)
        {
            break;
        }
    }

    // gotta iterate to count real number of entries
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    for (cnt = 0; itkey.dsize > 0; ++cnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    stringstream rfss;
    rfss << SUITE_PREFIX() << prefix
         << "FAILed to refill DB=" << dbName << endl
         << "It originally contained number of entries=" << entrycnt
         << " but was refilled with number of entries=" << cnt << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (cnt == entrycnt));
}
void
DataMgmtBaseTestSuite::FillDbCleanerCleansNothingCleanAllJ6()
{
    // fill DB
    string baseName = "dmtcJ6";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J6: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ6key";
    string value       = "tcJ6dummy";
    int limitNumPages  = 2;
    int entrycnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << " with number of pages=" << limitNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (entrycnt > 0));

    // set cleaner that cleans nothing
    mdbm_set_cleanfunc(dbh, keyclean, 0);

    int ret = mdbm_clean(dbh, -1, 0);

    stringstream clss;
    clss << SUITE_PREFIX() << prefix
         << "Should NOT have cleaned any entries in DB=" << dbName << endl
         << "The DB was limited to number of pages=" << limitNumPages << endl
         << "It contained number of entries=" << entrycnt
         << " but mdbm_clean cleaned number of entries=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(clss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillMultiPageDbCleanerCleansKeyCleanAllStoreKeyJ7()
{
    // fill DB
    string baseName = "dmtcJ7";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J7: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ7key";
    string value       = "tcJ7dummy";
    int limitNumPages  = 2;
    int entrycnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << " with number of pages=" << limitNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (entrycnt > 0));

    // set a clean function that cleans a given key and quits
    mdbm_clean_data cleandata;
    string keymatch = makeKeyName(7, keyBaseName);
    cleandata.user_data = (void*)keymatch.c_str();
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    int ret = mdbm_clean(dbh, -1, 0);

    // try and store some data - expect it to succeed
    datum dkey;
    dkey.dptr  = const_cast<char*>(keymatch.c_str());
    dkey.dsize = keymatch.size();
    datum dval;
    value[0] = '7';
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size() + 1;
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    stringstream errss;
    errss << SUITE_PREFIX() << prefix
          << "Store should have succeeded although the DB is full but the Cleaner was set for key=" << keyBaseName
           << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));

    datum fdata = mdbm_fetch(dbh, dkey);
    if (fdata.dsize == 0)
    {
        stringstream fdss;
        fdss << SUITE_PREFIX() << prefix
             << " Store returned success with key=" << keymatch
             << " for the DB=" << dbName
             << " but fetch did not retrieve this key!" << endl;
        CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize > 0));
    }
}
void
DataMgmtBaseTestSuite::FillMultiPageDbCleanerCleansKeyCleanUsingGetPageKeyStoreKeyJ8()
{
    // fill DB
    string baseName = "dmtcJ8";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J8: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ8key";
    string value       = "tcJ8dummy";
    int limitNumPages  = 2;
    int entrycnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);

    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << " and number of pages=" << limitNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (entrycnt > 0));

    // set a clean function that cleans a given key and quits
    mdbm_clean_data cleandata;
    string keymatch = makeKeyName(7, keyBaseName);
    cleandata.user_data = (void*)keymatch.c_str();
    mdbm_set_cleanfunc(dbh, keycleanandquit, &cleandata);

    datum dkey;
    dkey.dptr  = const_cast<char*>(keymatch.c_str());
    dkey.dsize = keymatch.size();
    mdbm_ubig_t pageNum = mdbm_get_page(dbh, &dkey);
    int ret = mdbm_clean(dbh, (int)pageNum, 0);

    // try and store some data - expect it to succeed
    datum dval;
    value[0] = '7';
    dval.dptr  = const_cast<char*>(value.c_str());
    dval.dsize = value.size() + 1;
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT);
    stringstream errss;
    errss << SUITE_PREFIX() << prefix
          << "Store should have succeeded although the DB is full but the Cleaner was set for key=" << keyBaseName
           << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));

    datum fdata = mdbm_fetch(dbh, dkey);
    if (fdata.dsize == 0)
    {
        stringstream fdss;
        fdss << SUITE_PREFIX() << prefix
             << " Store returned success with key=" << keymatch
             << " for the DB=" << dbName
             << " but fetch did not retrieve this key!" << endl;
        CPPUNIT_ASSERT_MESSAGE(fdss.str(), (fdata.dsize > 0));
    }
}
void
DataMgmtBaseTestSuite::FillDbDuplicateKeysUniqueValsCleanerCleansValsStoreJ9()
{
    string baseName = "dmtcJ9";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J9: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ9key";
    string value       = "tcJ9dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages = 1;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, 0); // limit with no shake function

    // fill single page DB with duplicate keys but unique values
    string dupKeyName = keyBaseName;
    dupKeyName += "dup";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        datum dval;
        string val = makeKeyName(cnt, value);
        dval.dptr  = const_cast<char*>(val.c_str());
        dval.dsize = val.size();
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; itkey.dsize > 0; ++entrycnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    // set a clean function that cleans a specific value
    mdbm_clean_data cleandata;
    string valmatch = makeKeyName(9, value);
    cleandata.user_data = (void*)valmatch.c_str();
    mdbm_set_cleanfunc(dbh, valclean, &cleandata);

    ret = mdbm_clean(dbh, 0, 0); // only a 1 page DB so clean page 0

    // store duplicate key with unique value
    datum dval;
    string val = value;
    val += "jjj";
    dval.dptr  = const_cast<char*>(val.c_str());
    dval.dsize = val.size();
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP);
    stringstream errss;
    errss << SUITE_PREFIX() << prefix
          << "Store should have succeeded although the DB is full but the Cleaner was set for value=" << valmatch << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillDb0SizedValsCleanerCleansKeysStoreValSizeN_J10()
{
    // fill 1 page DB with 0 size values
    string baseName = "dmtcJ10";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J10: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ10key";
    string value       = "";
    int limitNumPages  = 1;
    int entrycnt = createCacheModeDB(prefix, dbh, keyBaseName, value, limitNumPages);

    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << "FAILed to create a filled multi paged DB=" << dbName
         << " and number of pages=" << limitNumPages << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (entrycnt > 0));

    // set clean function that cleans a couple of specific keys(K, L)
    mdbm_clean_data cleandata;
    string mkmatch = makeKeyName(3, keyBaseName);
    cleandata.user_data = (void*)mkmatch.c_str();
    mdbm_set_cleanfunc(dbh, multikeyclean, &cleandata);

    int ret = mdbm_clean(dbh, 0, 0); // only a 1 page DB so clean page 0

    // store key(K) with value of size n(size key K + size key L)
    string newval = "jupiter10";
    ret = store(dbh, mkmatch.c_str(), const_cast<char*>(newval.c_str()), newval.size()+1, 0);
    stringstream errss;
    errss << SUITE_PREFIX() << prefix
          << "Store should have succeeded although the DB is full but the Cleaner was set for key=" << mkmatch << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillDbDuplicateKeysMostValsSizeNminus1_StoreCleanSizeNStoreSizeN_J11()
{
    string baseName = "dmtcJ11";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J11: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ11key";
    string value       = "tcJ11dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages = 1;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, 0); // limit with no shake function

    // fill single page DB with duplicate keys but unique values
    string dupKeyName = keyBaseName;
    dupKeyName += "dup";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    int cnt;
    int maxCnt = limitNumPages * 50;
    int cleanlen = -1;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        datum dval;
        string val = makeKeyName(cnt, value);
        if (cnt == 5)
        {
            val += "J";
            cleanlen = val.size();
        }
        dval.dptr  = const_cast<char*>(val.c_str());
        dval.dsize = val.size();
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; itkey.dsize > 0; ++entrycnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)&cleanlen;
    mdbm_set_cleanfunc(dbh, cleanvalofsize, &cleandata);

    ret = mdbm_clean(dbh, 0, 0); // only a 1 page DB so clean page 0

    // store duplicate key with unique value of desired length
    datum dval;
    string val(cleanlen, 'j');
    dval.dptr  = const_cast<char*>(val.c_str());
    dval.dsize = val.size();
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP);
    stringstream errss;
    errss << SUITE_PREFIX() << prefix
          << "Store should have succeeded although the DB is full but the Cleaner was set for value length=" << cleanlen << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::FillDbDuplicateKeysMostValsSizeNminus1_StoreCleanSizeNStoreSizeNplus1_J12()
{
    string baseName = "dmtcJ11";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC J11: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcJ11key";
    string value       = "tcJ11dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    ret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages = 1;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, 0); // limit with no shake function

    // fill single page DB with duplicate keys but unique values
    string dupKeyName = keyBaseName;
    dupKeyName += "dup";
    datum dkey;
    dkey.dptr  = const_cast<char*>(dupKeyName.c_str());
    dkey.dsize = dupKeyName.size();
    int cnt;
    int maxCnt = limitNumPages * 50;
    int cleanlen = -1;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        datum dval;
        string val = makeKeyName(cnt, value);
        if (cnt == 5)
        {
            val += "J";
            cleanlen = val.size();
        }
        dval.dptr  = const_cast<char*>(val.c_str());
        dval.dsize = val.size();
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum itkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; itkey.dsize > 0; ++entrycnt)
    {
        itkey = mdbm_nextkey_r(dbh, &iter);
    }

    mdbm_clean_data cleandata;
    cleandata.user_data = (void*)&cleanlen;
    mdbm_set_cleanfunc(dbh, cleanvalofsize, &cleandata);

    ret = mdbm_clean(dbh, 0, 0); // only a 1 page DB so clean page 0

    // store duplicate key with unique value of desired length
    datum dval;
    string val(cleanlen + 1, 'j');
    dval.dptr  = const_cast<char*>(val.c_str());
    dval.dsize = val.size();
    ret = mdbm_store(dbh, dkey, dval, MDBM_INSERT_DUP);
    stringstream errss;
    errss << SUITE_PREFIX() << prefix
          << "Store should pass, the DB is full and the Cleaner was set for value length=" << cleanlen
          << " but value stored has length=" << dval.dsize << endl;
    CPPUNIT_ASSERT_MESSAGE(errss.str(), (ret == 0));
}
void
DataMgmtBaseTestSuite::CreateDbNODIRTYflagCleanerCleansAllCleanPage0J13()
{
    // Create DB using MDBM_NO_DIRTY flag
    // This means that the clean function should not be called
    // We determine this by using a callback cleaner that will assert if called
    string baseName = "dmtcI13";
    string dbName   = GetTmpName(baseName);
    string prefix   = "TC I13: ";
    MdbmHolder dbh(dbName);
    string keyBaseName = "tcI13key";
    string value       = "tcI13dummy";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | MDBM_NO_DIRTY | versionFlag;
    int dbret = dbh.Open(openFlags, 0644, 512, 0);
    int errnum = errno;
    stringstream cdss;
    cdss << SUITE_PREFIX() << prefix
         << " MdbmHolder:Open FAILed with errno=" << errnum
         << " to open DB=" << (const char*)dbh << endl;

    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbret == 0));

    dbret = mdbm_set_cachemode(dbh, MDBM_CACHEMODE_EVICT_CLEAN_FIRST|MDBM_CACHEMODE_GDSF);

    // limit size of the DB
    int limitNumPages  = 1;
    mdbm_limit_size_v3(dbh, limitNumPages, 0, NULL); // limit with no shake function

    int cnt;
    int maxCnt = limitNumPages * 50;
    for (cnt = 0; cnt < maxCnt; ++cnt)
    {
        string key = makeKeyName(cnt, keyBaseName);
        string val = key;
        if (store(dbh, key.c_str(), const_cast<char*>(val.c_str()), val.size()+1, 0) == -1)
        {
            break;
        }
    }

    // now lets count the real number of entries added to this DB
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(dbh, &iter);

    int entrycnt;
    for (entrycnt = 0; dkey.dsize > 0; ++entrycnt)
    {
        dkey = mdbm_nextkey_r(dbh, &iter);
    }

    // set clean function that will assert if called
    mdbm_set_cleanfunc(dbh, cleananything, 0);

    int ret = mdbm_clean(dbh, 0, 0); // only a 1 page DB so clean page 0
    stringstream stss;
    stss << SUITE_PREFIX() << prefix
         << "mdbm_clean should return -1 for DB=" << dbName << endl
         << "since it was opened with the MDBM_NO_DIRTY flag, but instead it returned number of entries cleaned=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == -1));
}

void
DataMgmtBaseTestSuite::ErrorCasesMdbmSetCleanFun()
{
    string prefix = SUITE_PREFIX();
    prefix += "TC : NULL MDBM DB Test: ";
    MDBM *mdbm = NULL;
    TRACE_TEST_CASE(prefix);

    CPPUNIT_ASSERT_EQUAL(-1, mdbm_set_cleanfunc(mdbm, cleananything, 0));
}

void
DataMgmtBaseTestSuite::TestTruncateWarningMsgs()
{
    string baseName = "warnmsg";
    string dbName   = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    string keyBaseName = "warnkey";
    string value       = "warnvalue";
    int defAlignVal;
    int defHashID;

    int numRecsAdded = createAndAddData(dbh, 4, keyBaseName, value.c_str(), defAlignVal,
                                        defHashID, MDBM_LARGE_OBJECTS);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC F4: FAILed to create a filled multi paged DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (numRecsAdded > 0));

    string prefix = SUITE_PREFIX();
    prefix += "Warning messages: ";
    verifyDefaultConfig(dbh, prefix, true, defAlignVal, defHashID);
}

