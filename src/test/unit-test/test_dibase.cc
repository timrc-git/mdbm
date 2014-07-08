/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// FIX BZ 5421708 - v2: mdbm_save: bad compression level parameter ignored, defaults to no compression - should make log message
// FIX BZ 5431921 - v2: mdbm_dump_page: SEGV when invalid page number specified
//
#include <unistd.h>
#include <string.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <iostream>
#include <map>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "mdbm.h"
#include "test_dibase.hh"


int DataInterchangeBaseTestSuite::_setupCnt = 0;


int DataInterchangeV3::_DIv3HashIDs[] = {
    MDBM_HASH_CRC32, MDBM_HASH_EJB, MDBM_HASH_PHONG, MDBM_HASH_OZ,
    MDBM_HASH_TOREK, MDBM_HASH_FNV, MDBM_HASH_STL, MDBM_HASH_MD5,
    MDBM_HASH_SHA_1, MDBM_HASH_JENKINS, MDBM_HASH_HSIEH
};

int DataInterchangeV3::_DIv3HashIDlen = sizeof(DataInterchangeV3::_DIv3HashIDs) / sizeof(int);


void DataInterchangeBaseTestSuite::setUp()
{
    if (_setupCnt++ == 0)
    {
        cout << endl << "DB Interchange Test Suite Beginning..." << endl << flush;
    }

    _hashIDs = 0;
    _hashIDlen = 0;
}


void
DataInterchangeV3::setUp()
{
    DataInterchangeBaseTestSuite::setUp();

    _hashIDs   = _DIv3HashIDs;
    _hashIDlen = _DIv3HashIDlen;
}

string
DataInterchangeBaseTestSuite::makeKeyName(int incr, const string &keyBaseName)
{
    stringstream dummykeyss;
    dummykeyss << keyBaseName;
    if (incr < 10)
    {
        dummykeyss << "00";
    }
    else if (incr < 100)
    {
        dummykeyss << "0";
    }

    dummykeyss << incr;

    string dkey = dummykeyss.str();
    return dkey;
}

MDBM*
DataInterchangeBaseTestSuite::addData(MDBM *dbh, int numPages, const string &keyBaseName, const string &value, int &numRecsAdded)
{
    int numRecs = 1;
    if (numPages > 0)
    {
        mdbm_limit_size_v3(dbh, numPages, 0, NULL); // limit to numPages and no shake function
        numRecs = -1;
    }

    int cnt = 0;
    for (; numRecs != 0; --numRecs)
    {
        string dkey = makeKeyName(++cnt, keyBaseName);
        string dbval = value;
        if (dbval.empty())  // create unique data
        {
            stringstream unqval;
            unqval << "uq" << numRecs;
        }
        if (store(dbh, dkey.c_str(), dbval.c_str()) == -1)
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
    numRecsAdded = numRecs;

    return dbh;
}
// numPages : 0 means create with 1 page and store only one key/value pair
//          : > 0 means create with numPages and fill with key/value pairs
MDBM*
DataInterchangeBaseTestSuite::createDbAndAddData(MdbmHolder &dbh, int numPages, const string &keyBaseName, const string &value, int &numRecsAdded)
{
    if (numPages < 0) // valid range is >= 0
    {
        numPages = 0;
    }

    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 512 * numPages);
    if (ret == -1)
    {
        return 0;
    }
    numRecsAdded = 0;
    return addData(dbh, numPages, keyBaseName, value, numRecsAdded);
}
MDBM*
DataInterchangeBaseTestSuite::createDbAndAddData(MdbmHolder &dbh, int numPages, const string &keyBaseName, const string &value)
{
    int numRecs = 0;
    return createDbAndAddData(dbh, numPages, keyBaseName, value, numRecs);
}

void
DataInterchangeBaseTestSuite::EmptyDBgetpageA1()
{
#if 0
// FIX BZ 5414284: v2, v3: mdbm_get_page: returns pagenumber 0 or 1 for invalid key
    // create empty DB
    string baseName = "tcA1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC A1: Empty DB Test: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openflags, 0644, 512, 0);
    string openmsg = prefix;
    openmsg += "Open DB FAILed ";
    CPPUNIT_ASSERT_MESSAGE(openmsg, (ret == 0));

    string skey = "nosuchkey";
    datum key;
    key.dptr = const_cast<char*>(skey.c_str());
    key.dsize = skey.size();

    getpage(prefix, dbh, &key, false);
#endif
}

void
DataInterchangeBaseTestSuite::GetpageNullKeyA2()
{
    string baseName = "tcA2";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC A2: Null Key Test: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    MdbmHolder dbh(EnsureTmpMdbm(prefix, openflags, 0644, 512, 0));

    getpage(prefix, dbh, 0, false);
}

void
DataInterchangeBaseTestSuite::PartialDBgetPageValidKeyA3()
{
    // create a partial filled DB
    string baseName = "tcA3";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA3key";
    string value       = "tc3dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A3: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string keyname = makeKeyName(1, keyBaseName);
    datum dkey;
    dkey.dptr = const_cast<char*>(keyname.c_str());
    dkey.dsize = keyname.size();

    string tcmsg = "TC A3: Valid Key test. ";
    getpage(tcmsg, dbh, &dkey, true);
}

void
DataInterchangeBaseTestSuite::getpage(const string &tcmsg, MDBM *dbh, datum *key, bool expectOK)
{
    mdbm_ubig_t pagenum = mdbm_get_page(dbh, key);

    // expect failure
    stringstream pnss;
    pnss << SUITE_PREFIX()
         << tcmsg << " mdbm_get_page should have ";
    if (expectOK)
    {
        pnss << "Succeeded but instead it FAILed"
             << endl;
        CPPUNIT_ASSERT_MESSAGE(pnss.str(), (((int)pagenum) != -1));
    }
    else
    {
        pnss << "FAILed but instead returned page number=" << pagenum
             << endl;
        CPPUNIT_ASSERT_MESSAGE(pnss.str(), (((int)pagenum) == -1));
    }
}
void
DataInterchangeBaseTestSuite::PartialDBgetPageInvalidKeysA4()
{
#if 0
// FIX BZ 5414284: v2, v3: mdbm_get_page: returns pagenumber 0 or 1 for invalid key
    // create a partial filled DB
    string baseName = "tcA4";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA4key";
    string value       = "tc4dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A4: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string keyname = makeKeyName(1, keyBaseName);
    datum dkey;
    dkey.dptr = const_cast<char*>(keyname.c_str());
    dkey.dsize = 0;
    string tcmsg = "TC A4: 0-length key test. Use key=";
    tcmsg += keyname += " but specify 0 length";
    getpage(tcmsg, dbh, &dkey, false);

    dkey.dsize = keyname.size() - 1;
    stringstream skss;
    skss << "TC A4: Partial key match test. Use key=" << keyname
         << " but specify length less than size of key. key length=" << keyname.size()
         << " but use key length=" << dkey.dsize;
    getpage(skss.str(), dbh, &dkey, false);

    string noexistkey = "nonexistantkey";
    dkey.dptr = const_cast<char*>(noexistkey.c_str());
    dkey.dsize = noexistkey.size();
    tcmsg = "TC A4: Use key not in DB test. Use key=";
    tcmsg += noexistkey;
    getpage(tcmsg, dbh, &dkey, false);
#endif
}

void
DataInterchangeBaseTestSuite::FullDBgetPageValidKeysA5()
{
    // fill a 1 page DB
    string baseName = "tcA5";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA5key";
    string value       = "tc5dummy";
    MDBM *dbh = createDbAndAddData(db, 1, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A5: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // iterate the DB; do mdbm_get_page for each key and verify always same page=0
    MDBM_ITER iter;
    datum     key = mdbm_firstkey_r(dbh, &iter);
    mdbm_ubig_t pagenum = mdbm_get_page(dbh, &key);
    stringstream idss;
    idss << SUITE_PREFIX()
         << "TC A5: Iterating DB, found mis-match pagenumbers. Page number should be=" << pagenum
         << " but got page number=";
    for (int cnt = 0; key.dsize != 0; ++cnt)
    {
        key = mdbm_nextkey_r(dbh, &iter);
        if (key.dptr == NULL) {
            break;
        }
        mdbm_ubig_t nextpagenum = mdbm_get_page(dbh, &key);
        if (pagenum != nextpagenum)
        {
            idss << nextpagenum << endl;
            CPPUNIT_ASSERT_MESSAGE(idss.str(), (pagenum == nextpagenum));
        }
    }
}

void
DataInterchangeBaseTestSuite::FullDBgetPageInvalidKeysA6()
{
#if 0
// FIX BZ 5414284: v2, v3: mdbm_get_page: returns pagenumber 0 or 1 for invalid key
    // fill a 1 page DB
    string baseName = "tcA6";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA6key";
    string value       = "tc6dummy";
    MDBM *dbh = createDbAndAddData(db, 1, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A6: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string tcmsg = SUITE_PREFIX();
    tcmsg += "TC A6: Invalid Key test. ";
    stringstream invalidkey;
    invalidkey << "doesntexist";
    for (int cnt = 0; cnt < 3; ++cnt)
    {
        invalidkey << cnt;
        datum dkey;
        dkey.dptr = const_cast<char*>(invalidkey.str().c_str());
        dkey.dsize = strlen(dkey.dptr);;

        getpage(tcmsg, dbh, &dkey, false);
    }
#endif
}

void
DataInterchangeBaseTestSuite::VariousHashFunctionsValidKeyA7()
{
#if 0
// FIX BZ 5414284: v2, v3: mdbm_get_page: returns pagenumber 0 or 1 for invalid key
    string keyname = "keyA7";
    datum key;
    key.dptr = const_cast<char*>(keyname.c_str());
    key.dsize = keyname.size();

    datum val;
    val.dptr = const_cast<char*>(keyname.c_str());
    val.dsize = keyname.size();

    string prefix = SUITE_PREFIX();
    prefix = "TC A7: Hash Function and Key test. Key=";

    stringstream ssmsg;
    // for each hash function :
    for (int cnt = 0; cnt < _hashIDlen; ++cnt)
    {
        // -- create empty DB
        string baseName = "tcA7";
        string dbName = GetTmpName(baseName);
        MdbmHolder dbh(dbName);

        int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
        int ret = dbh.Open(openflags, 0644, 512, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

        ssmsg << prefix << keyname << " and Hash Function ID=" << _hashIDs[cnt];

        // -- call mdbm_sethash;
        ret = mdbm_sethash(dbh, _hashIDs[cnt]);
        if (ret == 0)
        {
            continue;
        }

        // -- call mdbm_get_page with key A; expect failure
        getpage(ssmsg.str(), dbh, &key, false);

        // -- call mdbm_store using key A
        ret = mdbm_store(dbh, key, val, MDBM_INSERT);
        if (ret == -1)
        {
            continue;
        }

        // -- call mdbm_get_page with key A; expect success
        getpage(ssmsg.str(), dbh, &key, true);
        ssmsg.clear();
    }
#endif
}

void
DataInterchangeBaseTestSuite::FullMultiPageDBgetPageKeyCountA8()
{
    // create a multi-page DB and fill with keys and values where all keys are the same
    // length, and all values are the same length
    string baseName = "tcA8";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA8key";
    string value       = "tc8dummy";
    MDBM *dbh = createDbAndAddData(db, 3, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A8: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // iterate the DB
    map<mdbm_ubig_t, int> pageKeyCntMap;

    MDBM_ITER iter;
    datum     key = mdbm_firstkey_r(dbh, &iter);
    mdbm_ubig_t pagenum = mdbm_get_page(dbh, &key);

    pageKeyCntMap[pagenum] = 1;

    int keyCnt = 1;
    for (; key.dsize != 0; ++keyCnt)
    {
        key = mdbm_nextkey_r(dbh, &iter);
        if (key.dsize == 0)
        {
            break;
        }
        // for each key returned; call mdbm_get_page; record key/value count per page number
        pagenum = mdbm_get_page(dbh, &key);

        map<mdbm_ubig_t, int>::iterator pageKeyMapIter = pageKeyCntMap.find(pagenum);
        if (pageKeyMapIter != pageKeyCntMap.end())
        {
            pageKeyCntMap[pagenum] += 1;
        }
        else
        {
            pageKeyCntMap[pagenum] = 1;
        }
    }

    // When iteration completed, confirm the number of key/value records is equally divided
    // between page number's
    // NOTE: in v2, there will be 1 less record in page 0 since it is observed it keeps a
    // header record
    int avgKeyCntPerPage = (keyCnt + 1) / pageKeyCntMap.size();
    mdbm_ubig_t badPage  = (mdbm_ubig_t)-1;
    int badPageCnt       = -1;
    for (map<mdbm_ubig_t, int>::iterator it = pageKeyCntMap.begin() ;
         it != pageKeyCntMap.end(); ++it)
    {
        int sum = (*it).second;
        if (sum != avgKeyCntPerPage)
        {
            if (sum == (avgKeyCntPerPage - 1) && (*it).first == 0)
            {
                continue;
            }
            else
            {
                badPage    = (*it).first;
                badPageCnt = sum;
            }
        }
    }

    stringstream idss;
    idss << SUITE_PREFIX()
         << "TC A8: After Iterating DB, found unbalanced number of keys per page. "
         << " Expect each page to have same number of keys=" << avgKeyCntPerPage
         << " but page=" << badPage
         << " has number of keys=" << badPageCnt << endl;
    CPPUNIT_ASSERT_MESSAGE(idss.str(), (badPage == ((mdbm_ubig_t)-1)));
}

void
DataInterchangeBaseTestSuite::AlternateValidInvalidKeysA9()
{
#if 0
// FIX BZ 5414284: v2, v3: mdbm_get_page: returns pagenumber 0 or 1 for invalid key
    // filled single-page DB: specify valid then specify invalid key
    string baseName = "tcA9";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA9key";
    string value       = "tc9dummy";
    MDBM *dbh = createDbAndAddData(db, 1, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A9: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // use series of valid keys followed by same invalid key
    string keyname = "invalidkeyA9";
    datum invalidkey;
    invalidkey.dptr = const_cast<char*>(keyname.c_str());
    invalidkey.dsize = keyname.size();
    stringstream ikss;
    ikss << SUITE_PREFIX()
          << "TC A9: Alternate Valid and Invalid Key. Using Invalid Key=" << keyname;

    stringstream vkss;
    vkss << SUITE_PREFIX()
         << "TC A9: Alternate Valid and Invalid Key. Using Valid Key=";
    for (int cnt = 1; cnt < 4; ++cnt)
    {
        // call mdbm_get_page with a valid key;
        // immediately after call mdbm_get_page with an invalid key
        string keyname = makeKeyName(cnt, keyBaseName);
        datum dkey;
        dkey.dptr = const_cast<char*>(keyname.c_str());
        dkey.dsize = keyname.size();

        string vkmsg = vkss.str();
        vkmsg += keyname;
        getpage(vkmsg, dbh, &dkey, true);

        vkmsg = ikss.str();
        vkmsg += " After using Valid Key=";
        vkmsg += keyname;
        getpage(vkmsg, dbh, &invalidkey, false);
    }
#endif
}

void
DataInterchangeBaseTestSuite::MultiPageDBalternateValidInvalidKeysA10()
{
#if 0
// FIX BZ 5414284: v2, v3: mdbm_get_page: returns pagenumber 0 or 1 for invalid key
    // filled multi-page DB: specify valid then specify invalid key
    // use series of valid keys followed by same invalid key
    string baseName = "tcA10";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcA10key";
    string value       = "tc10dummy";
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC A10: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string invkeyname = "invalidkeyA10";
    datum invalidkey;
    invalidkey.dptr = const_cast<char*>(invkeyname.c_str());
    invalidkey.dsize = invkeyname.size();
    stringstream ikss;
    ikss << SUITE_PREFIX()
         << "TC A10: Iterate DB. Alternate Valid and Invalid Key. Using Invalid Key(" << invkeyname << "). ";

    MDBM_ITER iter;
    datum     dkey = mdbm_firstkey_r(dbh, &iter);

    stringstream vkss;
    vkss << SUITE_PREFIX()
         << "TC A10: Iterate DB. Alternate Valid and Invalid Key. Using Valid Key(";
    string vkmsg = vkss.str();
    vkmsg.append(dkey.dptr, dkey.dsize).append("). ");
    getpage(vkmsg, dbh, &dkey, true);

    vkmsg = ikss.str();
    vkmsg += " After using Valid Key(";
    vkmsg.append(dkey.dptr, dkey.dsize).append("). ");
    getpage(vkmsg, dbh, &invalidkey, false);

    while (dkey.dsize != 0)
    {
        dkey = mdbm_nextkey_r(dbh, &iter);
        vkmsg = vkss.str();
        vkmsg.append(dkey.dptr, dkey.dsize);
        getpage(vkmsg, dbh, &dkey, true);

        vkmsg = ikss.str();
        vkmsg += " After using Valid Key(";
        vkmsg.append(dkey.dptr, dkey.dsize).append("). ");
        getpage(vkmsg, dbh, &invalidkey, false);
    }
#endif
}

void
DataInterchangeBaseTestSuite::PartialDbSaveToNewB1()
{
    string baseName = "ditcB1";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB1key";
    string value       = "tcB1dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B1: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT | MDBM_O_RDWR, 0644, 0);
    int errnum = errno;

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B1: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Its return code=" << ret << " and the errno=" << errnum << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));
}
void
DataInterchangeBaseTestSuite::PartialDbSaveToNewFileModeReadOnlyB2()
{
    string baseName = "ditcB2";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB2key";
    string value       = "tcB2dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0400, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B2: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // check the perms
    struct stat savefilebuf;
    ret = statFile(saveFile, savefilebuf);
    if (ret == -1)
    {
        int errnum = errno;
        cout << "WARNING: TC B2: " << SUITE_PREFIX()
             << " stat of saved file=" << saveFile
             << " FAILed with errno=" << errnum
             << " Could not compare stats" << endl << flush;
    }
    // want to see that it does not have write or execute bits
    // verify read bit for owner is set, but not for group or other
    // S_IRUSR S_IRGRP S_IROTH, S_IWUSR S_IWGRP S_IWOTH, S_IXUSR S_IXGRP S_IXOTH
    mode_t unwantedperms = savefilebuf.st_mode & (S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH | S_IXUSR | S_IXGRP | S_IXOTH);
    sfss.clear();
    sfss <<  SUITE_PREFIX()
         << "TC B2: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Either it doesnt exist or permisssions are wrong. Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0 && savefilebuf.st_size > 0 && unwantedperms == 0 && (savefilebuf.st_mode & S_IRUSR)));
}
void
DataInterchangeBaseTestSuite::PartialDbSaveToNewNoCreateFlagB3()
{
    string baseName = "ditcB3";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB3key";
    string value       = "tcB3dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B3: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), 0, 0644, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B3: mdbm_save returned Success upon saving the partial DB to file=" << saveFile
         << " But it should have FAILed, Return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == -1));
}
int
DataInterchangeBaseTestSuite::statFile(const string &dbname, struct stat &statbuf)
{
    memset(&statbuf, 0, sizeof(statbuf));
    return stat(dbname.c_str(), &statbuf);
}
void
DataInterchangeBaseTestSuite::PartialDbSaveToOldMultiPageB4()
{
    // make a DB and save it
    string mpbaseName = "ditcB4";
    string mpdbName = GetTmpName(mpbaseName);
    MdbmHolder oldmpdb(mpdbName);
    string keyBaseName = "tcB4key";
    string value       = "tcB4dummy";
    MDBM *oldmpdbh = createDbAndAddData(oldmpdb, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B4: FAILed to create a filled DB=" << mpdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (oldmpdbh != 0));

    string sfbaseName = mpbaseName;
    sfbaseName += "savefile";
    string saveFile =  GetTmpName(sfbaseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(oldmpdbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B4: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    struct stat savefilebuf;
    int statret = statFile(saveFile, savefilebuf);
    if (statret == -1)
    {
        int errnum = errno;
        cout << "WARNING: TC B4: " << SUITE_PREFIX()
             << " stat of multi-page saved file=" << saveFile
             << " FAILed with errno=" << errnum
             << " continuing..." << endl << flush;
    }

    // create a new partially filled DB and save it over the old one
    string baseName = "tcB4new";
    string dbName = GetTmpName(baseName);
    MdbmHolder newdb(dbName);
    string newKeyBaseName = "tcB4keynew";
    string newValue       = "tcB4dummynew";
    MDBM *newdbh = createDbAndAddData(newdb, 0, newKeyBaseName, newValue);

    ret = mdbm_save(newdbh, saveFile.c_str(), MDBM_O_CREAT | MDBM_O_TRUNC, 0644, Z_BEST_COMPRESSION);

    sfss.clear();
    sfss <<  SUITE_PREFIX()
         << "TC B4: mdbm_save FAILed to save the partial DB to the same save file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    if (statret == 0)
    {
        struct stat newsavefilebuf;
        if (statFile(saveFile, newsavefilebuf) == -1)
        {
            int errnum = errno;
            cout << "WARNING: TC B4: " << SUITE_PREFIX()
                 << " stat of partial filled saved file=" << saveFile
                 << " FAILed with errno=" << errnum
                 << " continuing..." << endl << flush;
        }
        else
        {
            stringstream stss;
            stss <<  SUITE_PREFIX()
                 << "TC B4: mdbm_save of the partial filled DB to the same file=" << saveFile
                 << " gave file of size=" << newsavefilebuf.st_size
                 << " which should be smaller than first full DB saved file which is of size=" << savefilebuf.st_size
                 << endl;
            CPPUNIT_ASSERT_MESSAGE(stss.str(), (savefilebuf.st_size > newsavefilebuf.st_size));
        }
    }

    // ensure we can still fetch data that was stored in the current DB
    string key = "";
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(newdbh, &iter);
    if (dkey.dsize != 0)
    {
        // verify its our key and not a key from the first DB
        key.assign(dkey.dptr, dkey.dsize);
    }
    size_t pos = key.find(newKeyBaseName);
    if (pos == string::npos)
    {
        // ohoh, we didnt find our key!
        stringstream mkss;
        mkss << SUITE_PREFIX()
             << "TC B4: We lost our data after mdbm_save of the DB over another saved DB file."
             << " We expected to find a key containing=" << newKeyBaseName
             << " but got the key=" << key << endl;
        CPPUNIT_ASSERT_MESSAGE(mkss.str(), (pos != string::npos));
    }
}
void
DataInterchangeBaseTestSuite::PartialDbSaveToOldMultiPageNoTruncFlagB5()
{
    // make a DB and save it
    string mpbaseName = "ditcB5";
    string mpdbName = GetTmpName(mpbaseName);
    MdbmHolder mpdb(mpdbName);
    string keyBaseName = "tcB5key";
    string value       = "tcB5dummy";
    MDBM *mpdbh = createDbAndAddData(mpdb, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B5: FAILed to create a filled multi-paged DB=" << mpdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (mpdbh != 0));

    mpbaseName += "savefile";
    string saveFile =  GetTmpName(mpbaseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(mpdbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B5: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // create a new one and save it over the old one without the TRUNC flag
    string baseName = "dittcB5new";
    string dbName = GetTmpName(baseName);
    MdbmHolder newdb(dbName);
    string newKeyBaseName = "tcB5keynew";
    string newValue       = "tcB5dummynew";
    MDBM *newdbh = createDbAndAddData(newdb, 0, newKeyBaseName, newValue);

    ret = mdbm_save(newdbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    // expect the save to fail
    stringstream snss;
    snss <<  SUITE_PREFIX()
         << "TC B5: the 2nd mdbm_save Should FAIL to save the partial DB to the same file=" << saveFile
         << " But it erroneously Succeeded! Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(snss.str(), (ret == -1));
}
void
DataInterchangeBaseTestSuite::PartialDbSaveToNewUseDbVersionFlagB6()
{
    string baseName = "ditcB6";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB6key";
    string value       = "tcB6dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B6: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int openFlags = MDBM_O_CREAT | versionFlag;
    int ret = mdbm_save(dbh, saveFile.c_str(), openFlags, 0644, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B6: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));
}
void
DataInterchangeBaseTestSuite::PartialDbSaveToInvalidPathB7()
{
    string baseName = "ditcB7";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB7key";
    string value       = "tcB7dummy";
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B7: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string saveFile = "/tmp/jupitercalling/but/he/wont/answer/please/saveme";
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B7: mdbm_save Should have FAILed to save the partial DB to file=" << saveFile
         << " But instead it Succeeded! Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == -1));
}
void
DataInterchangeBaseTestSuite::EmptyDbSaveToNewB8()
{
    string baseName = "ditcB8";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC B8: Open DB FAILed ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));
    // HAVE: empty DB

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B8: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // validate existence of the DB
    struct stat savefilebuf;
    ret = statFile(saveFile, savefilebuf);
    int errnum = errno;
    stringstream stss;
    stss <<  SUITE_PREFIX()
         << "TC B8: mdbm_save reported success but actually it FAILed to save to file=" << saveFile;
    if (ret == -1)
    {
        stss << " System stat FAILed with errno=" << errnum;
    }
    else
    {
        stss << " System stat size of saved file=" << savefilebuf.st_size;
    }
    stss << endl;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0 && savefilebuf.st_size > 0));
}
void
DataInterchangeBaseTestSuite::EmptyDbSaveToNewFileModeReadOnlyB9()
{
    string baseName = "ditcB9";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC B9: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));
    // HAVE: empty DB

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0400, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B9: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // want to see that it does not have write or execute bits
    // verify read bit for owner is set, but not for group or other
    // S_IRUSR S_IRGRP S_IROTH, S_IWUSR S_IWGRP S_IWOTH, S_IXUSR S_IXGRP S_IXOTH
    struct stat savefilebuf;
    ret = statFile(saveFile, savefilebuf);
    if (ret == -1)
    {
        int errnum = errno;
        cout << "WARNING: TC B9: " << SUITE_PREFIX()
             << " stat of saved file=" << saveFile
             << " FAILed with errno=" << errnum
             << " Could not compare stats" << endl << flush;
    }
    mode_t unwantedperms = savefilebuf.st_mode & (S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH | S_IXUSR | S_IXGRP | S_IXOTH);
    stringstream pcss;
    pcss <<  SUITE_PREFIX()
         << "TC B9: mdbm_save FAILed to save the partial DB to file=" << saveFile
         << " Either it doesnt exist or permisssions are wrong. stat return code=" << ret
         << " Saved DB file size=" << savefilebuf.st_size
         << " Permissions=" << oct << unwantedperms << endl;
    CPPUNIT_ASSERT_MESSAGE(pcss.str(), (ret == 0 && savefilebuf.st_size > 0 && unwantedperms == 0 && (savefilebuf.st_mode & S_IRUSR)));
}
void
DataInterchangeBaseTestSuite::EmptyDbSaveToToOldMultiPageB10()
{
    string mpbaseName = "ditcB10";
    string mpdbName = GetTmpName(mpbaseName);
    MdbmHolder oldmpdb(mpdbName);
    string keyBaseName = "tcB10key";
    string value       = "tcB10dummy";
    MDBM *oldmpdbh = createDbAndAddData(oldmpdb, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B10: FAILed to create a filled DB=" << mpdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (oldmpdbh != 0));

    mpbaseName += "savefile";
    string saveFile =  GetTmpName(mpbaseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(oldmpdbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B10: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    struct stat savefilebuf;
    int statret = statFile(saveFile, savefilebuf);
    if (statret == -1)
    {
        int errnum = errno;
        cout << "WARNING: TC B10: " << SUITE_PREFIX()
             << " stat of multi-page saved file=" << saveFile
             << " FAILed with errno=" << errnum
             << " continuing..." << endl << flush;
    }

    // now create the new empty DB
    string baseName = "ditcB10empty";
    string dbName = GetTmpName(baseName);
    MdbmHolder emptydb(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC B10: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    ret = emptydb.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));
    // HAVE: empty DB so now lets save it over the old save file

    ret = mdbm_save(emptydb, saveFile.c_str(), MDBM_O_CREAT | MDBM_O_TRUNC, 0644, Z_BEST_COMPRESSION);

    stringstream sdss;
    sdss <<  SUITE_PREFIX()
         << "TC B10: mdbm_save FAILed to save the empty DB to the same save file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sdss.str(), (ret == 0));

    if (statret == 0)
    {
        struct stat newsavefilebuf;
        statret = statFile(saveFile, newsavefilebuf);
        int errnum = errno;
        stringstream sfss;
        sfss << "TC B10: " << SUITE_PREFIX()
             << " stat of empty filled saved file=" << saveFile
             << " FAILed with errno=" << errnum
             << endl;
        CPPUNIT_ASSERT_MESSAGE(sfss.str(), (statret == 0));

        stringstream fpss;
        fpss <<  SUITE_PREFIX()
             << "TC B10: mdbm_save of the partial filled DB to the same file=" << saveFile
             << " gave file of size=" << newsavefilebuf.st_size
             << " which should be smaller than first full DB saved file which is of size=" << savefilebuf.st_size
             << endl;
        CPPUNIT_ASSERT_MESSAGE(fpss.str(), (savefilebuf.st_size > newsavefilebuf.st_size));
    }
}
void
DataInterchangeBaseTestSuite::FullDbSaveToNewB11()
{
    string baseName = "ditcB11";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB11key";
    string value       = "tcB11dummy";
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B11: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B11: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));
}
void
DataInterchangeBaseTestSuite::FullDbSaveToNewFileModeMinPermsB12()
{
    string baseName = "ditcB12";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB12key";
    string value       = "tcB12dummy";
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B12: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0001, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B12: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // check permissions of the save file
    struct stat savefilebuf;
    ret = statFile(saveFile, savefilebuf);
    if (ret == -1)
    {
        int errnum = errno;
        cout << "WARNING: TC B12: " << SUITE_PREFIX()
             << " stat of saved file=" << saveFile
             << " FAILed with errno=" << errnum
             << " Could not compare stats" << endl << flush;
    }
    mode_t unwantedperms = savefilebuf.st_mode & (S_IRUSR | S_IRGRP | S_IROTH | S_IWUSR | S_IWGRP | S_IWOTH | S_IXUSR | S_IXGRP);
    stringstream cpss;
    cpss <<  SUITE_PREFIX()
         << "TC B12: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Either it doesnt exist or permissions are wrong. stat return code=" << ret
         << " Saved DB file size=" << savefilebuf.st_size
         << " Permissions=" << oct << unwantedperms << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0 && savefilebuf.st_size > 0 && unwantedperms == 0 && (savefilebuf.st_mode & S_IXOTH)));
}

// Use unsupported compression level: -99
// Expect it to fail.
// from /usr/include/zlib.h
//   Z_NO_COMPRESSION         0
//   Z_BEST_SPEED             1
//   Z_BEST_COMPRESSION       9
//   Z_DEFAULT_COMPRESSION  (-1)
void
DataInterchangeBaseTestSuite::FullDbUseBadCompressionLevelB13()
{
#if 0
// FIX BZ 5421708 - v2: mdbm_save: bad compression level parameter ignored, defaults to no compression - should make log message
    string baseName = "ditcB13";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcB13key";
    string value       = "tcB13dummy";
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B13: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, -99);

    // lets stat it
    struct stat savefilebuf;
    int statret = statFile(saveFile, savefilebuf);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B13: mdbm_save Succeeded to save the full DB to file=" << saveFile
         << " But it Should have FAILed. Its return code=" << ret
         << " Its size=" << savefilebuf.st_size << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == -1 && (statret == -1 || savefilebuf.st_size == 0)));
#endif
}

// compress full DB with all duplicate data
// compress full DB with all unique data
// -- compare the sizes of the 2 DB's
void
DataInterchangeBaseTestSuite::DbFullDupDataCompareToDbFullUniqueDataB14()
{
    // create duplicate data filled DB
    string dupbaseName = "ditcB14dup";
    string dupdbName = GetTmpName(dupbaseName);
    MdbmHolder dupdb(dupdbName);
    string keyBaseName = "tcB14key";
    string dupvalue    = "00000000000000000000";
    dupvalue += dupvalue;
    dupvalue += dupvalue;
    MDBM *dupdbh = createDbAndAddData(dupdb, 2, keyBaseName, dupvalue);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC B14: FAILed to create a filled duplicate-data DB=" << dupdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dupdbh != 0));

    dupbaseName += "savefile";
    string dupsaveFile =  GetTmpName(dupbaseName);
    MdbmHolder dupsavedb(dupsaveFile);

    int ret = mdbm_save(dupdbh, dupsaveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC B14: mdbm_save FAILed to save the duplicate-data DB to file=" << dupsaveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // now create unique data filled DB
    string unqbaseName = "ditcB14unq";
    string unqdbName = GetTmpName(unqbaseName);
    MdbmHolder unqdb(unqdbName);
    string unqvalue;
    MDBM *unqdbh = createDbAndAddData(unqdb, 2, keyBaseName, unqvalue);

    stringstream cuss;
    cuss << SUITE_PREFIX()
         << "TC B14: FAILed to create a filled unique-data DB=" << unqdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cuss.str(), (unqdbh != 0));

    unqbaseName += "savefile";
    string unqsaveFile =  GetTmpName(unqbaseName);
    MdbmHolder unqsavedb(unqsaveFile);

    ret = mdbm_save(unqdbh, unqsaveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION);

    stringstream suss;
    suss <<  SUITE_PREFIX()
         << "TC B14: mdbm_save FAILed to save the unique-data DB to file=" << unqsaveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(suss.str(), (ret == 0));

    // lets stat the files to check their sizes
    struct stat dupfilebuf;
    ret = statFile(dupsaveFile, dupfilebuf);
    int errnum = errno;
    stringstream stss;
    stss << "WARNING: TC B14: " << SUITE_PREFIX()
         << " stat of the duplicate-data saved file=" << dupsaveFile
         << " FAILed with errno=" << errnum
         << " Could not compare stats" << endl << flush;
    CPPUNIT_ASSERT_MESSAGE(stss.str(), (ret == 0));

    struct stat unqfilebuf;
    ret = statFile(unqsaveFile, unqfilebuf);
    errnum = errno;
    stringstream stuss;
    stuss << "WARNING: TC B14: " << SUITE_PREFIX()
          << " stat of the unique-data saved file=" << unqsaveFile
          << " FAILed with errno=" << errnum
          << " Could not compare stats" << endl << flush;
    CPPUNIT_ASSERT_MESSAGE(stuss.str(), (ret == 0));

    stringstream csss;
    csss << "TC B14: " << SUITE_PREFIX()
         << "Unique-data saved file size=" << unqfilebuf.st_size
         << " Expected to be Bigger than Duplicate-data saved file size=" << dupfilebuf.st_size
         << endl;
    CPPUNIT_ASSERT_MESSAGE(csss.str(), (unqfilebuf.st_size > dupfilebuf.st_size));
}

void
DataInterchangeBaseTestSuite::SaveUsingV3FlagThenRestoreC1()
{
    string baseName = "ditcC1";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcC1key";
    string value       = "tcC1dummy";
    MDBM *dbh = createDbAndAddData(db, 1, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C1: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int openFlags = MDBM_O_CREAT | versionFlag;
    int ret = mdbm_save(dbh, saveFile.c_str(), openFlags, 0644, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC C1: mdbm_save FAILed to save the DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // get a new DB handle and restore
    string emptybaseName = "ditcC1empty";
    string emptydbName = GetTmpName(emptybaseName);
    MdbmHolder emptydbh(emptydbName);
    string prefix = SUITE_PREFIX();
    prefix += "TC C1: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    ret = emptydbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

    ret = mdbm_restore(emptydbh, saveFile.c_str());
    // check ret code and get data
    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C1: mdbm_restore FAILed to restore the saved DB from file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == 0));

    // ensure we can still fetch data that was stored in the new DB
    string key = "";
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(emptydbh, &iter);
    if (dkey.dsize != 0)
    {
        // verify its our key and not a key from the first DB
        key.assign(dkey.dptr, dkey.dsize);
    }
    size_t pos = key.find(keyBaseName);
    if (pos == string::npos)
    {
        // ohoh, we didnt find our key!
        stringstream mkss;
        mkss << SUITE_PREFIX()
             << "TC C1: We lost our data after mdbm_restore of the DB from the saved DB file."
             << " We expected to find a key containing=" << keyBaseName
             << " but got the key=" << key << endl;
        CPPUNIT_ASSERT_MESSAGE(mkss.str(), (pos != string::npos));
    }
}
void
DataInterchangeBaseTestSuite::RestoreFromNonExistentFileC2()
{
    // open and create and empty DB
    string emptybaseName = "ditcC2empty";
    string emptydbName = GetTmpName(emptybaseName);
    MdbmHolder emptydbh(emptydbName);
    string prefix = SUITE_PREFIX();
    prefix += "TC C2: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = emptydbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

    // restore from a non existent file path
    string saveFile = "/tmp/saved/by/the/bell";
    ret = mdbm_restore(emptydbh, saveFile.c_str());
    // expect failure
    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C2: mdbm_restore Succeeded but Should have FAILed since the save file is non-existent=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == -1));
}
void
DataInterchangeBaseTestSuite::RestoreFromDBthatWasNot_mdbm_savedC3()
{
    // create a DB with some data then close
    string baseName = "ditcC3";
    string dbName = GetTmpName(baseName);
    string keyBaseName = "tcC3key";
    string value       = "tcC3dummy";
    MdbmHolder db(dbName);
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C3: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // open an empty DB
    string emptybaseName = "ditcC3empty";
    string emptydbName = GetTmpName(emptybaseName);
    MdbmHolder emptydbh(emptydbName);
    string prefix = SUITE_PREFIX();
    prefix += "TC C3: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = emptydbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

    // restore from the normal DB that was previously created
    ret = mdbm_restore(emptydbh, dbName.c_str());
    // expect failure
    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C3: mdbm_restore Succeeded but Should have FAILed since the save file is normal DB file=" << dbName
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == -1));
}
void
DataInterchangeBaseTestSuite::SaveWithChmod0001_ThenRestoreC4()
{
    // create DB with data
    string baseName = "ditcC4";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcC4key";
    string value       = "tcC4dummy";
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C4: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // save with minimal permissions
    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0001, Z_BEST_SPEED);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC C4: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // open and create an empty DB
    string emptybaseName = "ditcC4empty";
    string emptydbName = GetTmpName(emptybaseName);
    MdbmHolder emptydbh(emptydbName);
    string prefix = SUITE_PREFIX();
    prefix += "TC C4: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    ret = emptydbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

    // restore as normal
    ret = mdbm_restore(emptydbh, saveFile.c_str());

    // expect failure since file doesnt have read/write permissions
    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C4: mdbm_restore Succeeded but Should have FAILed since the save file=" << dbName
         << " Has limited permissions=0001(no read/write). Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == -1));
}

// iterate the DB and verify all keys contain keyBaseName
// assert if any key does not contain keyBaseName
// assert if there is a mismatch of number of keys iterated and expected number of keys in the DB
void
DataInterchangeBaseTestSuite::verifyData(string &keyBaseName, MDBM *newdbh, const string &saveFile, int numRecs, const string &prefix)
{
    stringstream mdss;
    mdss << SUITE_PREFIX() << prefix
         << " The data key(" << keyBaseName
         << ") from the save file=" << saveFile
         << " is missing after performing an mdbm_restore"
         << " The DB should contain key/value record count=" << numRecs << endl;
    string key = "";
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(newdbh, &iter);
    CPPUNIT_ASSERT_MESSAGE(mdss.str(), (dkey.dsize != 0));

    // verify its our key and not a key from the first DB
    key.assign(dkey.dptr, dkey.dsize);
    size_t pos = key.find(keyBaseName);
    if (pos == string::npos)
    {
        mdss << " Found mis-match key=" << key << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(mdss.str(), (pos != string::npos));

    int keyCnt = 1;
    for (; dkey.dsize != 0; ++keyCnt)
    {
        key = "";
        dkey = mdbm_nextkey_r(newdbh, &iter);
        if (dkey.dsize == 0)
        {
            break;
        }

        key.assign(dkey.dptr, dkey.dsize);
        pos = key.find(keyBaseName);
        if (pos == string::npos)
        {
            mdss << " Found mis-match key=" << key << endl;
        }
        CPPUNIT_ASSERT_MESSAGE(mdss.str(), (pos != string::npos));
    }

    stringstream kcss;
    kcss << SUITE_PREFIX() << prefix
         << " The number of key/values(" << numRecs
         << ") from the save file=" << saveFile
         << " does not match the number(" << keyCnt
         << ") iterated from the mdbm_restore'd DB" << endl;
    CPPUNIT_ASSERT_MESSAGE(kcss.str(), (numRecs == keyCnt));
}
void
DataInterchangeBaseTestSuite::OpenDBwithDataRestoreFromDBwithDiffDataC5()
{
    // create a DB with data
    string baseName = "ditcC5";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcC5key";
    string value       = "tcC5dummy";
    int    numRecs     = 0;
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value, numRecs);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C5: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // save the DB
    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_SPEED);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC C5: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // create a new DB with different data
    string newbaseName = "newditcC5";
    string newdbName = GetTmpName(newbaseName);
    MdbmHolder newdb(newdbName);
    string newkeyBaseName = "tcC5k";
    string newvalue       = "tcC5v";
    int newKeyCnt = 0;
    MDBM *newdbh = createDbAndAddData(newdb, 2, newkeyBaseName, newvalue, newKeyCnt);

    stringstream newss;
    newss << SUITE_PREFIX()
         << "TC C5: FAILed to create a new filled DB=" << newdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(newss.str(), (newdbh != 0));

    // restore the saved DB and verify the data is only from the saved DB, and none from the new one
    ret = mdbm_restore(newdbh, saveFile.c_str());

    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C5: mdbm_restore FAILed to restore the save file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == 0));

    string prefix = "TC C5: ";
    verifyData(keyBaseName, newdbh, saveFile, numRecs, prefix);
}
void
DataInterchangeBaseTestSuite::SaveThenRestoreUsingSameDbHandleC6()
{
    // create and fill a DB
    string baseName = "ditcC6";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcC6key";
    string value       = "tcC6dummy";
    int    numRecs     = 0;
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value, numRecs);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C6: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // save the DB
    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_SPEED);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC C6: mdbm_save FAILed to save the full DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // using same MDBM handle, restore from the saved file
    ret = mdbm_restore(dbh, saveFile.c_str());

    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C6: mdbm_restore FAILed to restore the save file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == 0));

    // verify all data still exists
    string prefix = "TC C6: ";
    verifyData(keyBaseName, dbh, saveFile, numRecs, prefix);
}
void
DataInterchangeBaseTestSuite::SaveEmptyDbRestoreToFullDbC7()
{
    // create an empty DB
    string baseName = "ditcC7";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC C7: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

    // save the empty DB
    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_SPEED);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC C7: mdbm_save FAILed to save the empty DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // create a new DB and add some data
    string newbaseName = "newditcC7";
    string newdbName = GetTmpName(newbaseName);
    MdbmHolder newdb(newdbName);
    string newkeyBaseName = "tcC7key";
    string newvalue       = "tcC7dummy";
    int newKeyCnt = 0;
    MDBM *newdbh = createDbAndAddData(newdb, 1, newkeyBaseName, newvalue, newKeyCnt);

    stringstream newss;
    newss << SUITE_PREFIX()
         << "TC C7: FAILed to create a new filled DB=" << newdbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(newss.str(), (newdbh != 0));

    // restore from the saved empty DB
    ret = mdbm_restore(newdbh, saveFile.c_str());

    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C7: mdbm_restore FAILed to restore the save file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == 0));

    // expect that there is no data after the restore
    string key = "";
    MDBM_ITER iter;
    datum dkey = mdbm_firstkey_r(newdbh, &iter);
    if (dkey.dsize > 0)
    {
        key.assign(dkey.dptr, dkey.dsize);
    }

    stringstream mdss;
    mdss << SUITE_PREFIX()
         << "TC C7: The data key(" << newkeyBaseName
         << ") is in the new DB. Where as the save file=" << saveFile
         << " is Empty. Therefore after performing an mdbm_restore"
         << " the DB should NOT contain any key/value records, but found key=" << key << endl;
    CPPUNIT_ASSERT_MESSAGE(mdss.str(), (dkey.dsize == 0));
}
void
DataInterchangeBaseTestSuite::SaveDbDiffCompressionLevelsRestoreC8()
{
    // create a full DB
    string baseName = "ditcC8";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcC8key";
    string value       = "tcC8dummy";
    int    numRecs     = 0;
    MDBM *dbh = createDbAndAddData(db, 2, keyBaseName, value, numRecs);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C8: FAILed to create a filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    stringstream saveFiless;
    saveFiless << saveFile;

    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    string prefix = SUITE_PREFIX();
    prefix += "TC C8: ";
    string vdprefix = "TC C8: ";
    string emptybaseName = "emptyditcC8";
    string emptydbName = GetTmpName(emptybaseName);

    // for all supported compression levels
    int compressLevels[] = { Z_NO_COMPRESSION, Z_BEST_SPEED, Z_BEST_COMPRESSION };
    for (unsigned int cnt = 0; cnt < sizeof(compressLevels) / sizeof(int); ++cnt)
    {
        // save the full DB to a new file using compression level N
        saveFiless << "_" << compressLevels[cnt];
        saveFile = saveFiless.str();
        MdbmHolder savedb(saveFile);

        {
            stringstream trmsg;
            trmsg << SUITE_PREFIX() << "TC C8: TRACE: save DB to file=" << saveFile;
            TRACE_TEST_CASE(trmsg.str())
        }

        int ret = mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, compressLevels[cnt]);
        stringstream sfss;
        sfss <<  SUITE_PREFIX()
             << "TC C8: mdbm_save FAILed to save the empty DB to file=" << saveFile
             << " Its return code=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

        // create an empty DB
        MdbmHolder emptydbh(emptydbName);
        ret = emptydbh.Open(openflags, 0644, 512, 0);
        CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

        // restore
        ret = mdbm_restore(emptydbh, saveFile.c_str());

        stringstream rfss;
        rfss <<  SUITE_PREFIX()
             << "TC C8: mdbm_restore FAILed to restore the save file=" << saveFile
             << " Its return code=" << ret << endl;
        CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == 0));

        // verify data intact
        verifyData(keyBaseName, emptydbh, saveFile, numRecs, vdprefix);
    }
}
void
DataInterchangeBaseTestSuite::SaveUsingBinaryKeysAndDataThenRestoreC9()
{
    string baseName = "ditcC9";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int numPages = 2;
    int ret = dbh.Open(openFlags, 0644, 512, 512 * numPages);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC C9: FAILed to create a DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (ret == 0));

    mdbm_limit_size_v3(dbh, numPages, 0, NULL); // limit to numPages and no shake function

    // now lets add a bunch of binary keys and values
    char keybuff[11];
    datum dkey;
    dkey.dptr = keybuff;
    dkey.dsize = sizeof(keybuff);
    char valbuff[21];
    datum dval;
    dval.dptr = valbuff;
    dval.dsize = sizeof(valbuff);
    int numRecs = 0;
    for (; numRecs < 1000; ++numRecs)
    {
        char dummy = numRecs % (dkey.dsize / sizeof(char));
        memset(keybuff, dummy, sizeof(keybuff));
        memcpy(keybuff, &numRecs, sizeof(numRecs));
        dummy = numRecs % (dval.dsize / sizeof(char));
        memset(valbuff, dummy, sizeof(valbuff));
        memcpy(valbuff, &numRecs, sizeof(numRecs));
        if (mdbm_store(dbh, dkey, dval, MDBM_INSERT) == -1)
        {
            break;
        }
    }

    baseName += "savefile";
    string saveFile =  GetTmpName(baseName);
    MdbmHolder savedb(saveFile);

    openFlags = MDBM_O_CREAT | versionFlag;
    ret = mdbm_save(dbh, saveFile.c_str(), openFlags, 0644, 0);

    stringstream sfss;
    sfss <<  SUITE_PREFIX()
         << "TC C9: mdbm_save FAILed to save the DB to file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(sfss.str(), (ret == 0));

    // get a new DB handle and restore
    string emptybaseName = "ditcC9empty";
    string emptydbName = GetTmpName(emptybaseName);
    MdbmHolder emptydbh(emptydbName);
    string prefix = SUITE_PREFIX();
    prefix += "TC C9: ";
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    ret = emptydbh.Open(openflags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));

    ret = mdbm_restore(emptydbh, saveFile.c_str());
    // check ret code and get data
    stringstream rfss;
    rfss <<  SUITE_PREFIX()
         << "TC C9: mdbm_restore FAILed to restore the saved DB from file=" << saveFile
         << " Its return code=" << ret << endl;
    CPPUNIT_ASSERT_MESSAGE(rfss.str(), (ret == 0));

    // ensure we can still fetch data that was stored in the new DB
    // lets fetch from the new DB for the number of entries originally written
    dkey.dptr = keybuff;
    dkey.dsize = sizeof(keybuff);
    dval.dptr = valbuff;
    dval.dsize = sizeof(valbuff);
    for (int cnt = 0; cnt < numRecs; ++cnt)
    {
        char dummy = cnt % (dkey.dsize / sizeof(char));
        memset(keybuff, dummy, sizeof(keybuff));
        memcpy(keybuff, &cnt, sizeof(cnt));
        datum fval = mdbm_fetch(emptydbh, dkey);

        // did we find our key? was the value as expected?
        dummy = cnt % (dval.dsize / sizeof(char));
        memset(valbuff, dummy, sizeof(valbuff));
        memcpy(valbuff, &cnt, sizeof(cnt));
        bool fetched = (fval.dsize == dval.dsize && (memcmp(fval.dptr, valbuff, sizeof(valbuff)) == 0));
        if (fetched == false)
        {
            stringstream mkss;
            mkss << SUITE_PREFIX()
                 << "TC C9: We lost our data after mdbm_restore of the DB from a saved DB file" << endl
                 << "upon processing entry number=" << cnt
                 << " in a DB that has number of entries=" << numRecs << endl
                 << " We expected to find a binary key of size=" << dkey.dsize
                 << " with a calculated value of size="<< dval.dsize << endl
                 << " but value is incorrect or key not found" << endl;
            CPPUNIT_ASSERT_MESSAGE(mkss.str(), (fetched));
        }
    }
}

int
DataInterchangeBaseTestSuite::parseNumber(string &token, const char *delim)
{
    // <token>=nnn
    size_t pos = token.find(delim);
    if (pos != string::npos)
    {
        string num = token.substr(pos+1);
        return atoi(num.c_str());
    }
    return -1;
}
void
DataInterchangeBaseTestSuite::parseDumpOutput(stringstream &istrm, const string &prefix, int expectedNumPages, int expectedNumEntries, int pid)
{
    char buff[256];
    int pageNum       = -1;
    int numPages      = 0;
    int numEntries    = -1;
    int numEntriesSum = 0;
    int totalEntriesCalced = 0;
    int keySumBytes = 0;
    int valSumBytes = 0;
    while (!istrm.eof())
    {
        memset(buff, 0, sizeof(buff));
        istrm.getline(buff, sizeof(buff), '\n');
        if (istrm.fail() || istrm.bad())
        {
            break;
        }

        string line = buff;
        // for start of page, need "Page=nnn" and "entries=nn"
        // how do I know its the start of a page?

        if (line.find("Page=") != string::npos) // start of a page
        {
            stringstream pstrm(line);
            // get the page number and the number of entries
            string pagetoken, addrtoken, entriestoken;
            pstrm >> pagetoken >> addrtoken >> entriestoken;

            // parse pagetoken
            pageNum    = parseNumber(pagetoken, "=");
            if (pageNum != -1)
            {
                numPages++;
                numEntries = parseNumber(entriestoken, "=");
                {
                    stringstream trmsg;
                    trmsg << prefix << "Starting on page=" << pageNum
                          << " that has the number of entries=" << numEntries;
                    TRACE_TEST_CASE(trmsg.str())
                }
                if (numEntries != -1)
                {
                    numEntriesSum += numEntries;
                }
            }
        }
        else if (line.find("[header]") != string::npos || line.find("[deleted]") != string::npos)
        {
            continue; // ignore these lines
        }
        else // parse this line
        {
            totalEntriesCalced++;

            // parse for "key[", "val[", "sz="
            size_t pos = line.find("]");
            if (pos != string::npos)
            {
                line.replace(pos, 1, " ");
            }
            stringstream pstrm(line);
            string pagetoken, keyvaltoken, sztoken;
            pstrm >> pagetoken >> keyvaltoken >> sztoken;

            int sz = parseNumber(sztoken, "=");
            //cout << " page=" << pageNum << " token size=" << sz << endl;;

            bool keytoken = line.find("key[") != string::npos;
            if (keytoken)
            {
                keySumBytes += sz;
            }
            else
            {
                valSumBytes += sz;
            }
        }
    }

    // NOTE: should do numEntriesSum-- since they include "[header]" with
    // the count entries and header is not a user key/value pair
    numEntries = (totalEntriesCalced/2);
    {
        stringstream trmsg;
        trmsg << endl << prefix
              << "Number of pages parsed=" << numPages << endl
              << "Total Number of entries parsed=" << totalEntriesCalced << endl
              << " containing parsed   sum number of entries=" << numEntriesSum << endl
              << " containing calced total number of entries=" << numEntries << endl
              << " with total number of bytes for keys=" << keySumBytes << endl
              << " and with total number of bytes for values=" << valSumBytes;
        TRACE_TEST_CASE(trmsg.str())
    }

    stringstream numss;
    numss << prefix
          << "Number of pages parsed=" << numPages
          << " should be=" << expectedNumPages
          << " Number of entries parsed=" << numEntries
          << " should be=" << expectedNumEntries << endl;
    CPPUNIT_ASSERT_MESSAGE(numss.str(), (expectedNumEntries == numEntries && expectedNumPages == numPages));

    // wait on child to get exit status
    int status;
    pid_t pidret = waitpid(pid, &status, 0);  // wait for child

    stringstream ssmsg;
    ssmsg << prefix;
    if (pidret == -1)
    {
        perror("DataInterchangeBaseTestSuite::parent wait on child failed");
        int errnum = errno;
        ssmsg << "waitpid(" << pid << ") returned errno=" << errnum << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (pidret != -1));

    int signo = 0;
    if (WIFSIGNALED(status))
    {
        signo = WTERMSIG(status);
    }

    if (pidret == pid)
    {
        if (WIFEXITED(status)) // normal termination - aka child called exit
        {
            int exstatus = WEXITSTATUS(status);
            ssmsg << "Child exit value=" << exstatus << endl;
            CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (exstatus == 0));
        }
        ssmsg << " System reports the child was terminated with signal=" << signo << endl;
    }
    else
    {
        ssmsg << "Indeterminate what happened with the child. Received status on Unknown child pid=" << pidret << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (pidret == pid && WIFEXITED(status)));
}

void
DataInterchangeBaseTestSuite::parseDumpV3Output(stringstream &istrm, const string &prefix, int expectedNumPages, int expectedNumEntries, int pid)
{
    char buff[256];
    int pageNum       = -1;
    int numPages      = 0;
    int numEntries    = 0;
    int numEntriesSum = 0;
    while (!istrm.eof())
    {
        memset(buff, 0, sizeof(buff));
        istrm.getline(buff, sizeof(buff), '\n');
        if (istrm.fail() || istrm.bad())
        {
            break;
        }

        string line = buff;
        // for start of page, need "Page=nnn" and "entries=nn"
        // how do I know its the start of a page?

        if (line.find("PAGE=") != string::npos) // start of a page
        {
            stringstream pstrm(line);
            // get the page number and the number of entries
            string pagetoken, addrtoken, entriestoken;
            pstrm >> pagetoken >> addrtoken >> entriestoken;

            // parse pagetoken
            pageNum    = parseNumber(pagetoken, "=");
            if (pageNum != -1)
            {
                numPages++;
                numEntries += parseNumber(entriestoken, "=");
                {
                    stringstream trmsg;
                    trmsg << prefix << "Starting on page=" << pageNum
                          << " that has the number of entries=" << numEntries;
                    TRACE_TEST_CASE(trmsg.str())
                }
                if (numEntries != -1)
                {
                    numEntriesSum += numEntries;
                }
            }
        }
    }
    stringstream numss;
    numss << prefix
          << "Number of pages parsed=" << numPages
          << " should be=" << expectedNumPages
          << " Number of entries parsed=" << numEntries
          << " should be=" << expectedNumEntries << endl;
    CPPUNIT_ASSERT_MESSAGE(numss.str(), (expectedNumEntries == numEntries && expectedNumPages == numPages));

    // wait on child to get exit status
    int status;
    pid_t pidret = waitpid(pid, &status, 0);  // wait for child

    stringstream ssmsg;
    ssmsg << prefix;
    if (pidret == -1)
    {
        perror("DataInterchangeBaseTestSuite::parent wait on child failed");
        int errnum = errno;
        ssmsg << "waitpid(" << pid << ") returned errno=" << errnum << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (pidret != -1));

    int signo = 0;
    if (WIFSIGNALED(status))
    {
        signo = WTERMSIG(status);
    }
    if (pidret == pid)
    {
        if (WIFEXITED(status)) // normal termination - aka child called exit
        {
            int exstatus = WEXITSTATUS(status);
            ssmsg << "Child exit value=" << exstatus << endl;
            CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (exstatus == 0));
        }
        ssmsg << " System reports the child was terminated with signal=" << signo << endl;
    }
    else
    {
        ssmsg << "Indeterminate what happened with the child. Received status on Unknown child pid=" << pidret << endl;
    }
    CPPUNIT_ASSERT_MESSAGE(ssmsg.str(), (pidret == pid && WIFEXITED(status)));
}

void
DataInterchangeBaseTestSuite::forkAndParseDump(const string &prefix, MdbmHolder &db, int numRecsAdded, int numPagesInDB)
{
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
        // tell child to begin
        parSendFD.sendMsg(LockBaseTestSuite::_Continue);

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

        {
            stringstream trmsg;
            trmsg << SUITE_PREFIX() << prefix
                  << "Parent: Child output stream=" << strstrm.str() << endl;
            TRACE_TEST_CASE(trmsg.str())
        }

        // parse childs output
        parseDumpV3Output(strstrm, prefix, numPagesInDB, numRecsAdded, pid);
    }
    else  // child
    {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        stringstream errstr;
        errstr << prefix
               << "Child: dup2 FAILed, cannot continue the test. errno=" << errnum << endl;
        CPPUNIT_ASSERT_MESSAGE(errstr.str(), (ret != -1));

        // wait for parent to tell us to perform the dump
        string resp;
        string errMsg = "Parent should have told us to continue: ";
        bool selret = LockBaseTestSuite::waitForResponse(parReadFD, resp, errMsg);
        CPPUNIT_ASSERT_MESSAGE(errMsg.insert(0, prefix), (selret == true));

        // perform the dump
        MdbmHolder c_dbh(mdbm_open((const char*)db, versionFlag | MDBM_O_RDWR, 0644, 0, 0));

        stringstream odss;
        odss << prefix
             << "Child: mdbm_open FAILed for DB=" << ((const char*)db) << endl;
        CPPUNIT_ASSERT_MESSAGE(odss.str(), ((MDBM*)c_dbh != 0));

        mdbm_dump_all_page(c_dbh);
        exit(0);
    }
}
void
DataInterchangeBaseTestSuite::DumpAllEmptyDbD1()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcD1";
    string dbName = GetTmpName(baseName);
    MdbmHolder dbh(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC D1: ";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = dbh.Open(openFlags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));
    // HAVE: empty DB

    prefix = "TC D1: ";
    forkAndParseDump(prefix, dbh, 0, 1);
}
void
DataInterchangeBaseTestSuite::DumpAllPartialFilledDbD2()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcD2";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcD2key";
    string value       = "tcD2dummy";
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D2: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC D2: ";
    forkAndParseDump(prefix, db, numRecsAdded, 1);
}
void
DataInterchangeBaseTestSuite::DumpAllSinglePageFilledDbD3()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcD3";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcD3key";
    string value       = "tcD3dummy";
    int numPages     = 1;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D3: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC D3: ";
    forkAndParseDump(prefix, db, numRecsAdded, numPages);
}
void
DataInterchangeBaseTestSuite::DumpAllMultiPageFilledDbD4()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcD4";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcD4key";
    string value       = "tcD4dummy";
    int numPages     = 2;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC D4: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC D4: ";
    forkAndParseDump(prefix, db, numRecsAdded, numPages);
}

void
DataInterchangeBaseTestSuite::DumpAllCacheDbD5()
{
    string prefix = "TC D5: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "ditcD5";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY;
    datum k, v;
    int ret = -1;

    k.dptr = (char*)"key";
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)"value";
    v.dsize = strlen(v.dptr);
    MDBM* dbh = mdbm_open(dbName.c_str(), openflags, 0644, DEFAULT_PAGE_SIZE, 0);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(dbh, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(dbh, MDBM_CACHEMODE_GDSF));
    CPPUNIT_ASSERT((ret = mdbm_store(dbh, k, v, MDBM_INSERT)) == 0);
    mdbm_dump_all_page(dbh);
    mdbm_close(dbh);
}

void
DataInterchangeBaseTestSuite::DumpAllCacheDbD6()
{
    string prefix = "TC D6: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "ditcD6";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;
    int ret = -1;
    int _pageSize = 512;
    int _largeObjSize = 512;

    string key = "lokey01", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    MDBM* dbh = mdbm_open(dbName.c_str(), openflags, 0644, _pageSize, 0);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(dbh, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(dbh, MDBM_CACHEMODE_GDSF));
    CPPUNIT_ASSERT((ret = mdbm_store(dbh, dk, dv, MDBM_INSERT)) == 0);
    mdbm_dump_all_page(dbh);
    mdbm_close(dbh);
}

void
DataInterchangeBaseTestSuite::SaveLargeDB()
{
    string prefix = "TC D6: ";
    TRACE_TEST_CASE(prefix);
    string baseName = "ditcD6";
    string dbName = GetTmpName(baseName);
    int openflags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag | MDBM_CACHE_MODIFY | MDBM_LARGE_OBJECTS | MDBM_DBSIZE_MB;
    int ret = -1;
    int _pageSize = 512;
    int _largeObjSize = 512;

    string sfbaseName = baseName;
    sfbaseName += "savefile";
    string saveFile =  GetTmpName(sfbaseName);
    MdbmHolder savedb(saveFile);

    string key = "lokey01", val;
    val.assign(_largeObjSize, 'l');
    datum dk;
    dk.dptr  = (char*)key.c_str();
    dk.dsize = key.size();
    datum dv;
    dv.dptr  = (char*)val.c_str();
    dv.dsize = _largeObjSize;

    MDBM* dbh = mdbm_open(dbName.c_str(), openflags, 0644, _pageSize, 0);
    CPPUNIT_ASSERT((ret = mdbm_store(dbh, dk, dv, MDBM_INSERT)) == 0);
    CPPUNIT_ASSERT_EQUAL(0, mdbm_save(dbh, saveFile.c_str(), MDBM_O_CREAT, 0644, Z_BEST_COMPRESSION));
    mdbm_close(dbh);
}

void
DataInterchangeBaseTestSuite::forkAndParseDumpPages(const string &prefix, MdbmHolder &db, vector<int> &pages, int expectedNumEntriesOnPage, bool expectSuccess)
{
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
        // tell child to begin
        parSendFD.sendMsg(LockBaseTestSuite::_Continue);

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

        {
            stringstream trmsg;
            trmsg << prefix << "Parent: Child output stream=" << strstrm.str();
            TRACE_TEST_CASE(trmsg.str())
        }

        // parse childs output
        if (expectSuccess)
        {
            int numRecs = expectedNumEntriesOnPage;
            parseDumpOutput(strstrm, prefix, pages.size(), numRecs, pid);
        }
        else
        {
            parseDumpOutput(strstrm, prefix, 0, 0, pid);
        }
    }
    else  // child
    {
      try {
        // setup our stdout to be the pipe
        int ret = dup2(childSendFD, STDOUT_FILENO);
        int errnum = errno;
        stringstream errstr;
        errstr << prefix
               << "Child: dup2 FAILed, cannot continue the test. errno=" << errnum << endl;
        if (ret < 0) {
          fprintf(stderr, "ERROR: %s\n", errstr.str().c_str());
          exit(2);
        }

        // wait for parent to tell us to perform the dump
        string resp;
        string errMsg = "Parent should have told us to continue: ";
        bool selret = LockBaseTestSuite::waitForResponse(parReadFD, resp, errMsg);
        if (selret != true) {
          fprintf(stderr, "ERROR: %s %s\n", prefix.c_str(), errMsg.c_str());
          exit(2);
        }


        // perform the dump
        MdbmHolder c_dbh(mdbm_open((const char*)db, versionFlag | MDBM_O_RDWR, 0644, 0, 0));

        stringstream odss;
        odss << prefix
             << "Child: mdbm_open FAILed for DB=" << ((const char*)db) << endl;
        if (NULL == (MDBM*)c_dbh) {
          fprintf(stderr, "ERROR: %s\n", odss.str().c_str());
          exit(2);
        }

        for (unsigned int cnt = 0; cnt < pages.size(); ++cnt)
            mdbm_dump_page(c_dbh, pages[cnt]);

        exit(0);
      } catch (exception &e) {
        fprintf(stderr, "UNEXPECTED EXCEPTION: %s\n", e.what());
        exit(3);
      }
    }
}
void
DataInterchangeBaseTestSuite::DumpPage0EmptyDbE1()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE1";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC E1: Open DB FAILed: ";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = db.Open(openFlags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));
    // HAVE: empty DB

    prefix = "TC E1: ";
    vector<int> pages;
    pages.push_back(0);
    forkAndParseDumpPages(prefix, db, pages, 0);
}
void
DataInterchangeBaseTestSuite::DumpBadPageEmptyDbE2()
{
#if 0
// FIX BZ 5431921 - v2: mdbm_dump_page: SEGV when invalid page number specified
    string baseName = "ditcE2";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);

    string prefix = SUITE_PREFIX();
    prefix += "TC E2: ";
    int openFlags = MDBM_O_RDWR | MDBM_O_CREAT | MDBM_O_TRUNC | versionFlag;
    int ret = db.Open(openFlags, 0644, 512, 0);
    CPPUNIT_ASSERT_MESSAGE(prefix, (ret == 0));
    // HAVE: empty DB

    prefix = "TC E2: ";
    vector<int> pages;
    pages.push_back(1);
    forkAndParseDumpPages(prefix, db, pages, 0, false);
#endif
}
void
DataInterchangeBaseTestSuite::DumpPage0PartFilledDbE3()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE3";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE3key";
    string value       = "tcE3dummy";
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E3: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC E3: ";
    vector<int> pages;
    pages.push_back(0);
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded);
}
void
DataInterchangeBaseTestSuite::DumpBadPagePartFilledDbE4()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE4";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE4key";
    string value       = "tcE4dummy";
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, 0, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E4: FAILed to create a partial filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC E4: ";
    vector<int> pages;
    pages.push_back(2);
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded, false);
}
void
DataInterchangeBaseTestSuite::DumpPage0SinglePageFilledDbE5()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE5";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE5key";
    string value       = "tcE5dummy";
    int numPages     = 1;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E5: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC E5: ";
    vector<int> pages;
    pages.push_back(0);
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded);
}
void
DataInterchangeBaseTestSuite::DumpBadPageSinglePageFilledDbE6()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE6";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE6key";
    string value       = "tcE6dummy";
    int numPages     = 1;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E6: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC E6: ";
    vector<int> pages;
    pages.push_back(3);
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded, false);
}
void
DataInterchangeBaseTestSuite::DumpPage1MultiPageFilledDbE7()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE7";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE7key";
    string value       = "tcE7dummy";
    int numPages     = 2;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E7: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC E7: ";
    vector<int> pages;
    pages.push_back(1);
    numRecsAdded /= numPages;
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded);
}
void
DataInterchangeBaseTestSuite::DumpBadPageMultiPageFilledDbE8()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE8";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE8key";
    string value       = "tcE8dummy";
    int numPages     = 2;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E8: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    string prefix = "TC E8: ";
    vector<int> pages;
    pages.push_back(4);
    numRecsAdded /= numPages;
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded, false);
}
void
DataInterchangeBaseTestSuite::DumpAllPagesMultiPageFilledDbE9()
{
    if (!runStdoutTest()) {
        return;
    }
    string baseName = "ditcE8";
    string dbName = GetTmpName(baseName);
    MdbmHolder db(dbName);
    string keyBaseName = "tcE8key";
    string value       = "tcE8dummy";
    int numPages     = 2;
    int numRecsAdded = 0;
    MDBM *dbh = createDbAndAddData(db, numPages, keyBaseName, value, numRecsAdded);

    stringstream cdss;
    cdss << SUITE_PREFIX()
         << "TC E8: FAILed to create a single paged filled DB=" << dbName
         << endl;
    CPPUNIT_ASSERT_MESSAGE(cdss.str(), (dbh != 0));

    // get all the valid page numbers
    string prefix = "TC E8: ";
    vector<int> pages;
    for (int cnt = 0; cnt < numPages; ++cnt)
    {
        pages.push_back(cnt);
    }
    forkAndParseDumpPages(prefix, db, pages, numRecsAdded);
}

void
DataInterchangeBaseTestSuite::TestNullGetPage()
{
    string prefix = string("TestNullGetPage") + versionString + ":";
    TRACE_TEST_CASE(__func__)

    MdbmHolder mdbm(GetTmpPopulatedMdbm(prefix, MDBM_O_RDWR, 0644, DEFAULT_PAGE_SIZE, 0));
    datum ky;

    ky.dptr = NULL;   // Test Null Key
    ky.dsize = 10;
    string msg("Null key");

    getpage(msg, mdbm, &ky, false);

    ky.dsize = 0;            // Test of (dsize == 0)
    ky.dptr = const_cast<char *>(msg.c_str());   // Not NULL
    msg = string("Empty key");

    getpage(msg, mdbm, &ky, false);
}

