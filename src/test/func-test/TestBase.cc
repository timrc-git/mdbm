/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// cheap imitation of a symbolic link for platforms that don't have them.
#include "../unit-test/TestBase.cc"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <iostream>
#include <ostream>
#include <sstream>

#include <cppunit/TestAssert.h>
#include <cppunit/TestCase.h>
#include <cppunit/TestFixture.h>
#include <cppunit/TestResult.h>
#include <cppunit/TestResultCollector.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include <mdbm.h>


using namespace std;

std::string getMdbmToolPath(const std::string& tool_name) {
  // TODO TODO TODO break this out into a function, get path from Makefile
  // TODO TODO TODO test for existence and complain if non-existent
  char* plat = getenv("OBJDIR");
  if (!plat) { plat = (char*)"object"; }
  string path = string("../../tools/") + plat +string("/")+tool_name;
  return path;
}

//  Function (Overloading "<<" operator) to print special data structures like datum.
std::ostream& operator<<(std::ostream& os, const datum& d) {
    char* dptr = const_cast<char*>(d.dptr);
    unsigned int dsize = d.dsize;

    os << "[datum" << ",dptr_=" << ((void *)dptr) << ", dsize_=" << dsize<<"]";
    return os;
}

void getUserOrUid(string& str) {
    char *username = getenv("USER");
    if (!username) { username = getlogin(); }

    if (username) {
        str = username;
    } else {
        char buf[32];
        sprintf(buf, "%d", (int)getuid());
        str=buf;
    }
}

//  Function to create directory structure where Mdbm files will be stored
string createBaseDir(void) {
    string id;
    getUserOrUid(id);
    string path = string("/tmp/mdbm/")+id+string("/func-test");
    string cmd = string("mkdir -p ")+path;

    if (system(cmd.c_str())) {
        cerr << "Failed to create base directory, errno= " << errno << "," << strerror(errno) << "command: " << cmd << endl;
        return "";
    }

    return path;
}


// Function to create Mdbm as a cache
MDBM* createCacheMdbm(const char* db, int flags, int pgSize, int dbSize, int limitSize) {
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db, flags, 0600, pgSize, dbSize)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, static_cast<size_t>(limitSize), NULL, NULL));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_set_cachemode(mdbm, MDBM_CACHEMODE_GDSF));
    return mdbm;
}


// Function to create Mdbm as a backing store without LO support
MDBM* createBSMdbmNoLO(const char* db, int flags, int pgSize, int dbSize, int limitSize, int windowSize) {
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db, flags, 0600, pgSize, dbSize)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, static_cast<size_t>(limitSize), NULL, NULL));
    if (windowSize) {
      CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, static_cast<size_t>(windowSize)));
    }
    return mdbm;
}

// Function to create Mdbm as a backing store without LO support
MDBM* createBSMdbmWithLO(const char* db, int flags, int pgSize, int dbSize, int limitSize, int spillSize, int windowSize) {
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db, flags, 0600, pgSize, 0)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_setspillsize(mdbm, spillSize));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_limit_size_v3(mdbm, static_cast<size_t>(limitSize), NULL, NULL));
    if (windowSize) {
      CPPUNIT_ASSERT_EQUAL(0, mdbm_set_window_size(mdbm, static_cast<size_t>(windowSize)));
    }
    return mdbm;
}

// Function to create Mdbm
MDBM* createMdbm(const char* db, int flags, int pgSize, int dbSize, int spillSize) {
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db, flags, 0600, pgSize, dbSize)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_MD5));
    CPPUNIT_ASSERT_EQUAL(0, mdbm_setspillsize(mdbm, spillSize));
    return mdbm;
}

// Function to create Mdbm
MDBM* createMdbmNoLO(const char* db, int flags, int pgSize, int dbSize) {
    MDBM* mdbm = NULL;
    CPPUNIT_ASSERT((mdbm = mdbm_open(db, flags, 0600, pgSize, dbSize)) != NULL);
    CPPUNIT_ASSERT_EQUAL(1, mdbm_sethash(mdbm, MDBM_HASH_MD5));
    return mdbm;
}

// Function to find number of Large objects in Mdbm
int getNumOfLargeObjects(string dirName, string dbName) {
    string mdbmName = dirName + dbName;
    string statFile = dirName + dbName + ".txt";
    system("mdbm_stat mdbmName > statFile");
    int num = 0;
    system("grep -i large-obj statFile | grep \"min level\" | awk \'{print ($7)}\'");
    return num;
}

// Function to fetch number of records in an Mdbm
int getNumberOfRecords(MDBM* mdbm, datum k) {
    datum v;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    int numOfRecords = -1, ret = 0;
    while (ret == 0) {
        ret = mdbm_fetch_dup_r(mdbm, &k, &v, &iter);
        numOfRecords++;
    }
    return numOfRecords;
}

// Function to check if a record exists in Mdbm or not
int findRecord(MDBM* mdbm, datum k, datum v) {
    datum v1;
    MDBM_ITER iter;
    MDBM_ITER_INIT(&iter);
    int ret = 0;
    while (ret == 0) {
        ret = mdbm_fetch_dup_r(mdbm, &k, &v1, &iter);
        if (!v1.dptr) { return -1; }
        if (strncmp(v1.dptr, v.dptr, v1.dsize) == 0) {
            return 0;
        }
    }
    return -1;
}

// This function stores and fetches the record with normal size
int storeAndFetch(MDBM* db, int _storeFlag) {
    datum k, v, value;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Store a record
    if (0 != mdbm_store(db, k, v, _storeFlag)) {
        return -1;
    }

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (0 == strncmp(value.dptr, v.dptr, value.dsize)) {
        return 0;
    } else {
        return -1;
    }
}


// This function stores and fetches the record with any size
// mainly useful for large records
int storeAndFetchLO(MDBM* db, int _storeFlag, int size) {
    datum k, v, value;
    char* large_obj = new char[size];

    // Create key-value pair
    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large value
    for (int i = 0; i < size; i++) {
        large_obj[i] = 'a';
    }
    large_obj[size-1] = '\0';
    v.dptr = large_obj;
    v.dsize = size;

    // Store a record
    if (0 != mdbm_store(db, k, v, _storeFlag)) {
        delete[] large_obj;
        return -1;
    }

    if (!(_storeFlag & MDBM_INSERT_DUP)) {
      // Fetch the value for stored key
      value = mdbm_fetch(db, k);
    } else {
        // duplicate entries... find the right one to compare
        int ret = 0;
        MDBM_ITER iter;
        MDBM_ITER_INIT(&iter);
        while (ret == 0) {
            ret = mdbm_fetch_dup_r(db, &k, &value, &iter);
            if ((size == value.dsize) &&
                (0 == strncmp(value.dptr, v.dptr, value.dsize))) {
              break;
            }
        }
    }

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(size, value.dsize);

    if (0 == strncmp(value.dptr, v.dptr, value.dsize)) {
        delete[] large_obj;
        return 0;
    }
    delete[] large_obj;
    return -1;
}


// This function stores and fetches the record with LO. Update same
// record with new LO and verify that its getting updated correctly
int storeUpdateAndFetchLO(MDBM* db, int _storeFlag, int size) {
    datum k, v, value;
    char* large_obj = new char[size];

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large value
    for (int i = 0; i < size; i++) {
        large_obj[i] = 'a';
    }
    large_obj[size-1] = '\0';
    v.dptr = large_obj;
    v.dsize = size;

    // Store a record using parent thread
    if (0 != mdbm_store(db, k, v, _storeFlag)) {
        delete[] large_obj;
        return -1;
    }

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (0 != strncmp(value.dptr, v.dptr, value.dsize)) {
        delete[] large_obj;
        return -1;
    }

    // Create large value
    for (int i = 0; i < size; i++) {
        large_obj[i] = 'b';
    }
    large_obj[size-1] = '\0';
    v.dptr = large_obj;
    v.dsize = size;

    // Replace/Update record with new value
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_REPLACE));

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as updates value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (strncmp(value.dptr, v.dptr, value.dsize) == 0) {
        delete[] large_obj;
        return 0;
    }
    delete[] large_obj;
    return -1;
}


// This function stores and fetches the record with LO. Update same
// record with new LO and verify that its getting updated correctly
int storeDeleteAndFetchLO(MDBM* db, int _storeFlag, int size) {
    datum k, v, value;
    char* large_obj = new char[size];

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);

    // Create large value
    for (int i = 0; i < size; i++) {
        large_obj[i] = 'a';
    }
    large_obj[size-1] = '\0';
    v.dptr = large_obj;
    v.dsize = strlen(v.dptr);

    // Store a record using parent thread
    if (0 != mdbm_store(db, k, v, _storeFlag)) {
        delete[] large_obj;
        return -1;
    }

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (0 != strncmp(value.dptr, v.dptr, value.dsize)) {
        delete[] large_obj;
        return -1;
    }

    // Delete record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(db, k));

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is NULL
    if (value.dsize == 0) {
        delete[] large_obj;
        return 0;
    }
    delete[] large_obj;
    return -1;
}


// This function stores and fetches the record with normal size
// Update same record and verify that its getting updated correctly
int storeUpdateAndFetch(MDBM* db, int _storeFlag) {
    datum k, v, value;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, _storeFlag));

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (0 != strncmp(value.dptr, v.dptr, value.dsize)) {
        return -1;
    }

    v.dptr = (char*)("newvalue");
    v.dsize = strlen(v.dptr);

    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, MDBM_REPLACE));

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (0 == strncmp(value.dptr, v.dptr, value.dsize)) {
        return 0;
    }
    return -1;
}


// This function stores and fetches the record with normal size
// Delete same record and verify that its getting deleted correctly
int storeDeleteAndFetch(MDBM* db, int _storeFlag) {
    datum k, v, value;

    k.dptr = (char*)("key");
    k.dsize = strlen(k.dptr);
    v.dptr = (char*)("value");
    v.dsize = strlen(v.dptr);

    // Store a record using parent thread
    CPPUNIT_ASSERT_EQUAL(0, mdbm_store(db, k, v, _storeFlag));

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    CPPUNIT_ASSERT_EQUAL(v.dsize, value.dsize);
    if (0 != strncmp(value.dptr, v.dptr, value.dsize)) {
        return -1;
    }

    // Delete existing record
    CPPUNIT_ASSERT_EQUAL(0, mdbm_delete(db, k));

    // Fetch the value for stored key
    value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    if (0 == value.dsize) {
        return 0;
    }
    return -1;
}

// Function to check if the record exists in Mdbm and
// the values match
int checkRecord(MDBM* db, datum k, datum v) {
    // Fetch the value for stored key
    datum value = mdbm_fetch(db, k);

    // Verify that fetched value is same as stored value
    if (v.dsize != value.dsize) {
        cout<<"The sizes of values are different"<<endl;
        return -1;
    }
    CPPUNIT_ASSERT_EQUAL(0, memcmp(value.dptr, v.dptr, value.dsize));
    return 0;
}

// Function to sleep for random time between 1 ms to 100 ms
void sleepForRandomTime() {
    int time = ((rand()%90001) + 1000)/10;
    //cout<<"Time is -> "<<time<<endl;
    // usleep(x) sleeps for x microseconds
    usleep(time);
}

