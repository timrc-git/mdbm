/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

// cheap imitaion of a symbolic link for platforms that don't have them.
#include "../unit-test/TestBase.hh"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <ostream>
#include <mdbm.h>

using namespace std;

//  Common functions which can be used across files for functional API test cases

std::string getMdbmToolPath(const std::string& tool_name);

std::ostream& operator<<(std::ostream& os, const datum& d);

string createBaseDir(void);

MDBM* createMdbm(const char*, int, int, int, int);

MDBM* createMdbmNoLO(const char*, int, int, int);

int getNumberOfRecords(MDBM*, datum);

int findRecord(MDBM*, datum, datum);

MDBM* createCacheMdbm(const char*, int, int, int, int);

MDBM* createBSMdbmNoLO(const char*, int, int, int, int, int);

MDBM* createBSMdbmWithLO(const char*, int, int, int, int, int, int);

int storeAndFetch(MDBM*, int);

int storeAndFetchLO(MDBM*, int, int);

int storeUpdateAndFetch(MDBM*, int);

int storeDeleteAndFetchLO(MDBM*, int, int);

int storeDeleteAndFetch(MDBM*, int);

int storeUpdateAndFetchLO(MDBM*, int, int);

int checkRecord(MDBM*, datum, datum);

void sleepForRandomTime(void);
