/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM_UTIL_H_ONCE
#define MDBM_UTIL_H_ONCE

#include <stdint.h>

#ifdef  __cplusplus
extern "C" {
#endif

/*
// Here are defined names and other strings used in the config JSON output.
// An example of the JSON output could be:
// { "dbversion" : 2, "pagesize" : "512", "maxpageslimit" : 8,
//   "hashcode" : 5, "largeobjects" : true, "spillsize" : "256",
//   "alignment" : 0, "currentdbsize" : "5G", "fileperms" : "0644",
//   "cachemode" : "NONE" }
//
// These define all the names for config JSON output
*/
#define JSVERSION_NAME   "dbversion"
#define JSPAGESIZE_NAME  "pagesize"
#define JSMAXPAGES_NAME  "maxpageslimit"
#define JSHASHCODE_NAME  "hashcode"
#define JSLARGEOBJ_NAME  "largeobjects"
#define JSSPILLSZ_NAME   "spillsize"
#define JSALIGNMENT_NAME "alignment"
#define JSCURDBSIZE_NAME "currentdbsize"
#define JSFILEPERMS_NAME "fileperms"
#define JSCACHEMODE_NAME "cachemode"

/* valid strings for the JSON JSCACHEMODE_NAME value, ex: "cachemode" : "LFU" */
#define JSCACHEMODE_NONE "NONE"
#define JSCACHEMODE_LFU  "LFU"
#define JSCACHEMODE_LRU  "LRU"
#define JSCACHEMODE_GDSF "GDSF"

/* suffixes used for JSON size values; ex: "currentdbsize" : "2M" */
#define JSSUFFIX_G  "G"
#define JSSUFFIX_M  "M"
#define JSSUFFIX_K  "K"

extern void
reset_getopt();

/* prints a message and calls exit() on error */
extern uint64_t
mdbm_util_get_size (const char* arg, int default_multiplier);

/* returns the value in 'val' and does not call exit() */
extern int
mdbm_util_get_size_ref (const char* arg, int default_multiplier, uint64_t *val);


#define lockstr_to_flags_usage(prefix)                                    \
  prefix "exclusive  -  Exclusive locking \n"                             \
  prefix "partition  -  Partition locking (requires a fixed size MDBM)\n" \
  prefix "shared     -  Shared locking\n"                                 \
  prefix "any        -  use whatever locks exist\n"                       \
  prefix "none       -  no locking\n"

/* Converts lock_string to suitable mdbm_open flags, placed in lock_flags.
 * lock_string should be one of "exclusive", "partition", "shared", or "none".
 * returns 0 on success, -1 on failure.
 * NOTE: this destructively *sets* lock_flags to an exact value. Don't pass in
 * existing flags and expect them to be "or'ed".
 */
extern int 
mdbm_util_lockstr_to_flags(const char* lock_string, int *lock_flags);

#ifdef  __cplusplus
}
#endif

#endif  /* MDBM_UTIL_H_ONCE */
