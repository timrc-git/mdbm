/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM_LOG_H_ONCE
#define MDBM_LOG_H_ONCE

#include <syslog.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>


#ifdef  __cplusplus
extern "C" {
#endif


/*============================================================*/
/* Core logging functions.                                    */
/*============================================================*/

/* Manual logging. Use the macros below instead */
int mdbm_log_at (const char* file, int line, int level, const char* format, ...)
        __attribute__ ((format (printf,4,5)))
        __attribute__ ((visibility ("default")));
int mdbm_log_vlog_at (const char* file, int line, int level, const char* format, va_list args)
        __attribute__ ((format (printf,4,0)))
        __attribute__ ((visibility ("default")));

int mdbm_logerror_at (const char* file, int line, int level, int error, const char* format, ...)
        __attribute__ ((format (printf,5,6)))
        __attribute__ ((visibility ("default")));
int mdbm_log_vlogerror_at (const char* file, int line, int level, int error, const char* format, va_list args)
        __attribute__ ((format (printf,5,0)))
        __attribute__ ((visibility ("default")));

/* Set the minimum logging level. Lower priority messages are discarded */
void mdbm_log_minlevel(int lvl);

/* disable variadic macro pedantic warning */
/* #pragma GCC diagnostic ignored "-Wvariadic-macros" */

/* Logs a message. */
#define mdbm_log(lvl,fmt ...)  \
  mdbm_log_at (__FILE__, __LINE__, lvl, fmt)

/* Logs a message with error code. */
/* LOG_EMERG will cause an abort() */
/* LOG_ALERT will cause a non-zero exit*/
#define mdbm_logerror(lvl,err,fmt ...)  \
  mdbm_logerror_at (__FILE__, __LINE__, lvl, err, fmt)

/* Like mdbm_log above, but takes a va_list for 'args'.  */
#define mdbm_vlog(lvl,fmt,args)  \
  mdbm_log_vlog_at (__FILE__, __LINE__, lvl, fmt, args)
/* Like mdbm_logerror above, but takes a va_list for 'args'.  */
#define mdbm_log_vlogerror(lvl,err,fmt,args)  \
  mdbm_log_vlogerror_at (__FILE__, __LINE__, lvl, err, fmt, args)

/*============================================================*/
/* plugin definition and registration                         */
/*============================================================*/

/* select a logging plugin by name */
/* standard plugin names are "stderr", "file", and "syslog" */
/* default plugin is "stderr" */
/* (can also be set at startup via "MDBM_LOG_DEST" env variable */
int mdbm_select_log_plugin(const char* name);

/* select a filename for the "file" log plugin. */
/* (can also be set at startup via "MDBM_LOG_DEST_NAME" env variable */
int mdbm_set_log_filename(const char* name);

/* plugin function to set the minimum log level */
typedef void (*log_minlevel_func)(int lvl);

/* plugin function to log a message (pre-formatted) */
/* file and line are the source code location of the log message */
typedef void (*log_func)(const char* file, int line, int level, char* msg, int msglen);

struct mdbm_log_plugin {
  const char*       name;          /* the name of this plugin */
  log_minlevel_func set_min_level; /* function to set minimum log level */
  log_func          do_log;        /* function to ouput log messages */
};

typedef struct mdbm_log_plugin mdbm_log_plugin_t;

/* manually register a plugin. */
int mdbm_log_register_plugin(mdbm_log_plugin_t plugin);

/* use this macro to automatically register a plugin when the lib/exe is first loaded */
#define MDBM_LOG_REGISTER_PLUGIN(name,set_level_func,do_log_func) \
  static mdbm_log_plugin_t name##plugin_record = \
    {#name, (log_minlevel_func)set_level_func, (log_func)do_log_func}; \
  static void __attribute__ ((constructor)) name##log_plugin_register() { \
    mdbm_log_register_plugin(name##plugin_record); \
  }


#ifdef  __cplusplus
}
#endif


#endif /* MDBM_LOG_H_ONCE */
