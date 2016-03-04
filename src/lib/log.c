/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <errno.h>
#include <syslog.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/time.h>
#include <unistd.h>

#include "atomic.h"
#include "mdbm_log.h"

/* not used...
  enum MDBM_LOG_DEST { // matching ysys_log declarations
      MDBM_LOG_STDOUT = 0x1,
      MDBM_LOG_SYSLOG = 0x2,
      MDBM_LOG_LOGFILE = 0x4,
      MDBM_LOG_STDERR = 0x8
  };
  
  enum MDBM_LOG_LEVEL { // matching ysys_log declarations
      MDBM_LOG_NONE = -1,
      MDBM_LOG_OFF = -1,
  
      MDBM_LOG_EMERGENCY = LOG_EMERG,
      MDBM_LOG_ALERT = LOG_ALERT,
      MDBM_LOG_CRITICAL = LOG_CRIT,
      MDBM_LOG_ERROR = LOG_ERR,
      MDBM_LOG_WARNING = LOG_WARNING,
      MDBM_LOG_NOTICE = LOG_NOTICE,
      MDBM_LOG_INFO = LOG_INFO,
      MDBM_LOG_DEBUG = LOG_DEBUG,
      MDBM_LOG_DEBUG2 = LOG_DEBUG+1,
      MDBM_LOG_DEBUG3 = LOG_DEBUG+2,
      MDBM_LOG_MAXLEVEL,
  
      MDBM_LOG_ABORT = LOG_EMERG,
      MDBM_LOG_FATAL = LOG_ALERT
  };
*/


static int mdbm_min_log_level = LOG_ERR;
static mdbm_log_plugin_t log_plugin;
static mdbm_log_plugin_t *log_plugin_list = NULL;
static int log_plugin_count=0;

int mdbm_log_register_plugin(mdbm_log_plugin_t plugin) {
  /* choose output stderr/stdout/file/plugin based on env var */
  const char *dest = getenv("MDBM_LOG_DEST");
  void* tmp = realloc(log_plugin_list, (log_plugin_count+1)*sizeof(mdbm_log_plugin_t));
  if (!tmp) { abort(); }
  log_plugin_list = (mdbm_log_plugin_t*)tmp;
  log_plugin_list[log_plugin_count++]=plugin;

  /* default to stderr */
  if (!(dest && dest[0])) { 
    dest = "stderr"; 
  }
  if (!strcmp(dest, plugin.name)) {
    /* we have a match.. select it */
    mdbm_select_log_plugin(dest);
  }
  return 0;
}

int mdbm_select_log_plugin(const char* name) {
  int i;
  /* we don't expect many plugins, or frequent calls to select */
  /* so linear scan is enough */
  for (i=log_plugin_count-1; i>=0; --i) {
    mdbm_log_plugin_t cur = log_plugin_list[i];
    if (!strcmp(name, cur.name)) { 
      log_plugin=cur;
      return 0;
    }
  }
  return -1;
}

void mdbm_log_minlevel(int lvl) {
  /*mdbm_log_minlevel_inner(lvl); */
  log_plugin.set_min_level(lvl);
}

void mdbm_log_core(const char* file, int line, int level, char* msg, int msglen) {
  /*mdbm_log_core_inner(file, line, level, msg, msglen); */
  log_plugin.do_log(file, line, level, msg, msglen);
}


size_t mdbm_strlcpy (char* dst, const char* src, size_t dstlen) {
    size_t srclen = strlen(src);
    if (srclen >= dstlen) {
        srclen = dstlen-1;
    }
    memcpy(dst,src,srclen);
    dst[srclen] = 0;
    return srclen;
}

int mdbm_log_at (const char* file, int line, int level, const char* format, ...) {
    int ret;
    va_list args;
    va_start(args,format);
    ret = mdbm_log_vlog_at(file, line, level, format, args);
    va_end(args);
    return ret;
}

#define MESSAGE_MAX 4096

int mdbm_log_vlogerror_at (const char* file, int line, int level, int error, const char* format, va_list args) {
    char buf[MESSAGE_MAX];
    int len;

    len = vsnprintf(buf,sizeof(buf)-2,format,args);
    strcpy(buf+len,": ");
    len += 2;
    if (!error) {
        error = errno;
    }
    if (len < sizeof(buf)) {
        mdbm_strlcpy(buf+len,strerror(error),sizeof(buf)-len);
    }
    return mdbm_log_vlog_at(file, line, level,buf,NULL);
}


int mdbm_logerror_at (const char* file, int line, int level, int error, const char* format, ...) {
    int ret;
    va_list args;
    va_start(args,format);
    ret = mdbm_log_vlogerror_at(file, line, level,error,format,args);
    va_end(args);
    return ret;
}

int mdbm_log_vlog_at (const char* file, int line, int level, const char* format, va_list args) {
    int pid;
    struct timeval tv;
    char buf[MESSAGE_MAX];
    int prefix;
    int offset;
    int buflen;

    if (level > mdbm_min_log_level) {
        return 0;
    }

    gettimeofday(&tv, NULL);
#ifdef __linux__
    pid = gettid();
#else
    pid = getpid();
#endif
    prefix = snprintf(buf,
                      sizeof(buf),
                      "%x:%08lx:%05lx:%05x %s:%d ",
                      level,tv.tv_sec,tv.tv_usec,pid, file, line);
    offset = prefix;

    if (level == LOG_EMERG || level == LOG_ALERT) {
        static const char FATAL[] = "FATAL ERROR: ";
        strncpy(buf+offset,FATAL,sizeof(buf)-offset);
        offset += sizeof(FATAL)-1;
    }

    if (args) {
        vsnprintf(buf+offset,sizeof(buf)-offset-2,format,args);
    } else {
        mdbm_strlcpy(buf+offset,format,sizeof(buf)-offset-2);
    }

    buflen = strlen(buf);
    if (buf[buflen-1] != '\n') {
        buf[buflen++] = '\n';
        buf[buflen] = 0;
    }

    mdbm_log_core(file, line, level, buf, buflen);

    if (level == LOG_EMERG) {
        abort();
    }
    if (level == LOG_ALERT) {
        exit(1);
    }
    return buflen;
}

/*//////////////////////////////////////////////////////////////////////////////
// stderr version
//////////////////////////////////////////////////////////////////////////////*/
static void mdbm_log_minlevel_stderr(int lvl) {
  mdbm_min_log_level = lvl;
}

static void mdbm_log_core_stderr(const char* file, int line, int level, char* msg, int msglen) {
  fwrite(msg,msglen,1,stderr);
}

MDBM_LOG_REGISTER_PLUGIN(stderr, mdbm_log_minlevel_stderr, mdbm_log_core_stderr)

/*//////////////////////////////////////////////////////////////////////////////
// file version
//////////////////////////////////////////////////////////////////////////////*/
static FILE* mdbm_log_dest = NULL;

int mdbm_set_log_filename(const char* name) {
  if (!name || !name[0]) {
    return -1;
  }
  if (mdbm_log_dest) {
    fclose(mdbm_log_dest);
    mdbm_log_dest = NULL;
  }
  mdbm_log_dest = fopen(name, "a");
  if (!mdbm_log_dest) { /* fall back to stderr */
    mdbm_log_dest = stderr;
    return -1;
  }
  return 0;
}

static void mdbm_log_init_file() {
  static int initialized = 0;

  if (!initialized) {
    char *dest = getenv("MDBM_LOG_DEST_NAME");
    /* assume it's a filename */
    mdbm_set_log_filename(dest);
    /* TODO where can we call close?... */
    initialized = 1;
  }
}

static void mdbm_log_minlevel_file(int lvl) {
  mdbm_min_log_level = lvl;
}

static void mdbm_log_core_file(const char* file, int line, int level, char* msg, int msglen) {
  mdbm_log_init_file();
  fwrite(msg,msglen,1,mdbm_log_dest);
}

MDBM_LOG_REGISTER_PLUGIN(file, mdbm_log_minlevel_file, mdbm_log_core_file)

/*//////////////////////////////////////////////////////////////////////////////
// syslog version
//////////////////////////////////////////////////////////////////////////////*/

static void mdbm_log_init_syslog() {
  static int initialized = 0;

  if (!initialized) {
    openlog("mdbm",LOG_PID,LOG_USER);
    /* TODO where can we call closelog?... */
    initialized = 1;
  }
}

static void mdbm_log_minlevel_syslog(int lvl) {
  mdbm_log_init_syslog();
  setlogmask(LOG_UPTO(lvl));
  mdbm_min_log_level = lvl;
}

static void mdbm_log_core_syslog(const char* file, int line, int level, char* msg, int msglen) {
  mdbm_log_init_syslog();
  syslog(level, msg);
}

MDBM_LOG_REGISTER_PLUGIN(syslog, mdbm_log_minlevel_syslog, mdbm_log_core_syslog)

