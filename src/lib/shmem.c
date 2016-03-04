/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <sys/fcntl.h>
#include <errno.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/param.h>

#include "mdbm_log.h"
#include "mdbm_shmem.h"
#include "mdbm_internal.h"

typedef struct mdbm_shmem_internal_s {
    void* base;
    size_t size;
    int flags;
    int fd;
    char fname[MAXPATHLEN+2];
} mdbm_shmem_internal_t;


static const char* PREFIX = "mdbm_shmem";

static int
zeroes (int fd, size_t size)
{
    char buf[64*1024];
    int bytes;
    bzero(buf,sizeof(buf));
    bytes = 0;
    while (bytes < size) {
        int b = sizeof(buf);
        if (b > size-bytes) {
            b = size-bytes;
        }
        if (write(fd,buf,b) != b) {
            mdbm_logerror(LOG_ERR,0,"%s: write failure",PREFIX);
            return -1;
        }
        bytes += b;
    }
    return 0;
}


mdbm_shmem_t*
mdbm_shmem_open (const char* filename,
                 int flags, size_t initial_size, int* pinit)
{
    static const char* PREFIX = "mdbm_shmem_open";
    int f;
    int prot;
    int fd;
    int want_init = 0;
    int init = 0;
    int minit = 0;
    int size;
    void* p;
    int tries = 0;
    mdbm_shmem_internal_t* shmem;

    if (!filename || !filename[0] || strlen(filename) > MAXPATHLEN) {
        errno = EINVAL;
        return NULL;
    }
    if ((flags & (MDBM_SHMEM_CREATE|MDBM_SHMEM_TRUNC)) && initial_size == 0) {
        errno = EINVAL;
        return NULL;
    }
    if (flags & MDBM_SHMEM_TRUNC) {
        /* If TRUNC is set, means we want to init even if file exists */
        want_init = 1;
    }
    do {
        f = (flags & MDBM_SHMEM_RDWR) ? O_RDWR : O_RDONLY;
        if ((fd = open(filename,f,0666)) == -1) {
            if (errno != ENOENT || (flags & MDBM_SHMEM_CREATE) == 0) {
                mdbm_logerror(LOG_ERR,0,"%s: %s",PREFIX,filename);
                return NULL;
            }
            f |= O_CREAT;
            if (!want_init) {
                /* CREATE is set but not TRUNC.  Init only if we create. */
#ifdef __linux__
                f |= O_EXCL;
#else /* FREEBSD, __MACH__ */
                f |= O_EXCL|O_EXLOCK;
                init = 1;
#endif
            }
            fd = open(filename,f,0666);
            if (fd == -1) {
                if (errno != EEXIST) {
                    mdbm_logerror(LOG_ERR,0,"%s: %s",PREFIX,filename);
                    return NULL;
                }
            }
#ifdef __linux__
            if (!want_init) {
                if (flock(f,LOCK_EX|LOCK_NB) < 0) {
                    close(f);
                    fd = -1;
                }
            }
#endif
            /* Either TRUNC was set or we created the file (EXCL), so init. */
            want_init = 1;
        }
    } while (fd == -1);
    while (!init) {
        static const int MAX_TRIES = 100;
        struct timespec ts;

        if (want_init && !flock(fd,LOCK_EX|LOCK_NB)) {
            init = 1;
            break;
        }
        if (!want_init || errno == EWOULDBLOCK) {
            if (!flock(fd,LOCK_SH|LOCK_NB)) {
                init = 0;
                break;
            }
        }
        if (errno != EWOULDBLOCK) {
            mdbm_logerror(LOG_ERR,0,"%s: %s: flock failure",
                          PREFIX,filename);
            close(fd);
            return NULL;
        }
        if (++tries >= MAX_TRIES) {
            mdbm_logerror(LOG_ERR,0,
                          "%s: %s: initialization timeout",
                          PREFIX,filename);
            close(fd);
            return NULL;
        }
        ts.tv_sec = 0;
        ts.tv_nsec = 10000000;
        nanosleep(&ts,NULL);
    }
    size = lseek(fd,0,SEEK_END);
    if (size == -1) {
        mdbm_logerror(LOG_ERR,0,"%s: %s: lseek(SEEK_EOF) failure",
                      PREFIX,filename);
        close(fd);
        return NULL;
    }
    if (init) {
        if (size != initial_size) {
            if (ftruncate(fd,initial_size) < 0) {
                if (errno != EINVAL) {
                    mdbm_logerror(LOG_ERR,0,"%s: %s: ftruncate failure",
                                  PREFIX,filename);
                    close(fd);
                    return NULL;
                }
                minit = 1;
            } else {
                if (lseek(fd,0,SEEK_SET) < 0) {
                    mdbm_logerror(LOG_ERR,0,"%s: %s: lseek(SEEK_SET) failure",
                                  PREFIX,filename);
                    close(fd);
                    return NULL;
                }
                if (zeroes(fd,initial_size) == -1) {
                    close(fd);
                    return NULL;
                }
            }
            size = initial_size;
        }
    }
    prot = PROT_READ;
    if (flags & MDBM_SHMEM_RDWR) {
        prot |= PROT_WRITE;
    }
    f = (flags & MDBM_SHMEM_PRIVATE) ? 0 : MAP_SHARED;
#ifdef FREEBSD
    if ((flags & MDBM_SHMEM_SYNC) == 0) {
        f |= MAP_NOSYNC;
    }
#endif
    if (flags & MDBM_SHMEM_GUARD) {
        size_t pgsz = sysconf(_SC_PAGESIZE);
        size_t sz = size + pgsz*2;
        void* pg;
        pg = mmap(NULL,sz,PROT_NONE,MAP_PRIVATE|MAP_ANON,-1,0);
        if (pg == MAP_FAILED) {
            mdbm_logerror(LOG_ERR,0,"%s: %s: mmap failure",PREFIX,filename);
            close(fd);
            return NULL;
        }
        p = mmap((char*)pg + pgsz,size,prot,f | MAP_FIXED,fd,0);
        if (p == MAP_FAILED) {
            mdbm_logerror(LOG_ERR,0,"%s: %s: mmap failure",PREFIX,filename);
            munmap(pg,sz);
            close(fd);
            return NULL;
        }
    } else {
        p = mmap(NULL,size,prot,f,fd,0);
        if (p == MAP_FAILED) {
            mdbm_logerror(LOG_ERR,0,"%s: %s: mmap failure",
                          PREFIX,filename);
            close(fd);
            return NULL;
        }
    }
    if (minit) {
        memset(p, 0, size);
    }
    shmem = (mdbm_shmem_internal_t*)malloc(sizeof(*shmem));
    shmem->base = p;
    shmem->flags = flags;
    shmem->size = size;
    shmem->fd = fd;
    strncpy(shmem->fname,filename,sizeof(shmem->fname));
    shmem->fname[sizeof(shmem->fname)-1] = 0; /* ensure trailing null */
    if (pinit) {
        *pinit = init;
    } else {
        mdbm_shmem_init_complete((mdbm_shmem_t*)shmem);
    }
    return (mdbm_shmem_t*)shmem;

}


int
mdbm_shmem_init_complete (mdbm_shmem_t* shmem)
{
    static const char* PREFIX = "mdbm_shmem_init_complete";
    mdbm_shmem_internal_t* s = (mdbm_shmem_internal_t*)shmem;
    if (flock(s->fd,LOCK_SH|LOCK_NB) == -1) {
        mdbm_log(LOG_ERR,"%s: flock failure: %s\n",PREFIX,strerror(errno));
        return -1;
    }
    return 0;
}


int
mdbm_shmem_close (mdbm_shmem_t* shmem, int flags)
{
    static const char* PREFIX = "mdbm_shmem_close";
    mdbm_shmem_internal_t* s = (mdbm_shmem_internal_t*)shmem;
    if (flags & MDBM_SHMEM_SYNC) {
        msync(s->base,s->size,MS_SYNC);
    }
    if (s->base) {
        if (s->flags & MDBM_SHMEM_GUARD) {
            size_t pgsz = sysconf(_SC_PAGESIZE);
            s->base = (char*)s->base - pgsz;
            s->size += 2*pgsz;
        }
        munmap(s->base,s->size);
    }
    if (flags & MDBM_SHMEM_UNLINK) {
        char fn[MAXPATHLEN+2];
        int f;
        struct stat s1, s2;

        /* create a deletion lock filename */
        strcpy(fn,s->fname);
        strcat(fn,"-");
        /* open the deletion lock */
        if ((f = open(fn,O_RDWR|O_CREAT,0666)) < 0) {
            mdbm_logerror(LOG_ERR,0,"%s: open(%s)",PREFIX,fn);
        } else {
            /* Get an exlusive lock on the deletion lock, then
               check that the file we have open is the same as
               the file currently linked with the name we used
               to open it.  If it's the same file, then unlink
               it, unlink the deletion lock, then close the
               deletion lock file.

               A second proc cannot mistakenly delete a new version
               of the mapped file because by the time it obtains
               the deletion lock (whether it's the initial version
               of that lock file or something that gets created
               after the first proc has unlinked it) the mapped
               file has already been unlinked. */
            if (flock(f,LOCK_EX) < 0) {
                mdbm_logerror(LOG_ERR,0,"%s: flock(%s)",PREFIX,fn);
            } else {
                if (fstat(s->fd,&s1) < 0) {
                    mdbm_logerror(LOG_ERR,0,"%s: fstat(%d)",PREFIX,s->fd);
                } else if (stat(s->fname,&s2) < 0) {
                    if (errno != ENOENT) {
                        mdbm_logerror(LOG_ERR,0,"%s: stat(%s)",PREFIX,s->fname);
                    }
                } else if (s1.st_dev == s2.st_dev && s1.st_ino == s2.st_ino) {
                    char mvfn[MAXPATHLEN+2];
                    strcpy(mvfn,s->fname);
                    strcat(mvfn,"#");
                    if (rename(s->fname,mvfn) < 0) {
                        if (errno != ENOENT) {
                            mdbm_logerror(LOG_ERR,0,"%s: rename(%s,%s)",PREFIX,
                                          mvfn,s->fname);
                        }
                    }
                }
                if (unlink(fn) < 0) {
                    if (errno != ENOENT) {
                        mdbm_logerror(LOG_ERR,0,"%s: unlink(%s)",PREFIX,fn);
                    }
                }
            }
            close(f);
        }
    }
    close(s->fd);
    free(s);
    return 0;
}

int
mdbm_shmem_fd (mdbm_shmem_t* shmem)
{
    mdbm_shmem_internal_t* s = (mdbm_shmem_internal_t*)shmem;
    if (!s) {
        return errno = EINVAL, -1;
    }
    return s->fd;
}
