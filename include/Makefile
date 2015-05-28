
TOPDIR=$(shell cd .. && pwd)
include $(TOPDIR)/Makefile.base

SUBDIRS=

HEADERS= \
  mdbm.h               \
  mdbm_handle_pool.h   \
  mdbm_log.h           \
  mdbm_shmem.h         \
  mdbm_stats.h         \
  mdbm_util.h


install:: default-make-target
	$(INSTALL) -d $(INC_PREFIX)
	$(INSTALL) -t $(INC_PREFIX) $(HEADERS)

