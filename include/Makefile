
TOPDIR=$(shell realpath $(shell pwd)/..)
include $(TOPDIR)/Makefile.base

SUBDIRS=

HEADERS= \
  mdbm.h               \
  mdbm_handle_pool.h   \
  mdbm_log.h           \
  mdbm_shmem.h         \
  mdbm_stats.h         \
  mdbm_util.h


HDR_DEST=$(PREFIX)/include
install:: default-make-target
	$(INSTALL) -d $(HDR_DEST)
	$(INSTALL) -t $(HDR_DEST) $(HEADERS)

