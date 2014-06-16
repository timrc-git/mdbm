
TOPDIR=$(shell realpath $(shell pwd)/../..)
include $(TOPDIR)/Makefile.base


SCRIPTS=                     \
  mdbm_big_data_builder.pl   \
  mdbm_environment.sh        \
  mdbm_reset_all_locks

EXE_DEST=$(PREFIX)/bin$(ARCH_SUFFIX)
install::
	$(INSTALL) -d $(EXE_DEST)
	$(INSTALL) -t $(EXE_DEST) $(SCRIPTS)


