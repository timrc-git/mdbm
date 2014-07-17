CANONICALIZE=readlink -m -n -q
TOPDIR=$(shell $(CANONICALIZE) $(shell pwd)/..)
include $(TOPDIR)/Makefile.base

# NOTE: lib must be first, so libmdbm is built before other targets
SUBDIRS=lib tools test scripts


