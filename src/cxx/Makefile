
TOPDIR=$(shell realpath $(shell pwd)/../..)
include $(TOPDIR)/Makefile.base

default-make-target: build-lib

HEADERS=            \
  $(NULL)

SOURCES=          \
  $(NULL)

# mdbm_plock.cc
CPP_SOURCES=      \
  mdbm_cxx.cc


#include $(SOURCES:.c=.d)
#include $(CPP_SOURCES:.cc=.d)

# TODO
# TODO version library
# TODO

$(OBJDIR)/libmdbm_cxx.so : $(SOURCES:%.c=$(OBJDIR)/%.o) $(CPP_SOURCES:%.cc=$(OBJDIR)/%.o) $(HEADERS)
	$(CC) $(CFLAGS) $(PEDANTIC) $(filter-out %.h %.hh,$^) $(LDADD) -fPIC -shared -o $@

build-lib: $(OBJDIR)/libmdbm_cxx.so 

clean :: clean-objs


