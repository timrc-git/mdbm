
CANONICALIZE=readlink -m -n -q
TOPDIR=$(shell $(CANONICALIZE) $(shell pwd)/../..)
include $(TOPDIR)/Makefile.base

HEADERS=            \
  atomic.h          \
  mdbm_lock.hh      \
  multi_lock.hh     \
  stall_signals.h   \
  util.hh

SOURCES=          \
  hash.c          \
  log.c           \
  mdbm_handle_pool.c \
  mdbm.c          \
  shmem.c         \
  stall_signals.c

CPP_SOURCES=      \
  mdbm_lock.cc    \
  mdbm_util.cc    \
  multi_lock.cc


LIBNAME=libmdbm.so
LIBVER=4
SONAME=-Wl,-soname,$(LIBNAME).$(LIBVER)
#-Wl,-rpath,$(DEFAULT_LIB_INSTALL_PATH)

# add real-time lib for clock_gettime
LIB_BUILD_FLAGS += -lrt

LIB_DEST=$(PREFIX)/lib$(ARCH_SUFFIX)
ifeq ($(SET_RPATH),1)
LIB_BUILD_FLAGS += -Wl,-rpath=$(LIB_DEST)
endif

default-make-target: $(OBJDIR)/$(LIBNAME) $(OBJDIR)/$(LIBNAME).$(LIBVER)

# generic library
$(OBJDIR)/$(LIBNAME) : $(SOURCES:%.c=$(OBJDIR)/%.o) $(CPP_SOURCES:%.cc=$(OBJDIR)/%.o) $(HEADERS)
	$(CC) $(CFLAGS) $(PEDANTIC) $(filter-out %.h %.hh,$^) $(LDADD) $(LIB_BUILD_FLAGS) -fPIC -shared -o $@

# versioned library
$(OBJDIR)/$(LIBNAME).$(LIBVER) : $(SOURCES:%.c=$(OBJDIR)/%.o) $(CPP_SOURCES:%.cc=$(OBJDIR)/%.o) $(HEADERS)
	$(CC) $(CFLAGS) $(PEDANTIC) $(filter-out %.h %.hh,$^) $(LDADD) $(LIB_BUILD_FLAGS) -fPIC -shared $(SONAME) -o $@

install:: default-make-target
	$(INSTALL) -d $(LIB_DEST)
	$(INSTALL) -D $(OBJDIR)/$(LIBNAME).$(LIBVER) $(LIB_DEST)
	ln -s -f $(LIB_DEST)/$(LIBNAME).$(LIBVER) $(LIB_DEST)/$(LIBNAME)

clean :: clean-objs

