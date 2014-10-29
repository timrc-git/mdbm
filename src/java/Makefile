CANONICALIZE=readlink -m -n -q
TOPDIR=$(shell $(CANONICALIZE) $(shell pwd)/../..)
include $(TOPDIR)/Makefile.base

HEADERS=            \

SOURCES=          \

CPP_SOURCES=      \
  com_yahoo_db_mdbm_internal_NativeMdbmAccess.cc


LIBNAME=libmdbm_java.so
LIBVER=4
SONAME=-Wl,-soname,$(LIBNAME).$(LIBVER)
#-Wl,-rpath,$(DEFAULT_LIB_INSTALL_PATH)

# TODO: Fix this to not be rh specific
INCDIR += -I/usr/lib/jvm/java-1.7.0/include -I/usr/lib/jvm/java-1.7.0/include/linux
LIB_BUILD_FLAGS += -lrt  -L$(TOPDIR)/src/lib/$(OBJDIR)  -lmdbm -Wall  -fno-strict-aliasing -Wno-unused-function -D_FILE_OFFSET_BITS=64

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
	ln -s -f -r $(LIB_DEST)/$(LIBNAME).$(LIBVER) $(LIB_DEST)/$(LIBNAME)

clean :: clean-objs

maven:
	mvn -B clean verify -Djava.awt.headless=true -DfailIfNoTests=false 
