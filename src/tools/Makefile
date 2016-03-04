TOPDIR=$(shell cd ../.. && pwd)
include $(TOPDIR)/Makefile.base

#  bench_data_utils.hh 
HEADERS=

COMMON_SOURCES=

CPP_EXE=                 \
  mdbm_config            \
  mdbm_export_splitter   \
  mdbm_import

EXE=                     \
  bench_open             \
  mdbm_check             \
  mdbm_compare           \
  mdbm_compress          \
  mdbm_copy              \
  mdbm_create            \
  mdbm_delete_lockfiles  \
  mdbm_digest            \
  mdbm_export            \
  mdbm_fetch             \
  mdbm_lock              \
  mdbm_purge             \
  mdbm_replace           \
  mdbm_restore           \
  mdbm_rstats            \
  mdbm_save              \
  mdbm_sync              \
  mdbm_trunc             \
  mdbm_dump              \
  mdbm_stat


MYLIBS =  -L$(TOPDIR)/src/lib/$(OBJDIR) -lmdbm
MYLIBS += -lpthread $(LIBRT) -lstdc++

#include $(SOURCES:.c=.d)
#include $(CPP_SOURCES:.cc=.d)

EXEOBJS= $(patsubst %, $(OBJDIR)/%.o, $(EXE))
EXES= $(patsubst %, $(OBJDIR)/%, $(EXE))
CPPEXES= $(patsubst %, $(OBJDIR)/%, $(CPP_EXE))

LDADD += $(MYLIBS)
CFLAGS += -I$(TOPDIR)/src/lib

default-make-target: build-exes

build-exes: $(EXES) $(CPPEXES) $(OBJDIR)/mdbm_bench $(OBJDIR)/mash
# custom deps/types, handled explicitly
$(eval $(call CppExeTargetRules,mdbm_bench,,,))
$(eval $(call CppExeTargetRules,mash,,$(LINK_READLINE)))

# Build the per-command targets.
$(foreach exe, $(EXE), $(eval $(call ExeTargetRules,$(exe))))
$(foreach exe, $(CPP_EXE), $(eval $(call CppExeTargetRules,$(exe))))

clean :: clean-objs clean-exe-mash clean-exe-mdbm_bench clean-exe-mdbm_import \
  $(foreach exe, $(EXE), clean-exe-$(exe)) $(foreach exe, $(CPP_EXE), clean-exe-$(exe))

EXE_DEST=$(BIN_PREFIX)
install:: build-exes
	$(INSTALL) -d $(EXE_DEST)
	$(INSTALL) -t $(EXE_DEST) $(EXES) $(CPPEXES) $(OBJDIR)/mdbm_bench $(OBJDIR)/mash


