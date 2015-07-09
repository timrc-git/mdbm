
TOPDIR=$(shell cd ../../.. && pwd)
include $(TOPDIR)/Makefile.base

HEADERS=            \
  TestBase.hh       \
  test_backstore.hh \
  test_cache.hh     \
  test_dibase.hh    \
  test_dmbase.hh    \
  test_lockbase.hh

SOURCES=

COMMON_SOURCES=              \
  CppUnitTestRunnerLocal.cc  \
  TestBase.cc                \
  test_lockbase.cc

TESTS=                       \
  test_align                 \
  test_backstore             \
  test_cache                 \
  test_close_sync_repl       \
  test_compare               \
  test_delete                \
  test_dibase                \
  test_dmbase                \
  test_export_api            \
  test_fetch                 \
  test_getlimit              \
  test_getsize               \
  test_hash                  \
  test_import                \
  test_iter                  \
  test_lego_large_objects    \
  test_limitdir              \
  test_limitsize             \
  test_mag_vers              \
  test_mash                  \
  test_other                 \
  test_open                  \
  test_pagesize              \
  test_presplit              \
  test_signals               \
  test_spillsize             \
  test_stats                 \
  test_store                 \
  test_tsc                   \
  test_util   

# LOCKING TESTS:
TESTS+=                       \
  test_dup_replace           \
  test_lockv3                \


MYLIBS =  -L$(TOPDIR)/src/lib/$(OBJDIR) -lmdbm -L$(LIBDIR)
MYLIBS += -lpthread $(LIBRT) -lstdc++ -lreadline -lcppunit
LD_EXTRA += $(MYLIBS) -lcppunit $(LOCAL_LDADD)
CXXFLAGS += -I$(TOPDIR)/src/lib

TESTEXES= $(patsubst %, $(OBJDIR)/%, $(TESTS))
#TESTTESTS= $(patsubst %, $(OBJDIR)/%, $(TESTS))

# generate xml test results...
RUN_ARGS= -tall -x $(1).xml 

default-make-target: $(TESTEXES)

# # Build the per-test targets.
# $(foreach test, $(TESTS), $(eval $(call BuildTestTarget,$(test))))
# $(foreach test, $(TESTS), $(eval $(call RunTestTarget,$(test))))

# Create the per-test targets.
$(foreach test, $(TESTS), $(eval $(call CppExeTargetRules,$(test),,$(LD_EXTRA))))

test:: $(TESTEXES)
	@echo "RUNNING UNIT TESTS MODE=$(MODE), FAST=$(FAST) !!!!"
	$(foreach test, $(TESTS), make run-$(test) && ) $(TRUECMD)

valg:: $(TESTEXES)
	$(foreach test, $(TESTS), make valg-$(test) && ) $(TRUECMD)

clean :: clean-objs $(foreach exe, $(TESTS), clean-exe-$(exe))
#clean :
#	rm -f *.d $(OBJDIR)/*.o $(OBJDIR)/*.lib $(OBJDIR)/*.so *.xml 
#	rm -f $(TESTEXES) $(OBJDIR)/CppUnitTestRunner


