
TOPDIR=$(shell cd ../../.. && pwd)
include $(TOPDIR)/Makefile.base

HEADERS=

SOURCES=

# only test_ccmp uses CppUnitTestRunnerLocal, so don't include it here
COMMON_SOURCES=

TESTS=                       \
  acquire_mt                 \
  create_destroy             \
  test_ccmp


MYLIBS =  -L$(TOPDIR)/src/lib/$(OBJDIR) -lmdbm -L$(LIBDIR)
MYLIBS += -lpthread $(LIBRT) -lstdc++ -lreadline -lcppunit 
LD_EXTRA += $(MYLIBS) -lcppunit $(LOCAL_LDADD)
CXXFLAGS += -I$(TOPDIR)/src/lib

TESTEXES= $(patsubst %, $(OBJDIR)/%, $(TESTS))

# generate xml test results...
RUN_ARGS= -tall -x $(1).xml 

default-make-target: $(TESTEXES)

# Create the per-test targets.
#$(foreach test, $(TESTS), $(eval $(call CppExeTargetRules,$(test),,$(LD_EXTRA))))
$(eval $(call CppExeTargetRules,acquire_mt,,$(LD_EXTRA)))
$(eval $(call CppExeTargetRules,create_destroy,,$(LD_EXTRA)))
$(eval $(call CppExeTargetRules,test_ccmp,,$(LD_EXTRA) CppUnitTestRunnerLocal.cc))

test:: $(TESTEXES)
	@echo "RUNNING HANDLE-POOL TESTS MODE=$(MODE), FAST=$(FAST) !!!!"
	make run-test_ccmp
	make RUN_ARGS=4   run-create_destroy
	make RUN_ARGS=32  run-create_destroy
	make RUN_ARGS=128 run-create_destroy
	make RUN_ARGS="32 16" run-acquire_mt
	make RUN_ARGS="32 32" run-acquire_mt
	make RUN_ARGS="32 48" run-acquire_mt

valg:: $(TESTEXES)
	make valg-test_ccmp
	make RUN_ARGS=4   valg-create_destroy
	make RUN_ARGS=32  valg-create_destroy
	make RUN_ARGS=128 valg-create_destroy
	make RUN_ARGS="32 16" valg-acquire_mt
	make RUN_ARGS="32 32" valg-acquire_mt
	make RUN_ARGS="32 48" valg-acquire_mt

# test_create_destroy:
# 	$(OBJDIR)/create_destroy 4
# 	$(OBJDIR)/create_destroy 32
# 	$(OBJDIR)/create_destroy 128
# 
# test_acquire_mt:
# 	$(OBJDIR)/acquire_mt 32 16
# 	$(OBJDIR)/acquire_mt 32 32
# 	$(OBJDIR)/acquire_mt 32 48

clean :: clean-objs $(foreach exe, $(TESTS), clean-exe-$(exe))
#clean :
#	rm -f *.d $(OBJDIR)/*.o $(OBJDIR)/*.lib $(OBJDIR)/*.so *.xml 
#	rm -f $(TESTEXES) $(OBJDIR)/CppUnitTestRunner


