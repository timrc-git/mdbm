
TOPDIR=$(shell cd .. && pwd)
include $(TOPDIR)/Makefile.base
default-make-target: all


all::	docs

.PHONY: docs
docs:	c-api developer

#.PHONY: developer
# create a proxy timestamp to test against the document sources
developer: developer.stamp
developer.stamp: *.rst conf.py README
	$(MAKE) -f Makefile.reST docs && touch developer.stamp


# Unfortunately, the doxygen C API has to go in the top-level directory
# because our doc tool uses the checkout directory for the build directory.
#.PHONY: c-api
# create a proxy timestamp to test against the document sources
c-api: c-api.stamp
c-api.stamp: ../include/*.h doxygen.conf
	$(MAKE) -f Makefile.c_api docs && touch c-api.stamp

clean::
	$(MAKE) -f Makefile.reST clean
	$(MAKE) -f Makefile.c_api clean
	rm -f developer.stamp
	rm -f c-api.stamp

.PHONY: docs-clean
docs-clean: clean

.PHONY: docs-force
docs-force: clean
	$(MAKE) docs

install:: docs
	$(INSTALL) -d $(MAN_PREFIX)
	$(INSTALL) -d $(MAN_PREFIX)/man1
	$(INSTALL) -t $(MAN_PREFIX)/man1 $(shell find -L ./man/man1 -type f)

