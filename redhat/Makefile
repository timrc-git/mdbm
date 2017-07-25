PACKAGE := mdbm
BUILD_NUMBER := 0
VERSION := $(shell egrep ^Version ../README -m1 | awk -v BUILD=$(BUILD_NUMBER) '{print $$2 "." BUILD}')
RELEASE := 0
RPM_VERSION := ${VERSION}-${RELEASE}
PACKAGE_WITH_VERSION := $(PACKAGE)-$(RPM_VERSION)
TARBALL := $(PACKAGE_WITH_VERSION).tgz

DIRNAME := $(shell echo $${PWD})
DIRBASE := $(shell basename $${PWD})


rpm: buildrpmdist realclean

buildrpmdist: specfile buildtgz
	rpmbuild -ta ./build/$(TARBALL)

buildrpm: specfile buildtgz
	rpmbuild --define "_rpmdir ./build/" --define "_sourcedir ./build/" --define "_srcrpmdir ./build/" -ta ./build/$(TARBALL)

buildtgz: prepbuildtarball
	tar czv -C ./build/ -f ./build/$(TARBALL) ${PACKAGE_WITH_VERSION}

prepbuildtarball: git-export
	cp ${PACKAGE}.spec ./build/${PACKAGE_WITH_VERSION}/redhat/ 

specfile: $(PACKAGE).spec

$(PACKAGE).spec: $(PACKAGE).spec.in
	sed	-e "s|PACKAGE|$(PACKAGE)|" -e "s|VERSION|$(VERSION)|"	-e "s|RELEASE|$(RELEASE)|" $(PACKAGE).spec.in > $@

git-export: builddir prepclean
	cd ../ && git archive --format=tar --prefix=$(PACKAGE_WITH_VERSION)/ HEAD | (cd redhat/build && tar xf -)

builddir:
	mkdir -p ./build

distdir:
	mkdir -p ./dist

prepclean:
	rm -rf ./build/$(PACKAGE_WITH_VERSION)*

clean:
	rm -rf ./build/* ./dist/* 2>/dev/null || :
	rm -rf mdbm.spec

realclean: clean 
	rmdir ./build/ ./dist/ 2>/dev/null || :

# Makefile debugging helper. Use `make echo_FOO' to display `FOO = $(FOO)'.
echo_%:
	@echo "$* = ${$*}"

