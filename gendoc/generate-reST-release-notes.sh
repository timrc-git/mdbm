#!/bin/sh
# $Id$
# $URL$

# The caller is responsible to set up the appropriate environment
# variables for processing the README file.  For example,
# environment variable MDBM_BUILD must be defined.

readme=${1:-README}
readmeName=`basename $readme`
readmeTxt=${2:-$readmeName.txt}

if [ -e /usr/local/bin/perl ]; then
perl=/usr/local/bin/perl
else
perl=/usr/bin/perl
fi

# Because this script can be executed by an doc tool plugin, there is no
# parent `make' process that set up the environment.  If MDBM_BUILD is
# not defined, assume that v3 release notes need to be generated.
if [ -z "$MDBM_BUILD" ]; then
    export MDBM_BUILD=3
fi


# Create readmeOut.
# Replace leading ` - ' with ` + '.
# Replace [BUG nnnnnn] with a hyperlink to the ticket in Bugzilla.
$perl $readme | \
sed -r \
    -e 's/`/\\`/g' \
    -e 's/^([ ]+)-([ ]+)/\1+\2/g' \
    > $readmeTxt
