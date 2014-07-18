#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
BEGIN { plan tests => 1, onfail => sub { exit(-1);}  };
ok(eval { require MDBM_File });
