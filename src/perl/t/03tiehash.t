#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
use Fcntl;
use MDBM_File;

BEGIN { plan tests => 6 };

my %h;
my $ref;
eval {unlink("_test_.mdbm");};
eval {unlink("_test_.mdbm._int_");}; # remove any existing lockfile as well

$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
ok($ref);

ok($ref->lock());
ok(eval {++$h{test}}, 1);
ok($ref->unlock());
ok($h{test}, 1);
ok($ref->FETCH("test"), 1);

# Cleanup
undef $ref;
untie %h;
undef %h;
unlink("_test_.mdbm");
