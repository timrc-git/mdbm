#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
use MDBM_File;
use Fcntl;

BEGIN { plan tests => 12 };

my %h;
my $ref;
eval {unlink("_test_.mdbm");};
eval {unlink("_test_.mdbm._int_");}; # remove any existing lockfile as well

$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();

ok($ref->STORE("test", 123), 0);
ok($ref->EXISTS("test"), 1);
ok($ref->FETCH("test"), 123);
ok($ref->DELETE("test"), 0);
ok($ref->FETCH("test"), undef);

$h{a} = 1;
$h{b} = 2;
$h{c} = 3;

ok($ref->get_page_size(), 4096);
#ok($ref->get_size(), 16384);
ok($ref->get_size() >= 16384);
ok($ref->get_size() <= 16384*2);

my $str = "";
my $sum = 0;
foreach my $key (sort(keys(%h))) {
  $str .= $key;
  $sum += $ref->FETCH($key);
}

$ref->unlock();
ok($str, "abc");
ok($sum, 6);

# Test som various functions
ok($ref->sync(), undef);
ok($ref->fsync(), undef);

# Cleanup
undef $ref;
untie %h;
undef %h;
unlink("_test_.mdbm");
