#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
use MDBM_File;
use Fcntl;

BEGIN { plan tests => 6 };

my %h;
my %h2;
my $ref;
my $ref2;

eval {unlink("_test_.mdbm");};
eval {unlink("_test_.mdbm._int_");}; # remove any existing lockfile as well
eval {unlink("_test2_.mdbm");};
eval {unlink("_test2_.mdbm._int_");}; # remove any existing lockfile as well

# First test mdbm_replace_db()
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref2 = tie(%h2, 'MDBM_File', '_test2_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);

$ref->lock();
$ref->STORE("Odin", "Loki");
$ref->unlock();
ok($h{Odin}, "Loki");

$ref2->lock();
$ref2->STORE("Loki", "Odin");
$ref2->unlock();
ok($h2{Loki}, "Odin");
$ref2->replace_db('_test_.mdbm');
ok($h2{Odin}, "Loki");

$ref = tie(%h, 'MDBM_File', '_test2_.mdbm', MDBM_O_RDWR, 0640);
ok($h{Odin}, "Loki");

# Now test mdbm_replace_file()
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
$ref->STORE("Loki", "Odin");
$ref->unlock();
ok($h{Loki}, "Odin");

MDBM_File::replace_file('_test_.mdbm', '_test2_.mdbm');

$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR, 0640);
ok($h{Odin}, "Loki");

# Cleanup
unlink("_test2_.mdbm");
unlink("_test_.mdbm");
