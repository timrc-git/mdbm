#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
use MDBM_File;
use Fcntl;
use IO::Handle;

BEGIN { plan tests => 12 };

my %h;
my $ref;
my $oldfile = "_test_.mdbm";
my $newfile = "_testpart_.mdbm";
eval {unlink($oldfile);};
# remove real lockfile - otherwise tests fail
my $cwd;
chomp($cwd = `pwd`);
my $lockname = "/tmp/.mlock-named$cwd/$oldfile._int_";
eval {unlink($lockname);};

$ref = tie(%h, 'MDBM_File', $newfile, MDBM_O_RDWR|MDBM_O_CREAT|MDBM_PARTITIONED_LOCKS,
	0640, 0, 16*1024);

# Test full DB locks
ok($ref->lock(), 1);
ok($ref->islocked(), 1);
ok($ref->trylock(), 1);
ok($ref->unlock(), 1);
ok($ref->islocked(), 1);
ok($ref->isowned(), 1);
ok($ref->unlock(), 1);
ok($ref->islocked(), 0);
ok($ref->isowned(), 0);

$ref->lock();
$h{a} = 1;
$h{b} = 2;
$h{c} = 3;
$ref->unlock();

# Test page locks
ok($ref->plock("a", 0), 1);
ok($ref->plock("c", 0), -1);
ok($ref->punlock("a", 0), 1);


# Cleanup
undef $ref;
untie %h;
undef %h;
unlink($newfile);
