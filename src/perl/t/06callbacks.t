#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
use MDBM_File;
use Fcntl;

BEGIN { plan tests => 2 };

my %h;
my $ref;

eval {unlink("_test_.mdbm");};
eval {unlink("_test_.mdbm._int_");}; # remove any existing lockfile as well

%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);

my $cnt = 0;
$ref->lock();
$ref->STORE("Odin", "Loki");
$ref->unlock();

# Prune
my $paramOK = 0;
my $pruned = 0;
sub prune_me
{
  my ($db, $key, $val, $param) = @_;
  $paramOK = ($param eq "Odin");
  return 0 if ($val eq $param);
  $pruned=1;
  return 1;
}

$ref->lock();
$ref->prune(\&prune_me, "Odin");
$ref->unlock();

ok($paramOK, 1);
ok($pruned, 1);

# Cleanup
undef $ref;
untie %h;
undef %h;
unlink("_test_.mdbm");
