#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use strict;
use warnings;

use Test;
use MDBM_File;
use Fcntl;
use IO::Handle;

BEGIN { plan tests => 9 };

my %h;
my %h2;
my %h3;
my $ref;
my $ref2;
my $ref3;


eval {unlink("_test_.mdbm");};
eval {unlink("_test2_.mdbm");};

# Shaker callbacks
my $shaken = 0;
my $shaken2 = 0;

sub shake_me($$$$)
{
  my ($db, $key, $val, $shakeObj) = @_;

  my $items = $shakeObj->getCount();
  my $idx = 0;
  my $toDelete = $shakeObj->getSpaceNeeded();

  my ($curKey, $curVal, $freeUp);
  while (($idx < $items) and ($toDelete > 0)) {
    $curKey = $shakeObj->getPageEntryKey($idx);
    $curVal = $shakeObj->getPageEntryValue($idx);
    if (($curKey ne "Odin") and $idx % 3 == 0) {
      $freeUp = length($curKey) + length($curVal);
      $shakeObj->setEntryDeleted($idx);
      $toDelete -= $freeUp;
    }
    $idx++;
  } 

  $shaken=1;
  return 1;  # Changes were made: items were deleted means we should return 1
}

sub shake_me2($$$$)
{
  my ($db, $key, $val, $shakeObj) = @_;

  my $page_items = $shakeObj->getCount();
  $shaken2=1;
  return 0;
}

%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 4*1024);
$ref->limit_size_v3(2, \&shake_me, $ref);

%h2 = ();
$ref2 = tie(%h2, 'MDBM_File', '_test2_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 4*1024);
$ref2->limit_size_v3(3, \&shake_me2, $ref2);

$ref->lock();
$ref->STORE("Odin", "Loki");

my $cnt = 0;

while (!$shaken) {
  $h{"key-$cnt"} = "DataShakeV3-$cnt";
  $cnt++;
}
$ref->unlock();

# Test case when shake does not succeed in freeing anything and returns zero

my $error_found = 0;
$ref2->lock();
$ref2->STORE("Loki", "Odin");

$cnt = 0;
$error_found = 0;
while (!$error_found) {
  eval { $h2{"V3key-$cnt"} = "Datathree-$cnt"; }; $error_found = 1 if $@;
  $cnt++;
}
$ref2->unlock();

ok($shaken, 1);
ok($shaken2, 1);
ok($error_found, 1);
ok($h{Odin}, "Loki");
ok($h2{Loki}, "Odin");

# Test passing NULL shake function
%h3 = ();
$ref3 = tie(%h3, 'MDBM_File', '_test3_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 4*1024);
$ref3->limit_size_v3(5);

$cnt = 0;
$error_found = 0;
$ref3->lock();
while (($cnt < 2000) and ($error_found == 0)) {
  eval { $h3{"KeyThree_$cnt"} = "StuffThree-$cnt"; }; $error_found = 1 if $@;
  $cnt++;
}
$ref3->unlock();

ok($error_found, 1);

# Go back to using and updating first hash to re-verify shaking

$ref->lock();
delete $h{'key-19'};
delete $h{'key-20'};
delete $h{'key-22'};

$shaken = 0;
$cnt = 0;
while (!$shaken) {
  $h{"key-$cnt"} = "ShakeTry_$cnt";
  $cnt++;
}
$ref->unlock();

ok($shaken, 1);

# Test increasing limit size

$ref->limit_size_v3(3, \&shake_me2, $ref);
$ref->lock();
$shaken2 = 0;
$error_found = 0;
$cnt = 0;
while (!$shaken2) {
  eval { $h{"Uplimit-$cnt"} = "DataUp-$cnt"; }; $error_found = 1 if $@;
  $cnt++;
}
$ref->unlock();

ok($shaken2, 1);
ok($error_found, 1);

# Cleanup

undef $ref3;
untie %h3;
undef %h3;
unlink("_test3_.mdbm");

untie %h2;
undef $ref2;
undef %h2;
unlink("_test2_.mdbm");

untie %h;
undef $ref;
undef %h;
unlink("_test_.mdbm");
