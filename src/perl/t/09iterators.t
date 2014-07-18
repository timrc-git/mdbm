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
my %h2;
my $ref;
my $ref2;
my $val;
my $iter = new MDBM_Iter;
my %gods = ("key1" => "Loki",
            "key2" => "Odin",
            "key3" => "Thor");

ok($iter);

eval {unlink("_test_.mdbm");};
eval {unlink("_test2_.mdbm");};

my $cwd; # remove lockfiles - otherwise tests fail
chomp($cwd = `pwd`);
my $lockname = "/tmp/.mlock-named$cwd/_test_.mdbm._int_";
eval {unlink($lockname);};
$lockname = "/tmp/.mlock-named$cwd/_test2_.mdbm._int_";
eval {unlink($lockname);};

$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref2 = tie(%h2, 'MDBM_File', '_test2_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);

$ref->STORE("Odin", "Loki1", MDBM_INSERT_DUP);
$ref->STORE("Odin", "Loki2", MDBM_INSERT_DUP);
$ref->STORE("Odin", "Loki3", MDBM_INSERT_DUP);
$ref->STORE("Odin", "Loki4", MDBM_INSERT_DUP);

ok($ref->fetch_dup_r("Odin", $iter), "Loki1");
ok($ref->fetch_dup_r("Odin", $iter), "Loki2");
ok($ref->fetch_dup_r("Odin", $iter), "Loki3");
ok($ref->fetch_dup_r("Odin", $iter), "Loki4");
ok($ref->fetch_dup_r("Odin", $iter), undef);

$iter->reset();
ok($ref->fetch_dup_r("Odin", $iter), "Loki1");

foreach (keys(%gods)) {
  $ref2->STORE($_, $gods{$_});
}

$iter->reset();
$val = $ref2->firstkey_r($iter);
while (defined($val)) {
  ok($h2{$val}, $gods{$val});
  $val = $ref2->nextkey_r($iter);
}

$iter->reset();
$val = $ref2->fetch_r("key2", $iter);
ok($val, $gods{"key2"});
$ref2->delete_r($iter);
$val = $ref2->fetch_r("key2", $iter);
ok($val, undef);

# Cleanup
unlink("_test2_.mdbm");
unlink("_test_.mdbm");
