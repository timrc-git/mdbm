#!/usr/bin/perl -w
#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl MDBM_File.t'

use Test;
use MDBM_File;
use Fcntl;

BEGIN { plan tests => 37 };

my %h;
my $ref;
eval {unlink("_test_.mdbm");};
eval {unlink("_test_.mdbm._int_");}; # remove any existing lockfile as well

# CRC32
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_CRC32), 1);
ok($ref->get_hash(), MDBM_HASH_CRC32);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# EJB
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_EJB), 1);
ok($ref->get_hash(), MDBM_HASH_EJB);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# PHONG
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_PHONG), 1);
ok($ref->get_hash(), MDBM_HASH_PHONG);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# OZ
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_TOREK), 1);
ok($ref->get_hash(), MDBM_HASH_TOREK);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# FNV
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_FNV), 1);
ok($ref->get_hash(), MDBM_HASH_FNV);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# STL
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_STL), 1);
ok($ref->get_hash(), MDBM_HASH_STL);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# MD5
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_MD5), 1);
ok($ref->get_hash(), MDBM_HASH_MD5);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# SHA_1
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_SHA_1), 1);
ok($ref->get_hash(), MDBM_HASH_SHA_1);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# Jenkins
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(MDBM_HASH_JENKINS), 1);
ok($ref->get_hash(), MDBM_HASH_JENKINS);
ok(eval {++$h{test}}, 1);
ok($ref->FETCH("test"), 1);
$ref->unlock();

# BoGuS
%h = ();
$ref = tie(%h, 'MDBM_File', '_test_.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640, 0, 16*1024);
$ref->lock();
ok($ref->sethash(666), -1);  # Change return value to -1 because test fails in MDBM V3 - Bug 5220918
$ref->unlock();

# Cleanup
undef $ref;
untie %h;
undef %h;
unlink("_test_.mdbm");
