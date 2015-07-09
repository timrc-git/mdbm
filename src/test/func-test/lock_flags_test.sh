#!/bin/zsh

pass=1

function check() {
  if [ $? -ne 0 ]; then 
    echo "FAILED"
    pass=0
  else
    #echo "OK"
  fi
}

function check_nones() {
  for f in `find /tmp/.mlock-named/tmp/ -type f -and -iname '*none*'`; do
    echo "ERROR! Unexpected lockfile found for $f"
    pass=0
  done
}

echo "-------------------------- setup ---------------------"

export LD_LIBRARY_PATH=`pwd`/src/lib/object
export DYLD_LIBRARY_PATH=`pwd`/src/lib/object
export PATH=`pwd`/src/tools/object:$PATH

#rm -rf /tmp/.mlock-named /tmp/flarg*
rm -rf /tmp/.mlock-named/tmp/flarg* /tmp/flarg*

echo "------------------------ mdbm_create -----------------"

mdbm_create -N /tmp/flarg-none.mdbm; check;
mdbm_create -K none /tmp/flarg-none.mdbm; check;

mdbm_create /tmp/flarg-default.mdbm; check;
mdbm_create -K any /tmp/flarg-any.mdbm; check;
mdbm_create -K exclusive /tmp/flarg-exclusive.mdbm; check;
mdbm_create -K shared /tmp/flarg-shared.mdbm; check;
mdbm_create -K partition /tmp/flarg-partition.mdbm; check;

mdbm_create -K any -K exclusive /tmp/flarg-any-excl.mdbm; check;
mdbm_create -K any -K shared /tmp/flarg-any-shar.mdbm; check;
mdbm_create -K any -K partition /tmp/flarg-any-part.mdbm; check;

mdbm_create -K none -K any /tmp/flarg-none-any.mdbm; check;
mdbm_create -K none -K exclusive /tmp/flarg-none-excl.mdbm; check;
mdbm_create -K none -K shared /tmp/flarg-none-shar.mdbm; check;
mdbm_create -K none -K partition /tmp/flarg-none-part.mdbm; check;

mdbm_create -N -K exclusive /tmp/flarg-n-excl.mdbm; check;
mdbm_create -N -K shared /tmp/flarg-n-shar.mdbm; check;
mdbm_create -N -K partition /tmp/flarg-n-part.mdbm; check;

#ls -Rlh `find /tmp/.mlock-named -type f` | grep none
check_nones

echo "------------------------ mdbm_check ------------------"

mdbm_check -L /tmp/flarg-default.mdbm; check;
mdbm_check -l none /tmp/flarg-none.mdbm; check;
#mdbm_check -l shared /tmp/flarg-none.mdbm; check;      # bad.. creates locks
mdbm_check -l shared /tmp/flarg-default.mdbm; check;    # ok, ANY used by default
mdbm_check -l partition /tmp/flarg-default.mdbm; check; # ok, ANY used by default
mdbm_check -l exclusive /tmp/flarg-partition.mdbm; check; # ok, ANY used by default
mdbm_check -l any -l shared /tmp/flarg-default.mdbm; check;
mdbm_check -l any -l partition /tmp/flarg-default.mdbm; check;
mdbm_check -l any -l exclusive /tmp/flarg-partition.mdbm; check;

#ls -Rlh `find /tmp/.mlock-named -type f` | grep none
check_nones

echo "------------------------ mdbm_stat -------------------"

mdbm_stat -i ecount -L /tmp/flarg-default.mdbm >/dev/null; check;
mdbm_stat -i ecount -l none /tmp/flarg-none.mdbm >/dev/null; check;
#mdbm_stat -i ecount -l shared /tmp/flarg-none.mdbm >/dev/null; check;      # bad.. creates locks
mdbm_stat -i ecount -l shared /tmp/flarg-default.mdbm >/dev/null; check;    # ok, ANY used by default
mdbm_stat -i ecount -l partition /tmp/flarg-default.mdbm >/dev/null; check; # ok, ANY used by default
mdbm_stat -i ecount -l exclusive /tmp/flarg-partition.mdbm >/dev/null; check; # ok, ANY used by default
mdbm_stat -i ecount -l any -l shared /tmp/flarg-default.mdbm >/dev/null; check;
mdbm_stat -i ecount -l any -l partition /tmp/flarg-default.mdbm >/dev/null; check;
mdbm_stat -i ecount -l any -l exclusive /tmp/flarg-partition.mdbm >/dev/null; check;
mdbm_stat -i ecount -l any -l exclusive /tmp/flarg-partition.mdbm >/dev/null; check;

#ls -Rlh `find /tmp/.mlock-named -type f` | grep none
check_nones


echo "--------------------------- done ---------------------"

if [ $pass -ne 1 ]; then 
  echo "SOME TESTS FAILED"
  exit 1
else
  echo "ALL TESTS PASSED"
  exit 0
fi
