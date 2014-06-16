#!/bin/bash

name=$1
ret=0
shift

if [ "x$name" == "x" ]; then
  echo ERROR NO TEST FILENAME PROVIDED TO valgrind_runner
  exit 1
fi

vgout=${name}.valgrind
echo VALG=yes valgrind --log-file=${vgout} ${name} $*
VALG=yes valgrind --log-file=${vgout} ${name} $*
ret=$?
if [ $ret -ne 0 ]; then
  echo ERROR! VALGRIND FAILED FOR ${name}
  echo ${vgout} may contain details 
  exit $ret
fi

# NOTE: the space before the '0' in '0 bytes' is very important, 
#   so we don't accidentally ignore things like '100 bytes'
#leaks=`grep "\(definitely\|indirectly\|possibly\) lost" ${name}.valgrind | grep -v '0 bytes'`
leaks=`grep "definitely lost" ${vgout} | grep -v ' 0 bytes' | wc -l`
possible=`grep "\(possibly\|indirectly\) lost" ${vgout} | grep -v ' 0 bytes' | wc -l`
errors=`grep "ERROR SUMMARY" ${vgout} | grep -v ' 0 errors' | wc -l`

if [ $errors -gt 0 ]; then
  cat ${vgout}
  echo
  echo ==================================================
  echo ERROR! VALGRIND FOUND MEMORY ERRORS FOR ${name}
  echo ${vgout} contains details 
  echo ==================================================
  echo
  ret=1
elif [ $leaks -gt 0 ]; then
  cat ${vgout}
  echo
  echo ==================================================
  echo ERROR! VALGRIND FOUND DEFINITE LEAKS FOR ${name}
  echo ${vgout} contains details 
  echo ==================================================
  echo
  ret=1
elif [ $possible -gt 0 ]; then
  echo
  echo ==================================================
  echo WARNING: valgrind found possible leaks for ${name}
  echo ${vgout} contains details 
  echo ==================================================
  echo
fi

exit $ret

#Sample valgrind output:

#==27916== HEAP SUMMARY:
#==27916==     in use at exit: 128 bytes in 2 blocks
#==27916==   total heap usage: 1,067,547 allocs, 1,067,545 frees, 2,492,794,823 bytes allocated
#==27916== 
#==27916== LEAK SUMMARY:
#==27916==    definitely lost: 0 bytes in 0 blocks
#==27916==    indirectly lost: 0 bytes in 0 blocks
#==27916==      possibly lost: 0 bytes in 0 blocks
#==27916==    still reachable: 128 bytes in 2 blocks
#==27916==         suppressed: 0 bytes in 0 blocks
#==27916== Rerun with --leak-check=full to see details of leaked memory
#==27916== 
#==27916== For counts of detected and suppressed errors, rerun with: -v
#==27916== Use --track-origins=yes to see where uninitialised values come from
#==27916== ERROR SUMMARY: 2058 errors from 8 contexts (suppressed: 6 from 6)
