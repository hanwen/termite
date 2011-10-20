#!/bin/sh

set -o errexit
for t in *$1*/test.sh
do
  echo
  echo 'Testing:' $t
  echo
  sh test.sh $(dirname $t)
done 

 
