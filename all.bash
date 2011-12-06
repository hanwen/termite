#!/bin/sh

set -eux

rm -f termite/version.gen.go

for target in "clean" ""
do
  for d in stats attr cba fs termite \
      bin/coordinator \
      bin/worker bin/master bin/shell-wrapper ; \
  do
    gomake -C $d $target
  done
done

for d in stats attr cba termite
do
  (cd $d && gotest )
done

