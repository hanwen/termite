#!/bin/sh

set -eux

rm -f termite/version.gen.go

for target in "clean" "install"
do
  for d in stats attr cba fs termite \
      bin/coordinator \
      bin/worker bin/master bin/shell-wrapper ; \
  do
    (cd $d && go $target . )
  done
done

for d in stats attr cba termite
do
  (cd $d && go test . )
done

