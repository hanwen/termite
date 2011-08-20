#!/bin/sh

set -eux

rm -f termite/version.gen.go

for target in "clean" ""
do
  for d in termite \
      bin/coordinator \
      bin/worker bin/fsserver \
      bin/rpcfs bin/master bin/wrapper bin/shell-wrapper ; \
  do
    gomake -C $d $target
  done
done

for d in termite
do
  (cd $d && gotest )
done

