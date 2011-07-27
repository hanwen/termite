#!/bin/sh

set -eux

rm -f termite/version.gen.go

for d in termite \
    bin/coordinator \
    bin/chroot bin/worker bin/fsserver \
    bin/rpcfs bin/master bin/wrapper bin/shell-wrapper ; \
do
  gomake -C $d "$@"
done

for d in termite
do
  (cd $d && gotest )
done

