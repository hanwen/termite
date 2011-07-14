#!/bin/sh

set -eux

for d in termite \
    bin/chroot bin/worker bin/fsserver \
    bin/rpcfs bin/master bin/wrapper ; \
do
  gomake -C $d "$@"
done

for d in termite
do
  (cd $d && gotest )
done

