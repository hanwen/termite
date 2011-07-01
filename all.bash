#!/bin/sh

set -eux

for d in rpcfs \
    bin/chroot bin/worker bin/fsserver \
    bin/rpcfs bin/master bin/tool ; \
do
  gomake -C $d "$@"
done

for d in rpcfs
do
  (cd $d && gotest )
done

