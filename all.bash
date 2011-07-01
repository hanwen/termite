#!/bin/sh

set -eux

for d in rpcfs \
    termite/chroot termite/worker termite/fsserver \
    termite/rpcfs termite/master termite/tool ; \
do
  gomake -C $d "$@"
done

for d in rpcfs
do
  (cd $d && gotest )
done

