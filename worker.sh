#!/bin/sh
set -eux

CPU_COUNT=$(grep '^processor'  /proc/cpuinfo | wc -l)
export GOMAXPROCS=${CPU_COUNT}
gomake -C bin/worker
gomake -C bin/chroot
cp -f ./bin/worker/worker /tmp
cp -f ./bin/chroot/chroot /tmp
sudo /tmp/worker -chroot /tmp/chroot -jobs ${CPU_COUNT} "$@"
