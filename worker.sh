#!/bin/sh
set -eux


CPU_COUNT=$(grep '^processor'  /proc/cpuinfo | wc -l)
export GOMAXPROCS=${CPU_COUNT}
gomake -C termite/worker
gomake -C termite/chroot
cp -f ./termite/worker/worker /tmp
cp -f ./termite/chroot/chroot /tmp
sudo /tmp/worker -chroot /tmp/chroot
