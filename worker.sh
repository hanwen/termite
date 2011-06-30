#!/bin/sh
set -eux
gomake -C termite/worker
gomake -C termite/chroot
cp -f ./termite/worker/worker /tmp
cp -f ./termite/chroot/chroot /tmp
sudo /tmp/worker -chroot /tmp/chroot
