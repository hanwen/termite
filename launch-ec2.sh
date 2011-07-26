#!/bin/sh
#
# Fire off workers on Amazon's EC2.  The script was written for a
# RHEL6.1-32 bits instance, and should be run from the master,
# on an SSH connection that runs with -oForwardAgent.
#
# Before kicking off, you also need to
# - run coordinator on the master
# - drop iptables on the master.

set -eux
worker=$1
shift

jobs=$1
shift

master=$(hostname)

ssh -oStrictHostKeyChecking=no root@${worker} '/bin/true'
scp secret.txt root@${worker}:
ssh root@${worker} "
  rm -f worker chroot ;
  modprobe fuse;
  yum install -y fuse ;
  service iptables stop ;
  killall worker ;
  wget --quiet http://${master}:1233/bin/worker ;
  wget --quiet http://${master}:1233/bin/chroot ;
  chmod +x worker chroot ;
  rm -rf /var/cache/termite ;
  ./worker -coordinator ${master}:1233 -jobs ${jobs} -chroot chroot >& worker.log &
"

