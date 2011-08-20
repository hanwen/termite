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

master=$(hostname --long)

ssh -oStrictHostKeyChecking=no root@${worker} '/bin/true'
scp secret.txt root@${worker}:
ssh root@${worker} "
  rm -f worker chroot ;
  modprobe fuse;
  yum install -y fuse ;
  yum erase -y samba-client libgcj postgresql \
    postgresql-server atlas qt-x11 valgrind \
    java-1.6.0-openjdk samba-client
  service iptables stop ;
  killall worker ;
  wget --quiet http://${master}:1233/bin/worker ;
  chmod +x worker ;
  rm -rf /var/cache/termite ;
  ./worker -coordinator ${master}:1233 -jobs ${jobs} >& worker.log &
"

