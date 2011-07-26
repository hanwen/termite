#!/bin/sh

set -eux
rm -rf termite-bin
mkdir termite-bin

for x in \
    bin/coordinator bin/chroot bin/worker bin/master bin/wrapper ;
do
    gomake -C $x
    cp $x/$(basename $x) termite-bin/
done 

tar cjf termite-bin.tar.bz2 termite-bin/
