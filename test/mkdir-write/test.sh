#!/bin/sh

set -eux

for binary in coordinator master worker shell-wrapper
do
  gomake -C ${TERMITE_DIR}/bin/${binary}
done

pprefix=200
coord=localhost:${pprefix}8
dd if=/dev/random of=secret.txt bs=20 count=1 && chmod 0600 secret.txt
${TERMITE_DIR}/bin/coordinator/coordinator -port ${pprefix}8 &

sudo -n true
for w in 1 2 3 4
do 
  sudo -b ${TERMITE_DIR}/bin/worker/worker -coordinator ${coord} -port ${pprefix}$w -jobs 1 -logfile w$w.log &> w$w.stderr
done
sleep 2
${TERMITE_DIR}/bin/master/master -jobs 50 -port ${pprefix}9 -coordinator ${coord} >& master.log & 
make clean
${TERMITE_DIR}/termite-make -j50
${TERMITE_DIR}/termite-make -j50
make clean
${TERMITE_DIR}/termite-make -j50

shell-wrapper -shutdown
make clean
curl ${coord}/workerkill?host=all
curl ${coord}/shutdown
