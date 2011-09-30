#!/bin/sh

pprefix=200
coord=localhost:${pprefix}8

function clean() {
shell-wrapper -shutdown
curl ${coord}/workerkill?host=all
curl ${coord}/shutdown
}

set -eux

for binary in coordinator master worker shell-wrapper
do
  gomake -C ${TERMITE_DIR}/bin/${binary}
done

dd if=/dev/random of=secret.txt bs=20 count=1 && chmod 0600 secret.txt

${TERMITE_DIR}/bin/coordinator/coordinator -port ${pprefix}8 &

sudo -n true
for w in 1 2 3 4
do
  rm -f w$w.log w$w.stderr
  sudo -b ${TERMITE_DIR}/bin/worker/worker -coordinator ${coord} -port ${pprefix}$w -jobs 1 -logfile w$w.log &> w$w.stderr
done
sleep 1
${TERMITE_DIR}/bin/master/master -jobs 50 -port ${pprefix}9 -coordinator ${coord} >& master.log & 
make clean
sleep 1
${TERMITE_DIR}/termite-make -j50
${TERMITE_DIR}/termite-make -j50
make clean
${TERMITE_DIR}/termite-make -j50

clean
