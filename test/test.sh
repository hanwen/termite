#!/bin/sh
set -x
pprefix=200
coord=localhost:${pprefix}8

name=$1
shift

export TERMITE_DIR=$(cd .. ; pwd)
dd if=/dev/random of=secret.txt bs=20 count=1 && chmod 0600 secret.txt
${TERMITE_DIR}/bin/coordinator/coordinator -port ${pprefix}8 &

sudo -n true
for w in 1 2 3 4
do
  rm -f w$w.log w$w.stderr
  sudo -b ${TERMITE_DIR}/bin/worker/worker -coordinator ${coord} -port ${pprefix}$w -jobs 4 -logfile w$w.log &> w$w.stderr
done
sleep 1
${TERMITE_DIR}/bin/master/master -socket ${name}/.termite-socket -jobs 50 -port ${pprefix}9 -coordinator ${coord} -exclude usr/lib/locale/locale-archive >& master.log &
sleep 1

(cd ${name} ;sh -eux test.sh)
status="$?"
echo "ran test"

(cd ${name}; shell-wrapper -shutdown)
curl ${coord}/workerkill?host=all
curl ${coord}/shutdown
if test "$status" != "0"
then
    echo FAIL
else
    echo PASS
fi


