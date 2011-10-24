#!/bin/sh
set -x
pprefix=200
coord=localhost:${pprefix}8

name=$1
shift

# Guard against memory leaks: max 512mb.
ulimit -m 524288

export TERMITE_DIR=$(cd .. ; pwd)
dd if=/dev/urandom of=secret.txt bs=20 count=1 && chmod 0600 secret.txt
${TERMITE_DIR}/bin/coordinator/coordinator -port ${pprefix}8 &
sleep 1
sudo -n true

workers=2
workerjobs=2
masterjobs=8
if test -f $name/params.sh
then
  . $name/params.sh
fi

for w in $(seq 1 $workers)
do
  rm -f w$w.log w$w.stderr
  sudo -b ${TERMITE_DIR}/bin/worker/worker -coordinator ${coord} -port ${pprefix}$w -jobs $workerjobs -logfile w$w.log &> w$w.stderr
done
sleep 1
rm master.log
${TERMITE_DIR}/bin/master/master -secret secret.txt -socket ${name}/.termite-socket -jobs $masterjobs -port ${pprefix}9 -coordinator ${coord} >& master.log &
sleep 1

( export PATH="${TERMITE_DIR}/bin/shell-wrapper:$PATH";
  cd ${name} ;sh -eux test.sh)
status="$?"
echo "ran test"

(cd ${name}; ${TERMITE_DIR}/bin/shell-wrapper/shell-wrapper -shutdown)
curl ${coord}/killall
curl ${coord}/shutdown

# wait a bit for everything to come down.
sleep 1
if test "$status" != "0"
then
    echo FAIL
    exit 1
else
    echo PASS
    exit 0
fi


