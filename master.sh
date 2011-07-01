#!/bin/sh

gomake -C termite/master

set -eu

CPU_COUNT=$(grep '^processor'  /proc/cpuinfo | wc -l)
export GOMAXPROCS=${CPU_COUNT}

export TERMITE_SOCKET=/tmp/termite-socket
export TERMITE_TOOLS=/tmp/tools/termite
rm -rf ${TERMITE_TOOLS}
mkdir -p ${TERMITE_TOOLS}

for bin in gcc g++ bison
do
  ln -s $(pwd)/termite/tool/tool ${TERMITE_TOOLS}/${bin}
done

echo "put this in the environment to run:"
echo ""
echo "  export TERMITE_SOCKET=${TERMITE_SOCKET}"
echo "  export PATH=${TERMITE_TOOLS}:\${PATH}"
echo ""

set -eux

rm -f ${TERMITE_SOCKET}
./termite/master/master -socket ${TERMITE_SOCKET} "$@"
