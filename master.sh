#!/bin/sh

gomake -C termite/master

export TERMITE_SOCKET=/tmp/termite-socket
echo "put this in the environment to run:"
echo ""
echo "  export TERMITE_SOCKET=${TERMITE_SOCKET}"
echo ""
set -eux

rm -f /tmp/termite-socket
./termite/master/master -socket /tmp/termite-socket
