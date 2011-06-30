#!/bin/sh

set -eux
gomake -C termite/master
./termite/master/master "$@"
