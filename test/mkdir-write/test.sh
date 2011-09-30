#!/bin/sh

shell-wrapper -inspect 'Makefile'
make clean
${TERMITE_DIR}/termite-make -j50
${TERMITE_DIR}/termite-make -j50
make clean
${TERMITE_DIR}/termite-make -j50

