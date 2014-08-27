#!/bin/sh

set -eux

DIR=$(PWD)
(cd bin/mkbox && make && ln -sf ${DIR}/bin/mkbox/mkbox $GOPATH/bin/termite-mkbox )

for target in "clean" "install"
do
  for d in stats attr cba fs termite \
      bin/coordinator \
      bin/worker bin/master bin/shell-wrapper ; \
  do
    (cd $d && go $target . )
  done
done

for d in stats attr cba termite
do
  (cd $d && go test . )
done
