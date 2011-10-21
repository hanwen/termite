#!/bin/sh

rm -rf d[0-9]* out

N=10
echo 'default:' > Makefile
for x in $(seq 1 $N)
do
  for y in $(seq 1 $N)
  do
    mkdir -p d$x/d$y
    echo hello > d$x/d$y/f

    cat <<EOF>>Makefile
default: out/d$x/f$y

out/d$x/f$y:
	mkdir -p out/d$x
	cp -f d$x/d$y/f out/d$x/f$y
EOF
    
  done
done 

termite-make -j40
