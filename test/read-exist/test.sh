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
default: out/dst$x/f$y

out/dst$x/f$y:
	mkdir -p out/dst$x
	cp -f d$x/d$y/f out/dst$x/f$y
EOF
    
  done
done 

termite-make -j40
