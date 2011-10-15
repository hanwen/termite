#!/bin/sh

fail=$1
shift
succ=$1
shift

sed 's/0x[a-f0-9]*/0xhex/g' < $fail > failstrip.txt
sed 's/0x[a-f0-9]*/0xhex/g' < $succ > succstrip.txt

diff -u failstrip.txt succstrip.txt



