#!/bin/sh

rm -rf dir
mkdir -p dir/baz
sleep 1
shell-wrapper -c '/bin/true;' &
shell-wrapper -c '/bin/true;' &
shell-wrapper -c '/bin/true;' &
shell-wrapper -c '/bin/true;' &
sleep 1
shell-wrapper -worker 2002 -c 'ls -1 .' 
shell-wrapper -worker 2001 -c 'mkdir dir/foobar;'
shell-wrapper -worker 2002 -c 'ls -1 dir' | grep baz

