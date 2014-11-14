Termite is a generic distributed compilation system.

The master distributes the compilation to workers.  Workers run
arbitrary binaries in a containerized FUSE mirror of the master's file
system, and then ship the results back to the master.


CAVEATS

Work in progress.


COMPILE/INSTALL

* Install go.

* Install prerequisites:

  go install code.google.com/p/go.crypto/ssh
  go install github.com/hanwen/go-fuse/fuse

* Compiling:

  git clone https://github.com/hanwen/termite
  mkdir go ; cd go
  export GOPATH=$(pwd)
  (cd bin/mkbox ; make )
  for d in bin/coordinator bin/worker bin/master bin/shell-wrapper
  do
    go install github.com/hanwen/termite/$d
  done
  sudo cp termite-make /usr/local/bin/
  sudo cp bin/mkbox/mkbox /usr/local/bin/termite-mkbox
  sudo cp /tmp/go/bin/* /usr/local/bin/

* Make needs to be patched to use termite's shell wrapper:

  # Add MAKE_SHELL variable to make.
  wget http://ftp.gnu.org/gnu/make/make-3.82.tar.bz2
  tar xjf make-3.28.tar.bz2
  cd make-3.82 && patch -p1 < ../termite/patches/make-*patch
  ./configure && make && make install

* Coreutils before 8.0 has buggy directory traversal, making 'rm -rf' flaky.

* Set resource limits: add the following to your /etc/security/limits.conf

  root  soft    nofile       5000
  root  hard    nofile       5000
  *  soft    nofile       5000
  *  hard    nofile       5000

* Mount the source/object directories so termite can write xattrs, and
  noatime for performance improvements:

  mount -o remount,user_xattr,noatime my/device my/mountpoint


OVERVIEW

There are 5 binaries:

* Mkbox: a wrapper that sets up the containerization. Based on Brian Swetland's
https://github.com/swetland/mkbox

* Coordinator: a simple server that administers a list of live
workers.  Workers periodically contact the coordinator.

* Worker: should run as root, and typically runs on multiple machines.

* Master: the daemon that runs on the machine.  It contacts the
coordinator to get a list of workers, and reserves job slots on the
workers.  Run it in the root of the writable directory for the
compile.  It creates a .termite-socket that the wrapper below uses.

* Shell-wrapper: a wrapper to use with make's SHELL variable.

The choice between remote and local can be set through the file
.termite-localrc in the same dir as .termite-socket.  The file is in
json format, and you can find examples in the patches/ subdirectory.
The default

  [{
    "Regexp": ".*termite-make",
    "Local": true,
    "Recurse": true,
    "SkipRefresh": true
  }, {
    "Regexp": ".*",
    "Local": false
  }]

(ie., only recursive make calls are run locally) should work for most
projects, but for performance reasons, you might want to run more
commands locally.

Typically, build-system commands should run locally (eg. make, cmake).

Commands that modify build artefacts should not run locally: local
commands do not run inside a FUSE sandbox, so termite can't tell what
files they modify, and how to update filesystem caches on the workers.
By default, after executing a local command, the termite master scans
for changed files.  If you know this is not the case, you can skip
this with SkipRefresh: true.



RUNNING

  ssh-keygen -t rsa -b 1024 -f termite_rsa
  ${TERMITE_DIR}/bin/coordinator/coordinator -secret termite_rsa &
  ${TERMITE_DIR}/bin/worker/worker -jobs 4 -coordinator localhost:1233 \
    -secret termite_rsa

  cd ${PROJECT}
  ${TERMITE_DIR}/bin/master/master -jobs 4 \
    -secret termite_rsa &
  termite-make -j20


PERFORMANCE

See below.  The overhead of running in FUSE is 50 to 100%


SECURITY

* The worker runs binaries inside a containerized mount of a FUSE file
  system.

* Worker and master use plaintext TCP/IP, and use a shared secret with
  HMAC-SHA1 to authenticate the connection.  See
  https://github.com/hanwen/termite/blob/master/termite/connection.go
  for details.

* Worker and master must trust each other, for the following reasons:

  - workers can request all publicly readable files from the master.

  - workers can cause the master to run arbitrary binaries as the user
    compiling.

  - the master can make the worker run arbitrary binaries as 'nobody'.

* The master will never serve files that have no group/other
  permissions.

* Wrapper and master run as the same user and use IPC unix domain
  sockets to communicate.  The socket mode is 0700.



CAVEATS

* Hardlinks on the workers are translated to copies on the master.


TODO (by decreasing priority)

* Worker -> worker fetch
* Connection scheme: exp/ssh, security review?


SUCCESSFUL COMPILES

Termite timings by running master and single worker on the same
machine.  The smaller the package, the larger the overhead.

* coreutils 8.12 (1.6x slower, Lenovo T60, 2-core, make -j2)

* Make 3.82 (1.85x slower, Lenovo T60, 2-core)

* LLVM 2.9 (1.5 slower, Dell T5300 6-core, make -j12)

* GUILE 2.0.
 - Must run inside srcdir.
 - 1.1x slower, Dell T5300 6-core, make -j6

* Emacs 24
 - 1.8x slower (Lenovo T60, make -j2)
 - Must run in srcdir.

* Android Gingerbread.


DISCLAIMER

This is not an official Google product.
