# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/go-fuse/rpcfs
DEPS=../fuse ../unionfs

GOFILES=fsserver.go \
	rpcfs.go \
	executor.go \
	diskcache.go 

include $(GOROOT)/src/Make.pkg

