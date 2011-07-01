# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/go-fuse/termite
DEPS=../fuse ../unionfs

GOFILES=fsserver.go \
	rpcfs.go \
	executor.go \
	diskcache.go \
	connection.go \
	workerdaemon.go \
	contentserver.go \
	replay.go

include $(GOROOT)/src/Make.pkg

