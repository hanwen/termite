# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/go-fuse/termite
DEPS=../fuse ../unionfs

GOFILES=\
	connection.go \
	contentserver.go \
	diskcache.go \
	fsserver.go \
	master.go \
	rpcfs.go \
	task.go \
	workerdaemon.go \

include $(GOROOT)/src/Make.pkg

