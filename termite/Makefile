# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/termite/termite
DEPS=../../go-fuse/fuse ../../go-fuse/unionfs

GOFILES=\
	connection.go \
	contentcache.go \
	contentserver.go \
	devnullfs.go \
	diskcache.go \
	fsserver.go \
	fuse.go \
	master.go \
	mirror.go \
	mirrorconnection.go \
	misc.go \
	registration.go \
	rpcfs.go \
	task.go \
	worker.go \
	workerstatus.go \

include $(GOROOT)/src/Make.pkg

