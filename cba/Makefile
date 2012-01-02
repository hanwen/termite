# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/termite/cba
DEPS=../stats ../splice

GOFILES=lrucache.go \
	misc.go \
	net.go \
	rpc.go \
	spliceserver.go \
	store.go \
	throughput.go 

include $(GOROOT)/src/Make.pkg

