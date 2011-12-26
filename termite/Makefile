# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/termite/termite
DEPS=../fs ../attr ../cba ../stats ../splice

MANUAL_GOFILES=\
	connection.go \
	coordinator.go \
	fsserver.go \
	fuse.go \
	getopt.go \
	lazyfile.go \
	localmaster.go \
	localrun.go \
	master.go \
	masterrun.go \
	masterstatus.go \
	mirror.go \
	mirrors.go \
	mirrorconnection.go \
	misc.go \
	rpc.go \
	rpcfs.go \
	status.go \
	task.go \
	version.go \
	worker.go \

GOFILES=$(MANUAL_GOFILES) version.gen.go

include $(GOROOT)/src/Make.pkg


version.gen.go: $(MANUAL_GOFILES)
	sh genversion.sh > $@
