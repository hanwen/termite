# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/termite/termite
DEPS=../fs

MANUAL_GOFILES=\
	attrcache.go \
	connection.go \
	contentcache.go \
	coordinator.go \
	copy.go \
	cpustat.go \
	fileattr.go \
	fileset.go \
	filesetwaiter.go \
	fsserver.go \
	fuse.go \
	getopt.go \
	lazyfile.go \
	lrucache.go \
	localmaster.go \
	localrun.go \
	master.go \
	masterrun.go \
	masterstatus.go \
	mirror.go \
	mirrors.go \
	mirrorconnection.go \
	misc.go \
	periodicsampler.go \
	rpc.go \
	rpcfs.go \
	serverstats.go \
	status.go \
	task.go \
	timerpc.go \
	version.go \
	worker.go \

GOFILES=$(MANUAL_GOFILES) version.gen.go

include $(GOROOT)/src/Make.pkg


version.gen.go: $(MANUAL_GOFILES)
	sh genversion.sh > $@
