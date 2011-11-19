# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/termite/termite
DEPS=../fs ../attr ../cba

MANUAL_GOFILES=\
	connection.go \
	coordinator.go \
	copy.go \
	cpustat.go \
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
