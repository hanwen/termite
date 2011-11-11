# Use "gomake install" to build and install this package.
include $(GOROOT)/src/Make.inc

TARG=github.com/hanwen/termite/termite
DEPS=../../go-fuse/fuse ../../go-fuse/unionfs

MANUAL_GOFILES=\
	attrcache.go \
	connection.go \
	contentcache.go \
	contentserver.go \
	coordinator.go \
	copy.go \
	cpustat.go \
	counter.go \
	devnullfs.go \
	diskcache.go \
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
	masterstats.go \
	masterstatus.go \
	mirror.go \
	mirrorconnection.go \
	misc.go \
	periodicsampler.go \
	procfs.go \
	rpc.go \
	rpcfs.go \
	status.go \
	task.go \
	timerpc.go \
	timer.go \
	version.go \
	worker.go \

GOFILES=$(MANUAL_GOFILES) version.gen.go

include $(GOROOT)/src/Make.pkg


version.gen.go: $(MANUAL_GOFILES)
	sh genversion.sh > $@
