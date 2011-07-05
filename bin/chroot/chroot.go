package main

import (
	"log"
	"flag"
	"syscall"
	"os"
)

func main() {
	exedir := flag.String("dir", "/", "directory to chroot to.")
	uid := flag.Int("uid", 0, "uid to use.")
	gid := flag.Int("gid", 0, "gid to use.")
	binary := flag.String("binary", "", "full path of binary.")
	flag.Parse()
	if len(flag.Args()) < 2 {
		log.Fatal("use: chroot DIR COMMAND [ARG ..]")
	}

	args := flag.Args()[1:]
	dir := flag.Arg(0)

	en := syscall.Chroot(dir)
	if en != 0 {
		log.Fatalln("Chroot error:", os.Errno(en))
	}

	en = syscall.Setgid(*gid)
	if en != 0 {
		log.Fatalln("Setgid error:", os.Errno(en))
	}

	en = syscall.Setuid(*uid)
	if en != 0 {
		log.Fatalln("Setuid error:", os.Errno(en))
	}

	err := os.Chdir(*exedir)
	if err != nil {
		log.Fatalln("Can't cd to", *exedir, err)
	}

	bin := *binary
	if bin == "" {
		bin = args[0]
	}

	en = syscall.Exec(bin, args, os.Environ())
	if en != 0 {
		log.Fatalln("Can't exec", args, os.Errno(en))
	}
}
