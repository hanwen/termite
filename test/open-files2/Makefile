default: $(foreach n, $(shell seq 1 1024), out/file.$(n))

out/file.%:
	mkdir -p out
	echo $@ > $@ 

clean:
	rm -rf out
