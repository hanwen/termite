default: $(foreach n, $(shell seq 1 100), out/touch.$(n))

.PRECIOUS:
out/dir.%:
	mkdir -p $@

out/touch.%: out/dir.%
	echo hoi |tee $(dir $@)/foo > $(dir $@)/bar
	touch $@

clean:
	rm -rf out
