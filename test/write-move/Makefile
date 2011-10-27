default: out concat

out:
	mkdir $@

.PRECIOUS:
out/%.o:
	touch $@.bla
	mv $@.bla $@

clean:
	rm -rf out

concat: $(foreach n, $(shell seq 1 100), out/$(n).o)
	cat $^ > $@
