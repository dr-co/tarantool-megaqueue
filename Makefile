VERBOSE	?= 0

VERSION	= $(shell grep VERSION megaqueue/init.lua	\
	|awk '{print $$3}'				\
	|sed "s/[,']//g"				\
)


SPEC_NAME 	= megaqueue-$(VERSION).rockspec

all:
	@echo usage: 'make test'


test:
	@echo '# Run tests for version: $(VERSION)'
	prove -r$(shell if test "$(VERBOSE)" -gt 0; then echo v; fi) t


update-spec: $(SPEC_NAME)


$(SPEC_NAME): $(megaqueue/init.lua) megaqueue.spec.tpl
	rm -fr megaqueue-*.rockspec
	cp -v megaqueue.spec.tpl $@.prepare
	sed -Ei 's/@@VERSION@@/$(VERSION)/g' $@.prepare
	mv -v $@.prepare $@
	git add $@


upload: update-spec
	rm -f megaqueue-*.src.rock
	luarocks upload $(SPEC_NAME)	
	

.PHONY: \
	all \
	test \
	update-spec
