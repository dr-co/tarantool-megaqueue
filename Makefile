VERBOSE	?= 0

VERSION	= $(shell grep VERSION megaqueue/init.lua	\
	|awk '{print $$3}'				\
	|sed "s/[,']//g"				\
)


SPEC_NAME 	= megaqueue-$(VERSION).rockspec

DOCKER_VERSIONS	?= \
	2.8.3 		\
	2.8
DOCKER_LATEST ?= 2.8.3

GITVERSION	= $(shell git describe)

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


dockers:
	@set -e; \
	for version in $(DOCKER_VERSIONS); do \
		TAGS="-t unera/tarantool-megaqueue:$$version-$(GITVERSION)"; \
		test $$version = $(DOCKER_LATEST) && TAGS="-t unera/tarantool-megaqueue:latest $$TAGS"; \
		echo "\\nDockers creating: $$TAGS..."; \
		sed -E "s/@@VERSION@@/$$version/g" docker/Dockerfile.in \
			| docker build -f- . \
				$$TAGS 2>&1 |sed -u -E 's/^/\t/' \
		; \
	done

docker-upload: dockers
	@set -e; \
	cd docker; \
	for version in $(DOCKER_VERSIONS); do \
		TAGS="unera/tarantool-megaqueue:$$version-$(GITVERSION)"; \
		test $$version = $(DOCKER_LATEST) && TAGS="$$TAGS unera/tarantool-megaqueue:latest"; \
		echo "\\n/ $$version / Uploading: $$TAGS..."; \
		for tag in $$TAGS; do \
			echo + docker push $$tag; \
			docker push $$tag; \
		done; \
	done


.PHONY: \
	all \
	test \
	update-spec
