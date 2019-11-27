.PHONY: default
default: docker-build

ifeq ($(shell uname),Linux)
LDFLAGS="-w -s -extldflags=-static"
else
LDFLAGS="-w -s"
endif

###############
# local build #
###############

.PHONY: build
build: lacheln sample.so

.PHONY: lacheln # always rebuild
lacheln:
	go build -o $@ -ldflags=$(LDFLAGS)

.PHONY: sample.so # always rebuild
sample.so:
	go build -o $@ -ldflags=$(LDFLAGS) -buildmode plugin ./plugins/sample

###################
# build in docker #
###################

IMAGE_NAME=lacheln-builder

.PHONY: docker-build
docker-build:
	docker run --rm -ti -v $(PWD):/go $(IMAGE_NAME) make build

.PHONY: docker-image
docker-image:
	docker build -t $(IMAGE_NAME) scripts
