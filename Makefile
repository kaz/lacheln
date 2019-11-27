.PHONY: build
build:
	docker run --rm -ti -v $(PWD):/go lacheln-builder go build -ldflags="-extldflags=-static -w -s"

.PHONY: image
image:
	docker build -t lacheln-builder scripts
