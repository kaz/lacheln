.PHONY: build
build:
	docker run --rm -ti -v $(PWD):/go sql-replay-builder go build -ldflags="-extldflags=-static -w -s"

.PHONY: image
image:
	docker build -t sql-replay-builder scripts
