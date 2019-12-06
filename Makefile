GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
BINARY_NAME=esmini
BINARY_UNIX=$(BINARY_NAME)_unix

build:
	$(GOBUILD) -o $(BINARY_NAME) -v

test:
	$(GOTEST) -v ./...

examples:
	$(GOTEST) . -v -run=Example*

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)

lint:
	golangci-lint run --disable-all --enable=goimports --enable=golint --enable=govet --enable=errcheck --enable=staticcheck

