all: build

fmt:
	gofmt -l -w -s ./

dep:env
	go get github.com/Shopify/sarama
	go get github.com/golang/glog
	go get github.com/go-redis/redis
	go get github.com/go-yaml/yaml
	go get github.com/stretchr/testify/assert

build: dep fmt
	go build -ldflags "-w -s" -o bin/listener ./main/listener.go
	go build -ldflags "-w -s" -o bin/listener-retry ./retry/retry.go

env:
GOPATH:=$(CURDIR)
GOBIN:=$(CURDIR)/bin
export GOPATH
export GOBIN
