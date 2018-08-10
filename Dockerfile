FROM golang:1.8.7 as dev
RUN apt-get update && apt-get -y install iptables

#RUN go get github.com/golang/lint/golint \
#		golang.org/x/tools/cmd/cover \
#		github.com/mattn/goveralls \
#		github.com/gordonklaus/ineffassign \
#		github.com/client9/misspell/cmd/misspell \
#		honnef.co/go/tools/cmd/gosimple

WORKDIR /go/src/github.com/docker/libnetwork

FROM dev

COPY . .
