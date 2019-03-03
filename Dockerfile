# Debian image with go installed and configured at /go
FROM golang

# Adding src
ADD src/ /go/src/

# Build
RUN go get github.com/golang/protobuf/proto
run go get github.com/twmb/murmur3
RUN go install main

# Run the protoserver
ENTRYPOINT /go/bin/main

# Default listen port
EXPOSE 8087