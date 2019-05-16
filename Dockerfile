# Debian image with go installed and configured at /go
FROM golang as base

# Dependencies
RUN go get github.com/golang/protobuf/proto
RUN go get github.com/twmb/murmur3
RUN go get github.com/streadway/amqp

# Adding src and building
ADD src/ /go/src/
RUN go install main


#Final image
FROM rabbitmq

#Copy both go and potionDB binaries
COPY --from=base /usr/local/go/bin/ /go/
COPY --from=base /go/bin/ /go/bin/

#Add remaining potionDB stuff
ADD configs /go/bin/configs
ADD dockerStuff/start.sh /go/bin/

#Default listen port
EXPOSE 8087

# Run the protoserver
CMD ["bash", "go/bin/start.sh", "-config=/go/bin/configs/default"]
