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

# TC Support for testing. Must be ran here since golang includes this, but not rabbitmq.
RUN apt-get update && apt-get install -y \
	iproute2 \
	iputils-ping \
	&& rm -rf /var/lib/apt/lists/*

#Copy both go and potionDB binaries
COPY --from=base /usr/local/go/bin/ /go/
COPY --from=base /go/bin/ /go/bin/

#Add start script
ADD dockerStuff/start.sh /go/bin/

#Default listen port
EXPOSE 8087
#RabbitMQ port
EXPOSE 5672

#Arguments
ENV CONFIG "/go/bin/configs/cluster/default"
#ENV SERVERS, ENV RABBITMQ
ENV RABBITMQ_WAIT 10s
ENV RABBITMQ_VHOST /crdts
ENV ADDED_LATENCY 0

#Add config folders late to avoid having to rebuild multiple images
ADD configs /go/bin/configs

# Run the protoserver
#CMD ["sh", "-c", "go/bin/start.sh $CONFIG"]
CMD ["bash", "go/bin/start.sh"]