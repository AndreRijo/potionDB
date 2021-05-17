# Debian image with go installed and configured at /go
FROM golang as base

# Dependencies
#RUN go get github.com/golang/protobuf/proto
#RUN go get github.com/twmb/murmur3
#RUN go get github.com/streadway/amqp

# Adding src and building
ADD src/ /go/potionDB/src/
ADD go.mod /go/potionDB
ADD go.sum /go/potionDB
#RUN go version
#RUN go install -race main
#RUN cd potionDB && go install main
RUN cd potionDB && go mod download
RUN cd potionDB/src/main && go build

#Final image
FROM rabbitmq

#Copy both go and potionDB binaries
COPY --from=base /usr/local/go/bin/ /go/
COPY --from=base /go/potionDB/src/main/main /go/bin/

#Add start script
ADD dockerStuff/start.sh /go/bin/

#Default listen port
EXPOSE 8087
#RabbitMQ port
EXPOSE 5672

#Arguments
ENV CONFIG "/go/bin/configs/cluster/default"
#ENV SERVERS, ENV RABBITMQ
ENV RABBITMQ_WAIT 20s
#ENV RABBITMQ_WAIT 5s
ENV RABBITMQ_VHOST /crdts
ENV BUCKETS "none"
ENV DISABLE_REPLICATOR "none"
ENV DISABLE_LOG "none"
ENV DISABLE_READ_WAITING "none"

#Add config folders late to avoid having to rebuild multiple images
ENV RABBITMQ_PID_FILE /var/lib/rabbitmq/mnesia/rabbitmq
#ADD dockerStuff/rabbitmq.config /etc/rabbitmq/
#ADD dockerStuff/definitions.json /etc/rabbitmq/
#RUN chown rabbitmq:rabbitmq /etc/rabbitmq/rabbitmq.config /etc/rabbitmq/definitions.json
ADD configs /go/bin/configs

# Run the protoserver
#CMD ["sh", "-c", "go/bin/start.sh $CONFIG"]
CMD ["bash", "go/bin/start.sh"]