# Debian image with go installed and configured at /go
FROM golang:1.20.4 as base

# Adding src and building
COPY potionDB/potionDB/go.mod potionDB/potionDB/go.sum /go/potionDB/potionDB/
COPY potionDB/crdt/go.mod potionDB/crdt/go.sum /go/potionDB/crdt/
COPY potionDB/shared/go.mod /go/potionDB/shared/
COPY tpch_data_processor/go.mod tpch_data_processor/go.sum /go/tpch_data_processor/
COPY sqlToKeyValue/go.mod sqlToKeyValue/go.sum /go/sqlToKeyValue/
#COPY goTools/go.mod goTools/go.sum /go/goTools/
RUN cd potionDB/potionDB && go mod download
ADD potionDB/potionDB/ /go/potionDB/potionDB/
ADD potionDB/crdt/ /go/potionDB/crdt/
ADD potionDB/shared/ /go/potionDB/shared/
ADD tpch_data_processor/ /go/tpch_data_processor/
ADD sqlToKeyValue/ /go/sqlToKeyValue/
#ADD goTools/ /go/goTools/
RUN cd potionDB/potionDB/main && go build


#Final image
#FROM rabbitmq:3.7.14
FROM rabbitmq:3.13.1

#Apps needed for the image
RUN apt-get update && apt-get install -y iproute2 && apt install -y iputils-ping

#Copy both go and potionDB binaries
COPY --from=base /usr/local/go/bin/ /go/
COPY --from=base /go/potionDB/potionDB/main/main /go/bin/

#Add start script
ADD potionDB/dockerStuff/start.sh /go/bin/

#Default listen port
EXPOSE 8087
#RabbitMQ port
EXPOSE 5672

#Arguments. You can add here more that you need, check dockerStuff/start.sh and src/main/protoServer.go, method loadConfigs().
ENV CONFIG "/go/bin/configs/cluster/default"
ENV RABBITMQ_WAIT 10s
ENV BUCKETS "none"
ENV POOL_MAX "none"
ENV POTIONDB_WAIT 0s
ENV TOPK_SIZE 100
ENV DO_DATALOAD false
ENV SCALE 1
ENV DATALOC "go/data/"
ENV REGION "none"

#If RabbitMQ takes a long time to start, add some delay to PotionDB startup
#ENV RABBITMQ_VHOST /crdts
#ENV POTIONDB_WAIT 0s

#Add config folders late to avoid having to rebuild multiple images
ADD potionDB/dockerStuff/rabbitmq.conf /etc/rabbitmq/
ADD potionDB/configs /go/bin/configs

# Run the protoserver
#CMD ["sh", "-c", "go/bin/start.sh $CONFIG"]
CMD ["bash", "go/bin/start.sh"]
#CMD ["sh", "go/bin/start.sh"]