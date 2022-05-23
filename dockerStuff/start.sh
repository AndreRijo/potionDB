#!/bin/bash

echo $DO_DATALOAD;
echo $SCALE;
echo $DATALOC;
echo $REGION;
/opt/rabbitmq/sbin/rabbitmq-server & (
/go/bin/main --config=$CONFIG --servers=$SERVERS --rabbitMQIP=$RABBITMQ --rabbitVHost=$RABBITMQ_VHOST --buckets=$BUCKETS --disableReplicator=$DISABLE_REPLICATOR --disableLog=$DISABLE_LOG --disableReadWaiting=$DISABLE_READ_WAITING --useTC=$USE_TC --tcIPs=$TC_IPS --selfIP=$SELF_IP --poolMax=$POOL_MAX --topKSize=$TOPK_SIZE --doDataload=$DO_DATALOAD --scale=$SCALE --dataLoc=$DATALOC --region=$REGION
)

#srv=${SERVERS:-nil}
#rab=${RABBITMQ:-nil}
#test which variables are set
#if [[[-z $SERVERS]] && [[-z $RABBITMQ]]]
#if [[srv == nil] && [rab == nil]]
#then
#	/go/bin/main --config=$CONFIG;
#elif [-z $SERVERS]
#elif [srv == nil]
#then
#	/go/bin/main --config=$CONFIG --rabbitMQIP=$RABBITMQ;
#elif [-z $RABBITMQ]
#elif [rab == nil]
#then
#	/go/bin/main --config=$CONFIG --servers=$SERVERS;
#else
#	/go/bin/main --config=$CONFIG --servers=$SERVERS --rabbitMQIP=$RABBITMQ;
#fi