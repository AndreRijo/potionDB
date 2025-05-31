#!/bin/bash

echo $DO_DATALOAD
echo $SCALE
echo $DATALOC
echo $REGION
(/opt/rabbitmq/sbin/rabbitmq-server &)
echo RabbitMQ started, starting PotionDB.
exec /go/bin/main --config=$CONFIG --servers=$SERVERS --rabbitMQIP=$RABBITMQ --rabbitVHost=$RABBITMQ_VHOST --buckets=$BUCKETS --disableReplicator=$DISABLE_REPLICATOR --disableLog=$DISABLE_LOG --disableReadWaiting=$DISABLE_READ_WAITING --useTC=$USE_TC --tcIPs=$TC_IPS --selfIP=$SELF_IP --poolMax=$POOL_MAX --topKSize=$TOPK_SIZE --doDataload=$DO_DATALOAD --scale=$SCALE --dataLoc=$DATALOC --region=$REGION --doIndexload=$DO_INDEXLOAD --isGlobal=$IS_GLOBAL --indexFullData=$IS_FULL --useTopKAll=$USE_TOPK_ALL --useTopSum=$USE_TOP_SUM --initialMem=$INITIAL_MEM --queryNumbers=$QUERY_NUMBERS --protoTestMode=$PROTO_TEST_MODE --fastSingleRead=$FAST_SINGLE_READ
echo Reached end of script.


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