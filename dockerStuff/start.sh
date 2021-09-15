#!/bin/bash

/opt/rabbitmq/sbin/rabbitmq-server & (
#echo $PWD ;
#echo $1 ;
#echo $0 ;
#echo $CONFIG ;
#echo $SERVERS ;
#echo $RABBITMQ ;
sleep $RABBITMQ_WAIT ;
rabbitmqctl wait --timeout 60 $RABBITMQ_PID_FILE ; 
#rabbitmqctl await_startup ;
#rabbitmqctl wait $RABBITMQ_PID_FILE ;  
#sleep 5s ; 
rabbitmqctl add_vhost $RABBITMQ_VHOST ;
#sleep 3s ;
rabbitmqctl add_user test test ; 
rabbitmqctl set_user_tags test administrator;
rabbitmqctl set_permissions -p $RABBITMQ_VHOST test ".*" ".*" ".*";
sleep 3s;
sleep $POTIONDB_WAIT ; 
#sleep 20s;
#/go/bin/monitor-fetch/main $1 ;
#/opt/rabbitmq/sbin/cuttlefish -c /etc/rabbitmq/rabbitmq.conf -s /opt/rabbitmq/priv/schema/ ;

/go/bin/main --config=$CONFIG --servers=$SERVERS --rabbitMQIP=$RABBITMQ --rabbitVHost=$RABBITMQ_VHOST --buckets=$BUCKETS --disableReplicator=$DISABLE_REPLICATOR --disableLog=$DISABLE_LOG --disableReadWaiting=$DISABLE_READ_WAITING --useTC=$USE_TC --tcIPs=$TC_IPS;
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