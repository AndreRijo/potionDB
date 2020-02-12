#!/bin/bash

/opt/rabbitmq/sbin/rabbitmq-server &
#echo $PWD ;
#echo $1 ;
#echo $0 ;
#echo $CONFIG ;
#echo $SERVERS ;
#echo $RABBITMQ ;
sleep $RABBITMQ_WAIT ; 
rabbitmqctl add_vhost $RABBITMQ_VHOST ;
sleep 5s ;
rabbitmqctl add_user test test ; 
rabbitmqctl set_user_tags test administrator;
rabbitmqctl set_permissions -p $RABBITMQ_VHOST test ".*" ".*" ".*";
sleep 5s;
#/go/bin/monitor-fetch/main $1 ;

/go/bin/main --config=$CONFIG --servers=$SERVERS --rabbitMQIP=$RABBITMQ --rabbitVHost=$RABBITMQ_VHOST;

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