#!/bin/bash

/opt/rabbitmq/sbin/rabbitmq-server &
#echo $PWD ;
#echo $1 ;
sleep 15s ;
rabbitmqctl add_user test test
rabbitmqctl set_user_tags test administrator;
rabbitmqctl set_permissions -p / test ".*" ".*" ".*";
sleep 1s;
/go/bin/main $1 ;