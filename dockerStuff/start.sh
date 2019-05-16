#!/bin/bash

/opt/rabbitmq/sbin/rabbitmq-server &
echo $PWD
echo $1
sleep 5
/go/bin/main $1