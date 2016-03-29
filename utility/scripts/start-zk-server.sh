#!/bin/bash

zk_node_ip=`cat ./../../utility/src/main/resources/config.properties|grep "zookeeper.server.ips"| awk -F"=" '{print $2}'| awk -F":" '{print $1}'`



for node in `echo $zk_node_ip`
do
   echo "Starting zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh start"
   break
done
