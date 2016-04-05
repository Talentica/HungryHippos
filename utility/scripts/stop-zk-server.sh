#!/bin/bash

chmod 777 zookeeper_ip
zk_node_ip=`cat zookeeper_ip`


for node in `echo $zk_node_ip`
do
   echo "Starting zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh stop"
   break
done
