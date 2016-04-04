#!/bin/bash

zk_node_ip=`cat ./../../utility/scripts/zookeeper_ip`



for node in `echo $zk_node_ip`
do
   echo "Starting zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh start"
   break
done
