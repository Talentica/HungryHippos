#!/bin/bash

chmod 777 ../tmp/master_ip_file
zk_node_ip=`cat ../tmp/master_ip_file`


for node in `echo $zk_node_ip`
do
   echo "Starting zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh stop"
   break
done
