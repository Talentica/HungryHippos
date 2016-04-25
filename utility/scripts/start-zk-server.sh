#!/bin/bash
jobUuid=$1
chmod 777 ../$jobUuid/master_ip_file
zk_node_ip=`cat ../$jobUuid/master_ip_file`
for node in `echo $zk_node_ip`
do
   echo "Starting zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh start"
   break
done
