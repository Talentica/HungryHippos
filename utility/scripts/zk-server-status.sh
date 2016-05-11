#!/bin/bash
jobUuid=$1
zk_node_ip=`cat ../$jobUuid/master_ip_file`
echo "checking the zk server is running or not"
for node in `echo $zk_node_ip`
do
   echo "Checking status of zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh status"
   break
done
