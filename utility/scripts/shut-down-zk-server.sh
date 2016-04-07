#!/bin/bash

zk_node_ip=`cat /root/hungryhippos/tmp/master_ip_file`


for node in `echo $zk_node_ip`
do
   echo "Stopping zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh stop"
   break
done
