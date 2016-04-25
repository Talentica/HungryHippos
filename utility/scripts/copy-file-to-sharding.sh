#!/bin/bash
jobuuid=$2
sharding_node_ip=`cat ../$jobuuid/master_ip_file`


for node in `echo $sharding_node_ip`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/sharding
done
