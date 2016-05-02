#!/bin/bash
file=$1
jobuuid=$2
sharding_node_ip=`cat ../$jobuuid/master_ip_file`
#sharding_node_ip=`cat /root/hungryhippos/installation/$jobuuid/master_ip_file`


for node in `echo $sharding_node_ip`
do
   echo "Copying file to $node"
   scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $file root@$node:hungryhippos/sharding
done
