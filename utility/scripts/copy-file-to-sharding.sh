#!/bin/bash

sharding_node_ip=`/root/hungryhippos/tmp/master_ip_file`


for node in `echo $sharding_node_ip`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/sharding
done
