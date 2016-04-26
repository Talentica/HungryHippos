#!/bin/bash
file=$1
jobuuid=$2
cat ../$jobuuid/master_ip_file > ../$jobUuid/data_publisher_node_ips.txt

for node in `cat ../$jobUuid/data_publisher_node_ips.txt`
do
   echo "Copying file to $node"
   scp $file root@$node:hungryhippos/data-publisher
done