#!/bin/bash
file=$1
jobuuid=$2
cat ../$jobuuid/master_ip_file > data_publisher_node_ips.txt

for node in `cat data_publisher_node_ips.txt`
do
   echo "Copying file to $node"
   scp $file root@$node:hungryhippos/data-publisher
done