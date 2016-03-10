#!/bin/bash

cat ./data_publisher_nodes_config.txt|awk -F":" '{print $2}' > data_publisher_node_ips.txt

for node in `cat data_publisher_node_ips.txt`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/data-publisher
done