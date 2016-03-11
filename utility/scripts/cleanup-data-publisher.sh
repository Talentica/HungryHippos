#!/bin/bash
echo 'Cleaning up data publisher'
cat ./data_publisher_nodes_config.txt|awk -F":" '{print $2}' > data_publisher_node_ips.txt
for node in `cat data_publisher_node_ips.txt`
do
   echo "Cleaning data publisher node $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/data-publisher;rm datapublisher.log*;rm Application.log*;rm system.out;rm system.err"
done
