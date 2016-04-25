#!/bin/bash
jobuuid=$1
echo 'Shutting down all java processes on data publisher node'
cat ../$jobuuid/master_ip_file > data_publisher_node_ips.txt

for node in `cat data_publisher_node_ips.txt`
do
   echo "Stopping data publisher node $node"
   ssh -o StrictHostKeyChecking=no root@$node "pkill -9 java"
done
