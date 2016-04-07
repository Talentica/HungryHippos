#!/bin/bash

cat /root/hungryhippos/tmp/master_ip_file > data_publisher_node_ips.txt

for node in `cat data_publisher_node_ips.txt`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/data-publisher
done