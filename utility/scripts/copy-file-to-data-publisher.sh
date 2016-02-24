#!/bin/bash

data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`


for node in `echo $data_publisher_node_ip`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/data-publisher
done
