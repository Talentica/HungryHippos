#!/bin/bash

sharding_node_ip=`cat ./node_pwd_file.txt|grep "sharding_node_ip"|awk -F":" '{print $2}'`


for node in `echo $sharding_node_ip`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/sharding
done
