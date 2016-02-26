#!/bin/bash
echo 'Cleaning up sharding'
sharding_node_ip=`cat ./node_pwd_file.txt|grep "sharding_node_ip"|awk -F":" '{print $2}'`


for node in `echo $sharding_node_ip`
do
   echo "Cleaning up sharding node: $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;rm sharding.log*;rm sharding.log*;rm system.out;rm system.err"
done
