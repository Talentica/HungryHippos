#!/bin/bash
echo 'Cleaning up sharding'
sharding_node_ip=`cat ./node_pwd_file.txt|grep "sharding_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $sharding_node_ip`
do
   echo "Cleaning up sharding node: $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/sharding;rm sharding.log*;rm bucketToNodeNumberMap;rm bucketCombinationToNodeNumbersMap;rm keyToValueToBucketMap"
done
