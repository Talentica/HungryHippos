#!/bin/bash
echo 'Cleaning up manager'
manager_node_ip=`cat ./node_pwd_file.txt|grep "manager_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $manager_node_ip`
do
   echo "Cleaning HungryHippos node $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/manager;rm Application.log*;rm bucketToNodeNumberMap;rm bucketCombinationToNodeNumbersMap;rm keyToValueToBucketMap"
done
