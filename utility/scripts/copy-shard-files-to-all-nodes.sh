#!/bin/bash

cat ./serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
job_manager_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   sshpass -p $node_pwd scp ./keyToValueToBucketMap root@$node:hungryhippos
   sshpass -p $node_pwd scp ./bucketToNodeNumberMap root@$node:hungryhippos
   sshpass -p $node_pwd scp ./bucketCombinationToNodeNumbersMap root@$node:hungryhippos
done
echo 'copying files on data publisher'
   sshpass -p $node_pwd scp ./bucketCombinationToNodeNumbersMap root@$data_publisher_node_ip:hungryhippos/data-publisher
   sshpass -p $node_pwd scp ./keyToValueToBucketMap root@$data_publisher_node_ip:hungryhippos/data-publisher
   sshpass -p $node_pwd scp ./bucketToNodeNumberMap root@$data_publisher_node_ip:hungryhippos/data-publisher
echo 'copying files on job-manager'
   sshpass -p $node_pwd scp ./bucketCombinationToNodeNumbersMap root@$job_manager_ip:hungryhippos/job-manager
   sshpass -p $node_pwd scp ./keyToValueToBucketMap root@$job_manager_ip:hungryhippos/job-manager
   sshpass -p $node_pwd scp ./bucketToNodeNumberMap root@$job_manager_ip:hungryhippos/job-manager

