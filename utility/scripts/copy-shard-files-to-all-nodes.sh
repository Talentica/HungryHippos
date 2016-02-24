#!/bin/bash

cat ./serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`
for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   scp -o "StrictHostKeyChecking no" ./keyToValueToBucketMap root@$node:hungryhippos
   scp -o "StrictHostKeyChecking no" ./bucketToNodeNumberMap root@$node:hungryhippos
   scp -o "StrictHostKeyChecking no" ./bucketCombinationToNodeNumbersMap root@$node:hungryhippos
done
echo 'copying files on data publisher'
   scp -o "StrictHostKeyChecking no" ./bucketCombinationToNodeNumbersMap root@$data_publisher_node_ip:hungryhippos/data-publisher
   scp -o "StrictHostKeyChecking no" ./keyToValueToBucketMap root@$data_publisher_node_ip:hungryhippos/data-publisher
   scp -o "StrictHostKeyChecking no" ./bucketToNodeNumberMap root@$data_publisher_node_ip:hungryhippos/data-publisher
echo 'copying files on job-manager'
   scp -o "StrictHostKeyChecking no" ./bucketCombinationToNodeNumbersMap root@$job_manager_ip:hungryhippos/job-manager
   scp -o "StrictHostKeyChecking no" ./keyToValueToBucketMap root@$job_manager_ip:hungryhippos/job-manager
   scp -o "StrictHostKeyChecking no" ./bucketToNodeNumberMap root@$job_manager_ip:hungryhippos/job-manager
