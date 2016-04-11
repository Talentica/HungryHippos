#!/bin/bash

cat /root/hungryhippos/tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
cat /root/hungryhippos/tmp/master_ip_file > data_publisher_node_ips.txt

job_manager_ip=`cat /root/hungryhippos/tmp/master_ip_file`
for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/keyToValueToBucketMap root@$node:hungryhippos
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/bucketToNodeNumberMap root@$node:hungryhippos
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/bucketCombinationToNodeNumbersMap root@$node:hungryhippos
done

for data_publisher_node_ip in `cat data_publisher_node_ips.txt`
do
echo 'copying files on data publisher'
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/bucketCombinationToNodeNumbersMap root@$data_publisher_node_ip:hungryhippos/data-publisher
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/keyToValueToBucketMap root@$data_publisher_node_ip:hungryhippos/data-publisher
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/bucketToNodeNumberMap root@$data_publisher_node_ip:hungryhippos/data-publisher
done

echo 'copying files on job-manager'
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/bucketCombinationToNodeNumbersMap root@$job_manager_ip:hungryhippos/job-manager
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/keyToValueToBucketMap root@$job_manager_ip:hungryhippos/job-manager
   scp -o "StrictHostKeyChecking no" root@job_manager_ip:hungryhippos/sharding/bucketToNodeNumberMap root@$job_manager_ip:hungryhippos/job-manager

