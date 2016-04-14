#!/bin/bash

cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
cat ../tmp/master_ip_file > data_publisher_node_ips.txt

job_manager_ip=`cat ../tmp/master_ip_file`
for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   scp ~/.ssh/id_rsa.pub root@$node:~/.ssh
   scp ~/.ssh/id_rsa root@$node:~/.ssh
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/keyToValueToBucketMap root@$node:hungryhippos
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketToNodeNumberMap root@$node:hungryhippos
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketCombinationToNodeNumbersMap root@$node:hungryhippos
done

for data_publisher_node_ip in `cat data_publisher_node_ips.txt`
do
echo 'copying files on data publisher'
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketCombinationToNodeNumbersMap /root/hungryhippos/data-publisher/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/keyToValueToBucketMap /root/hungryhippos/data-publisher/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketToNodeNumberMap /root/hungryhippos/data-publisher/
done

echo 'copying files on job-manager'
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketCombinationToNodeNumbersMap /root/hungryhippos/job-manager/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/keyToValueToBucketMap /root/hungryhippos/job-manager/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketToNodeNumberMap /root/hungryhippos/job-manager/
