#!/bin/bash
jobUuid=$1
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt
cat ../$jobUuid/master_ip_file > ../$jobUuid/data_publisher_node_ips.txt

job_manager_ip=`cat ../$jobUuid/master_ip_file`
for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Copying file to $node"
   scp ~/.ssh/id_rsa.pub root@$node:~/.ssh
   scp ~/.ssh/id_rsa root@$node:~/.ssh
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir node"
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/keyToValueToBucketMap root@$node:hungryhippos/node/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketToNodeNumberMap root@$node:hungryhippos/node/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketCombinationToNodeNumbersMap root@$node:hungryhippos/node/
done

for data_publisher_node_ip in `cat ../$jobUuid/data_publisher_node_ips.txt`
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
