#!/bin/bash
#*******************************************************************************
# Copyright [2017] [Talentica Software Pvt. Ltd.]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*******************************************************************************
jobUuid=$1
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt
cat ../$jobUuid/master_ip_file > ../$jobUuid/data_publisher_node_ips.txt

job_manager_ip=`cat ../$jobUuid/master_ip_file`

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

for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Copying file to $node"
   ssh-keygen -f "/root/.ssh/known_hosts" -R $node
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir node"
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/keyToValueToBucketMap root@$node:hungryhippos/node/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketToNodeNumberMap root@$node:hungryhippos/node/
   scp -o "StrictHostKeyChecking no" /root/hungryhippos/sharding/bucketCombinationToNodeNumbersMap root@$node:hungryhippos/node/
done