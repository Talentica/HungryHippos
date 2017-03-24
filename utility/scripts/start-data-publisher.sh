#!/bin/bash
#*******************************************************************************
# Copyright 2017 Talentica Software Pvt. Ltd.
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
dataparserclass=$2

cat ../$jobUuid/master_ip_file > ../$jobUuid/data_publisher_node_ips.txt
sh shut-down-all-nodes.sh $jobUuid
sh start-zk-server.sh $jobUuid
sh remove-ssh-keygen.sh $jobUuid
for node in `cat ../$jobUuid/data_publisher_node_ips.txt`
do
    ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;sh copy-shard-files-to-all-nodes.sh $jobUuid"
   echo "Starting data publisher $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/data-publisher;java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp data-publisher.jar:../job-manager/test-jobs.jar com.talentica.hungryHippos.master.DataPublisherStarter $jobUuid $dataparserclass> ./system.out 2>./system.err &"
done

sh start-all-nodes.sh $jobUuid
