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
dataparserclass=$2

sh start-zk-server.sh $jobUuid
sharding_node_ip=`cat ../$jobUuid/master_ip_file`

for node in `echo $sharding_node_ip`
do
   echo "Starting script to copy logs file if any error/exception/success occurs.."
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar com.talentica.hungryHippos.sharding.main.StartCopyLogsMain $jobUuid > ./system_copy_logs.out 2>./system_copy_logs.err &"
   echo "Started.."
   
   echo "Starting process db entry script"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar com.talentica.hungryHippos.sharding.main.StartProcessDBEntryMain $jobUuid > ./system_db_process_entry.out 2>./system_db_process_entry.err &"
   echo "Started process db entry"

   echo "Starting kazoo server"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar com.talentica.hungryHippos.sharding.main.StartKazooScriptMain $jobUuid > ./system_kazoo_server.out 2>./system_kazoo_server.err &"
   echo "Kazoo Server started"

   echo "Starting sharding on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;java -cp sharding.jar:../job-manager/test-jobs.jar com.talentica.hungryHippos.sharding.main.ShardingStarter $jobUuid $dataparserclass > ./sharding_system.out 2>./sharding_system.err &"
done
