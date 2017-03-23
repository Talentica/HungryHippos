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

sh start-zk-server.sh $jobUuid
for node in `cat ../$jobUuid/node_ips_list.txt`
do
echo "Starting HungryHippos node for job execution $node"
ssh -o StrictHostKeyChecking=no root@$node "sh -c 'cd hungryhippos/node;java -Xmx1800m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp node.jar:test-jobs.jar com.talentica.hungryHippos.node.JobExecutor $jobUuid > ./system.out 2>./system.err &'"
done
