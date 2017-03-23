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
jobuuid=$1
echo 'Shutting down all java processes on sharding node'
sharding_node_ip=`cat ../$jobuuid/master_ip_file`


for node in `echo $sharding_node_ip`
do
   echo "Stopping sharding on $node"
   ssh -t -o StrictHostKeyChecking=no root@$node 'cd hungryhippos/sharding;ps -ef| grep talentica.hungryHippos|awk -F" " "{print $2}" > tempProcessesToKill.txt'
ssh -t -o StrictHostKeyChecking=no root@$node 'cd hungryhippos/sharding;cat tempProcessesToKill.txt|awk -F" " "{print $2}"> hungryhippos_java_processes_to_kill.txt'
done
