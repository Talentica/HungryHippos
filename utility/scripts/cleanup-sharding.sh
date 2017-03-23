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
echo 'Cleaning up sharding'
sharding_node_ip=`cat ../$jobuuid/master_ip_file|awk -F":" '{print $2}'`


for node in `echo $sharding_node_ip`
do
   echo "Cleaning up sharding node: $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/sharding;rm sharding.log*;rm sharding.log*;rm system.out;rm system.err"
done
