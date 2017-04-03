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
file=$1
jobuuid=$2
sharding_node_ip=`cat ../$jobuuid/master_ip_file`
#sharding_node_ip=`cat /root/hungryhippos/installation/$jobuuid/master_ip_file`


for node in `echo $sharding_node_ip`
do
   echo "Copying file to $node"
   scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $file root@$node:hungryhippos/sharding
done
