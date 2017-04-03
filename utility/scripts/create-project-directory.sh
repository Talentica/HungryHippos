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
echo "Creating required project directories"
cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt

manager_node_ip=`cat ../tmp/master_ip_file`
for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Creating required project directories on node $node"
   ssh -o StrictHostKeyChecking=no root@$node "mkdir hungryhippos"
   ssh -o StrictHostKeyChecking=no root@$node "mkdir hungryhippos/data"
done
