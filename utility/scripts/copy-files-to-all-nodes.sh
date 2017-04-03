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
echo 'Copying master ip file on all nodes'
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt
cat ../$jobUuid/output_ip_file >> ../$jobUuid/node_ips_list.txt
sh remove-ssh-keygen.sh $jobUuid
for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir $jobUuid"
   scp ../$jobUuid/master_ip_file root@$node:hungryhippos/$jobUuid/
   scp ../$jobUuid/serverConfigFile.properties root@$node:hungryhippos/$jobUuid/
done

cat ../$jobUuid/master_ip_file > ../$jobUuid/temp_master_ip
for node in `cat ../$jobUuid/temp_master_ip`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir $jobUuid"
   scp ../$jobUuid/master_ip_file root@$node:hungryhippos/$jobUuid/
   scp ../$jobUuid/output_ip_file root@$node:hungryhippos/$jobUuid/
   scp ../$jobUuid/serverConfigFile.properties root@$node:hungryhippos/$jobUuid/
done
