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
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list_for_keygen.txt
cat ../$jobUuid/output_ip_file >> ../$jobUuid/node_ips_list_for_keygen.txt
echo '\n' >> ../$jobUuid/node_ips_list_for_keygen.txt
cat ../$jobUuid/master_ip_file >> ../$jobUuid/node_ips_list_for_keygen.txt
sed '/^$/d' ../$jobUuid/node_ips_list_for_keygen.txt > ../$jobUuid/abc.txt
mv ../$jobUuid/abc.txt ../$jobUuid/node_ips_list_for_keygen.txt
for node in `cat ../$jobUuid/node_ips_list_for_keygen.txt`
do
   echo "Removing ssh keygen for $node"
   ssh-keygen -f "/root/.ssh/known_hosts" -R $node
done
