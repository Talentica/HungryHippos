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
jobuuid=$1
sharding_node_ip=`cat ../$jobuuid/master_ip_file`
sh shut-down-sharding.sh $jobuuid
sh cleanup-sharding.sh $jobuuid
echo 'Copying new build'
sh copy-file-to-sharding.sh ../lib/sharding*.jar $jobuuid
echo 'Copying common configuration file'
echo 'Copying data publishers node configuration file'
sh copy-file-to-sharding.sh data_publisher_nodes_config.txt $jobuuid
echo 'Copying nodes'' password file'
echo 'Copying shard file copy utility'
sh copy-file-to-sharding.sh copy-shard-files-to-all-nodes.sh $jobuuid
sh copy-file-to-sharding.sh start-kazoo-server.sh $jobuuid
scp ~/.ssh/id_rsa.pub root@$sharding_node_ip:~/.ssh
scp ~/.ssh/id_rsa root@$sharding_node_ip:~/.ssh
