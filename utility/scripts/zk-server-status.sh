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
zk_node_ip=`cat ../$jobUuid/master_ip_file`
echo "checking the zk server is running or not"
for node in `echo $zk_node_ip`
do
   echo "Checking status of zookeeper server on $node"
   ssh -o StrictHostKeyChecking=no root@$node "zkServer.sh status"
   break
done
