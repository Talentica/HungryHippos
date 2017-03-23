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

user=$1
fsRoot=$2
op=$3
loc=$4
node=$5
echo "user $user"
echo "fsroot $fsRoot"
echo "operation $op"
echo "fName  $loc"
echo "create Node Files if successful then create zookeeper files"
i=0;
echo "logging in to the cluster node $node"
ssh -o StrictHostKeyChecking=no $user@$node "java -cp file-system.jar com.talentica.hungryhippos.filesystem.main.NodeFileSystemMain $fsRoot $op $loc"
#	if [$node -eq $nodesInCluster[-1]]
#	then
#		"java -cp file-system-0.6.0 SNAPSHOT.jar com.talentica.hungryhippos.filesystem.main.  $op $loc"
#	fi


 
