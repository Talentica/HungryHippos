#!/bin/bash

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
ssh -o StrictHostKeyChecking=no $user@$node "java -cp /home/hhuser/distr/lib_client/file-system.jar com.talentica.hungryhippos.filesystem.main.NodeFileSystemMain $fsRoot $op $loc"
#	if [$node -eq $nodesInCluster[-1]]
#	then
#		"java -cp file-system-0.6.0 SNAPSHOT.jar com.talentica.hungryhippos.filesystem.main.  $op $loc"
#	fi


 
