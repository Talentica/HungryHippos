#!/bin/bash
user=$1
op=$2
loc=$3
node=$4
echo "user $user"
echo "operation $op"
echo "fName  $loc"
echo "create Node Files if successful then create zookeeper files"
i=0;
echo "logging in to the cluster node $node"
ssh -o StrictHostKeyChecking=no $user@$node "cd hungryhippos;java -cp file-system.jar com.talentica.hungryhippos.filesystem.main.NodeFileSystemMain  $op $loc"
#	if [$node -eq $nodesInCluster[-1]]
#	then
#		"java -cp file-system-0.6.0 SNAPSHOT.jar com.talentica.hungryhippos.filesystem.main.  $op $loc"
#	fi


 
