#!/bin/bash
# first argument the ip address of a node, second argument is command and third is fileName or folderName.
node=$1
command=$2
fileName=$3


   echo "Connecting to a node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/file-system;java -XX:HeapDumpPath=./ -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp file-system.jar:test-jobs.jar com.talentica.hungryhippos.filesystem.main.NodeFileSystemMain $command $fileName > ./file-system.out 2>./file-system.err &"


