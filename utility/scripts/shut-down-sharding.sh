#!/bin/bash
jobuuid=$1
echo 'Shutting down all java processes on sharding node'
sharding_node_ip=`cat ../$jobuuid/master_ip_file`


for node in `echo $sharding_node_ip`
do
   echo "Stopping sharding on $node"
   ssh -t -o StrictHostKeyChecking=no root@$node 'cd hungryhippos/sharding;ps -ef| grep talentica.hungryHippos|awk -F" " "{print $2}" > tempProcessesToKill.txt'
ssh -t -o StrictHostKeyChecking=no root@$node 'cd hungryhippos/sharding;cat tempProcessesToKill.txt|awk -F" " "{print $2}"> hungryhippos_java_processes_to_kill.txt'
done
