#!/bin/bash
echo 'Shutting all java processes on nodes'
cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

for node in `cat node_ips_list.txt`
do
   echo "Stopping HungryHippos node $node"
   ssh -o StrictHostKeyChecking=no root@$node "ps -ef| grep talentica.hungryHippos|awk -F" " '{print $2}' > hungryhippos_java_processes_to_kill.txt;"
done
