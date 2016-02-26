#!/bin/bash
echo 'Shutting all java processes on nodes'
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

for node in `cat node_ips_list.txt`
do
   echo "Stopping HungryHippos node $node"
   ssh -o StrictHostKeyChecking=no root@$node "pkill -9 java"
done
