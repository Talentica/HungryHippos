#!/bin/bash
jobUuid=$1
echo 'Shutting all java processes on nodes'
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt

for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Stopping HungryHippos node $node"
   ssh -o StrictHostKeyChecking=no root@$node "pkill -9 java"
done
