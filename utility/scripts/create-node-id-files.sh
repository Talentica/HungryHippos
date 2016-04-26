#!/bin/bash
jobUuid=$1
echo 'Creating nodeId files on nodes'
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt


i=0
one=1
for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Creating HungryHippos nodeId for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir node;rm node/nodeId;echo $i >> node/nodeId;mkdir node/data"
   i=$(($i+$one))
done
