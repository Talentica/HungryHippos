#!/bin/bash
echo 'Creating nodeId files on nodes'
cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt


i=0
one=1
for node in `cat node_ips_list.txt`
do
   echo "Creating HungryHippos nodeId for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;rm nodeId;echo $i >> nodeId"
   i=$(($i+$one))
done
