#!/bin/bash
jobUuid=$1
echo 'Cleaning up all nodes'
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt


for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Cleaning up HungryHippos node  $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;rm ./data/Application.log*;rm Application.log*;rm system.out;rm system.err;rm outputFile;"
done
