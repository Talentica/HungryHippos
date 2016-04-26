#!/bin/bash
jobUuid=$2
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt


for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Copying file to $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir node; mkdir node/data;"
   scp $1 root@$node:hungryhippos/node/
done
