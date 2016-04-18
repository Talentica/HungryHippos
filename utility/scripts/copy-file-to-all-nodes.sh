#!/bin/bash

cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt


for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir node; mkdir node/data;"
   scp $1 root@$node:hungryhippos/node/
done
