#!/bin/bash
echo 'Cleaning up all nodes'
cat /root/hungryhippos/tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt


for node in `cat node_ips_list.txt`
do
   echo "Cleaning up HungryHippos node  $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;rm ./data/Application.log*;rm Application.log*;rm system.out;rm system.err;rm outputFile;"
done
