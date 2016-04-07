#!/bin/bash
echo 'Creating nodeId files on nodes'
cat /root/hungryhippos/tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt


i=0
one=1
for node in `cat node_ips_list.txt`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir tmp; scp /root/hungryhippos/tmp/master_ip_file root@$node:hungryhippos/tmp/"
done