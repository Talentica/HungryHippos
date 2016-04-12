#!/bin/bash
cat /root/hungryhippos/tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

for node in `cat node_ips_list.txt`
do
   echo "Removing ssh keygen for $node"
   ssh-keygen -f "/root/.ssh/known_hosts" -R $node
done
