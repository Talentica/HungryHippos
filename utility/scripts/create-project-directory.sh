#!/bin/bash
echo "Creating required project directories"
cat /root/hungryhippos/tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

manager_node_ip=`cat /root/hungryhippos/tmp/master_ip_file`
for node in `cat node_ips_list.txt`
do
   echo "Creating required project directories on node $node"
   ssh -o StrictHostKeyChecking=no root@$node "mkdir hungryhippos"
   ssh -o StrictHostKeyChecking=no root@$node "mkdir hungryhippos/data"
done
