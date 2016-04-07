#!/bin/bash
echo 'Copying master ip file on all nodes'
cat /root/hungryhippos/tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt

for node in `cat node_ips_list.txt`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir tmp"
   scp /root/hungryhippos/tmp/master_ip_file root@$node:hungryhippos/tmp/
done

cat /root/hungryhippos/tmp//master_ip_file > temp_master_ip
for node in `cat temp_master_ip`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir tmp"
   scp /root/hungryhippos/tmp/master_ip_file root@$node:hungryhippos/tmp/
done
