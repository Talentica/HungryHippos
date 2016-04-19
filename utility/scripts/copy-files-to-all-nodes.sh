#!/bin/bash
echo 'Copying master ip file on all nodes'
cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
cat ../tmp/output_ip_file >> node_ips_list.txt
sh remove-ssh-keygen.sh
for node in `cat node_ips_list.txt`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir tmp"
   scp ../tmp/master_ip_file root@$node:hungryhippos/tmp/
   scp ../tmp/serverConfigFile.properties root@$node:hungryhippos/tmp/
done

cat ../tmp/master_ip_file > temp_master_ip
for node in `cat temp_master_ip`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir tmp"
   scp ../tmp/master_ip_file root@$node:hungryhippos/tmp/
   scp ../tmp/output_ip_file root@$node:hungryhippos/tmp/
   scp ../tmp/serverConfigFile.properties root@$node:hungryhippos/tmp/
done
