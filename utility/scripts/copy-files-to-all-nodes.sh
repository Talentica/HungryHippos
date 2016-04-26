#!/bin/bash
jobUuid=$1
echo 'Copying master ip file on all nodes'
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list.txt
cat ../$jobUuid/output_ip_file >> ../$jobUuid/node_ips_list.txt
sh remove-ssh-keygen.sh
for node in `cat ../$jobUuid/node_ips_list.txt`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir $jobUuid"
   scp ../$jobUuid/master_ip_file root@$node:hungryhippos/$jobUuid/
   scp ../$jobUuid/serverConfigFile.properties root@$node:hungryhippos/$jobUuid/
done

cat ../$jobUuid/master_ip_file > ../$jobUuid/temp_master_ip
for node in `cat ../$jobUuid/temp_master_ip`
do
   echo "Copying master_ip_file to HungryHippos for $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos;mkdir $jobUuid"
   scp ../$jobUuid/master_ip_file root@$node:hungryhippos/$jobUuid/
   scp ../$jobUuid/output_ip_file root@$node:hungryhippos/$jobUuid/
   scp ../$jobUuid/serverConfigFile.properties root@$node:hungryhippos/$jobUuid/
done
