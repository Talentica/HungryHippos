#!/bin/bash
jobUuid=$1
cat ../$jobUuid/serverConfigFile.properties|awk -F":" '{print $2}' > ../$jobUuid/node_ips_list_for_keygen.txt
cat ../$jobUuid/output_ip_file >> ../$jobUuid/node_ips_list_for_keygen.txt
echo '\n' >> ../$jobUuid/node_ips_list_for_keygen.txt
cat ../$jobUuid/master_ip_file >> ../$jobUuid/node_ips_list_for_keygen.txt
sed '/^$/d' ../$jobUuid/node_ips_list_for_keygen.txt > ../$jobUuid/abc.txt
mv ../$jobUuid/abc.txt ../$jobUuid/node_ips_list_for_keygen.txt
for node in `cat ../$jobUuid/node_ips_list_for_keygen.txt`
do
   echo "Removing ssh keygen for $node"
   ssh-keygen -f "/root/.ssh/known_hosts" -R $node
done
