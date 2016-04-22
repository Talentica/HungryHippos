#!/bin/bash
cat ../tmp/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list_for_keygen.txt
cat ../tmp/output_ip_file >> node_ips_list_for_keygen.txt
cat ../tmp/master_ip-file >> node_ips_list_for_keygen.txt
for node in `cat node_ips_list_for_keygen.txt`
do
   echo "Removing ssh keygen for $node"
   ssh-keygen -f "/root/.ssh/known_hosts" -R $node
done
