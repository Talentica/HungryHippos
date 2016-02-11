#!/bin/bash

cat ~/hungryhippos/manager/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
   echo "Copying file to $node"
   sshpass -p $node_pwd scp ./keyToValueToBucketMap root@$node:hungryhippos
   sshpass -p $node_pwd scp ./bucketToNodeNumberMap root@$node:hungryhippos
done
