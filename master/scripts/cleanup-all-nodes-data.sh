#!/bin/bash

cat ./../../utility/src/main/resources/serverConfigFile.properties|awk -F":" '{print $2}' > node_ips_list.txt
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `cat node_ips_list.txt`
do
   echo "Cleaning up HungryHippos node  $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos;rm ./data/data_*;rm Application.log*;rm outputFile;rm bucketToNodeNumberMap;rm keyToValueToBucketMap"
done
