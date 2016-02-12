#!/bin/bash
echo 'Cleaning up data publisher'
data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $data_publisher_node_ip`
do
   echo "Cleaning data publisher on $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/data-publisher;rm datapublisher.log*;rm Application.log*;"
done
