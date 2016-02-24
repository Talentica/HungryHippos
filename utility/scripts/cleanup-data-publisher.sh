#!/bin/bash
echo 'Cleaning up data publisher'
data_publisher_node_ip=`cat ./node_pwd_file.txt|grep "data_publisher_node_ip"|awk -F":" '{print $2}'`


for node in `echo $data_publisher_node_ip`
do
   echo "Cleaning data publisher on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/data-publisher;rm datapublisher.log*;rm Application.log*;"
done
