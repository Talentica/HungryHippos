#!/bin/bash

node=$1
job_uuid=$2

pattern="server."$node
path='/root/hungryhippos/'$job_uuid

node_ip=`cat $path/serverConfigFile.properties |grep -i $pattern|awk -F":" '{print $2}'`

scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/outputFile /root/hungryhippos/download-output/outputFile_$node
