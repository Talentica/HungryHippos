#!/bin/bash

node=$1
pattern="server."$node

node_ip=`cat /root/hungryhippos/tmp/serverConfigFile.properties |grep -i $pattern|awk -F":" '{print $2}'`

scp -o StrictHostKeyChecking=no root@$node_ip:/root/hungryhippos/node/outputFile /root/hungryhippos/download-output/outputFile_$node
