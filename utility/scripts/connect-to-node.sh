#!/bin/bash

node_num=$1
node_to_connect_ip=`grep $node_num ./../../utility/src/main/resources/serverConfigFile.properties| awk -F":" '{print $2}'`

ssh -o StrictHostKeyChecking=no root@$node_to_connect_ip
