#!/bin/bash

node_num=$1
node_to_connect_ip=`grep $node_num ./../../utility/src/main/resources/serverConfigFile.properties| awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
sshpass -p $node_pwd ssh root@$node_to_connect_ip