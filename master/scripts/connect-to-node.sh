#!/bin/bash

node_num=$1
node_ip=`grep server.$n ./../../utility/src/main/resources/serverConfigFile.properties| awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
sshpass -p $node_pwd ssh root@$node_ip
