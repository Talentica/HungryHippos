#!/bin/bash

node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`
sshpass -p $node_pwd ssh root@$1
