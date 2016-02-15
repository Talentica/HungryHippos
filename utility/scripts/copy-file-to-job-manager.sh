#!/bin/bash

job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $job_manager_ip`
do
   echo "Copying file to $node"
   sshpass -p $node_pwd scp $1 root@$node:hungryhippos/job-manager
done
