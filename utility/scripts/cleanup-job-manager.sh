#!/bin/bash
echo 'Cleaning up job manager'
job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`
node_pwd=`cat ./node_pwd_file.txt|grep "pwd"|awk -F":" '{print $2}'`

for node in `echo $job_manager_ip`
do
   echo "Cleaning job manager on $node"
   sshpass -p $node_pwd ssh root@$node "cd hungryhippos/job-manager;rm job-manager.log*;rm Application.log*;"
done
