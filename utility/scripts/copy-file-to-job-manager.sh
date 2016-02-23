#!/bin/bash

job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`


for node in `echo $job_manager_ip`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/job-manager
done
