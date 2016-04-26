#!/bin/bash
file=$1
jobUuid=$2
job_manager_ip=`cat ../$jobUuid/master_ip_file`


for node in `echo $job_manager_ip`
do
   echo "Copying file to $node"
   scp $file root@$node:hungryhippos/job-manager
done
