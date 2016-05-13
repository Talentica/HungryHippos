#!/bin/bash
jobUuid=$1
mysqlIp=$2
hours=$3

job_manager_ip=`cat ../$jobUuid/master_ip_file`
for node in `echo $job_manager_ip` 
do
   echo "Starting scheduler on $node"
   ssh -o StrictHostKeyChecking=no root@$node "sh /root/hungryhippos/scripts/bash_scripts/cleaning-long-waiting-jobs.sh $jobUuid $mysqlIp $hours"
done
