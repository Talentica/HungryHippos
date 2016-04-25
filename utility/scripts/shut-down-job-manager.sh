#!/bin/bash
jobUuid=$1
echo 'Shutting down all java processes on job-manager nodes'
job_manager_ip=`cat ../$jobUuid/master_ip_file`

for node in `echo $job_manager_ip`
do
   echo "Stopping job-manager on $node"
   ssh -o StrictHostKeyChecking=no root@$node "killall java"
done
