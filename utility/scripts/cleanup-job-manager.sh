#!/bin/bash
echo 'Cleaning up job manager'
job_manager_ip=`cat /root/hungryhippos/tmp/master_ip_file'`


for node in `echo $job_manager_ip`
do
   echo "Cleaning job manager on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/job-manager;rm job-manager.log*;rm Application.log*;rm system.out;rm system.err"
done
