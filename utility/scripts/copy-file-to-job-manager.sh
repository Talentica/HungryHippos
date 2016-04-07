#!/bin/bash

job_manager_ip=`/root/hungryhippos/tmp/master_ip_file`


for node in `echo $job_manager_ip`
do
   echo "Copying file to $node"
   scp $1 root@$node:hungryhippos/job-manager
done
