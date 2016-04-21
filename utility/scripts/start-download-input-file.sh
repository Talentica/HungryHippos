#!/bin/bash
# First argument is {jobuuid},second argument {url link}
job_manager_ip=`cat ../tmp/master_ip_file`

for node in `echo $job_manager_ip`
do
   echo "Starting download input file on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd /root/hungryhippos/scripts/bash_scripts;sh download-file-from-url.sh $1 $2 $3;"
done
