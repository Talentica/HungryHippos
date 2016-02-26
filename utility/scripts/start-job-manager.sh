#!/bin/bash

job_manager_ip=`cat ./node_pwd_file.txt|grep "job_manager_ip"|awk -F":" '{print $2}'`

sh start-zk-server.sh
for node in `echo $job_manager_ip` 
do
   echo "Starting job manager on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/job-manager;java -XX:HeapDumpPath=./ -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp job-manager.jar:test-jobs.jar com.talentica.hungryHippos.job.main.JobManagerStarter $1 > ./system.out 2>./system.err &"
done
