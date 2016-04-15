#!/bin/bash

job_manager_ip=`cat ../tmp/master_ip_file`

sh start-zk-server.sh
for node in `echo $job_manager_ip` 
do
   echo "Starting job manager on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/job-manager;java -XX:HeapDumpPath=./ -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp job-manager.jar:test-jobs.jar com.talentica.hungryHippos.job.main.JobManagerStarter $1 > ./system.out 2>./system.err &"
done

sh start-all-nodes-job-execution.sh
