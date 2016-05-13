#!/bin/bash
# first argument is job matrix, second argument is jobuuid.
jobMatrix=$1
jobUuid=$2
job_manager_ip=`cat ../$jobUuid/master_ip_file`

sh start-zk-server.sh $jobUuid
for node in `echo $job_manager_ip` 
do
   echo "Starting job manager on $node"
   ssh -o StrictHostKeyChecking=no root@$node "cd hungryhippos/job-manager;java -XX:HeapDumpPath=./ -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./ -cp job-manager.jar:test-jobs.jar com.talentica.hungryHippos.job.main.JobManagerStarter $jobMatrix $jobUuid > ./system_job_matrix.out 2>./system_job_matrix.err &"
done

sh start-all-nodes-job-execution.sh $jobUuid
