#!/bin/bash



if [[ ( -z "$1") ]];
then
	echo "provide client-Configuration.xml file path "
	exit
fi
java com.talentica.hungryHippos.node.JobExecutorProcessBuilder $1 > $2/job.out 2>$2/job.err &
