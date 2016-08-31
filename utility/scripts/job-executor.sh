#!/bin/bash
if [[ ( -z "$1") ]];
then
	echo "provide client-Configuration.xml file path "
	exit
fi
java com.talentica.hungryHippos.node.JobExecutorProcessBuilder $1 > ../logs/job.out 2>../logs/job.err &
