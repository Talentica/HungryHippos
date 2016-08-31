#!/bin/bash
if [[ ( -z "$1") ]];
then
	echo "provide folder where all configuration files are present"
	exit
fi
java com.talentica.hungryHippos.coordination.CoordinationStarter $1/client-config.xml $1/coordination-config.xml $1/cluster-config.xml $1/datapublisher-config.xml $1/filesystem-config.xml $1/job-runner-config.xml > ../logs/coordination.out 2>../logs/coordination.err &
