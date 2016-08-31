#!/bin/bash
if [[ ( -z "$1") || ( -z "$2" ) || (-z "$3") || || (-z "$4")  || (-z "$5:wq")  ]];
then
	echo "provide client-Configuration.xml file path, jar location,implementation Class,input dir and output dir "
	exit
fi
java  com.talentica.hungryHippos.job.main.JobOrchestrator $1 $2 $3 $4 $5 > ../logs/job-man.out 2>../logs/job-man.err &
