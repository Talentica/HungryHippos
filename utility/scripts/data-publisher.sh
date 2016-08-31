#!/bin/bash
if [[ ( -z "$1") || ( -z "$2" ) || (-z "$3") ]];
then
	echo "provide client-Configuration.xml file path and input file path and distributed input path "
	exit
fi
java com.talentica.hungryHippos.master.DataPublisherStarter $1 $2 $3 > ../logs/datapub.out 2>../logs/datapub.err &
