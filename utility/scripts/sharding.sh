#!/bin/bash
if [[ ( -z "$1") || ( -z "$2" ) ]];
then
	echo "provide client-Configuration.xml file path and dir location"
	exit
fi
java com.talentica.hungryHippos.sharding.main.ShardingStarter $1 $2 > ../logs/sharding.out 2>../logs/sharding.err &
