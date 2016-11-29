#!/bin/bash



if [[ ( -z "$1") || ( -z "$2" ) ]];
then
	echo "provide zookeeper client ip and ip address of the current node"
	exit
fi
java com.talentica.torrent.DataSynchronizerStarter $1 $2 >$3/data-sync.out 2>$3/data-sync.err &
