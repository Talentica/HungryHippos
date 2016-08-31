#!/bin/bash
if [[ ( -z "$1") || ( -z "$2" )  ]];
then
	echo "provide zookeeper ip (i.e:- 127.0.0.1:2181) and ip of node where you want to start it on cluster "
	exit
fi
java com.talentica.torrent.TorrentTrackerStarter $1 $2 6969 >../logs/data-trac.out 2>../logs/data-trac.err
