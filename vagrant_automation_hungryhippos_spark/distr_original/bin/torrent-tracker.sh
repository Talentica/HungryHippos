#!/bin/bash


. hh-env.sh

HUNGRYHIPPOS_HOME=""
HUNGRYHIPPOS_CONFIG_DIR=""
HUNGRYHIPPOS_LOG_DIR=""

set_env

export CLASSPATH="$HUNGRYHIPPOS_HOME/lib/*"


if [[ ( -z "$1") || ( -z "$2" )  ]];
then
	echo "provide zookeeper ip (i.e:- 127.0.0.1:2181) and ip of node where you want to start it on cluster "
	exit
fi
java com.talentica.torrent.TorrentTrackerStarter $1 $2 6969 >$HUNGRYHIPPOS_LOG_DIR/data-trac.out 2>$HUNGRYHIPPOS_LOG_DIR/data-trac.err &
