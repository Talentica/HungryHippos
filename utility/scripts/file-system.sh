#!/bin/bash
if [[ ( -z "$1") || ( -z "$2" ) ]];
then
	echo "provide cluster-Configuration.xml and client-Configuration.xml file path"
	exit
fi
java com.talentica.hungryhippos.filesystem.main.CreateFileSystemInCluster $1 $2

