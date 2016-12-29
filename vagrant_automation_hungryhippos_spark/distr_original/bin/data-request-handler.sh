#!/bin/bash



if [[ ( -z "$1")]];
then
	echo "provide client-Configuration.xml file path"
	exit
fi
java com.talentica.hungryhippos.filesystem.server.DataRequestHandlerServer $1 > $2/datarequesthandler.out 2>$2/datarequesthandler.err &
