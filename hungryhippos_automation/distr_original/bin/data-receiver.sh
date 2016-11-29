#!/bin/bash



if [[ ( -z "$1")]];
then
	echo "provide client-Configuration.xml file path"
	exit
fi
java com.talentica.hungryHippos.node.DataReceiver $1 > $2/datareciever.out 2>$2/datareciever.err &
