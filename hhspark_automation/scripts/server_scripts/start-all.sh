#!/bin/bash

. hh-env.sh

HUNGRYHIPPOS_HOME=""
HUNGRYHIPPOS_CONFIG_DIR=""
HUNGRYHIPPOS_LOG_DIR=""

set_env

export CLASSPATH="$HUNGRYHIPPOS_HOME/lib/*"


zookeeper_string=$1
pathof_client_config="$HUNGRYHIPPOS_CONFIG_DIR/client-config.xml"
#ownIP=$(ip route get 8.8.8.8| grep src| sed 's/.*src \(.*\)$/\1/g')
ownIP=$2

./data-distributor.sh $pathof_client_config $HUNGRYHIPPOS_LOG_DIR $HUNGRYHIPPOS_HOME
