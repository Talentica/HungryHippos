#!/bin/bash


. hh-env.sh

HUNGRYHIPPOS_HOME=""
HUNGRYHIPPOS_CONFIG_DIR=""
HUNGRYHIPPOS_LOG_DIR=""

set_env

export CLASSPATH="$HUNGRYHIPPOS_HOME/lib/*"


java com.talentica.hungryHippos.coordination.CoordinationStarter $HUNGRYHIPPOS_CONFIG_DIR/client-config.xml $HUNGRYHIPPOS_CONFIG_DIR/cluster-config.xml $HUNGRYHIPPOS_CONFIG_DIR/datapublisher-config.xml $HUNGRYHIPPOS_CONFIG_DIR/filesystem-config.xml | tee  $HUNGRYHIPPOS_LOG_DIR/coordination.out 2>$HUNGRYHIPPOS_LOG_DIR/coordination.err 
