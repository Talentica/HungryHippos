#!/bin/bash

source ../bin/hh-env.sh
source utility.sh
source run-hh-scripts-on-cluster.sh


HUNGRYHIPPOS_HOME=""
HUNGRYHIPPOS_CONFIG_DIR=""
HUNGRYHIPPOS_LOG_DIR=""

set_env



ips=($(awk '{print $1}' ../config/hhnodes))
for ip in "${ips[@]}"
do
	run_all_scripts $ip &

done

echo "Waiting for Start completion"
wait
echo "HH start completed"




