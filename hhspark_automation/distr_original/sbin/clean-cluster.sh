#!/bin/bash

source ../bin/hh-env.sh
source utility.sh
source run-hh-scripts-on-cluster.sh

HUNGRYHIPPOS_HOME=""
HUNGRYHIPPOS_CONFIG_DIR=""
HUNGRYHIPPOS_LOG_DIR=""

set_env

base_path=($(grep -oP '(?<=root-directory>)[^<]+' "../config/filesystem-config.xml"))
format_hh(){
	ip=$1
	echo "Format HungryHippos on $ip"
	ssh hhuser@$ip "rm -rf $base_path"
}



ips=($(awk '{print $1}' ../config/hhnodes))
for ip in "${ips[@]}"
do
	format_hh $ip &

done

run_script_on_random_node ${ips[0]}
echo "Waiting for Clean up completion"
wait
echo "HH Clean up completed"




