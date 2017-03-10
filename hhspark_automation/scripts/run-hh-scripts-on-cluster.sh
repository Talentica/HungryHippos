#!/bin/bash


run_script_on_random_node(){
        zookeeperip_string=$1
        ip=$2
        pathof_config_folder="/home/hhuser/distr/config"
        ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./coordination.sh $pathof_config_folder"

}


run_all_scripts()
{
        zookeeperip_string=$1
        ip=$2
        ssh hhuser@$ip "cd /home/hhuser/distr/bin && ./start-all.sh $zookeeperip_string $ip"

}

