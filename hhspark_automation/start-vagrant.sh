#!/bin/bash



start_vagrantfile()
{

        #Start vagrant file

        if [ "$PROVIDER" == "digital_ocean"  ]
        then
                vagrant up --provider=digital_ocean
        elif [ "$PROVIDER" == "virtual_box"  ]
        then
                vagrant up
        fi

        #NODENUM=$no_of_nodes ZOOKEEPERNUM=$no_of_zookeeper  vagrant up

        sleep 10
}


