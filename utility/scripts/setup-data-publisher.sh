#!/bin/bash
jobuuid=$1
sh shut-down-data-publisher.sh $jobuuid
sh cleanup-data-publisher.sh $jobuuid
echo 'Copying new build'
sh copy-file-to-data-publisher.sh ../lib/data-publisher.jar
