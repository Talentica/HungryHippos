#!/bin/bash
sh shut-down-data-publisher.sh
sh cleanup-data-publisher.sh
echo 'Copying new build'
sh copy-file-to-data-publisher.sh ../../data-publisher/build/libs/data-publisher.jar
