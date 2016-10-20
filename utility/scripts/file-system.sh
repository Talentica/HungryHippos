#!/bin/bash

clientConfig=$1;
hh_command=$2;
fname=$3;
java  -cp file-system.jar com.talentica.hungryhippos.filesystem.main.HungryHipposFileSystemMain $clientConfig $hh_command $fname

