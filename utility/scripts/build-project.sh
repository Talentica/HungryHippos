#!/bin/bash
echo '#####Building project#####'
cd ../../
gradle buildDistribution build -x test
echo '#####Project Build completed'
echo '####Copying file to installation directory'
cd ./utility/scripts/
sh copying-file-to-installation-directory.sh
echo '####Completed####'
