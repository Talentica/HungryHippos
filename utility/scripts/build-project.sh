#!/bin/bash
echo '#####Building project#####'
cd ../../
gradle clean jar
echo '#####Project Build completed'
echo '####Copying file to installation directory'
sh copying-file-to-installation-directory.sh
echo '####Completed####'
