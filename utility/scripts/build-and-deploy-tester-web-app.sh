#!/bin/bash

echo "Building tester web application.."
cd ../../hungryhippos-tester-web;
gradle clean build;
echo "Copying tester web application build on $1.."
scp build/libs/hungryhippos-tester-web.jar root@$1:hungryhippos/tester-web
echo "Done"
