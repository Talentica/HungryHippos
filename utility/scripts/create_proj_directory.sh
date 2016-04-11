#!/bin/bash
cd /root/hungryhippos/
echo "Creating tmp directory"
rm -r tmp
mkdir tmp
echo "temp directory created"
echo "Copying files"
cp /root/hungryhippos/checkout/HungryHippos/utility/scripts/*zk-server* /root/hungryhippos/tmp/
cp /root/hungryhippos/checkout/HungryHippos/digital-ocean-manager/build/libs/digital-ocean.jar /root/hungryhippos/digital-ocean-manager/
cp /root/hungryhippos/checkout/HungryHippos/digital-ocean-manager/src/main/resources/json/* /root/hungryhippos/tmp/
cp /root/hungryhippos/checkout/HungryHippos/coordination/src/main/resources/config.properties /root/hungryhippos/tmp/
chmod -R 777 /root/hungryhippos/tmp/
chmod -R 777 /root/hungryhippos/digital-ocean-manager/
echo "Files are copied."
