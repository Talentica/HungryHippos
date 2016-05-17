#!/lib/bash
echo '####Copying jars to installation directory####'
mkdir -p ../../installation/lib ../../installation/conf/ ../../installation/bin/ ../../installation/json/

cp ../../digital-ocean-manager/build/libs/*.jar ../../installation/lib/
cp ../../data-publisher/build/libs/*.jar ../../installation/lib/
cp ../../job-manager/build/libs/*.jar ../../installation/lib/
cp ../../node/build/libs/*.jar ../../installation/lib/
cp ../../sharding/build/libs/*.jar ../../installation/lib/
cp ../../test-hungry-hippos/build/libs/*.jar ../../installation/lib/
cp ../../coordination/build/classes/main/config.properties ../../installation/conf/
cp ../../digital-ocean-manager/build/classes/main/json/* ../../installation/json/
cp ../../utility/scripts/*.sh ../../installation/bin/
echo "192.241.248.197" > ../../installation/conf/ngnixip
chmod 777 ../../installation/conf/ngnixip
echo '####Completed####'
