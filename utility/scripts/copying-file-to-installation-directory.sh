#!/lib/bash
echo '####Copying jars to installation directory####'
cp ../../digital-ocean-manager/build/libs/*.jar ../../installation/lib/
cp ../../data-publisher/build/libs/*.jar ../../installation/lib/
cp ../../job-manager/build/libs/*.jar ../../installation/lib/
cp ../../node/build/libs/*.jar ../../installation/lib/
cp ../../sharding/build/libs/*.jar ../../installation/lib/
cp ../../test-hungry-hippos/build/libs/*.jar ../../installation/lib/
cp ../../coordination/build/classes/main/config.properties ../../installation/conf/
cp ../../digital-ocean-manager/build/classes/main/json/* ../../installation/json/
cp ../../utility/scripts/*.sh ../../installation/bin/
echo '####Completed####'
