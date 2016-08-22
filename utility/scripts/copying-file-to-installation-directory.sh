#!/lib/bash
echo "####		Creation installation directory		####"
mkdir -p ../../installation ../../installation/conf ../../installation/lib
echo '####   Copying jars to installation directory   ####'
cp ../../data-publisher/build/libs/*.jar ../../installation/lib/
cp ../../job-manager/build/libs/*.jar ../../installation/lib/
cp ../../node/build/libs/*.jar ../../installation/lib/
cp ../../sharding/build/libs/*.jar ../../installation/lib/
cp ../../test-hungry-hippos/build/libs/*.jar ../../installation/lib/
cp ../../coordination/build/libs/*.jar ../../installation/lib/
cp ../../file-system/build/libs/*.jar ../../installation/lib/
cp ../../configuration-schema/build/libs/*.jar ../../installation/lib/
cp ../../configuration-schema/src/main/resources/schema/*.xml ../../installation/conf/
echo '####Completed####'
