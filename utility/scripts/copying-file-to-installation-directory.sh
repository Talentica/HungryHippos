#!/lib/bash
echo "####		Creation installation directory		####"
rm -r ../../installation
mkdir -p ../../installation ../../installation/conf ../../installation/lib
echo '####   Copying jars to installation directory   ####'
cp ../../client-api/build/libs/*.jar ../../installation/lib/
cp ../../common/build/libs/*.jar ../../installation/lib/
cp ../../configuration-schema/build/libs/*.jar ../../installation/lib/
cp ../../coordination/build/libs/*.jar ../../installation/lib/
cp ../../data-publisher/build/libs/*.jar ../../installation/lib/
cp ../../data-synchronizer/build/libs/*.jar ../../installation/lib/
cp ../../file-system/build/libs/*.jar ../../installation/lib/
cp ../../hungryhippos_tester-services-client/build/libs/*.jar ../../installation/lib/
cp ../../hungryhippos-tester-web/build/libs/*.jar ../../installation/lib/
cp ../../job-manager/build/libs/*.jar ../../installation/lib/
cp ../../node/build/libs/*.jar ../../installation/lib/
cp ../../sharding/build/libs/*.jar ../../installation/lib/
cp ../../spark/build/libs/*.jar ../../installation/lib/
cp ../../storage/build/libs/*.jar ../../installation/lib/
cp ../../test-hungry-hippos/build/libs/*.jar ../../installation/lib/
cp ../../utility/build/libs/*.jar ../../installation/lib/
cp ../../configuration-schema/src/main/resources/distribution/*.xml ../../installation/conf
echo '####Completed####'
