#!/lib/bash
echo '####Copying jars to installation directory####'
cp ../../digital-ocean-manager/build/libs/*.jar /root/hungryhippos/intallation/HungryHippos/lib/
cp ../../data-publisher/build/libs/*.jar /root/hungryhippos/intallation/HungryHippos/lib/
cp ../../job-manager/build/libs/*.jar /root/hungryhippos/intallation/HungryHippos/lib/
cp ../../node/build/libs/*.jar /root/hungryhippos/intallation/HungryHippos/lib/
cp ../../sharding/build/libs/*.jar /root/hungryhippos/intallation/HungryHippos/lib/
cp ../../test-hungry-hippos/build/libs/*.jar /root/hungryhippos/intallation/HungryHippos/lib/
echo '####Completed####'