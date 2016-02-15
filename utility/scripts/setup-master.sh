sh shut-down-manager.sh
sh cleanup-manager.sh
sh copy-master-jar.sh
echo 'Copying test jobs file on master'
sh copy-file-to-manager.sh ../../test-hungry-hippos/build/libs/test-jobs.jar
