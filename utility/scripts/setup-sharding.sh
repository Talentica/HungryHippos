sh shut-down-sharding.sh
sh cleanup-sharding.sh
echo 'Copying new build'
sh copy-file-to-sharding.sh ../../sharding/build/libs/sharding.jar
echo 'Copying common configuration file'
sh copy-file-to-sharding.sh ../../utility/src/main/resources/config.properties
echo 'Copying nodes configuration file'
sh copy-file-to-sharding.sh ../../utility/src/main/resources/serverConfigFile.properties
echo 'Copying nodes'' password file'
sh copy-file-to-sharding.sh ./node_pwd_file.txt
echo 'Copying shard file copy utility'
sh copy-file-to-sharding.sh ./copy-shard-files-to-all-nodes.sh



