echo 'Copying configuration file to all nodes'
sh copy-file-to-all-nodes.sh ../../utility/src/main/resources/config.properties
echo 'Copying configuration file to sharding node'
sh copy-file-to-sharding.sh ../../utility/src/main/resources/config.properties
echo 'Copying configuration file to job manager node'
sh copy-file-to-job-manager.sh ../../utility/src/main/resources/config.properties
echo 'Copying configuration file to data publisher node'
sh copy-file-to-data-publisher.sh ../../utility/src/main/resources/config.properties