echo 'Copying configuration file in environment'
sh copy-file-to-all-nodes.sh ../../utility/src/main/resources/config.properties
sh copy-file-to-sharding.sh ../../utility/src/main/resources/config.properties
sh copy-file-to-job-manager.sh ../../utility/src/main/resources/config.properties
sh copy-file-to-data-publisher.sh ../../utility/src/main/resources/config.properties
