echo 'Building all projects'
sh prepare-new-build.sh
echo 'Shutting down zookeeper server'
sh shut-down-zk-server.sh

echo '################	        Sharding setup started      ################'
sh setup-sharding.sh
echo '################	        Sharding setup completed      ################'

echo '################	        Data publisher setup started      ################'
sh setup-data-publisher.sh
echo '################	        Data publisher setup completed      ################'

echo '################	        Job manager setup started      ################'
sh setup-job-manager.sh
echo '################	        Job manager setup completed      ################'

echo '################	        Nodes setup started      ################'
sh setup-nodes.sh
echo '################	        Nodes setup completed      ################'

echo 'Staring zookeeper server'
sh start-zk-server.sh
echo '################	        Environment is ready      ################'
