echo 'Shutting down zookeeper server'
sh shut-down-zk-server.sh
echo '################	        Sharding setup started      ################'
sh setup-master.sh
echo '################	        Sharding setup completed      ################'
echo '################	        Nodes setup started      ################'
sh setup-nodes.sh
echo '################	        Nodes setup completed      ################'
echo 'Staring zookeeper server'
sh start-zk-server.sh
echo '################	        Environment is ready      ################'
