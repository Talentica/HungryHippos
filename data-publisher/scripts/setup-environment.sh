echo 'Shutting down zookeeper server'
sh shut-down-zk-server.sh
echo '################	        Master setup started      ################'
sh setup-master.sh
echo '################	        Master setup completed      ################'
echo '################	        Nodes setup started      ################'
sh setup-nodes.sh
echo '################	        Nodes setup completed      ################'
echo 'Staring zookeeper server'
sh start-zk-server.sh
echo '################	        Environment is ready      ################'
