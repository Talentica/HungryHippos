echo 'Building all projects'
sh prepare-new-build.sh

ssh-keygen -R 127.0.0.1

echo '################          Create project directory      ################'
sh create_proj_directory.sh
echo '################          Project directory is created and files are copied     ################'

echo '################          Create droplets      ################'
sh create_droplets.sh
echo '################          Droplet creation is initiated      ################'

echo '################          Sharding setup started      ################'
sh setup-sharding.sh
echo '################          Sharding setup completed      ################'

echo '################          Data publisher setup started      ################'
sh setup-data-publisher.sh
echo '################          Data publisher setup completed      ################'

echo '################          Job manager setup started      ################'
sh setup-job-manager.sh
echo '################          Job manager setup completed      ################'

echo '################          Nodes setup started      ################'
sh setup-nodes.sh
echo '################          Nodes setup completed      ################'
