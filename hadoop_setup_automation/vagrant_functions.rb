class Vagrant_functions

	def self.configuration(config,no_of_nodes,ip_file)
		 (1..no_of_nodes).each do |i|
		
               		 config.vm.define "hadoop-#{i}" do |node|
                        	node_name="hadoop-#{i}"
	                        node.vm.box = "digital_ocean"
        	                curr_dir=File.dirname(__FILE__)
                	        #puts curr_dir

                       		node.vm.provision :chef_solo do |chef|
                                # Paths to your cookbooks (on the host)
                                	chef.cookbooks_path = ["#{curr_dir}/chef/src/cookbooks"]
                                # Add chef recipes
                               		chef.add_recipe 'hadoop_installation'
                                	chef.add_recipe 'hadoop_ssh_keygen_trans_mastertoslaves'
                                	if i == 1
                                        	chef.add_recipe 'hadoop_master_conf_setup'
	                                else
        	                                chef.add_recipe 'hadoop_slave_conf_setup'
                	                end
                        	        chef.add_recipe 'hadoop_ssh_keygen_master'
                                	chef.add_recipe 'hadoop_ssh_keycopy_slave'
                       		end


                     		node.trigger.after :up do
                                	puts "running trigger after node up"
	                                ipAddr = `vagrant ssh #{node_name} -c 'ifconfig | grep -oP "inet addr:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | grep -oP "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | tail -n 2 | head -n 1'`
        	                        sleep 5
                	                ipAddr=ipAddr.strip
                        	        if i == 1
                                	        ip_file.puts "#{ipAddr}\tHadoopMaster"
	                                else
        	                                 ip_file.puts "#{ipAddr}\tHadoopSlave#{(i-1)}"
                	                end
                        	end

	               	 end
        	end

	end


	def  self.spawner(config)
		config.ssh.private_key_path = "~/.ssh/id_rsa"
	        config.vm.provider :digital_ocean do |provider|
                	#  provider.client_id = "YOUR CLIENT ID"
	                #  provider.api_key = "639d421b041fed3566bc3da896d31347befdad49f8c9add911eee5a554865294"
        	        provider.token = "639d421b041fed3566bc3da896d31347befdad49f8c9add911eee5a554865294"
                	provider.image = "ubuntu-14-04-x64"
	                provider.region = "nyc2"
	                provider.size = "2GB"
        	end
	end

end


