class Vagrant_functions

	def self.configuration(config,no_of_nodes,ip_file,provider)
		 (1..no_of_nodes).each do |i|
		
                        if i == 1
                                nodename = "HadoopMaster"
                        else
                                nodename = "HadoopSlave#{(i-1)}"
                        end
                         config.vm.define "#{nodename}" do |node|
                                node_name="#{nodename}"

				 if provider == "digital_ocean"
                                  node.vm.box = "digital_ocean"                               
                                 elsif provider == "virtual_box"
				  node.vm.box = "ubuntu/trusty64"
			         end


	                        
        	                curr_dir=File.dirname(__FILE__)
                	        #puts curr_dir

                       		node.vm.provision :chef_solo do |chef|
                            chef.channel = "stable"
                            chef.version = "12.17.44"

                                # Paths to your cookbooks (on the host)
                                	chef.cookbooks_path = ["#{curr_dir}/chef/src/cookbooks"]
                                # Add chef recipes
                               		chef.add_recipe 'hadoop_installation'
					chef.add_recipe 'download_spark'
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
                	                ip_file.puts "#{ipAddr}\t#{node_name}"
                        	end

	               	 end
        	end

	end


	def  self.spawner(config,private_key_path,token,image,region,ram,ssh_key_name)
		config.ssh.private_key_path = private_key_path
		    config.vm.provider :digital_ocean do |provider|
                        provider.ssh_key_name = ssh_key_name
                        provider.token = token
                        provider.image = image
                        provider.region = region
                        provider.size = ram
        	end
	    end
    end


