class Vagrant_functions 

 def  self.configuration(config,no_of_nodes,no_of_zookeeper,ip_file,provider)
                (1..no_of_nodes).each do |i|

                        config.vm.define "sparkOnHH-#{i}" do |node|
                                node_name="sparkOnHH-#{i}"
				
			       if provider == "digital_ocean"
                                node.vm.box = "digital_ocean"                               
                               elsif provider == "virtual_box"
				node.vm.box = "ubuntu/trusty64"
			       end
				
				curr_dir=File.dirname(__FILE__)
                                

                                node.vm.provision :chef_solo do |chef|
                                # Paths to your cookbooks (on the host)
                                        chef.cookbooks_path = ["#{curr_dir}/chef/src/cookbooks"]
                                        chef.add_recipe 'create_group_user'
                                        chef.add_recipe 'hadoop_ssh_keygen_trans_mastertoslaves'
                                        chef.add_recipe 'hungryhippos_ssh_keygen'
                                        chef.add_recipe 'hungryhippos_ssh_keycopy'
                                        chef.add_recipe 'HH_java'
                                        
										if i <=no_of_zookeeper
                                              

                                                chef.add_recipe 'download_zookeeper'
                                                chef.add_recipe 'cluster_mode_zookeeper'
                   						end

										chef.add_recipe 'hungryhippos_download_spark'
                                     

                   			  end


                       node.trigger.after :up do
                                puts "running trigger after node up"
                                ipAddr = `vagrant ssh #{node_name} -c 'ifconfig | grep -oP "inet addr:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | grep -oP "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | tail -n 2 | head -n 1'`
                                sleep 5
                                ipAddr=ipAddr.strip
                                if i <=no_of_zookeeper
                                        ip_file.puts "#{ipAddr}\thh-#{i} (Zookeeper)"
                                else
                                         ip_file.puts "#{ipAddr}\thh-#{i}"
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
                provider.size = "4GB"
                end
  end 

end
