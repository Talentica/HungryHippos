class Vagrant_functions 

 def  self.configuration(config,no_of_nodes,no_of_zookeeper,ip_file,provider,name)
                (1..no_of_nodes).each do |i|

                        config.vm.define name+"-#{i}" do |node|
                                node_name=name+"-#{i}"
				
			       if provider == "digital_ocean"
                                node.vm.box = "digital_ocean"                               
                               elsif provider == "virtual_box"
				node.vm.box = "ubuntu/trusty64"
			       end
				
				curr_dir=File.dirname(__FILE__)
                                

                                node.vm.provision :chef_solo do |chef|
                                # Paths to your cookbooks (on the host)
					chef.channel = "stable"
					chef.version = "12.17.44"
                                       chef.cookbooks_path = ["#{curr_dir}/chef/src/cookbooks"]
                                       chef.add_recipe 'create_group_user'
                                      #creates directory and changes ownership
                                       chef.add_recipe 'hadoop_ssh_keygen_trans_mastertoslaves'
                                       #copies hhuser id_rsa.pub
				       chef.add_recipe 'hadoop_ssh_keygen_master'
                                       #copies authorized keys.txt
                                       chef.add_recipe 'hadoop_ssh_keycopy_slave'
					chef.add_recipe 'HH_java'
					if i <=no_of_zookeeper
                                              

                                                chef.add_recipe 'download_zookeeper'
                                                chef.add_recipe 'cluster_mode_zookeeper'
                                        end

						chef.add_recipe 'download_spark'
                                     

                                end


                       node.trigger.after :up do
                                puts "running trigger after node up"
                                ipAddr = `vagrant ssh #{node_name} -c 'ifconfig | grep -oP "inet addr:\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | grep -oP "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}" | tail -n 2 | head -n 1'`
                                sleep 5
                                ipAddr=ipAddr.strip
                                if i <=no_of_zookeeper
                                        ip_file.puts "#{ipAddr}:#{name}-#{i} (Zookeeper)"
                                else
                                         ip_file.puts "#{ipAddr}:#{name}-#{i}"
                                end
                                end

                        end
                end

        end



  def  self.spawner(config,private_key_path,token,image,region,ram,ssh_key_name)
                config.ssh.private_key_path = private_key_path
                config.vm.provider :digital_ocean do |provider|
                        #  provider.client_id = "YOUR CLIENT ID"
                        #  provider.api_key = "639d421b041fed3566bc3da896d31347befdad49f8c9add911eee5a554865294"
                provider.token = token
                provider.image = image
                provider.region = region
                provider.size = ram
		provider.ssh_key_name= ssh_key_name
                end
  end 


end
