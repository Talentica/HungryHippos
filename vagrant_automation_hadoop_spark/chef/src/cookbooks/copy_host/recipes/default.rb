# Cookbook Name:: hadoop_conf_files_setupi
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

#Copying master_slave_list to every node and appending /etc/hosts
cookbook_file "/etc/master_slave_name.txt" do
   source "master_slave_name.txt"
 end 

execute "appending master_slave_name.txt file to /etc/hosts" do
  user "root"
  command "cat /etc/master_slave_name.txt >> /etc/hosts"
end


execute "delete temporary copied file" do
  user "root"
  command "rm /etc/master_slave_name.txt"
end
