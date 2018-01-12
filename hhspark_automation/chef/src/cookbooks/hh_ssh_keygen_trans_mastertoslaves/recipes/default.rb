#
# Cookbook Name:: hadoop_ssh_keygen_trans_mastertoslaves
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#


execute 'create home directory if not exist' do
  command 'mkdir -p /home/hhuser/'
  user 'root'
  group 'root'
end


execute 'change owner of home' do
  command 'chown hhuser:hungryhippos -R /home/hhuser'
  user 'root'
  group 'root'
end



directory '/home/hhuser/.ssh' do
  owner 'hhuser'
  group 'hungryhippos'
  mode '0700'
  action :create
end


execute 'ssh-keygen' do
  command 'rm -rf /home/hhuser/.ssh/id_rsa && ssh-keygen -t rsa -N ""  -f /home/hhuser/.ssh/id_rsa'
  user 'hhuser'
  group 'hungryhippos'
end


template '/etc/ssh/sshd_config' do
  source 'sshd_config'
  mode '0644'
  owner 'root'
  group 'root'
  end
