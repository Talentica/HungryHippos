#
# Cookbook Name:: hadoop_ssh_keygen_trans_mastertoslaves
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

directory '/home/hduser/.ssh' do
  owner 'hduser'
  group 'hadoop'
  mode '0700'
  action :create
end


execute 'ssh-keygen' do
  command 'rm -rf /home/hduser/.ssh/id_rsa && ssh-keygen -t rsa -N ""  -f /home/hduser/.ssh/id_rsa'
  user 'hduser'
  group 'hadoop'
end


template '/etc/ssh/sshd_config' do
  source 'sshd_config'
  mode '0644'
  owner 'root'
  group 'root'
  end
