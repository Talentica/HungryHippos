#
# Cookbook Name:: hadoop_ssh_keygen_master
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
template '/home/hhuser/.ssh/id_rsa' do
  source 'id_rsa'
  mode '0600'
  owner 'hhuser'
  group 'hungryhippos'
  end


template '/home/hhuser/.ssh/id_rsa.pub' do
  source 'id_rsa.pub'
  mode '0644'
  owner 'hhuser'
  group 'hungryhippos'
  end

