#
# Cookbook Name:: hadoop_ssh_keycopy_slave
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
template '/home/hduser/.ssh/authorized_keys' do
  source 'authorized_keys.txt'
  mode '0600'
  owner 'hduser'
  group 'hadoop'
  end
