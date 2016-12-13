#
# Cookbook Name:: hadoop_slave_conf_setup
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
directory '/usr/local/hadoop_tmp/' do
  owner 'hduser'
 group 'hadoop'
 mode '0777'
action :create
end

directory '/usr/local/hadoop_tmp/hdfs' do
 owner 'hduser'
 group 'hadoop'
 mode '0777'
action :create
end


directory '/usr/local/hadoop_tmp/hdfs/datanode' do
  owner 'hduser'
  group 'hadoop'
  mode '0777'
  action :create
end
