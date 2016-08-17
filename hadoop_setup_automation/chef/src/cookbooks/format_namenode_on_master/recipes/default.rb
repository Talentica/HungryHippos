#
# Cookbook Name:: format_namenode_on_master
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

directory "/tmp/hadoop-hduser" do
  action :delete
  recursive true
end

bash 'reformat namenode' do
  user 'hduser'
  group 'hadoop'
  cwd '/usr/local/hadoop/'
  code <<-EOH
  bin/hdfs namenode -format
  EOH
end
