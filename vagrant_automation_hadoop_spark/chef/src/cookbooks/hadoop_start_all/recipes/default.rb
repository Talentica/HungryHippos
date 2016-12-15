#
# Cookbook Name:: hadoop_start_all
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

bash 'copying pub key to autorised key in master' do
  user 'hduser'
  group 'hadoop'
  code <<-EOH
  cat /home/hduser/.ssh/id_rsa.pub >> /home/hduser/.ssh/authorized_keys
  EOH
end


bash 'start_dfs.sh' do
  user 'hduser'
  group 'hadoop'
  cwd '/usr/local/hadoop/'
  code <<-EOH
  sbin/start-dfs.sh
  EOH
end

bash 'start_yarn.sh' do
  user 'hduser'
  group 'hadoop'
  cwd '/usr/local/hadoop/'
  code <<-EOH
  sbin/start-yarn.sh
  EOH
end


