#
# Cookbook Name:: hadoop_stop_all
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
bash 'stop_dfs.sh' do
  user 'hduser'
  group 'hadoop'
  cwd '/usr/local/hadoop/'
  code <<-EOH
  sbin/stop-dfs.sh
  EOH
end

bash 'stop_yarn.sh' do
  user 'hduser'
  group 'hadoop'
  cwd '/usr/local/hadoop/'
  code <<-EOH
  sbin/stop-yarn.sh
  EOH
end

