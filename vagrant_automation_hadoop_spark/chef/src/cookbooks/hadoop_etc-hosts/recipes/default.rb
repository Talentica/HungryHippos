#
# Cookbook Name:: hadoop_etc-hosts
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#

ruby_block "insert_line" do
	block do
		file = Chef::Util::FileEdit.new("/etc/hosts")
		file.insert_line_if_no_match("162.243.102.4	master","162.243.102.4		master")
		file.insert_line_if_no_match("162.243.102.165	slave","162.243.102.165   	slave")
		file.write_file
	end
end

