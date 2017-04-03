#
# Cookbook Name:: disable_IPV6
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
ruby_block "insert_line" do
	block do
	file = Chef::Util::FileEdit.new("/etc/sysctl.conf")
	file.insert_line_if_no_match("net.ipv6.conf.all.disable_ipv6 = 1","net.ipv6.conf.all.disable_ipv6 = 1")
	file.insert_line_if_no_match("net.ipv6.conf.default.disable_ipv6 = 1","net.ipv6.conf.default.disable_ipv6 = 1")
	file.insert_line_if_no_match("net.ipv6.conf.lo.disable_ipv6 = 1","net.ipv6.conf.lo.disable_ipv6 = 1")
	file.write_file
	end
end
