# Cookbook Name:: hadoop_conf_files_setupi
# Recipe:: default
#
# Copyright 2016, YOUR_COMPANY_NAME
#
# All rights reserved - Do Not Redistribute
#
#Copying bashrc_hadoop file to every node and appending it to .bashrc
cookbook_file "/home/hduser/bashrc_hadoop.txt" do
   source "bashrc_hadoop.txt"
 end

 execute "appending bash file for hadoop" do
  user "root"
  command "cat /home/hduser/bashrc_hadoop.txt >> /home/hduser/.bashrc"
end


execute "delete temporary copied file" do
  user "root"
  command "rm /home/hduser/bashrc_hadoop.txt"
end


#execute "Update java home in hadoop-env.sh" do
 #user "root"
# command "sed -i -r 's/JAVA_HOME/\/usr\/lib\/jvm\/jdk1.8.0_65/g' /usr/local/hadoop/etc/hadoop/hadoop-env.sh"
#command "sed -i 's/The java implementation to use./The java implementation to use. -added by chintamani/g' /usr/local/hadoop/etc/hadoop/hadoop-env.sh"
#end

#ruby_block "Update java home in hadoop-env.sh" do
#block do
#file = Chef::Util::FileEdit.new("/usr/local/hadoop/etc/hadoop/hadoop-env.sh")
#file.search_file_replace("\$\{JAVA_HOME\}","/usr/lib/jvm/jdk1.8.0_65")
#file.write_file
#end
#end

#Update java home in hadoop-env.sh
#delete line no 25 where java_home is defined
execute "delete line no 25" do
  user "root"
  command "sed -i '25d' /usr/local/hadoop/etc/hadoop/hadoop-env.sh"
end

#add export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_65 to line no 25
execute "add line no 25" do
  user "root"
  command "sed -i '25iexport JAVA_HOME=\/usr\/lib\/jvm\/jdk1.8.0_65' /usr/local/hadoop/etc/hadoop/hadoop-env.sh"
end


#configure core-site.xml
template '/usr/local/hadoop/etc/hadoop/core-site.xml' do
  source 'core-site-xml-template.erb'
  mode '0644'
  owner 'hduser'
  group 'hadoop'
  end

#configure hdfs-site.xml
template '/usr/local/hadoop/etc/hadoop/hdfs-site.xml' do
  source 'hdfs-site-xml-template.erb'
  mode '0644'
  owner 'hduser'
  group 'hadoop'
  end

#configure yarn-site.xml
template '/usr/local/hadoop/etc/hadoop/yarn-site.xml' do
  source 'yarn-site-xml-template.erb'
  mode '0644'
  owner 'hduser'
  group 'hadoop'
  end




#configure mapred-site.xml
template '/usr/local/hadoop/etc/hadoop/mapred-site.xml' do
  source 'mapred-site-xml-template.erb'
  mode '0644'
  owner 'hduser'
  group 'hadoop'
  end

#configure masters
template '/usr/local/hadoop/etc/hadoop/masters' do
  source 'masters-template.erb'
  mode '0644'
  owner 'hduser'
  group 'hadoop'
  end

#configure slaves
template '/usr/local/hadoop/etc/hadoop/slaves' do
  source 'slaves-template.erb'
  mode '0644'
  owner 'hduser'
  group 'hadoop'
  end
