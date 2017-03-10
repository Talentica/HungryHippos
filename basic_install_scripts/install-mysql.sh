#!/bin/bash

#uninstall mysql
apt-get autoremove mysql* --purge

#remove any version mysql already installed
rm -rf /var/lib/mysql/ 

#remove dir
rm -rf /usr/local/mysql
#delete mysql profile if any
rm -rf /etc/mysql/

#uninstall apparmor
apt-get -y remove apparmor

#download mysql 5.5 version from website
wget "http://dev.mysql.com/get/Downloads/MySQL-5.5/mysql-5.5.53-linux2.6-x86_64.tar.gz"

#create user mysql
groupadd mysql

#create user mysql and add it to the previous group
useradd -g mysql mysql

#extract mysql
tar -xvf mysql-5.5.53-linux2.6-x86_64.tar.gz

# move mysql file  to usr/local
mv mysql-5.5.53-linux2.6-x86_64 mysql 
mv mysql /usr/local/

#change ownership of mysql file
chown -R mysql:mysql /usr/local/mysql

#install libaio-1
apt-get -y install libaio1

#execute mysql installation scripts
cd /usr/local/mysql
scripts/mysql_install_db --user=mysql

#set mysql directory owner to root 
chown -R root ./..

#change ownership data directory to mysql
chown -R mysql data

#copy mysql configuration file
cp /usr/local/mysql/support-files/my-medium.cnf /etc/my.cnf

#start mysql
bin/mysqld_safe --user=mysql &

#copy mysql.server file
cp /usr/local/mysql/support-files/mysql.server /etc/init.d/mysql.server

#set root password to root
/usr/local/mysql/bin/mysqladmin -u root password 'root'

#start mysql server continue even after fail
/etc/init.d/mysql.server start

#stop mysql 
/etc/init.d/mysql.server stop 

#check status
/etc/init.d/mysql.server status

#enable mysql on startup
update-rc.d -f mysql.server defaults

#add mysql to the system path
ln -s /usr/local/mysql/bin/mysql /usr/local/bin/mysql

echo "Done installing mysql"
