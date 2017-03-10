#!/bin/bash

#remove vagrant if already installed
rm -rf /opt/vagrant
rm -f /usr/bin/vagrant
rm -f ~/.vagrant.d

#download vagrant
wget https://releases.hashicorp.com/vagrant/1.8.5/vagrant_1.8.5_x86_64.deb

#configure vagrant
dpkg -i vagrant_1.8.5_x86_64.deb

#install linux-headers
apt-get -y install linux-headers-$(uname -r)

#reconfigure virtual box
dpkg-reconfigure virtualbox-dkms

#install vagrant plugin
vagrant plugin install vagrant-digitalocean

# vagrant box -> download box
vagrant box add digital_ocean https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box

#install vagrant-triggers
vagrant plugin install vagrant-triggers
