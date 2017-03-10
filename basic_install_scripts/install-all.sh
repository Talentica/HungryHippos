#!/bin/bash

echo "installation started " 
date 
#installing java
sh ./install-java.sh

#install ruby
sh ./install-ruby.sh

#install chef
sh ./install-chef.sh

#install virtual-box
sh ./install-virtual-box.sh

#install vagrant 
sh ./install-vagrant.sh

#install bc
#sh ./install-bc.sh

#install jq	
sh ./install-jq.sh

rm -f vagrant_1.8.5_x86_64.deb 
#install mysql password if prompted type "root".
#sh ./install-mysql.sh

sh ./comment.sh

echo "finished installing all softwares "
date
