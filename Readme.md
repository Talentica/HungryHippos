
This readme contains prerequisite and basic installation details.


Prerequisite

1) minimum jdk 1.8 :- http://www.oracle.com/technetwork/java/javase/downloads/index.html

2) minimum Ruby 1.9 :- http://rubyinstaller.org/

3) Chef-solo        :-  curl -L https://www.opscode.com/chef/install.sh | bash #installs latest version of chef 

4) Vagrant 1.8.5    :- https://www.digitalocean.com/community/tutorials/how-to-use-digitalocean-as-your-provider-in-vagrant-on-an-ubuntu-12-10-vps

5) vagrant-digitalocean plugin (0.9.1) : vagrant plugin install vagrant-digitalocean; vagrant box add digital_ocean https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box

6) vagrant-triggers plugin (0.5.3) : vagrant plugin install vagrant-triggers

7) git :- git need to be installed in the machine so that you can clone the project.

8) gradle :- gradle need to be installed in the machine so that you build the project.


10) client Side O/S :- Linux distributions
   	   
         RAM       :- 2GB     
         HARD DISK :- 1GB 
   

Installation of Prerequisite software

1. you can install all prerequsite software by running ./install.sh  or  individual scripts. (supported on ubuntu)

   1.1 cd basic_install_scripts
   
   1.2 ./install.sh 
   
   1.2.1 install_*.sh to install respective software.
   
   NOTE :- If you have Java or Ruby already installed it will be better you install the software individually. Else it will   
      override you Ruby and Java to latest version.
2. for other distribution please follow the instructions provided by respectice software companies.

Build the project:

1. gradle clean build
2. jar file of each module will be created in respective modules/build/lib.
3. cp node/build/lib/node-*.jar hhspark_automation/distr_original/lib

Setting up the project.

1.  cd hhspark_automation ; #go to hhspark automation.
2.  please refer hhspark_automation/readme for further steps

After execution of the script.

1. spark will be downloaded on all servers.
2. java will be installed on all servers.
3. chef-solo will be installed on all servers
4. zokeeper will be installed on the specified number of nodes , can be standalone or as cluster depending on the number of nodes.

8. Server side O/S : Ubuntu 14.04

          ideal RAM: 8 GB
          HARD DISK: Depends on Data size of the file to be distributed.
          ideal No.of cores per machine: 4

 


