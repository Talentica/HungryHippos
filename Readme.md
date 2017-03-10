
This readme contains prerequisite and basic installation details.


Prerequisite

1) minimum jdk 1.8 :- http://www.oracle.com/technetwork/java/javase/downloads/index.html

2) minimum Ruby 1.9 :- http://rubyinstaller.org/

3) Chef-solo :-  curl -L https://www.opscode.com/chef/install.sh | bash #installs latest version of chef 

4) stable, Vagrant 1.8.5 :- https://www.digitalocean.com/community/tutorials/how-to-use-digitalocean-as-your-provider-in-vagrant-on-an-ubuntu-12-10-vps

5) vagrant-digitalocean plugin (0.9.1) : vagrant plugin install vagrant-digitalocean; vagrant box add digital_ocean https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box

6) vagrant-triggers plugin (0.5.3) : vagrant plugin install vagrant-triggers

7) client Side O/S :- Linux distributions
   	       RAM :- Minimum 2GB.
         HARD DISK :- 1GB 
   

Installation of Prerequisite software

1. you can install all prerequsite software by running ./install.sh  or individual scripts. (supported on ubuntu)
2. for other distribution please follow the instructions provided by respectice software companies.

1. go to hhspark_automation; cd hhspark_automation
2  please refer hhspark_automation/readme for further steps

after execution of the script.

1. spark will be downloaded on all servers.
2. java will be installed on all servers.
3. chef-solo will be installed on all servers
4. zokeeper will be installed on the specified number of nodes , can be standalone or as cluster depending on the number of nodes.

8. Server side O/S : Ubuntu 14.04
          ideal RAM: 8 GB
          HARD DISK: Depends on Data size of the file to be distributed.
 ideal No.of cores per machine: 4

9.  minimum  Jdk 1.8
10. Spark 2.1
1 


