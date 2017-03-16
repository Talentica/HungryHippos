# INSTALLATION
### Setting Initial Properties.

1. go to the scripts folder and create vagrant.properties file.  cd scripts; cp vagrant.properties.template vagant.properties

2. vagrant.properties has following variables with default values

   2.1 NODENUM = 1 //number of nodes to spawned  here 1 node will be spawned
   
   2.2 ZOOKEEPERNUM = 1 //number of nodes on which zookeeper has to be installed ; i.e; 1 node will install zookeeper;
                        //ZOOKEEPERNUM <= NODENUM
   
   2.3 PROVIDER = digital_ocean ; //default value , currently script supports only digital ocean
   
   2.4 PRIVATE_KEY_PATH = /root/.ssh/id_rsa ; //ssh key path that is added in the digital ocean, if its not there please create one and add it to digital ocean security settings.  refer [SSH KEY Generation](.#ssh_key-generation)
   
   2.5 TOKEN=---------------------------------- //token id by which you can access digital ocean api. #for more details refer    
      [Token Generation](.#token-generation)
   
   2.6 IMAGE=ubuntu-14-04-x64  // operating system to be used
   
   2.7 REGION=nyc1 // Node spawn location nyc1 -> NewYork Region 1.
   
   2.8 RAM=8GB // the ram of the node , here 8GB ram is allocated for each node
   
   2.9 SSH_KEY_NAME=<vagrant_SSH_KEY_NAME> // is the name of the ssh key that will be added in digital ocean as part of 2.4.
   
 3. cp spark.properties.template spark.properties. //default port number to use, override the values to use that specific port number
 
    3.1 SPARK_WORKER_PORT=9090 
    
    3.2 SPARK_MASTER_PORT=9091

 4. execute ./vagrant_init_caller.sh

### SSH_KEY Generation

1. ssh-keygen -t rsa ; after executing this command it will type something like below

       Generating public/private rsa key pair.
          Enter file in which to save the key (/home/"$user"/.ssh/id_rsa): 

   if you press enter with out changing the file location, 2 files will be created id_rsa and id_rsa.pub.
  
   �NOTE:- after pressing enter it will prompt something like below
   Enter passphrase (empty for no passphrase):
   ignore the passphrase by hitting enter again.
   
   2. After creating the SSH_KEY, lets say id_rsa its necessary to add the public key id_rsa.pub contents to digital ocean.
   
      2.1 login to  https://cloud.digitalocean.com
  
      2.2 go to settings and select security. 
   
      2.3 a new page will be open which has SSH keys as heading
   
      2.4 click on "add ssh key"
   
      2.5 copy the contents of id_rsa.pub to the content box, and give it a name.
   
      2.6 the provided name should be provided to the SSH_KEY_NAME. (Setting properties,2.9)
      
      ###### If you are not doing it manually you will run into an issue https://github.com/devopsgroup-io/vagrant-digitalocean/issues/178 , it seems multiple node tries to add new ssh key name at same time with out checking whether previous nodes already added it or not.


### Token Generation
  
  1. login to  https://cloud.digitalocean.com
  
  2. click on API
  
  3. click on Generate New Token 
  
  4. provide token name and click on Generate Token
  
  5. copy the token, as it will not be shown again. 
  
### Destroy Server (Digital ocean nodes created)
  
  1. to destroy the server nodes execute ./destroy-node.sh

