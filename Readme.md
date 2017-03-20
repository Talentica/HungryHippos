## This readme contains prerequisite and basic installation guilelines along with end to end execution of Hingry Hippos application.


## Prerequisite

1) minimum jdk 1.8 :- http://www.oracle.com/technetwork/java/javase/downloads/index.html

2) minimum Ruby 1.9 :- http://rubyinstaller.org/

3) Chef-solo        :-  curl -L https://www.opscode.com/chef/install.sh | bash #installs latest version of chef 

4) Vagrant 1.8.5    :- https://www.digitalocean.com/community/tutorials/how-to-use-digitalocean-as-your-provider-in-vagrant-on-an-ubuntu-12-10-vps

5) vagrant-digitalocean plugin (0.9.1) : vagrant plugin install vagrant-digitalocean; vagrant box add digital_ocean https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box

6) vagrant-triggers plugin (0.5.3) : vagrant plugin install vagrant-triggers

7) git :- git need to be installed in the machine so that you can clone the project.

8) gradle :- gradle need to be installed in the machine so that you build the project.
            
             apt-get install gradle .

10) client Side O/S :- Linux distributions
   	   
         RAM       :- 2GB     
         HARD DISK :- minimum 2GB free for installation
   

## Installation of Prerequisite software

1. you can install all prerequsite software by running ./install.sh  or  individual scripts. (supported on ubuntu)

   1.1 cd basic_install_scripts
   
   1.2 ./install-all.sh 
   
   1.2.1 install-*.sh to install respective software.
   
   NOTE :- If you have Java or Ruby already installed it will be better you install the software individually. Else it will   
      override you Ruby and Java to latest version.
2. for other distribution please follow the instructions provided by respectice software companies.

## Build the project:

1. gradle clean build
2. jar file of each module will be created in respective modules/build/libs.
3. cp node/build/libs/node-*.jar hhspark_automation/distr_original/lib

## Setting up the project.

1.  

### Setting Initial Properties.

1.  cd hhspark_automation/scripts // go to the scripts folder and 
2.  cp vagrant.properties.template vagrant.properties // create vagrant.properties file from vagrant.properties.template

    vagrant.properties has following variables with default values

    2.1 NODENUM = 1 //number of nodes to spawned here 1 node will be spawned

    2.2 ZOOKEEPERNUM = 1 //number of nodes on which zookeeper has to be installed ; i.e; 1 node will install zookeeper;      	
       //ZOOKEEPERNUM <= NODENUM

    2.3 PROVIDER = digital_ocean ; //default value , currently script supports only digital ocean  

    2.4 TOKEN=---------------------------------- //token id by which you can access digital ocean api. #for more details refer
    Token Generation

    2.5 IMAGE=ubuntu-14-04-x64 // operating system to be used

    2.60 REGION=nyc1 // Node spawn location nyc1 -> NewYork Region 1.

    2.7 RAM=8GB // the ram of the node , here 8GB ram is allocated for each node

    2.8 PRIVATE_KEY_PATH = /root/.ssh/id_rsa ; //ssh key path that is added in the digital ocean, if its not there please create one and add it to digital ocean security settings. refer SSH KEY Generation
      
    2.9 SSH_KEY_NAME=<vagrant_SSH_KEY_NAME> // is the name of the ssh key that will be added in digital ocean as part of 2.8.

3. cp spark.properties.template spark.properties . //default port number to use, override the values to use that specific port number

    3.1 SPARK_WORKER_PORT=9090

    3.2 SPARK_MASTER_PORT=9091

    execute ./vagrant-init-caller.sh

## SSH_KEY Generation

    ssh-keygen -t rsa ; after executing this command it will type something like below

    Generating public/private rsa key pair.
       Enter file in which to save the key (/home/"$user"/.ssh/id_rsa): 

    if you press enter with out changing the file location, 2 files will be created id_rsa and id_rsa.pub.

    �NOTE:- after pressing enter it will prompt something like below Enter passphrase (empty for no passphrase): ignore the passphrase by hitting enter again.

        After creating the SSH_KEY, lets say id_rsa its necessary to add the public key id_rsa.pub contents to digital ocean.

        2.1 login to https://cloud.digitalocean.com

        2.2 go to settings and select security.

        2.3 a new page will be open which has SSH keys as heading

        2.4 click on "add ssh key"

        2.5 copy the contents of id_rsa.pub to the content box, and give it a name.

        2.6 the provided name should be provided to the SSH_KEY_NAME. (Setting properties,2.9)
        If you are not doing it manually you will run into an issue https://github.com/devopsgroup-io/vagrant-digitalocean/issues/178 , it seems multiple node tries to add new ssh key name at same time with out checking whether previous nodes already added it or not.

## Token Generation

    login to https://cloud.digitalocean.com

    click on API

    click on Generate New Token

    provide token name and click on Generate Token

    copy the token, as it will not be shown again.

## Destroy Server (Digital ocean nodes created)

    to destroy the server nodes execute ./destroy-vagrant.sh present inside the scripts folder.

## After execution of the script.

1. spark will be downloaded on all servers.
2. java will be installed on all servers.
3. chef-solo will be installed on all servers
4. zokeeper will be installed on the specified number of nodes , can be standalone or as cluster depending on the number of nodes.

8. Server side O/S : Ubuntu 14.04

          ideal RAM: 8 GB
          HARD DISK: Depends on Data size of the file to be distributed.
          ideal No.of cores per machine: 4

## Hungry Hippos Version : 0.7.0v

## Sharding Module :
Sharding is the initial step in the enitre ecosystem of the Hungy Hippos application.User will have to run the sharding module prior to data publish. Execution of sharding module requires a "sample" file which finally creates "sharding table". Data publish requires this "sharding table" during execution. User is required to provide configuration related details before runnning the Sharding module. There are two configurations files for sharding which are explained below : 

### 1. sharding-client-config.xml

Assuming your input file contains lines with just two fields like below.
The fields are created using comma i.e "," as delimiter.

    samsung,78904566
    apple,865478
    nokia,732

Mobile is the column name given to field1 . i.e; samsung | apple | nokia<br/>
Number is the column name given to field2 . i.e; 7890566 | 865478 ..

### A sample sharding-client-config.xml file looks like below :

    <tns:sharding-client-config>
      <tns:input>
        <tns:sample-file-path>/home/sohanc/D_drive/dataSet/testDataForPublish.txt</tns:sample-file-path>
        <tns:distributed-file-path>/sohan/dir/input</tns:distributed-file-path>
        <tns:data-description>
          <tns:column>
            <tns:name>Mobile</tns:name>
            <tns:data-type>STRING</tns:data-type>
            <tns:size>2</tns:size>
          </tns:column>
          <tns:column>
            <tns:name>Number</tns:name>
            <tns:data-type>INT</tns:data-type>
            <tns:size>0</tns:size>
          </tns:column>
        </tns:data-description>
        <tns:data-parser-config>
          <tns:class-name>com.talentica.hungryHippos.client.data.parser.CsvDataParser</tns:class-name>
        </tns:data-parser-config>
      </tns:input>
      <tns:sharding-dimensions>key1</tns:sharding-dimensions>
      <tns:maximum-size-of-single-block-data>200</tns:maximum-size-of-single-block-data>
      <tns:bad-records-file-out>/home/sohanc/bad_rec</tns:bad-records-file-out>
    </tns:sharding-client-config>

### Explaination : 

    <tns:sharding-client-config>
      <tns:input>
        <tns:sample-file-path> provide sample file path. </tns:sample-file-path>
        <tns:distributed-file-path> distributed path of input file where HungryHippos will store the file. </tns:distributed-file-path>
        <tns:data-description> Describe all the columns in a record
          <tns:column> 
            <tns:name> name of the column </tns:name>
            <tns:data-type>Data-type of column</tns:data-type>
            <tns:size> max number of characters for String and 0 for other datatypes</tns:size>
          </tns:column>
          Repeat this column element for all columns description.
        </tns:data-description>
        <tns:data-parser-config> By default HungryHippos CsvDataParser provided.
          <tns:class-name>com.talentica.hungryHippos.client.data.parser.CsvDataParser</tns:class-name>
        </tns:data-parser-config>
      </tns:input>
      <tns:sharding-dimensions>comma separeted column names which user has identified as dimensions.</tns:sharding-dimensions>
      <tns:maximum-size-of-single-block-data> Max size of a single record </tns:maximum-size-of-single-block-data>
      <tns:bad-records-file-out>file path for storing records which does not fulfil the data-description given above.</tns:bad-records-file-out>
    </tns:sharding-client-config>


### 2. sharding-server-config.xml

A sample sharding-server-config.xml file looks like below :

    <tns:sharding-server-config
      xmlns:tns="http://www.talentica.com/hungryhippos/config/sharding"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.talentica.com/hungryhippos/config/sharding sharding-server-config.xsd ">

      <tns:maximum-shard-file-size-in-bytes>200428800</tns:maximum-shard-file-size-in-bytes>
      <tns:maximum-no-of-shard-buckets-size>20000</tns:maximum-no-of-shard-buckets-size>
    </tns:sharding-server-config>

### Explaination : 

    <tns:maximum-shard-file-size-in-bytes> Maximum Size of the sharding table files generated.</tns:maximum-shard-file-size-in-bytes>
    <tns:maximum-no-of-shard-buckets-size> Maximum number of buckets user wants to create.</tns:maximum-no-of-shard-buckets-size>

## Data publish Module :
Data publish module allows the user to publish large data set across the cluster of machines 
from client machine.This distributed data become eligible to get executed during job execution.

Data publish jar will be availaible in installation package of the Hungry Hippos.
Execute the following command to get start with data publish.

### Command :
    java -cp data-publisher-0.7.0.jar <main-class> <client-config-xml> <input-data>
    <relative-distributed-path> <optional-args>
### Command line arguments descriptions :    
            
    1. main-class : com.talentica.hungryHippos.master.DataPublisherStarter
    
    2. client-config-xml: provide the "client-config.xml" file path which is available in 
       Hungry Hippos installation package.i.e "conf/client-config.xml"
       
    3. input-data : provide path of input data set with file name. Currently we support text
       and csv files only in which fields need be comma seperated.
       
    4. relative-distributed-path : This path should be exactly same as provided 
       in "sharding-client-config.xml" having field name "distributed-file-path".
       
    5. optional-args : This arguments are optional which is to redirect the logs and also to run the 
       application in background.i.e " >  logs/data-publish.out 2> logs/data-publish.err &"
            
### Example  : 
     java -cp data-publisher-0.7.0.jar com.talentica.hungryHippos.master.DataPublisherStarter
     conf/client-config.xml ~/dataGenerator/sampledata.txt /dir/input >
     logs/datapub.out 2> logs/datapub.err &
            
            

## Job Execution Module :
As soon as data publish is completed, cluster machines are ready to accept the command to execute the jobs.
To execute the jobs, client should write the jobs and submit it with spark submit command. 
Moreover, you can find the examples as to how to write the jobs in module "examples" with package [com.talentica.hungryHippos.rdd.main](https://github.com/Talentica/HungryHippos/tree/modularization-code-cleanup/examples/src/main/java/com/talentica/hungryHippos/rdd/main)  namely [SumJob](https://github.com/Talentica/HungryHippos/blob/modularization-code-cleanup/examples/src/main/java/com/talentica/hungryHippos/rdd/main/SumJob.java) , [MedianJob](https://github.com/Talentica/HungryHippos/blob/modularization-code-cleanup/examples/src/main/java/com/talentica/hungryHippos/rdd/main/MedianJob.java) and [UniqueCountJob](https://github.com/Talentica/HungryHippos/blob/modularization-code-cleanup/examples/src/main/java/com/talentica/hungryHippos/rdd/main/UniqueCountJob.java).

Therefore, simply follow the below steps : 
### Steps :
	1. Write the job.
	
[Examples as how to write jobs](https://github.com/Talentica/HungryHippos/tree/modularization-code-cleanup/examples/src/main/java/com/talentica/hungryHippos/rdd/main)
	
	2. Build the module.
	
	3. Create the jar. Let's say it is "examples-0.7.0.jar".
	
	4. Transfer above created jar along with dependency jars such as "sharding-<version>.jar" and
	   "hhrdd-<version>.jar" to spark "master" node  in directory "/home/hhuser/distr/lib_client".
	   
	5. Run the following command in spark installation directory of master node:
### Command :
	   ./bin/spark-submit --class <job-main-class> --jars <dependency-jars>	 --master 
	   spark://<master-ip>:<port> <client-job-jar> spark://<master-ip>:<port> <application-name>
	   <relative-distributed-path> <client-config-xml> <output-directory> <optional-args>	.
						 
### Command line arguments descriptions :
					 
	  1. job-main-class : main class of client written jobs.
	     i.e com.talentica.hungryHippos.rdd.main.SumJob
	  
	  2. dependency-jars : all dependency jars with comma separated such as 
	  /home/hhuser/distr/lib_client/sharding-0.7.0.jar,/home/hhuser/distr/lib_client/hhrdd-0.7.0.jar.
	  
	  3. master-ip : spark master ip.
	  
	  4. port : configured spark master port number.
	  
	  5. application-name : application name for current submission programe.
	  
	  6. relative-distributed-path : This path should be exactly same as provided in
	     "sharding-client-config.xml" having field name "distributed-file-path". 
	     
	  7. client-config-xml : client-config.xml file.
	  
	  8. output-directory : output directory name wherein the results are stored inside job id
	     subfolder.
	  
	  9. optional-args : This arguments are optional which is to redirect the logs and also to run
	     the application in background. i.e ">../logs/spark.out 2>../logs/spark.err &"
### Example :						 
	
	./bin/spark-submit --class com.talentica.hungryHippos.rdd.main.SumJob
	--jars /home/hhuser/distr/lib_client/sharding-0.7.0.jar,
	/home/hhuser/distr/lib_client/hhrdd-0.7.0.jar --master spark://67.205.172.104:9091
	/home/hhuser/distr/lib_client/examples-0.7.0.jar spark://67.205.172.104:9091 hh-sum /dir/input
	/home/hhuser/distr/config/client-config.xml output >../logs/spark.out 2>../logs/spark.err &
	
 

