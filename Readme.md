## This readme contains prerequisite and basic installation guilelines along with end to end execution of HungryHippos application.


# Purpose

This document describes how to install, configure and run HungryHippos clusters.

This document also covers how to run jobs and get their results.

## Prerequisite

1) Minimum jdk 1.8 :- http://www.oracle.com/technetwork/java/javase/downloads/index.html

2) Minimum Ruby 1.9 :- http://rubyinstaller.org/

3) Chef-solo        :-  curl -L https://www.opscode.com/chef/install.sh | bash #installs latest version of chef 

4) Vagrant 1.8.5    :- https://www.digitalocean.com/community/tutorials/how-to-use-digitalocean-as-your-provider-in-vagrant-on-an-ubuntu-12-10-vps

5) vagrant-digitalocean plugin (0.9.1) : vagrant plugin install vagrant-digitalocean; vagrant box add digital_ocean https://github.com/smdahlen/vagrant-digitalocean/raw/master/box/digital_ocean.box

6) vagrant-triggers plugin (0.5.3) : vagrant plugin install vagrant-triggers

7) git :- git need to be installed in the machine so that you can clone the project.

8) gradle :- gradle need to be installed in the machine so that you build the project.
            
       apt-get install gradle .

10) Preferred Client Configuration

        O/S : Ubuntu 14.04
        RAM: 8 GB
        HARD DISK: Depends on the size of the file to be distributed.
        No.of cores per machine: 4

11) Preferred Server Configuration

        O/S : Ubuntu 14.04
        RAM: 8 GB
        HARD DISK: Depends on the size of the file to be distributed.
        No.of cores per machine: 4
   

## Installation of Prerequisite softwares on Client 

**You can install all prerequsite softwares by running ./install-all.sh  or  individual scripts inside the basic_install_scripts folder**

   1. Go to basic_install_scripts
   
          cd HungryHippos/basic_install_scripts
   
   1.1 Run install-all.sh to install all the prerequisite software 
      
       ./install-all.sh 
   
   1.2 To install softwares individually, run respective install-*.sh.
   
   * To install bc , --mathlib
   
         ./install-bc.sh  
	 
   * To install chef
   
         ./install-chef.sh
	
    * To install oracle-jdk 
  
          ./install-java.sh
	
     * To install jq , used for json parsing
     
           ./install-jq.sh
	   
      * To install ruby 
	
            ./install-ruby.sh

       * To install vagrant
       
             ./install-vagrant.sh
	     
       * To install virtual Box
       
             ./install-virtual-box.sh 
   
   NOTE :- If you have some of these softwares already installed, it is better to install rest of the softwares individually. Otherwise they will be overriden.
   
 For other linux distributions, please follow the instructions provided by respectice software distributors.

## HungryHippos Cluster setup:

**STEP 1.** Build the project to create and publish the jars to local maven repository.

    gradle clean build publishToMavenLocal

**STEP 2.** Copy node-\*.jar to the  hhspark_automation/distr_original/lib folder
 
    cp node/build/libs/node-*.jar hhspark_automation/distr_original/lib

**STEP 3.** Setting Cluster Properties

1.  Go to the scripts folder inside the hhspark_automation.
    
        cd hhspark_automation/scripts
    
2.  Create vagrant.properties file from vagrant.properties.template

    	cp vagrant.properties.template vagrant.properties

    **vagrant.properties** has following variables with default values
    
    | Name | Default Value | Description|
    | --- | --- | --- |
    | NODENUM | 1 | Number of nodes to spawned here 1 node will be spawned |
    | ZOOKEEPERNUM | 1 | Number of nodes on which zookeeper has to be installed,  **ZOOKEEPERNUM should be greater than 0 and ideally should be equal to maximum number of replicas** |
    | PROVIDER | digital_ocean | Nodes are created on digital_ocean. No other cloud services supported currently |
    | TOKEN | ----------- | Token id by which you can access digital ocean api. #for more details refer [Token Generation](Readme.md#token-generation) |
    | IMAGE | ubuntu-14-04-x64 | Operating system to be used, check https://cloud.digitalocean.com/ |
    | REGION | nyc1  |  Node spawn location **nyc1 -> NewYork Region 1**, for further details check https://cloud.digitalocean.com/ |
    | RAM | 8GB  |  The RAM for each node , here 8GB RAM is allocated for each node |
    | PRIVATE_KEY_PATH | /root/.ssh/id_rsa | Private sshkey path of the public key that is added in the digital ocean, if its not there please create one and add the public key of it to digital ocean , security settings. refer [SSH KEY Generation](Readme.md#ssh_key-generation) |
    | SSH_KEY_NAME | vagrant | The name of the public ssh key that is added in digital ocean |

3. Create spark.properties file from spark.properties.template.
  
       cp spark.properties.template spark.properties
    
   spark.properties contains details regarding the port number to used for spark master and spark worker. Override those values 
    if you want to configure some other port number.
    
    | Name | Default Value | Description|
    | --- | --- | --- |
    | SPARK_WORKER_PORT | 9090 | Spark Worker's port number |
    | SPARK_MASTER_PORT | 9091 | Spark Master's port number |

**STEP 4.** Execute ./vagrant-init-caller.sh inside the scripts folder. It will spawn the hungryhippos cluster with required softwares.
 
       ./vagrant-init-caller.sh
       
After execution of the script.
1. Spark-2.0.2-bin-hadoop2.7, Oracle JDK (1.8 Version) , Chef-solo (stable,12.17.44) will be downloaded and installed on all servers.
	
 2. Zookeeper (3.5.1 alpa) will be installed on the specified number of nodes. It will configure to be standalone or as cluster depending on the number of nodes.
 
 3. "ip_file.txt" file is created at ~/HungryHippos/hhspark_automation/scripts location. This file contain all the ips of nodes
 created in cluster. The first entry in this file is spark master-ip.
 
 4. User can open spark Web UI at `<master-ip>:<port>` for monitoring purpose. By default port is 8080. But there are chances that port 8080 will be occupied by some other service, in that case sser can find the correct port number by logging in master node and check log file created at location `/home/hhuser/spark-2.0.2-bin-hadoop2.7/logs/spark-hhuser-org.apache.spark.deploy.master.Master-1-sparkTTW-1.out` . In log file user can see something like `INFO Utils: Successfully started service 'MasterUI' on port 8081.` So port will be 8081.

## SSH_KEY Generation

Generate rsa key using below command. It will generate a private key file and a public key file.

**NOTE** :- Please don't input passphrase.

    ssh-keygen -t rsa 
  
 After creating the SSH_KEY( say id_rsa), its necessary to add the public key (id_rsa.pub) contents to digital ocean.

  * Login to https://cloud.digitalocean.com

  * Go to settings and select security. security page will open
  
  * Click on "Add SSH Key"
	
  * Copy the content of the public key that was generated by "ssh-keygen -t rsa" command to the content box, and give it a name.

  * This name should be used to set the SSH_KEY_NAME in the vagrant.properties.

## Token Generation

   * Login to https://cloud.digitalocean.com

   * Click on API Tab

   * Click on Generate New Token
   
   * Provide token name and click on Generate Token

   * Copy the token value, as it will not be shown again.

## Destroy Server (Digital ocean nodes created)

* To destroy the server nodes execute ./destroy-vagrant.sh that is present inside the scripts folder.
    
      ./destroy-vagrant.sh


## HungryHippos Version : 0.7.0v

# Sharding :
Sharding is the initial step in the enitre ecosystem of the HungryHippos application. User will have to perform sharding prior to data publish. To perform sharding, a sample file (that represents the near distribution of the actual data file) is required which finally creates "sharding table". Data publish requires this "sharding table" during execution. 

**NOTE:** User has to configure sharding configurations before performing the Sharding. 

### Configure sharding xml files :

Creating sharding-client-config.xml and sharding-server-config.xml file from template files.

	cp hhspark_automation/distr/config/sharding-client-config.xml.template hhspark_automation/distr/config/sharding-client-config.xml
	cp hhspark_automation/distr/config/sharding-server-config.xml.template hhspark_automation/distr/config/sharding-server-config.xml

### 1. sharding-client-config.xml

Assuming your input file contains lines with just two fields like below.
The fields are separrated using comma i.e "," as delimiter.

    samsung,78904566
    apple,865478
    nokia,732

Mobile is the column name given to field1 . i.e; samsung | apple | nokia<br/>
Number is the column name given to field2 . i.e; 7890566 | 865478 ..

### Purpose : 

The purpose of this file is to provide the data description of input file, dimensions on which sharding needs to be done and
the distributed path of input file in HungryHippo file system.
		
### A sample sharding-client-config.xml file looks like below :

    <tns:sharding-client-config>
      <tns:input>
        <tns:sample-file-path>/home/hhuser/D_drive/dataSet/testDataForPublish.txt</tns:sample-file-path>
        <tns:distributed-file-path>/hhuser/dir/input</tns:distributed-file-path>
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
      <tns:bad-records-file-out>/home/hhuser/bad_rec</tns:bad-records-file-out>
    </tns:sharding-client-config>

### Explaination : 

  | Name | Value | Description | 
  | ----- | --- | -------- |
  | tns:sample-file-path | `<sample-file-path>` | Path of the sample file on which sharding needs to be done |
  | tns:distributed-file-path | `<distributed-path>` | Path on HungryHippo filesystem where input file will be stored |
  | tns:data-description | - | Colume elements inside it will hold the description of columns in record |
  | tns:column | - | will hold the column description in record |
  | tns:name | Mobile | Name of the column |
  | tns:data-type | INT, LONG, DOUBLE, STRING | data-type of the column. Can contain one of the values
  given here.|
  | tns:size | 0 | size of data-type. max number of characters for String and 0 for other datatypes|
  | tns:data-parser-config | - | By default HungryHippos CsvDataParser provided|
  | tns:class-name | com.talentica.hungryHippos.client.data.parser.CsvDataParser | HungryHippos provides it's own data parser |
  | tns:sharding-dimensions | Mobile,Number | comma separeted column names which user has identified as dimensions |
  | tns:maximum-size-of-single-block-data | 80 | Max size of a single record in text format |
  | tns:bad-records-file-out | `<local-file-path>` | file path for storing records which does not fulfil the data-description given above |
  

### 2. sharding-server-config.xml

The purpose of this file is to provide the number of buckets the sharding module should create for given input data file.

A sample sharding-server-config.xml file looks like below :

    <tns:sharding-server-config
      xmlns:tns="http://www.talentica.com/hungryhippos/config/sharding"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.talentica.com/hungryhippos/config/sharding sharding-server-config.xsd ">

      <tns:maximum-shard-file-size-in-bytes>200428800</tns:maximum-shard-file-size-in-bytes>
      <tns:maximum-no-of-shard-buckets-size>20</tns:maximum-no-of-shard-buckets-size>
    </tns:sharding-server-config>
	
### Explaination : 

 | Name | Value | description | 
 | ---- | ----- | ----------- |
 | tns:maximum-shard-file-size-in-bytes | 200428800 | Maximum size of the sharding file in bytes |
 | tns:maximum-no-of-shard-buckets-size | 20 | Maximum number of buckets user wants to create |

### Note : 
A bucket represents a container for a key or a combination of keys. The Sharding module creates a number of buckets mentioned
in property `tns:maximum-no-of-shard-buckets-size`. Each record sharding module processes will be mapped to one of these buckets
based on the key or combination of keys.

### Sharding-module Execution

### Command :
Execute the following command from project parent folder.

    java -cp data-publisher/build/libs/data-publisher-0.7.0.jar com.talentica.hungryHippos.sharding.main.ShardingStarter <client-config.xml> <sharding-conf-path>

### Command line arguments descriptions : 
	
1. client-config.xml: provide the client-config.xml file path which is available in hhspark_automation/distr/config/client-config.xml

2. sharding-conf-path : parent folder path of sharding configuration files. i.e. hhspark_automation/distr/config

### Example :
    java -cp data-publisher/build/libs/data-publisher-0.7.0.jar com.talentica.hungryHippos.sharding.main.ShardingStarter  hhspark_automation/distr/config/client-config.xml hhspark_automation/distr/config

# Data publish :
Data publish allows the user to publish data across the cluster of machines 
from client machine.This distributed data become eligible to get executed during job execution.
Execute the following command to start data publish from project's parent folder.

### Command :

    java -cp data-publisher/build/libs/data-publisher-0.7.0.jar com.talentica.hungryHippos.master.DataPublisherStarter <client-config.xml> <input-data> <distributed-file-path> <optional-args>
    
### Command line arguments descriptions :    

1. client-config.xml: provide the client-config.xml file path which is available in hhspark_automation/distr/config/client-config.xml
2. input-data : provide path of input data set with file name. Currently we support text and csv files only in which fields need be comma seperated.
3. distributed-file-path : This path should be exactly same as provided in "sharding-client-config.xml" having field name "distributed-file-path".
4. optional-args : You can provide the optional argument for chunk size (in bytes). Otherwise 128 MB will be considered as  the chunk size. The input data file is splitted into small parts called Chunks, which are published to nodes where they are processed and stored.

### Example  : 
     java -cp data-publisher/build/libs/data-publisher-0.7.0.jar com.talentica.hungryHippos.master.DataPublisherStarter hhspark_automation/distr/config/client-config.xml ~/dataGenerator/sampledata.txt /dir/input > logs/datapub.out 2> logs/datapub.err &         
 
# Job Execution Module :
As soon as data publish is completed, cluster machines are ready to accept the command to execute the jobs.
To execute the jobs, client should write the jobs and submit it with spark submit command. 
Moreover, you can find the examples as to how to write the jobs in module "examples" with package [com.talentica.hungryhippos.examples](https://github.com/Talentica/HungryHippos/tree/modularization-2/examples/src/main/java/com/talentica/hungryhippos/examples)  namely [SumJob](https://github.com/Talentica/HungryHippos/blob/modularization-2/examples/src/main/java/com/talentica/hungryhippos/examples/SumJob.java) and [UniqueCountJob](https://github.com/Talentica/HungryHippos/blob/modularization-2/examples/src/main/java/com/talentica/hungryhippos/examples/UniqueCountJob.java).


### Steps :

1. In the build.gradle of your project add mavenLocal and mavenCentral in gradle repository and add the dependency for hungryhippos client api and hhrdd.

```
repositories {
    mavenCentral()
    mavenLocal()
}

dependencies{
compile 'com.talentica.hungry-hippos:client-api:0.7.0'
compile 'com.talentica.hungry-hippos:hhrdd:0.7.0'
}
```

2. Write the job using HungryHippos custom spark implementation. Refer [Examples](https://github.com/Talentica/HungryHippos/tree/modularization-2/examples/src/main/java/com/talentica/hungryhippos/examples) to have an overview.

3. Build your job jar.
	
4. Transfer above created jar(say, test.jar) along with dependency jars such as "hhrdd-0.7.0.jar" available in location hhrdd/build/libs to spark "master" node in directory "/home/hhuser/distr/lib_client.
 Run the following commands in project parent folder:
 
 ```
     scp hhrdd/build/libs/hhrdd-0.7.0.jar hhuser@<master-ip>:/home/hhuser/distr/lib_client
     
     scp <path to test.jar>  hhuser@<master-ip>:/home/hhuser/distr/lib_client
```
#### Note : 
 User can get all cluster nodes' IP from file ~/HungryHippos/hhspark_automation/scripts/ip_file.txt.
 First entry in ip_file.txt file represents the IP of spark master node i.e. `<master-ip>`
5. Run the following command in spark installation directory (/home/hhuser/spark-2.0.2-bin-hadoop2.7) on spark master node:

### User can follow the below command to run the above jobs or alternatively can follow the [spark job submission](http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit) command.
**Note 1** User has to provide path to client-config path in the driver program. User has to ensure that the path to the client-config path is valid.

**Note 2** User has to mention dependency-jars local:///home/hhuser/distr/lib/node-0.7.0.jar,/home/hhuser/distr/lib_client/hhrdd-0.7.0.jar.
### Command :
    ./bin/spark-submit --class <job-main-class> --master spark://<master-ip>:<port> --jars <dependency-jars> <application-jar> [application-arguments]
						 
### Command line arguments descriptions :
8 GB Memory / 80 GB Disk / NYC1 - Ubuntu 14.04.5 x64

				 
1. job-main-class : main class of client written jobs. e.g com.talentica.hungryhippos.examples.SumJob 
2. master-ip : spark master ip(First Entry in file ~/HungryHippos/hhspark_automation/scripts/ip_file.txt is master-ip).
3. port : configured spark master port number.  
4. dependency-jars : all dependency jars with comma separated such as 
  local:///home/hhuser/distr/lib/node-0.7.0.jar,/home/hhuser/distr/lib_client/hhrdd-0.7.0.jar.	  
5. application-jar : The jar where the hungryhippos job is implemented.
6. application-arguments : Arguments passed to the main method of your main class, if any
 	  
### Example :						 
	
    ./bin/spark-submit --class com.talentica.hungryhippos.examples.SumJob --master spark://67.205.156.149:9091 --jars local:///home/hhuser/distr/lib/node-0.7.0.jar,/home/hhuser/distr/lib_client/hhrdd-0.7.0.jar /home/hhuser/distr/lib_client/examples-0.7.0.jar spark://67.205.156.149:9091 SumJobType /dir/input /home/hhuser/distr/config/client-config.xml /dir/outputSumJob >logs/SumJob.out 2>logs/SumJob.err &
    
   **com.talentica.hungryhippos.examples.SumJob** is the class where addition Job is defined.
   it takes 4 argument first is the *spark-master ip with port* , *application name* ,*client-config* and */dir/outputSumJob*
   location to save the output file in the cluster*.
   
### Utility
  
  1. go to hhspark_automation/scripts.
   
          cd hhspark_automation/scripts
| Command | Functionality | 
| ---- |----- | 
| ./hh-download.sh | To download the output file of jobs|
| ./kill-all.sh | To kill the data distributor process running on the cluster or To remove input directory|
| ./transfer.sh | To start the data distributor process on the cluster or To copy the distr directory|
| ./server-status.sh | To check the nodes where data distributor is running. Failed server details is also shown |
| ./destroy-vagrant.sh| To destroy entire cluster | 
   
