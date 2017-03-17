
This readme contains prerequisite and basic installation details.


### Prerequisite

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
   

### Installation of Prerequisite software

1. you can install all prerequsite software by running ./install.sh  or  individual scripts. (supported on ubuntu)

   1.1 cd basic_install_scripts
   
   1.2 ./install-all.sh 
   
   1.2.1 install-*.sh to install respective software.
   
   NOTE :- If you have Java or Ruby already installed it will be better you install the software individually. Else it will   
      override you Ruby and Java to latest version.
2. for other distribution please follow the instructions provided by respectice software companies.

### Build the project:

1. gradle clean build
2. jar file of each module will be created in respective modules/build/libs.
3. cp node/build/libs/node-*.jar hhspark_automation/distr_original/lib

### Setting up the project.

1.  cd hhspark_automation ; #go to hhspark automation.
2.  please refer hhspark_automation/readme for further steps.

    https://github.com/Talentica/HungryHippos/tree/modularization-code-cleanup/hhspark_automation/Readme.md

### After execution of the script.

1. spark will be downloaded on all servers.
2. java will be installed on all servers.
3. chef-solo will be installed on all servers
4. zokeeper will be installed on the specified number of nodes , can be standalone or as cluster depending on the number of nodes.

8. Server side O/S : Ubuntu 14.04

          ideal RAM: 8 GB
          HARD DISK: Depends on Data size of the file to be distributed.
          ideal No.of cores per machine: 4


### Data publish.
Data publish module allows the user to publish large data set across the cluster of machines from client machine.
This distributed data become eligible to get executed during job execution.

Data publish jar will be availaible in installation package of the Hungry Hippos. Execute the following command to get start with data publish.

	java -cp data-publisher-<varsion>.jar <main-class> <client-config.xml> <input-data> <relative-distributed-directory-path> <optional-args>
            
    Description about above arguments provided -
              
    1. varsion  : 	data publish jar version. i.e data-publisher-0.7.0.jar. Here ver means version which is "0.7.0".
    2. main-class 	: 	com.talentica.hungryHippos.master.DataPublisherStarter
    3. client-config.xml   : 	provide the client-config.xml file path which is available in Hungry Hippos installation package. i.e conf/client-config.xml
    4. input-data    : 	provide path of input data set with file name. Currently we support text and csv files only which need be comma seperated.
    5. relative-distributed-path  : 	This path should be exactly same as provided in "sharding-client-config.xml" having field name "distributed-file-path".
    6. optional-args   :  This arguments are optional which is to redirect the logs and also to run the application in background. 
            						     i.e " >  logs/data-publish.out 2> logs/data-publish.err &"
            
            Example  : 
            java -cp data-publisher-0.7.0.jar com.talentica.hungryHippos.master.DataPublisherStarter conf/client-config.xml ~/dataGenerator/sampledata.txt 
            /dir/input > logs/datapub.out 2> logs/datapub.err &
            
            

### Job submission.
As soon as data publish is completed, cluster machines are ready to accept the command to execute the jobs.           																		
To execute the jobs, client should write the jobs and submit it with spark submit command. 
Moreover, you can find the examples as to how to write the jobs in module "examples" with package "com.talentica.hungryHippos.rdd.main"  namely "SumJob" , "MedianJob" and "UniqueCountJob".
Therefore, simply follow the below steps : 
 
	1. Write the job.
	2. Build the module.
	3. Create the jar. Let's say it is "examples-<varsion>.jar".
	4. Transfer above created jar along with dependency jars such as "sharding-<varsion>.jar" and "hhrdd-<varsion>.jar" to spark "master" node  in directory 
	    "/home/hhuser/distr/lib_client".
	5. Run the following command in spark installation directory of master node:
				
	    	./bin/spark-submit --class <job-main-class> --jars <dependency-jars>	 --master spark://<master-ip>:<port> <client-job-jar> spark://<master-ip>:<port> 
			<application-name> <relative-distributed-path> <client-config-xml-path> <output-directory> <optional-args>	.
						 
			 Description about above arguments provided -
						 
			 1. job-main-class 							:	 main class of client written jobs. i.e com.talentica.hungryHippos.rdd.main.SumJob
			 2. dependency-jars 						: 	 all dependency jars with comma separated  such as /home/hhuser/distr/lib_client/sharding-<varsion>.jar , 
			 																 /home/hhuser/distr/lib_client/hhrdd-<varsion>.jar.
			 3. master-ip					    			:    spark master ip.
			 4. port 												:    configured spark master port number.
			 5. application-name  					:    application name for current submission programe.
			 6. relative-distributed-path 		:    This path should be exactly same as provided in "sharding-client-config.xml" having field name "distributed-file-path". 
			 7. client-config-xml-path 			:    client-config.xml file path.
			 8. output-file-name 						:    output directory name wherein the results are stored inside job id subfolder.
			 9. optional-args 							:    This arguments are optional which is to redirect the logs and also to run the application in background. i.e ">../logs/spark.out 2>
			 																../logs/spark.err &"
						 
				 Example :
				 ./bin/spark-submit --class com.talentica.hungryHippos.rdd.main.SumJob --jars /home/hhuser/distr/lib_client/sharding-0.7.0.jar,
				 /home/hhuser/distr/lib_client/hhrdd-0.7.0.jar --master spark://67.205.172.104:9091 /home/hhuser/distr/lib_client/examples-0.7.0.jar 
				 spark://67.205.172.104:9091 hh-sum /dir/input /home/hhuser/distr/config/client-config.xml output >../logs/spark.out 2>../logs/spark.err &
 


