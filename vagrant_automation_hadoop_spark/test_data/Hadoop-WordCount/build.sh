#*******************************************************************************
# Copyright 2017 Talentica Software Pvt. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*******************************************************************************
if [ ! -d classes ]; then
        mkdir classes;
fi

# Compile WordCount
javac -classpath $HADOOP_HOME/hadoop-core-0.20.203.0.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d ./classes WordCount.java

# Create the Jar
jar -cvf wordcount.jar -C ./classes/ .
 
# Copy the jar file to the Hadoop distributions
cp wordcount.jar $HADOOP_HOME/bin/ 

