#!/bin/bash
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

# This script is used for downloading output files from the cluster
# it asks for two input values  one is the relative path of output folder which was created by job
# the second one is where to save the result.

source assign-global-variables.sh
source ../distr_original/sbin/utility.sh

#getting hungryhippos file system base folder path.
get_hh_base_folder

#echo $base_path
export distrOutputPath
export downloadLoc
export outputFolderName

echo "Enter distributed OutputPath"
read distrPath
distrOutputPath=$base_path/$distrPath
echo $distrOutputPath
echo "Enter the download location"
read loc
downloadLoc=$loc

echo "starting ssh-agent"
add_ssh_key
echo "started ssh-agent"


IFS='/' read -a folderName <<< "$distrOutputPath"

outputFolderName=${folderName[${#folderName[@]}-1]}
#echo $outputFolderName
ips=($(awk -F ':' '{print $1}' ip_file.txt))
for i in "${ips[@]}"
do
               mkdir -p $downloadLoc/${outputFolderName}	
               echo "downloading output file from $i"
              ssh -o StrictHostKeyChecking=no  hhuser@$i "cd $distrOutputPath  && find -type f -maxdepth 5 -size +0 -not -path '*/\.*' -exec cat {} +" >> $downloadLoc/${outputFolderName}/result-$i.txt
              
              file_size_kb=`du -k "$downloadLoc/${outputFolderName}/result-$i.txt" | cut -f1`
              if [ $file_size_kb -eq 0 ]
              then  
 	                rm -f $downloadLoc/${outputFolderName}/result-$i.txt
              fi

              echo "finished downloading output file  from $i"

  
 
done

#rm output_dirs.txt
