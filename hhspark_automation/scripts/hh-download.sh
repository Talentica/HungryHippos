#!/bin/bash

# This script is used for downloading output files from the cluster
# it asks for two input values  one is the relative path of output folder which was created by job
# the second one is where to save the result.

source assign-global-variables.sh
source utility.sh

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
