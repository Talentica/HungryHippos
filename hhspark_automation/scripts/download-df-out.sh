#!/bin/bash
export distrOutputPath
export downloadLoc
export jobDir
export dir
export outputFolderName
export job_id

jobDir="/home/hhuser/hh/output/aggOut"
#echo "Enter distributed OutputPath"
#read distrPath
#distrOutputPath=$distrPath
#echo $distrOutPut
echo "Enter the download location"
read loc
downloadLoc=$loc
#echo "Enter no.of jobs"
#read jobId
#job_id=$jobId

echo "starting ssh-agent"
eval `ssh-agent -s`
echo "started ssh-agent"
echo "adding hhuser identity"
ssh-add hhuser_id_rsa
echo "added hhuser identity"


#IFS='/' read -a folderName <<< "$distrOutputPath"

#outputFolderName=${folderName[${#folderName[@]}-1]}
#echo $outputFolderName
#dir=${distrOutputPath%/${outputFolderName}}
#echo "dir=$dir"
ips=($(awk  -F ':' '{print $1}' ip_file.txt))



for i in "${ips[@]}"
do
        echo "getting result from $i"
     
     #   if [ $j -eq 1 ]           
     #   then
     #		ssh -o StrictHostKeyChecking=no hhuser@$i "ls $dir | grep -i $outputFolderName | wc -l" >> output_dirs.txt
     #           job_id=($(awk '{print $1}' output_dirs.txt ))
     #           echo $job_id
     #   fi
       
        
               mkdir -p $downloadLoc	
               echo "downloading output file for job_id "
              ssh -o StrictHostKeyChecking=no  hhuser@$i "cd $jobDir  && find -type f -maxdepth 5 -size +0 -not -path '*/\.*' -exec cat {} +" >> $downloadLoc/result-$i.txt
              
              file_size_kb=`du -k "$downloadLoc/result-$i.txt" | cut -f1`
              if [ $file_size_kb -eq 0 ]
              then  
 	                rm -f $downloadLoc/result-$i.txt
              fi

              echo "finished downloading output file  from $i"
        
 
done

#rm output_dirs.txt
