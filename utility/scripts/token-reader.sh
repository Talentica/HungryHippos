#!/bin/bash

get_vagrant_values_associated_with_key(){
file=$1
name=$2

if [ -f "$file" ]
then

  while IFS='=' read -r key value
  do
    key=$key
    value=$value
    if [[ $key == *$name* ]]; then
         echo $value
         return
        
    fi
  done < "$file"

fi
}

