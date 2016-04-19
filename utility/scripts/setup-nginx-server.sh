#!/bin/bash
 echo "Setting up ngnix server on  $1"
 ssh -o StrictHostKeyChecking=no root@$1 'sudo mkdir -p /usr/local/nginx/html;echo "Start Updating apt get";sudo apt-get update;echo "Finished Updating apt get";echo "Start installing nginx server";sudo apt-get install nginx;echo "Finished installing nginx server";echo "Starting nginx server";sudo service nginx start;echo "Nginx 	server started"';


