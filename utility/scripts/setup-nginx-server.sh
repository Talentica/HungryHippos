#!/bin/bash

echo "Start Updating apt get"
sudo apt-get update
echo "Finished Updating apt get"

echo "Start installing nginx server"
sudo apt-get install nginx
echo "Finished installing nginx server"

echo "Starting nginx server"
sudo service nginx start
echo "Nginx server started"

