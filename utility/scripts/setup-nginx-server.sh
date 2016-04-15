#!/bin/bash

echo "Start Updating apt get"
apt-get update
echo "Finished Updating apt get"

echo "Start installing nginx server"
apt-get install nginx
echo "Finished installing nginx server"

echo "Starting nginx server"
sudo ervice nginx start
echo "Nginx server started"

