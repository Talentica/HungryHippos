#!/bin/bash

find . -type f -name $1 -print0 |  xargs -0 -I '{}' sh -c 'jar tf {} | grep Application.class &&  echo {}'
