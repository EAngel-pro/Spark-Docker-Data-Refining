#! /bin/bash

docker build . -t sparkhome

docker run -p 8888:8888 --name spark -d sparkhome

