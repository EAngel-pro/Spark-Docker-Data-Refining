#! /bin/bash

docker build . -t sparkhome

docker run -p 8888:8888 --name spark -d sparkhome

docker run -p 8888:8888 -p 4041:4040 --name spark -d sparkhome