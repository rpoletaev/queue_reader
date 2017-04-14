#!/bin/bash

CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /home/roma/projects/go/src/gitlab.com/garanteka/goszakupki/docker-nmp/queue_reader/queue_reader .