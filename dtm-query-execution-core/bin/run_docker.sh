#!/usr/bin/env bash

docker run -d -v ./doc/remote/config:/app/config:Z -p 8088:8088 --name dtm dtm-core:latest