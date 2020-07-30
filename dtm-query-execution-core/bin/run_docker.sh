#!/usr/bin/env bash

docker run -d -v ./config:/app/config:Z -p 8088:8088 -e SPRING_PROFILES_ACTIVE=dev --name dtm dtm-core:latest