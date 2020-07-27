#!/usr/bin/env bash

docker build --build-arg DTM_JAR=./target/dtm-query-execution-core-2.3.0-SNAPSHOT.jar -t dtm-core:latest .