#!/usr/bin/env bash

DTM_JAR_NAME=$(find target -iname 'dtm-query-execution-core-*.jar' | head -n 1)
if [ -z "${DTM_JAR_NAME}" ]; then
  echo 'Cannot find DTM JAR. Exiting'
  exit 1
fi

docker build --build-arg DTM_JAR="${DTM_JAR_NAME}" -t ci.arenadata.io/dtm-core:latest .