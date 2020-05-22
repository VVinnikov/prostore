#!/usr/bin/env bash

SCRIPT_PATH="$(dirname "$(readlink -f "$0")")"
docker-compose -f "${SCRIPT_PATH}/../environment/docker-compose-local.yml" up -d

# Service db
export SERVICEDB_DB_NAME=dtmservice
export SERVICEDB_USER=dtmuser
export SERVICEDB_PASS=Cat6Gmt7Ncr
export SERVICEDB_HOST=localhost
export SERVICEDB_PORT=3306
export WRITER_LOG_LEVEL=TRACE
export EDML_DATASOURCE=ADB
export EDML_DEFAULT_CHUNK_SIZE=1000

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export ZOOKEEPER_HOSTS=localhost
