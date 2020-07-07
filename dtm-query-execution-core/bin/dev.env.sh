#!/usr/bin/env bash

# Service db
export SERVICEDB_DB_NAME=dtmservice
export SERVICEDB_USER=dtmuser
export SERVICEDB_PASS=Cat6Gmt7Ncr
export SERVICEDB_HOST=10.18.84.17
export SERVICEDB_PORT=3306
export WRITER_LOG_LEVEL=TRACE
export EDML_DATASOURCE=ADB
export EDML_DEFAULT_CHUNK_SIZE=1000

# For ADB plugin
export KAFKA_BOOTSTRAP_SERVERS=kafka-1.dtm.local:9092
export ADB_DB_NAME=adb
export ADB_USERNAME=gpadmin
export ADB_PASS=
export ADB_HOST=10.18.84.12
export ZOOKEEPER_HOSTS=kafka-1.dtm.local
export KAFKA_CLUSTER_ROOTPATH=arenadata/cluster/21
export ADB_MPPR_CONNECTOR_HOST=10.18.84.18
export ADB_MPPR_CONNECTOR_PORT=8086

# For ADG plugin
export TARANTOOL_DB_HOST=10.18.84.10
export TARANTOOL_DB_PORT=3311
export TARANTOOL_DB_USER=admin
export TARANTOOL_DB_PASS=123
export TARANTOOL_CATRIDGE_URL=http://10.18.84.10:8811
export SCHEMA_REGISTRY_URL=http://10.18.84.6:8081
