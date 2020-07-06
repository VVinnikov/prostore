#!/usr/bin/env bash

# Service db
export SERVICEDB_DB_NAME=s_db_demo
export SERVICEDB_USER=s_demo
export SERVICEDB_PASS=Coin2Excels3Funder
export SERVICEDB_HOST=database-1.dtm.local
export SERVICEDB_PORT=3306
export WRITER_LOG_LEVEL=TRACE
export EDML_DATASOURCE=ADB
export EDML_DEFAULT_CHUNK_SIZE=1000

# For ADB plugin
export KAFKA_BOOTSTRAP_SERVERS=kafka-3.dtm.local:9092
export ADB_DB_NAME=adb_demo
export ADB_USERNAME=demo
export ADB_PASS=Usurer0Mold1Firmer
export ADB_HOST=adbmaster-1.dtm.local
export ZOOKEEPER_HOSTS=kafka-3.dtm.local
export KAFKA_CLUSTER_ROOTPATH=arenadata/cluster/24
export ADB_MPPR_CONNECTOR_HOST=demo-1.dtm.local
export ADB_MPPR_CONNECTOR_PORT=8086

# For ADG plugin
export TARANTOOL_DB_HOST=tarantool-3.dtm.local
export TARANTOOL_DB_PORT=3311
export TARANTOOL_DB_USER=admin
export TARANTOOL_DB_PASS=321
export TARANTOOL_CATRIDGE_URL=http://tarantool-3.dtm.local:8811
export SCHEMA_REGISTRY_URL=http://kafka-3.dtm.local:8081
