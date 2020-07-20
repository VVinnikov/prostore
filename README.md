# DTM core services & plugins
Main project of data mart for NSUD.

## Local development

### Run internal DB & kafka

Docker compose is required to run services on local:
```
docker-compose -f dtm-query-execution-core/environment/docker-compose-local.yml up -d
```

It runs:
* MariaDB
* Zookeeper
* Kafka
* Postgres as emulator of ADB

### Load initial data
To load schema changes build sub-project [dtm-migration](dtm-migration/README.md) and run:
```
java -jar dtm-migration-2.0.0-SNAPSHOT.jar
```

### Build & run main service

Build root project with profile `local`.

Setup configuration for core application:
 1. Working dir - `dtm-query-execution-core`.
 2. Main class - `ru.ibs.dtm.query.execution.core.ServiceQueryExecutionApplication`.
 3. VM options - `-Dspring.config.location=classpath:/application.yml,./doc/remote/config/adb/application-adb.yml,./doc/remote/config/adg/application-adg.yml,./doc/remote/config/adqm/application-adqm.yml`.
 4. Environment variables (if nessessary):
    - ZOOKEEPER_HOSTS=127.0.0.1
    - KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
    - ADB_HOST=10.92.3.14
    - TARANTOOL_DB_HOST=10.92.3.12
    - ADQM_HOSTS=10.92.3.24:8123,10.92.3.34:8123
    - ...

###Setup JDBC test client

Use [DTM JDBC driver](https://github.com/arenadata/dtm-jdbc-driver).
URL is `jdbc:adtm://<host>:<port>/`:
 - `host` is host of dtm-query-execution-core (`localhost`)
 - `port` is port of dtm-query-execution-core (see active `application.yml` for dtm-query-execution-core)
