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

Also you need local or remote ADB, ADG and ADQM.

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
 3. VM options - `-Dspring.config.location=classpath:/application.yml,./doc/remote/config/adb/application.yml,./doc/remote/config/adg/application.yml,./doc/remote/config/adqm/application.yml`.
 4. Environment variables (if nessessary):
    - ZOOKEEPER_HOSTS=127.0.0.1
    - KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
    - ADB_HOST=10.92.3.14
    - TARANTOOL_DB_HOST=10.92.3.12
    - ADQM_HOSTS=10.92.3.24:8123,10.92.3.34:8123
    - ...

### Run main service as a single jar

```
java -Dspring.config.location=classpath:/application.yml,<path-to-adb-config>/applicati.yml,<path-to-adg-config>/application.yml,<path-to-adqm-config>/application.yml -jar target/dtm-query-execution-core-2.2.1-SNAPSHOT.jar
```

###Setup JDBC test client

Use [DTM JDBC driver](https://github.com/arenadata/dtm-jdbc-driver).
URL is `jdbc:adtm://<host>:<port>/`:
 - `host` is host of dtm-query-execution-core (`localhost`)
 - `port` is port of dtm-query-execution-core (see active `application.yml` for dtm-query-execution-core)
