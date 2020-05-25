# dtm
Data mart for NSUD

### Local development

Docker compose to run required services on local - dtm-query-execution-core/environment/docker-compose-local.yml

It's run
* MariaDB
* Zookeeper
* Kafka

To deploy Liquibase scripts for local development, run
```shell script
cd dtm-query-execution-core
bin/run_liquibase.sh local
```

It's up local docker compose and load schema changes from dtm-query-execution-core
