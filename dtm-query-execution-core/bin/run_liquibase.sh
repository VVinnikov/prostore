#!/usr/bin/env bash

usage() {
  echo -e "Usage: bin/run_liquibase.sh <env>\nWhere env - name of environment, such as local, dev, ...\n"
  exit 1
}

if [ "$#" -ne 1 ]; then
  usage
fi

LENV="$1"

if [ ! -f "bin/${LENV}.env.sh" ]; then
  echo -e "Cannot find environment script for ${LENV}. Quitting\n"
  exit 1
fi

source "bin/${LENV}.env.sh"

mvn -P"${LENV}"\
    liquibase:update\
    -Dliquibase.username="${SERVICEDB_USER}"\
    -Dliquibase.password="${SERVICEDB_PASS}"\
    -Dliquibase.driver=org.mariadb.jdbc.Driver\
    -Dliquibase.url="jdbc:mysql://${SERVICEDB_HOST}:${SERVICEDB_PORT}/${SERVICEDB_DB_NAME}"