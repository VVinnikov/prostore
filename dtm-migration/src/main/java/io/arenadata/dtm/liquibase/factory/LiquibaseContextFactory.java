package io.arenadata.dtm.liquibase.factory;

import io.arenadata.dtm.liquibase.model.LiquibaseContext;
import liquibase.Liquibase;

public interface LiquibaseContextFactory {

    LiquibaseContext create(String command, Liquibase liquibase);
}
