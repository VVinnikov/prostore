package io.arenadata.dtm.liquibase.service;

import io.arenadata.dtm.liquibase.model.LiquibaseCommand;
import io.arenadata.dtm.liquibase.model.LiquibaseContext;
import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;

public interface LiquibaseCommandExecutor {

    void execute(SpringLiquibase dtmLiquibase, LiquibaseContext context) throws LiquibaseException;

    LiquibaseCommand getCommand();
}
