package ru.ibs.dtm.liquibase.service;

import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import ru.ibs.dtm.liquibase.model.LiquibaseCommand;
import ru.ibs.dtm.liquibase.model.LiquibaseContext;

public interface LiquibaseCommandExecutor {

    void execute(SpringLiquibase dtmLiquibase, LiquibaseContext context) throws LiquibaseException;

    LiquibaseCommand getCommand();
}
