package ru.ibs.dtm.liquibase.factory;

import ru.ibs.dtm.liquibase.model.LiquibaseContext;

public interface LiquibaseContextFactory {

    LiquibaseContext create(String command);
}
