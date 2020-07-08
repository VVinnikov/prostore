package ru.ibs.dtm.liquibase.factory;

import liquibase.Liquibase;
import ru.ibs.dtm.liquibase.model.LiquibaseContext;

public interface LiquibaseContextFactory {

    LiquibaseContext create(String command, Liquibase liquibase);
}
