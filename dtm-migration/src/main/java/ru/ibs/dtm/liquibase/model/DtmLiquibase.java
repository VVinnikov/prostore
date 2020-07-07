package ru.ibs.dtm.liquibase.model;

import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DtmLiquibase extends SpringLiquibase {

    private LiquibaseContext context;

    @Override
    protected void performUpdate(Liquibase liquibase) throws LiquibaseException {
        if (LiquibaseCommand.UPDATE.equals(context.getCommand())) {
            super.performUpdate(liquibase);
        }
    }
}
