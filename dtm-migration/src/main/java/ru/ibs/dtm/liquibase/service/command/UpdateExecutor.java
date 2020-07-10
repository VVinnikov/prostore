package ru.ibs.dtm.liquibase.service.command;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.liquibase.service.LiquibaseCommandExecutor;
import ru.ibs.dtm.liquibase.model.LiquibaseCommand;
import ru.ibs.dtm.liquibase.model.LiquibaseContext;

@Service
public class UpdateExecutor implements LiquibaseCommandExecutor {

    @Override
    public void execute(SpringLiquibase springLiquibase, LiquibaseContext context) throws LiquibaseException {
        Liquibase liquibase = context.getLiquibase();
        if (springLiquibase.isClearCheckSums()) {
            liquibase.clearCheckSums();
        }
        if (springLiquibase.isTestRollbackOnUpdate()) {
            if (springLiquibase.getTag() != null) {
                liquibase.updateTestingRollback(springLiquibase.getTag(), new Contexts(springLiquibase.getContexts()), new LabelExpression(springLiquibase.getLabels()));
            } else {
                liquibase.updateTestingRollback(new Contexts(springLiquibase.getContexts()), new LabelExpression(springLiquibase.getLabels()));
            }
        } else if (springLiquibase.getTag() != null) {
            liquibase.update(springLiquibase.getTag(), new Contexts(springLiquibase.getContexts()), new LabelExpression(springLiquibase.getLabels()));
        } else {
            liquibase.update(new Contexts(springLiquibase.getContexts()), new LabelExpression(springLiquibase.getLabels()));
        }
    }

    @Override
    public LiquibaseCommand getCommand() {
        return LiquibaseCommand.UPDATE;
    }
}
