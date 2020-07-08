package ru.ibs.dtm.liquibase.factory;

import liquibase.Liquibase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.liquibase.model.LiquibaseCommand;
import ru.ibs.dtm.liquibase.model.LiquibaseContext;

@Component
@Slf4j
public class LiquibaseContextFactoryImpl implements LiquibaseContextFactory {

    @Override
    public LiquibaseContext create(String commandStr, Liquibase liquibase) {
        try {
            LiquibaseCommand command = LiquibaseCommand.valueOf(commandStr.toUpperCase());
            return new LiquibaseContext(command, liquibase);
        } catch (Exception e) {
            log.error("Некорректный параметр запуска! Доступны следующие команды: {}", LiquibaseCommand.values(), e);
            throw e;
        }
    }
}
