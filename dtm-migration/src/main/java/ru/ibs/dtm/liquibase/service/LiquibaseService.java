package ru.ibs.dtm.liquibase.service;

import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.integration.spring.SpringLiquibase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.liquibase.configuration.properties.LiquibaseProperties;
import ru.ibs.dtm.liquibase.factory.LiquibaseContextFactory;
import ru.ibs.dtm.liquibase.model.LiquibaseCommand;
import ru.ibs.dtm.liquibase.model.LiquibaseContext;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class LiquibaseService extends SpringLiquibase {

    private final Map<LiquibaseCommand, LiquibaseCommandExecutor> executorMap;
    private final LiquibaseContextFactory liquibaseContextFactory;
    private final LiquibaseProperties liquibaseProperties;

    @Autowired
    public LiquibaseService(@Qualifier("serviceDs") DataSource dataSource,
                               @Value("${spring.liquibase.enabled}") boolean isEnabled, LiquibaseContextFactory liquibaseContextFactory,
                               List<LiquibaseCommandExecutor> executors, LiquibaseProperties liquibaseProperties) {
        this.liquibaseProperties = liquibaseProperties;
        this.setDataSource(dataSource);
        this.setChangeLog(liquibaseProperties.getChangeLog().get("serviceDb"));
        this.setShouldRun(isEnabled);
        this.executorMap = executors.stream().collect(Collectors.toMap(LiquibaseCommandExecutor::getCommand, it -> it));
        this.liquibaseContextFactory = liquibaseContextFactory;
    }

    @Override
    protected void performUpdate(Liquibase liquibase) throws LiquibaseException {
        LiquibaseContext context = liquibaseContextFactory.create(liquibaseProperties.getCommand(), liquibase);
        log.debug("Получена команда для выполнения: {}", context.getCommand());
        executorMap.get(context.getCommand()).execute(this, context);
    }

}
