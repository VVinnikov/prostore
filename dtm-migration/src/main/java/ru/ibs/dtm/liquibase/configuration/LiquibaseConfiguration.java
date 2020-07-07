package ru.ibs.dtm.liquibase.configuration;

import liquibase.integration.spring.SpringLiquibase;
import lombok.extern.slf4j.Slf4j;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.liquibase.configuration.properties.MariaProperties;
import ru.ibs.dtm.liquibase.factory.LiquibaseContextFactory;
import ru.ibs.dtm.liquibase.factory.LiquibaseContextFactoryImpl;
import ru.ibs.dtm.liquibase.model.DtmLiquibase;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
@Slf4j
public class LiquibaseConfiguration {

    @Bean
    public LiquibaseContextFactory contextFactory() {
        return new LiquibaseContextFactoryImpl();
    }

    @Bean("liquibaseService")
    public SpringLiquibase springLiquibaseService(@Qualifier("serviceDs") DataSource dataSource,
                                                  @Value("${liquibase.change-log-location.serviceDb}") String path,
                                                  @Value("${liquibase.command}") String command,
                                                  @Value("${spring.liquibase.enabled}") boolean isEnabled) {
        SpringLiquibase liquibase = new DtmLiquibase(contextFactory().create(command.toUpperCase()));
        liquibase.setDataSource(dataSource);
        liquibase.setChangeLog(path);
        liquibase.setShouldRun(isEnabled);
        return liquibase;
    }

    @Bean("serviceDs")
    public DataSource serviceDs(MariaProperties properties) throws SQLException {
        MariaDbPoolDataSource dataSource = new MariaDbPoolDataSource();
        dataSource.setDatabaseName(properties.getDatabase());
        dataSource.setMaxPoolSize(properties.getMaxPoolSize());
        dataSource.setServerName(properties.getHost());
        dataSource.setPortNumber(properties.getPort());
        dataSource.setPassword(properties.getPassword());
        dataSource.setUser(properties.getUser());
        log.info("Параметры подключения к сервисной бд: {}", dataSource);
        return dataSource;
    }
}
