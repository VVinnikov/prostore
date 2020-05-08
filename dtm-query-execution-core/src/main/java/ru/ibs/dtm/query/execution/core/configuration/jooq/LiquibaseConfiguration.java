package ru.ibs.dtm.query.execution.core.configuration.jooq;

import liquibase.integration.spring.SpringLiquibase;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
public class LiquibaseConfiguration {

  @Bean("coreLiquibaseService")
  public SpringLiquibase springLiquibaseService(@Qualifier("serviceDs") DataSource dataSource,
                                                @Value("${liquibase.change-log-location.serviceDb}") String path,
                                                @Value("${spring.liquibase.enabled}") boolean isEnabled
  ) {
    SpringLiquibase liquibase = new SpringLiquibase();
    liquibase.setDataSource(dataSource);
    liquibase.setChangeLog(path);
    liquibase.setShouldRun(isEnabled);
    return liquibase;
  }

  @Bean("serviceDs")
  public DataSource serviceDs(MariaProperties properties) throws SQLException {
    MariaDbPoolDataSource dataSource = new MariaDbPoolDataSource();
    dataSource.setDatabaseName(properties.getOptions().getDatabase());
    dataSource.setMaxPoolSize(properties.getPoolOptions().getMaxSize());
    dataSource.setServerName(properties.getOptions().getHost());
    dataSource.setPortNumber(properties.getOptions().getPort());
    dataSource.setPassword(properties.getOptions().getPassword());
    dataSource.setUser(properties.getOptions().getUser());
    return dataSource;
  }
}
