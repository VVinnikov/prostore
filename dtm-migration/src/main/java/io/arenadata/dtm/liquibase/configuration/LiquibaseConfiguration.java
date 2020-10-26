package io.arenadata.dtm.liquibase.configuration;

import io.arenadata.dtm.liquibase.configuration.properties.DatasourceProperties;
import lombok.extern.slf4j.Slf4j;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
@Slf4j
public class LiquibaseConfiguration {

    @Bean("serviceDs")
    public DataSource serviceDs(DatasourceProperties properties) throws SQLException {
        MariaDbPoolDataSource dataSource = new MariaDbPoolDataSource();
        dataSource.setDatabaseName(properties.getDatabase());
        dataSource.setMaxPoolSize(properties.getMaxPoolSize());
        dataSource.setServerName(properties.getHost());
        dataSource.setPortNumber(properties.getPort());
        dataSource.setPassword(properties.getPassword());
        dataSource.setUser(properties.getUsername());
        log.info("Параметры подключения к сервисной бд: database:{}; host:{}; port:{}; user:{}", dataSource.getDatabaseName(),
                dataSource.getServerName(), dataSource.getPort(), dataSource.getUser());
        return dataSource;
    }

}
