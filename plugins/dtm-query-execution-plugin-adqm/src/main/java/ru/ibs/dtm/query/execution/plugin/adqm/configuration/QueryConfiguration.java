package ru.ibs.dtm.query.execution.plugin.adqm.configuration;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DatabaseTypes;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.DatabaseTypes.CLICKHOUSE;

@Configuration
public class QueryConfiguration {

    @Bean("adqmDatabaseExecutors")
    public Map<DatabaseTypes, DatabaseExecutor> databaseExecutors(AdqmQueryExecutor adqmQueryExecutor) {
        Map<DatabaseTypes, DatabaseExecutor> beanMap = new HashMap<>();
        beanMap.put(CLICKHOUSE, adqmQueryExecutor);
        return beanMap;
    }


    @Bean("adqmQueryExecutor")
    public AdqmQueryExecutor clickhouse(@Qualifier("adqmVertx") Vertx vertx, ClickhouseProperties clickhouseProperties) {
        String url = String.format("jdbc:clickhouse://%s/%s", clickhouseProperties.getHosts(),
                clickhouseProperties.getDatabase());
        Properties props = new Properties();
        props.put("user", clickhouseProperties.getUser());
        props.put("password", clickhouseProperties.getPassword());
        DataSource dataSource = new BalancedClickhouseDataSource(url, props);
        return new AdqmQueryExecutor(vertx, dataSource);
    }
}
