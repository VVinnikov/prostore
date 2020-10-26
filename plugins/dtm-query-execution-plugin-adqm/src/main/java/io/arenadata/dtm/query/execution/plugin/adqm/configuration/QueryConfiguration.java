package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.datasource.AdqmBalancedClickhouseDataSource;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
public class QueryConfiguration {

    @Bean("adqmQueryExecutor")
    public AdqmQueryExecutor clickhouse(@Qualifier("coreVertx") Vertx vertx,
                                        ClickhouseProperties clickhouseProperties,
                                        @Qualifier("adqmTypeToSqlTypeConverter") SqlTypeConverter typeConverter) {
        String url = String.format("jdbc:clickhouse://%s/%s", clickhouseProperties.getHosts(),
                clickhouseProperties.getDatabase());
        Properties props = new Properties();
        props.put("user", clickhouseProperties.getUser());
        props.put("password", clickhouseProperties.getPassword());
        DataSource dataSource = new AdqmBalancedClickhouseDataSource(url, props);
        return new AdqmQueryExecutor(vertx, dataSource, typeConverter);
    }
}
