package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ClickhouseProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.datasource.AdqmBalancedClickhouseDataSource;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query.AdqmQueryExecutor;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import javax.sql.DataSource;

@Configuration
public class QueryConfiguration {

    @Bean("adqmQueryExecutor")
    public AdqmQueryExecutor clickhouse(@Qualifier("coreVertx") Vertx vertx,
                                        ClickhouseProperties clickhouseProperties,
                                        @Qualifier("adqmTypeToSqlTypeConverter") SqlTypeConverter adqmTypeConverter,
                                        @Qualifier("adqmTypeFromSqlTypeConverter") SqlTypeConverter sqlTypeConverter) {
        String url = String.format("jdbc:clickhouse://%s/%s", clickhouseProperties.getHosts(),
            clickhouseProperties.getDatabase());
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(clickhouseProperties.getUser());
        properties.setPassword(clickhouseProperties.getPassword());
        properties.setSocketTimeout(clickhouseProperties.getSocketTimeout());
        properties.setDataTransferTimeout(clickhouseProperties.getDataTransferTimeout());
        DataSource dataSource = new AdqmBalancedClickhouseDataSource(url, properties);
        return new AdqmQueryExecutor(vertx, dataSource, adqmTypeConverter, sqlTypeConverter);
    }
}
