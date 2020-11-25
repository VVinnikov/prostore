package io.arenadata.dtm.query.execution.core.integration.query.client;

import io.vertx.ext.sql.SQLClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SqlClientProviderImpl implements SqlClientProvider {

    private final SqlClientFactory sqlClientFactory;
    private final Map<String, SQLClient> sqlClientMap = new ConcurrentHashMap<>();

    @Autowired
    public SqlClientProviderImpl(SqlClientFactory sqlClientFactory) {
        this.sqlClientFactory = sqlClientFactory;
    }

    @Override
    public SQLClient get(String datamart) {
        final SQLClient sqlClient = sqlClientMap.get(datamart);
        if (sqlClient == null) {
            final SQLClient newSqlClient = sqlClientFactory.create(datamart);
            sqlClientMap.put(datamart, newSqlClient);
            return newSqlClient;
        } else {
            return sqlClient;
        }
    }
}
