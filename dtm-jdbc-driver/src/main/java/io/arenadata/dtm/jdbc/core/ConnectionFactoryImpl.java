package io.arenadata.dtm.jdbc.core;

import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactoryImpl extends ConnectionFactory {

    public ConnectionFactoryImpl() {
    }

    @Override
    public QueryExecutor openConnectionImpl(String host, String user, String schema, String url, Properties info) throws SQLException {
        return new QueryExecutorImpl(host, user, schema, info);
    }
}
