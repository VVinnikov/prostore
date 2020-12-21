package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.jdbc.util.DtmSqlException;

import java.sql.SQLException;
import java.util.Properties;

public abstract class ConnectionFactory {

    public ConnectionFactory() {
    }

    public static QueryExecutor openConnection(String host, String user, String database, String url, Properties info) throws SQLException {
            ConnectionFactory connectionFactory = new ConnectionFactoryImpl();
            QueryExecutor queryExecutor = connectionFactory.openConnectionImpl(host, user, database, url, info);
            if (queryExecutor != null) {
                return queryExecutor;
            } else {
                throw new DtmSqlException("Can't create query executor");
            }
    }

    public abstract QueryExecutor openConnectionImpl(String host, String user, String database, String url, Properties info) throws SQLException;
}
