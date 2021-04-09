package io.arenadata.dtm.jdbc.core;

import java.sql.Connection;

public interface BaseConnection extends Connection {

    String getUrl();

    String getUserName();

    String getDBVersionNumber();

    QueryExecutor getQueryExecutor();

    TypeInfo getTypeInfo();

}
