package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.jdbc.model.ColumnInfo;

import java.sql.Connection;
import java.util.List;

public interface BaseConnection extends Connection {

    String getUrl();

    String getUserName();

    String getDBVersionNumber();

    List<ColumnInfo> getCachedFieldMetadata();

    QueryExecutor getQueryExecutor();

}
