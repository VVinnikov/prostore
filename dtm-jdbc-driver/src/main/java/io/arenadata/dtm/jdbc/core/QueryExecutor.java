package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.model.SchemaInfo;
import io.arenadata.dtm.jdbc.model.TableInfo;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;

public interface QueryExecutor {

    void execute(Query query, QueryParameters parameters, ResultHandler resultHandler);

    void execute(List<Query> queries, List<QueryParameters> parametersList, ResultHandler resultHandler);

    void prepareQuery(Query query, ResultHandler resultHandler);

    List<Query> createQuery(String sql) throws SQLException;

    List<SchemaInfo> getSchemas();

    List<TableInfo> getTables(String schema);

    List<ColumnInfo> getTableColumns(String schema, String table);

    String getUser();

    String getDatabase();

    void setDatabase(String schema);

    String getServerVersion();

    String getUrl();

    SQLWarning getWarnings();

    boolean isClosed();

    void close();
}
