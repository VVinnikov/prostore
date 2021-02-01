package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.model.SchemaInfo;
import io.arenadata.dtm.jdbc.model.TableInfo;
import io.arenadata.dtm.jdbc.protocol.Protocol;
import io.arenadata.dtm.jdbc.protocol.http.HttpReaderService;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.IntStream;

public class QueryExecutorImpl implements QueryExecutor {
    /**
     * Host
     */
    private String host;
    /**
     * User
     */
    private String user;
    /**
     * Schema for current connection
     */
    private String schema;
    /**
     * Connection url
     */
    private String url;
    /**
     * Properties of current connection
     */
    private Properties info;
    /**
     * Http client for rest
     */
    private CloseableHttpClient client;
    /**
     * Protocol for receiving/sending data
     */
    protected Protocol protocol;


    public QueryExecutorImpl(String host, String user, String schema, Properties info) {
        this.host = host;
        this.user = user;
        this.schema = schema;
        this.info = info;
        this.client = HttpClients.createDefault();
        this.protocol = new HttpReaderService(this.client, this.host);
    }

    @Override
    public void execute(Query query, QueryParameters parameters, ResultHandler resultHandler) {
        executeInternal(query, parameters, resultHandler);
    }

    @Override
    public void execute(List<Query> queries, List<QueryParameters> parametersList, ResultHandler resultHandler) {
        try {
            for (int i = 0; i < queries.size(); i++) {
                QueryParameters parameters = parametersList.isEmpty() ? null : parametersList.get(i);
                executeInternal(queries.get(i), parameters, resultHandler);
            }
        } catch (Exception e) {
            resultHandler.handleError(new SQLException("Error executing queries", e));
        }
    }

    @Override
    public void prepareQuery(Query query, ResultHandler resultHandler) {
        try {
            QueryRequest queryRequest = prepareQueryRequest(query.getNativeSql(), null);
            this.protocol.prepareQuery(queryRequest);
        } catch (SQLException e) {
            resultHandler.handleError(e);
        }
    }

    private void executeInternal(Query query, QueryParameters parameters, ResultHandler resultHandler) {
        try {
            final QueryResult queryResult;
            QueryRequest queryRequest = prepareQueryRequest(query.getNativeSql(), parameters);
            queryResult = this.protocol.executeQuery(queryRequest);
            if (queryResult.getResult() != null) {
                setUsedSchemaIfExists(queryResult);
                List<Field[]> result = new ArrayList<>();
                List<Map<String, Object>> rows = queryResult.getResult();
                List<ColumnMetadata> metadata = queryResult.getMetadata() == null ?
                        Collections.emptyList() : queryResult.getMetadata();
                rows.forEach(row -> {
                    Field[] resultFields = new Field[row.size()];
                    IntStream.range(0, queryResult.getMetadata().size()).forEach(key -> {
                        String columnName = queryResult.getMetadata().get(key).getName();
                        resultFields[key] = new Field(columnName, row.get(columnName));
                    });
                    result.add(resultFields);
                });
                resultHandler.handleResultRows(query, result, metadata, ZoneId.of(queryResult.getTimeZone()));
            }
        } catch (SQLException e) {
            resultHandler.handleError(e);
        }
    }

    private void setUsedSchemaIfExists(QueryResult result) throws DtmSqlException {
        if (result.getMetadata() != null && result.getMetadata().size() == 1
                && SystemMetadata.SCHEMA == result.getMetadata().get(0).getSystemMetadata()) {
            if (!result.isEmpty()) {
                final Optional<Object> schemaOptional = result.getResult().get(0).values().stream().findFirst();
                if (schemaOptional.isPresent()) {
                    this.schema = schemaOptional.get().toString();
                } else {
                    throw new DtmSqlException("Schema value not found!");
                }
            } else {
                throw new DtmSqlException("Empty result for using schema!");
            }
        }
    }

    @Override
    public List<Query> createQuery(String sql) throws SQLException {
        return SqlParser.parseSql(sql);
    }

    @Override
    public List<SchemaInfo> getSchemas() {
        return this.protocol.getDatabaseSchemas();
    }

    @Override
    public List<TableInfo> getTables(String schema) {
        return this.protocol.getDatabaseTables(schema);
    }

    @Override
    public List<ColumnInfo> getTableColumns(String schema, String table) {
        return this.protocol.getDatabaseColumns(schema, table);
    }

    @Override
    public String getUser() {
        return this.user;
    }

    @Override
    public String getDatabase() {
        return this.schema;
    }

    @Override
    public void setDatabase(String database) {
        this.schema = database;
    }

    @Override
    public String getServerVersion() {
        return "3.3.0";
    }

    @Override
    public String getUrl() {
        return this.url;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public boolean isClosed() {
        return this.client == null;
    }

    @Override
    public void close() {
        try {
            client.close();
            client = null;
            protocol = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private QueryRequest prepareQueryRequest(String sql, QueryParameters parameters) {
        return new QueryRequest(UUID.randomUUID(), this.schema, sql, parameters);
    }
}
