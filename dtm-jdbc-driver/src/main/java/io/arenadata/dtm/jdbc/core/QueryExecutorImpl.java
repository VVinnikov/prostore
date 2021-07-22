package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.common.model.ddl.SystemMetadata;
import io.arenadata.dtm.jdbc.model.ColumnInfo;
import io.arenadata.dtm.jdbc.model.SchemaInfo;
import io.arenadata.dtm.jdbc.model.TableInfo;
import io.arenadata.dtm.jdbc.protocol.Protocol;
import io.arenadata.dtm.jdbc.protocol.http.HttpReaderService;
import io.arenadata.dtm.jdbc.util.DriverInfo;
import io.arenadata.dtm.jdbc.util.DtmSqlException;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.IntStream;

public class QueryExecutorImpl implements QueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutorImpl.class);
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
     * Http client for rest
     */
    private CloseableHttpClient client;
    /**
     * Chain of accumulated warnings
     */
    private SQLWarning warnings;
    /**
     * Protocol for receiving/sending data
     */
    protected Protocol protocol;


    public QueryExecutorImpl(String host, String user, String schema) {
        this.host = host;
        this.user = user;
        this.schema = schema;
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
                List<Map<String, Object>> rows = queryResult.getResult();
                List<ColumnMetadata> metadata = queryResult.getMetadata() == null ?
                        Collections.emptyList() : queryResult.getMetadata();
                final Field[] fields = new Field[metadata.size()];
                final List<Tuple> tuples = new ArrayList<>();
                IntStream.range(0, metadata.size()).forEach(n -> {
                    ColumnMetadata md = metadata.get(n);
                    fields[n] = new Field(md.getName(), md.getSize(), md.getType(), null);
                });
                rows.forEach(row -> {
                    Tuple tuple = new Tuple(metadata.size());
                    IntStream.range(0, queryResult.getMetadata().size()).forEach(key -> {
                        String columnName = queryResult.getMetadata().get(key).getName();
                        tuple.set(key, row.get(columnName));
                    });
                    tuples.add(tuple);
                });
                resultHandler.handleResultRows(query, fields, tuples, ZoneId.of(queryResult.getTimeZone()));
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
        return DriverInfo.DRIVER_VERSION;
    }

    @Override
    public String getUrl() {
        return this.url;
    }

    @Override
    public SQLWarning getWarnings() {
        SQLWarning chain = warnings;
        warnings = null;
        return chain;
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
            LOGGER.error("Error in closing client connection", e);
        }
    }

    private QueryRequest prepareQueryRequest(String sql, QueryParameters parameters) {
        return new QueryRequest(UUID.randomUUID(), this.schema, sql, parameters);
    }
}
