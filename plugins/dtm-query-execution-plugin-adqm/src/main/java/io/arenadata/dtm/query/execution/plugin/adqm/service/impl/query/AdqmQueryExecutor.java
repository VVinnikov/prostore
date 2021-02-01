package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AdqmQueryExecutor implements DatabaseExecutor {
    private final SQLClient sqlClient;
    private final SqlTypeConverter adqmTypeConverter;
    private final SqlTypeConverter sqlTypeConverter;

    public AdqmQueryExecutor(Vertx vertx,
                             DataSource adqmDataSource,
                             SqlTypeConverter adqmTypeConverter,
                             SqlTypeConverter sqlTypeConverter) {
        this.adqmTypeConverter = adqmTypeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
        this.sqlClient = JDBCClient.create(vertx, adqmDataSource);
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        log.debug(String.format("ADQM. Execute %s", sql));
        //TODO perhaps it's better to use RowStream interface for getting rows one by one and create chunks here
        return getSqlConnection()
                .compose(conn -> executeQuery(conn, sql))
                .map(resultSet -> {
                    try {
                        return createResult(metadata, resultSet);
                    } catch (Exception e) {
                        throw new DataSourceException("Error converting value to jdbc type", e);
                    }
                });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        log.debug(String.format("ADQM. Execute update %s", sql));
        return getSqlConnection()
                .compose(conn -> executeQueryUpdate(conn, sql));
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql,
                                                               QueryParameters params,
                                                               List<ColumnMetadata> metadata) {
        log.debug(String.format("ADQM. Execute with params %s", sql));
        //TODO perhaps it's better to use RowStream interface for getting rows one by one and create chunks here
        return getSqlConnection()
                .compose(conn -> executeQueryWithParams(conn, sql, createParamsArray(params)))
                .map(resultSet -> {
                    try {
                        return createResult(metadata, resultSet);
                    } catch (Exception e) {
                        throw new DataSourceException("Error converting value to jdbc type", e);
                    }
                });
    }

    private JsonArray createParamsArray(QueryParameters params) {
        if (params == null) {
            return new JsonArray(Collections.emptyList());
        } else {
            return new JsonArray(IntStream.range(0, params.getValues().size())
                    .mapToObj(n -> sqlTypeConverter.convert(params.getTypes().get(n),
                            params.getValues().get(n)))
                    .collect(Collectors.toList()));
        }
    }

    private Future<SQLConnection> getSqlConnection() {
        return Future.future(sqlClient::getConnection);
    }

    private Future<ResultSet> executeQuery(SQLConnection conn, String sql) {
        return Future.future(promise -> conn.query(sql, promise));
    }

    private Future<ResultSet> executeQueryWithParams(SQLConnection conn, String sql, JsonArray params) {
        return Future.future(promise -> conn.queryWithParams(sql, params, promise));
    }

    private Future<Void> executeQueryUpdate(SQLConnection conn, String sql) {
        return Future.future(promise -> conn.execute(sql, promise));
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata, ResultSet rs) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        Function<JsonObject, Map<String, Object>> func = metadata.isEmpty()
                ? JsonObject::getMap
                : row -> createRowMap(metadata, columnIndexMap, row);
        return Optional.ofNullable(rs)
                .map(resultSet -> resultSet.getRows().stream()
                        .map(func)
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    private void initColumnIndexMap(Map<String, Integer> columnIndexMap, JsonObject row) {
        final List<String> fields = new ArrayList<>(row.fieldNames());
        columnIndexMap.putAll(IntStream.range(0, fields.size())
                .boxed()
                .collect(Collectors.toMap(fields::get, i -> i)));
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, Map<String, Integer> columnIndexMap,
                                             JsonObject row) {
        if (columnIndexMap.isEmpty()) {
            initColumnIndexMap(columnIndexMap, row);
        }
        Map<String, Object> rowMap = new HashMap<>();
        row.stream().forEach(column -> {
            final ColumnMetadata columnMetadata = metadata.get(columnIndexMap.get(column.getKey()));
            rowMap.put(columnMetadata.getName(), adqmTypeConverter.convert(columnMetadata.getType(), column.getValue()));
        });
        return rowMap;
    }
}
