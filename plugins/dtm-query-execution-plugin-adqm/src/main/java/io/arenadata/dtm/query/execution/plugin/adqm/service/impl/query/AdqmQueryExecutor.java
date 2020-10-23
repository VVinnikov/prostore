package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AdqmQueryExecutor implements DatabaseExecutor {
    private final SQLClient sqlClient;
    private final SqlTypeConverter typeConverter;

    public AdqmQueryExecutor(Vertx vertx, DataSource adqmDataSource, SqlTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
        this.sqlClient = JDBCClient.create(vertx, adqmDataSource);
    }

    @Override
    public void execute(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler) {
        log.debug(String.format("ADQM. Execute %s", sql));
        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                conn.query(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        ResultSet rs = ar2.result();
                        try {
                            List<Map<String, Object>> result = createResult(metadata, rs);
                            resultHandler.handle(Future.succeededFuture(result));
                        } catch (Exception e) {
                            log.error("Error converting ADQM values to jdbc types!", e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        log.error("ADQM query execution error: " + ar2.cause().getMessage());
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("ADQM connection error: " + ar1.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata, ResultSet rs) {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Integer> columnIndexMap = new HashMap<>();
        rs.getRows().forEach(row -> {
            if (columnIndexMap.isEmpty()) {
                initColumnIndexMap(columnIndexMap, row);
            }
            result.add(createRowMap(metadata, columnIndexMap, row));
        });
        return result;
    }

    private void initColumnIndexMap(Map<String, Integer> columnIndexMap, JsonObject row) {
        final List<String> fields = new ArrayList<>(row.fieldNames());
        columnIndexMap.putAll(IntStream.range(0, fields.size())
                .boxed()
                .collect(Collectors.toMap(fields::get, i -> i)));
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, Map<String, Integer> columnIndexMap,
                                             JsonObject row) {
        Map<String, Object> rowMap = new HashMap<>();
        row.stream().forEach(column -> {
            final ColumnMetadata columnMetadata = metadata.get(columnIndexMap.get(column.getKey()));
            rowMap.put(columnMetadata.getName(), typeConverter.convert(columnMetadata.getType(), column.getValue()));
        });
        return rowMap;
    }

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {
        log.debug(String.format("ADQM. Execute update %s", sql));
        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                conn.execute(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        completionHandler.handle(Future.succeededFuture());
                    } else {
                        completionHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                    conn.close();
                });
            } else {
                log.error("ADQM connection error: " + ar1.cause().getMessage());
                completionHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    @Override
    public void executeWithParams(String sql, List<Object> params, List<ColumnMetadata> metadata, Handler<AsyncResult<?>> resultHandler) {
        log.debug(String.format("ADQM. Execute with params %s", sql));
        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                conn.queryWithParams(sql, new JsonArray(params), ar2 -> {
                    if (ar2.succeeded()) {
                        try {
                            List<Map<String, Object>> result = createResult(metadata, ar2.result());
                            resultHandler.handle(Future.succeededFuture(result));
                        } catch (Exception e) {
                            log.error("Error converting ADQM values to jdbc types!", e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("ADQM connection error: " + ar1.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

}
