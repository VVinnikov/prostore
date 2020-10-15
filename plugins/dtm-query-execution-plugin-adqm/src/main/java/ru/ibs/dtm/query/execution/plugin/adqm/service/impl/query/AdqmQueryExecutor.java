package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                final Map<String, ColumnType> columnTypeMap =
                        metadata.stream().collect(Collectors.toMap(ColumnMetadata::getName, ColumnMetadata::getType));
                conn.query(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        ResultSet rs = ar2.result();
                        try {
                            List<Map<String, Object>> result = createResult(columnTypeMap, rs);
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

    private List<Map<String, Object>> createResult(Map<String, ColumnType> columnTypeMap, ResultSet rs) {
        List<Map<String, Object>> result = new ArrayList<>();
        rs.getRows().forEach(r -> {
            Map<String, Object> row = new HashMap<>();
            r.stream().forEach(c -> row.put(c.getKey(),
                    typeConverter.convert(columnTypeMap.get(c.getKey()), c.getValue())));
            result.add(row);
        });
        return result;
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
                final Map<String, ColumnType> columnTypeMap =
                        metadata.stream().collect(Collectors.toMap(ColumnMetadata::getName, ColumnMetadata::getType));
                conn.queryWithParams(sql, new JsonArray(params), ar2 -> {
                    if (ar2.succeeded()) {
                        try {
                            List<Map<String, Object>> result = createResult(columnTypeMap, ar2.result());
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
