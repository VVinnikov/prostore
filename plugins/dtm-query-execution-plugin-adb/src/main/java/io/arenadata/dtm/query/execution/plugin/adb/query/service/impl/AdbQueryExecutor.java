package io.arenadata.dtm.query.execution.plugin.adb.query.service.impl;

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.LlrDatasourceException;
import io.reactiverse.pgclient.*;
import io.reactiverse.pgclient.impl.ArrayTuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AdbQueryExecutor implements DatabaseExecutor {

    private final PgPool pool;
    private final int fetchSize;
    private final SqlTypeConverter adbTypeConverter;
    private final SqlTypeConverter sqlTypeConverter;

    public AdbQueryExecutor(PgPool pool,
                            int fetchSize,
                            SqlTypeConverter adbTypeConverter,
                            SqlTypeConverter sqlTypeConverter) {
        this.pool = pool;
        this.fetchSize = fetchSize;
        this.adbTypeConverter = adbTypeConverter;
        this.sqlTypeConverter = sqlTypeConverter;
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return executeWithParams(sql, null, metadata);
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            final AdbConnectionCtx connectionCtx = new AdbConnectionCtx();
            log.debug("ADB. Execute query: [{}]", sql);
            AsyncUtils.measureMs(
                    getConnection()
                            .map(conn -> {
                                connectionCtx.setConnection(conn);
                                return conn;
                            })
                            .compose(conn -> prepareQuery(conn, sql))
                            .compose(pgPreparedQuery -> readDataWithCursor(pgPreparedQuery, metadata, fetchSize)),
                    duration -> log.debug("ADB. Query completed successfully: [{}] in [{}]ms", sql, duration))
                    .onSuccess(result -> {
                        tryCloseConnect(connectionCtx.getConnection());
                        promise.complete(result);
                    })
                    .onFailure(fail -> {
                        if (connectionCtx.getConnection() != null) {
                            tryCloseConnect(connectionCtx.getConnection());
                        }
                        promise.fail(fail);
                    });
        });
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithParams(String sql,
                                                               QueryParameters params,
                                                               List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            final AdbConnectionCtx connectionCtx = new AdbConnectionCtx();
            log.debug("ADB. Execute query: [{}] with params: [{}]", sql, params);
            AsyncUtils.measureMs(
                    getConnection()
                            .map(conn -> {
                                connectionCtx.setConnection(conn);
                                return conn;
                            })
                            .compose(conn -> executePreparedQuery(conn, sql, createParamsArray(params))),
                    duration -> log.debug("ADB. Query completed successfully: [{}] in [{}]ms", sql, duration))
                    .map(rowSet -> createResult(metadata, rowSet))
                    .onSuccess(result -> {
                        tryCloseConnect(connectionCtx.getConnection());
                        promise.complete(result);
                    })
                    .onFailure(fail -> {
                        log.error("ADB. Query failed to execute: [{}]", sql, fail);
                        if (connectionCtx.getConnection() != null) {
                            tryCloseConnect(connectionCtx.getConnection());
                        }
                        promise.fail(fail);
                    });
        });
    }

    private ArrayTuple createParamsArray(QueryParameters params) {
        if (params == null) {
            return new ArrayTuple(Collections.emptyList());
        } else {
            return new ArrayTuple(IntStream.range(0, params.getValues().size())
                    .mapToObj(n -> sqlTypeConverter.convert(params.getTypes().get(n),
                            params.getValues().get(n)))
                    .collect(Collectors.toList()));
        }
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            final AdbConnectionCtx connectionCtx = new AdbConnectionCtx();
            log.debug("ADB. Execute update: [{}]", sql);
            AsyncUtils.measureMs(
                    getConnection()
                            .map(conn -> {
                                connectionCtx.setConnection(conn);
                                return conn;
                            })
                            .compose(conn -> executeQueryUpdate(conn, sql)),
                    duration -> log.debug("ADB. Update completed successfully: [{}] in [{}]ms", sql, duration))
                    .onSuccess(result -> {
                        tryCloseConnect(connectionCtx.getConnection());
                        promise.complete();
                    })
                    .onFailure(fail -> {
                        if (connectionCtx.getConnection() != null) {
                            tryCloseConnect(connectionCtx.getConnection());
                        }
                        promise.fail(fail);
                    });
        });
    }

    private Future<List<Map<String, Object>>> readDataWithCursor(PgPreparedQuery preparedQuery,
                                                                 List<ColumnMetadata> metadata,
                                                                 Integer fetchSize) {
        return Future.future(promise -> {
            List<Map<String, Object>> result = new ArrayList<>();
            final PgCursor pgCursor = preparedQuery.cursor();
            readCursor(pgCursor, fetchSize, metadata, ar -> {
                        if (ar.succeeded()) {
                            result.addAll(ar.result());
                        } else {
                            promise.fail(ar.cause());
                        }
                    },
                    rr -> {
                        if (rr.succeeded()) {
                            promise.complete(result);
                        } else {
                            promise.fail(new DtmException("Error executing fetching data with cursor", rr.cause()));
                        }
                    });
        });
    }

    private void readCursor(PgCursor cursor,
                            int chunkSize,
                            List<ColumnMetadata> metadata,
                            Handler<AsyncResult<List<Map<String, Object>>>> itemHandler,
                            Handler<AsyncResult<List<Map<String, Object>>>> handler) {
        cursor.read(chunkSize, res -> {
            if (res.succeeded()) {
                val dataSet = createResult(metadata, res.result());
                itemHandler.handle(Future.succeededFuture(dataSet));
                if (cursor.hasMore()) {
                    readCursor(cursor,
                            chunkSize,
                            metadata,
                            itemHandler,
                            handler);
                } else {
                    cursor.close();
                    handler.handle(Future.succeededFuture(dataSet));
                }
            } else {
                handler.handle(Future.failedFuture(res.cause()));
            }
        });
    }

    @Override
    public Future<Void> executeInTransaction(List<PreparedStatementRequest> requests) {
        log.debug("ADB. Execute transaction: {}", requests);
        return Future.future(p -> AsyncUtils.measureMs(beginTransaction(pool)
                        .compose(tx -> Future.future((Promise<PgTransaction> promise) -> {
                            Future<PgTransaction> lastFuture = null;
                            for (PreparedStatementRequest st : requests) {
                                log.debug("ADB. Execute query in transaction: [{}] with params: [{}]", st.getSql(), st.getParams());
                                if (lastFuture == null) {
                                    lastFuture = AsyncUtils.measureMs(
                                            executeTx(st, tx),
                                            duration -> log.debug("ADB. Query in transaction completed successfully: [{}] in [{}]ms", st.getSql(), duration));
                                } else {
                                    lastFuture = lastFuture.compose(s -> AsyncUtils.measureMs(
                                            executeTx(st, tx),
                                            duration -> log.debug("ADB. Query in transaction completed successfully: [{}] in [{}]ms", st.getSql(), duration)));
                                }
                            }
                            if (lastFuture == null) {
                                p.complete();
                                return;
                            }
                            lastFuture.onSuccess(s -> promise.complete(tx))
                                    .onFailure(fail -> promise.fail(fail.toString()));
                        }))
                        .compose(this::commitTransaction),
                duration -> log.debug("ADB. Transaction completed successfully: [{}] in [{}]ms", requests, duration))
                .onSuccess(s -> p.complete())
                .onFailure(f -> p.fail(new LlrDatasourceException(
                        String.format("Error executing queries: %s",
                                f.getMessage())))));
    }

    private Future<PgConnection> getConnection() {
        return Future.future(pool::getConnection);
    }

    private Future<PgPreparedQuery> prepareQuery(PgConnection conn, String sql) {
        return Future.future(promise -> conn.prepare(sql, promise));
    }

    private Future<PgRowSet> executeQueryUpdate(PgConnection conn, String sql) {
        return Future.future(promise -> conn.query(sql, promise));
    }

    private Future<PgRowSet> executePreparedQuery(PgConnection conn, String sql, Tuple params) {
        return Future.future(promise -> conn.preparedQuery(sql, params, promise));
    }

    private void tryCloseConnect(PgConnection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            log.warn("Error closing connection: {}", e.getMessage());
        }
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata,
                                                   io.reactiverse.pgclient.PgRowSet pgRowSet) {
        List<Map<String, Object>> result = new ArrayList<>();
        Function<Row, Map<String, Object>> func = metadata.isEmpty()
                ? row -> createRowMap(row, pgRowSet.columnsNames().size())
                : row -> createRowMap(metadata, row);
        for (io.reactiverse.pgclient.Row row : pgRowSet) {
            result.add(func.apply(row));
        }
        return result;
    }

    private Map<String, Object> createRowMap(List<ColumnMetadata> metadata, io.reactiverse.pgclient.Row row) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < metadata.size(); i++) {
            ColumnMetadata columnMetadata = metadata.get(i);
            rowMap.put(columnMetadata.getName(),
                    adbTypeConverter.convert(columnMetadata.getType(), row.getValue(i)));
        }
        return rowMap;
    }

    private Map<String, Object> createRowMap(io.reactiverse.pgclient.Row row, int size) {
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            rowMap.put(row.getColumnName(i), row.getValue(i));
        }
        return rowMap;
    }

    private Future<PgTransaction> beginTransaction(PgPool pgPool) {
        return Future.future((Promise<PgTransaction> promise) -> pgPool.begin(ar -> {
            if (ar.succeeded()) {
                log.trace("Transaction began");
                promise.complete(ar.result());
            } else {
                promise.fail(ar.cause());
            }
        }));
    }

    private Future<PgTransaction> executeTx(PreparedStatementRequest request, PgTransaction tx) {
        return Future.future((Promise<PgTransaction> promise) -> tx.query(request.getSql(), rs -> {
            if (rs.succeeded()) {
                promise.complete(tx);
            } else {
                promise.fail(rs.cause());
            }
        }));
    }

    private Future<Void> commitTransaction(PgTransaction trx) {
        return AsyncUtils.measureMs(Future.future((Promise<Void> promise) ->
                        trx.commit(txCommit -> {
                            if (txCommit.succeeded()) {
                                promise.complete();
                            } else {
                                promise.fail(txCommit.cause());
                            }
                        })),
                duration -> log.debug("ADB. Commit transaction succeeded in [{}]ms", duration));
    }

    @Data
    @NoArgsConstructor
    private static class AdbConnectionCtx {
        private PgConnection connection;
    }

}
