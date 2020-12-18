package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Slf4j
public class AdbQueryExecutor implements DatabaseExecutor {

    private final PgPool pool;
    private final int fetchSize;
    private final SqlTypeConverter typeConverter;

    public AdbQueryExecutor(PgPool pool, int fetchSize, SqlTypeConverter typeConverter) {
        this.pool = pool;
        this.fetchSize = fetchSize;
        this.typeConverter = typeConverter;
    }

    @Override
    public Future<List<Map<String, Object>>> execute(String sql, List<ColumnMetadata> metadata) {
        return executeWithParams(sql, Collections.emptyList(), metadata);
    }

    @Override
    public Future<List<Map<String, Object>>> executeWithCursor(String sql, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            final AdbConnectionCtx connectionCtx = new AdbConnectionCtx();
            getConnection()
                    .map(conn -> {
                        connectionCtx.setConnection(conn);
                        return conn;
                    })
                    .compose(conn -> {
                        log.debug("ADB.Execute cursor query: {}", sql);
                        return prepareQuery(conn, sql);
                    })
                    .compose(pgPreparedQuery -> readDataWithCursor(pgPreparedQuery, metadata, fetchSize))
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
    public Future<List<Map<String, Object>>> executeWithParams(String sql, List<Object> params, List<ColumnMetadata> metadata) {
        return Future.future(promise -> {
            final AdbConnectionCtx connectionCtx = new AdbConnectionCtx();
            getConnection()
                    .map(conn -> {
                        connectionCtx.setConnection(conn);
                        return conn;
                    })
                    .compose(conn -> {
                        log.debug("ADB.Execute query: {} with params: {}", sql, params);
                        return executePreparedQuery(conn, sql, new ArrayTuple(params));
                    })
                    .map(rowSet -> createResult(metadata, rowSet))
                    .onSuccess(promise::complete)
                    .onFailure(fail -> {
                        if (connectionCtx.getConnection() != null) {
                            tryCloseConnect(connectionCtx.getConnection());
                        }
                        promise.fail(fail);
                    });
        });
    }

    @Override
    public Future<Void> executeUpdate(String sql) {
        return Future.future(promise -> {
            final AdbConnectionCtx connectionCtx = new AdbConnectionCtx();
            log.debug("ADB. execute update: [{}]", sql);
            getConnection()
                    .map(conn -> {
                        connectionCtx.setConnection(conn);
                        return conn;
                    })
                    .compose(conn -> executeQueryUpdate(conn, sql))
                    .onSuccess(result -> {
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
        return Future.future(p -> {
            beginTransaction(pool)
                    .compose(tx -> Future.future((Promise<PgTransaction> promise) -> {
                        Future<PgTransaction> lastFuture = null;
                        for (PreparedStatementRequest st : requests) {
                            log.debug("Execute query: {} with params: {}", st.getSql(), st.getParams());
                            if (lastFuture == null) {
                                lastFuture = executeTx(st, tx);
                            } else {
                                lastFuture = lastFuture.compose(s -> executeTx(st, tx));
                            }
                        }
                        if (lastFuture == null) {
                            p.complete();
                            return;
                        }
                        lastFuture.onSuccess(s -> promise.complete(tx))
                                .onFailure(fail -> promise.fail(fail.toString()));
                    }))
                    .compose(this::commitTransaction)
                    .onSuccess(s -> p.complete())
                    .onFailure(f -> p.fail(new LlrDatasourceException(
                            String.format("Error executing queries: %s",
                                    f.getMessage()))));
        });
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
                    typeConverter.convert(columnMetadata.getType(), row.getValue(i)));
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
        return Future.future((Promise<Void> promise) ->
                trx.commit(txCommit -> {
                    if (txCommit.succeeded()) {
                        log.debug("Transaction succeeded");
                        promise.complete();
                    } else {
                        promise.fail(txCommit.cause());
                    }
                }));
    }

    @Data
    @NoArgsConstructor
    private class AdbConnectionCtx {
        private PgConnection connection;
    }

}
