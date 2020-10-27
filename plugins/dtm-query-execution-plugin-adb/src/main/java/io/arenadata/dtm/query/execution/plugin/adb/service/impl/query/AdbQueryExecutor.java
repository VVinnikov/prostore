package io.arenadata.dtm.query.execution.plugin.adb.service.impl.query;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.plugin.sql.PreparedStatementRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgCursor;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgTransaction;
import io.reactiverse.pgclient.impl.ArrayTuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public void execute(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                conn.prepare(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        PgCursor cursor = ar2.result().cursor();
                        do {
                            cursor.read(fetchSize, res -> {
                                if (res.succeeded()) {
                                    try {
                                        List<Map<String, Object>> result = createResult(metadata, res.result());
                                        resultHandler.handle(Future.succeededFuture(result));
                                    } catch (Exception e) {
                                        conn.close();
                                        log.error("Error converting ADB values to jdbc types!", e);
                                        resultHandler.handle(Future.failedFuture(e));
                                    }
                                } else {
                                    conn.close();
                                    log.error("Error fetching cursor", res.cause());
                                    resultHandler.handle(Future.failedFuture(res.cause()));
                                }
                            });
                        } while (cursor.hasMore());
                        conn.close();
                    } else {
                        conn.close();
                        log.error("Request preparation error!", ar2.cause());
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("Connection error!", ar1.cause());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    private List<Map<String, Object>> createResult(List<ColumnMetadata> metadata,
                                                   io.reactiverse.pgclient.PgRowSet pgRowSet) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (io.reactiverse.pgclient.Row row : pgRowSet) {
            result.add(createRowMap(metadata, row));
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

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                conn.query(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        log.debug("ADB. execute update {}", sql);
                        completionHandler.handle(Future.succeededFuture());
                    } else {
                        completionHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                    conn.close();
                });
            } else {
                log.error("Connection error!", ar1.cause());
                completionHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    @Override
    public void executeWithParams(String sql, List<Object> params, List<ColumnMetadata> metadata, Handler<AsyncResult<?>> resultHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                conn.preparedQuery(sql, new ArrayTuple(params), ar2 -> {
                    if (ar2.succeeded()) {
                        try {
                            List<Map<String, Object>> result = createResult(metadata, ar2.result());
                            resultHandler.handle(Future.succeededFuture(result));
                        } catch (Exception e) {
                            conn.close();
                            log.error("Error converting ADB values to jdbc types!", e);
                            resultHandler.handle(Future.failedFuture(e));
                        }
                    } else {
                        resultHandler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                log.error("Connection error!", ar1.cause());
                resultHandler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    @Override
    public void executeInTransaction(List<PreparedStatementRequest> requests, Handler<AsyncResult<Void>> handler) {
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
                        handler.handle(Future.succeededFuture());
                        return;
                    }
                    lastFuture.onSuccess(s -> promise.complete(tx))
                            .onFailure(fail -> promise.fail(fail.toString()));
                }))
                .compose(this::commitTransaction)
                .onSuccess(s -> handler.handle(Future.succeededFuture()))
                .onFailure(f -> handler.handle(Future.failedFuture(
                        String.format("Error executing queries: %s", f.getMessage()))));
    }

    private Future<PgTransaction> beginTransaction(PgPool pgPool) {
        return Future.future((Promise<PgTransaction> promise) -> pgPool.begin(ar -> {
            if (ar.succeeded()) {
                log.trace("Transaction began");
                promise.complete(ar.result());
            } else {
                log.error("Connection error", ar.cause());
                promise.fail(ar.cause());
            }
        }));
    }

    private Future<PgTransaction> executeTx(PreparedStatementRequest request, PgTransaction tx) {
        return Future.future((Promise<PgTransaction> promise) -> tx.query(request.getSql(), rs -> {
            if (rs.succeeded()) {
                promise.complete(tx);
            } else {
                log.error("Error executing query [{}]", request.getSql(), rs.cause());
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
                        log.error("Transaction failed [{}]", txCommit.cause().getMessage());
                        promise.fail(txCommit.cause());
                    }
                }));
    }

}