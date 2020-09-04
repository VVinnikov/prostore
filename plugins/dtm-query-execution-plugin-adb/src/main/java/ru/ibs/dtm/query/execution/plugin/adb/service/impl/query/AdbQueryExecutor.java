package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgCursor;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgTransaction;
import io.reactiverse.pgclient.impl.ArrayTuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.PreparedStatementRequest;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AdbQueryExecutor implements DatabaseExecutor {

    private final PgPool pool;
    private final int fetchSize;

    public AdbQueryExecutor(PgPool pool, int fetchSize) {
        this.pool = pool;
        this.fetchSize = fetchSize;
    }

    @Override
    public void execute(String sql, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                PgTransaction tx = conn.begin();
                tx.prepare(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        PgCursor cursor = ar2.result().cursor();
                        List<Pair<Integer, String>> columnIndexes = new ArrayList<>();
                        do {
                            cursor.read(fetchSize, res -> {
                                if (res.succeeded()) {
                                    List<JsonObject> dataSet = new ArrayList<>();
                                    List<String> columnsNames = res.result().columnsNames();
                                    for (io.reactiverse.pgclient.Row row : res.result()) {
                                        if (columnIndexes.isEmpty()) {
                                            for (int x = 0; x < row.size(); x++) {
                                                val columnName = row.getColumnName(x);
                                                val index = columnsNames.indexOf(columnName);
                                                columnIndexes.add(Pair.of(index, columnName));
                                            }
                                        }
                                        JsonObject values = new JsonObject();
                                        columnIndexes.forEach(p -> values.put(p.getValue(), row.getValue(p.getKey())));
                                        dataSet.add(values);
                                    }
                                    resultHandler.handle(Future.succeededFuture(dataSet));
                                }
                            });
                        } while (cursor.hasMore());
                        tx.commit();
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
    public void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler) {
        pool.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                PgConnection conn = ar1.result();
                conn.preparedQuery(sql, new ArrayTuple(params), ar2 -> {
                    if (ar2.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar2.result()));
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
                log.error("Ошибка выполнения запроса [{}]", request.getSql(), rs.cause());
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
