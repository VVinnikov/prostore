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
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;

import javax.sql.DataSource;
import java.util.List;

@Slf4j
public class AdqmQueryExecutor implements DatabaseExecutor {
    private final SQLClient sqlClient;

    public AdqmQueryExecutor(Vertx vertx, DataSource adqmDataSource) {
        this.sqlClient = JDBCClient.create(vertx, adqmDataSource);
    }

    @Override
    public void execute(String sql, Handler<AsyncResult<JsonArray>> resultHandler) {

        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                conn.query(sql, ar2 -> {
                    if (ar2.succeeded()) {
                        ResultSet rs = ar2.result();
                        JsonArray result = new JsonArray(rs.getRows());
                        resultHandler.handle(Future.succeededFuture(result));
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

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<Void>> completionHandler) {
        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                conn.query(sql, ar2 -> {
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
    public void executeWithParams(String sql, List<Object> params, Handler<AsyncResult<?>> resultHandler) {
        sqlClient.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                SQLConnection conn = ar1.result();
                conn.queryWithParams(sql, new JsonArray(params), ar2 -> {
                    if (ar2.succeeded()) {
                        resultHandler.handle(Future.succeededFuture(ar2.result()));
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
