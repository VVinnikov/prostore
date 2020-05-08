package ru.ibs.dtm.query.execution.plugin.adb.service.impl.query;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgCursor;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgTransaction;
import io.reactiverse.pgclient.impl.ArrayTuple;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AdbQueryExecutor implements DatabaseExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdbQueryExecutor.class);

  private final PgPool pool;
  private final int fetchSize;

  public AdbQueryExecutor(PgPool pool, int fetchSize) {
    this.pool = pool;
    this.fetchSize = fetchSize;
  }

  @Override
  public void execute(String sql, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler) {
    pool.getConnection(ar1 -> {
      if (ar1.succeeded()) {
        PgConnection conn = ar1.result();
        PgTransaction tx = conn.begin();
        tx.prepare(sql, ar2 -> {
          if (ar2.succeeded()) {
            PgCursor cursor = ar2.result().cursor();
            do {
              cursor.read(fetchSize, res -> {
                if (res.succeeded()) {
                  List<Map<String, Object>> dataSet = new ArrayList<>();
                  for (io.reactiverse.pgclient.Row row : res.result()) {
                    Map<String, Object> values = new HashMap<>();
                    for (int x = 0; x < row.size(); x++) {
                      values.put(row.getColumnName(x), row.getValue(x));
                    }
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
            LOGGER.error("Ошибка подготовки запроса: " + ar2.cause().getMessage());
            resultHandler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        LOGGER.error("Ошибка подключения: " + ar1.cause().getMessage());
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
            completionHandler.handle(Future.succeededFuture());
          } else {
            completionHandler.handle(Future.failedFuture(ar2.cause()));
          }
          conn.close();
        });
      } else {
        LOGGER.error("Ошибка подключения: " + ar1.cause().getMessage());
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
        LOGGER.error("Ошибка подключения: " + ar1.cause().getMessage());
        resultHandler.handle(Future.failedFuture(ar1.cause()));
      }
    });
  }

}
