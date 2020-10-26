package io.arenadata.dtm.query.execution.core.service.hsql.impl;

import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;

@Slf4j
public class HSQLClientImpl implements HSQLClient {

    private static final String JDBC_DRIVER = "org.hsqldb.jdbc.JDBCDriver";
    private static final String INMEMORY_DATABASE_URL = "jdbc:hsqldb:mem:cachedb";

    private JDBCClient jdbcClient;

    public HSQLClientImpl(Vertx vertx) {
        this.jdbcClient = JDBCClient.create(vertx, new JsonObject()
                .put("url", INMEMORY_DATABASE_URL)
                .put("driver_class", JDBC_DRIVER)
                .put("max_pool_size", 30)
                .put("user", "SA")
                .put("password", "")
                .put("acquire_retry_attempts", 3)
                .put("break_after_acquire_failure", true));

    }

    @Override
    public Future<Void> executeQuery(String query){
        return Future.future(promise -> {
            jdbcClient.getConnection(conn -> {
                if (conn.failed()) {
                    log.error("Could not open hsqldb connection", conn.cause());
                    promise.fail(conn.cause());
                } else {
                    val connection = conn.result();
                    connection.execute(query, r -> {
                        connection.close();
                        if (r.failed()) {
                            log.error("Error occurred while executing query: {}", query, r.cause());
                            promise.fail(r.cause());
                        } else {
                            promise.complete();
                        }
                    });
                }
            });
        });
    }

    @Override
    public Future<Void> executeBatch(List<String> queries) {
        return Future.future(promise -> {
            jdbcClient.getConnection(conn -> {
                if (conn.failed()) {
                    log.error("Could not open hsqldb connection", conn.cause());
                    promise.fail(conn.cause());
                } else {
                    val connection = conn.result();
                    connection.batch(queries, r -> {
                        connection.close();
                        if (r.failed()) {
                            log.error("Error while executing queries batch:\n {}", String.join(";\n", queries), r.cause());
                            promise.fail(r.cause());
                        } else {
                            promise.complete();
                        }
                    });
                }
            });
        });
    }
}
