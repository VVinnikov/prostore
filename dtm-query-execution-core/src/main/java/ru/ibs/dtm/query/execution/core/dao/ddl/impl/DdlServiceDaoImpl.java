package ru.ibs.dtm.query.execution.core.dao.ddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.core.dao.ddl.DdlServiceDao;

import java.util.List;

@Repository
@Slf4j
public class DdlServiceDaoImpl implements DdlServiceDao {

    private final AsyncClassicGenericQueryExecutor executor;

    @Autowired
    public DdlServiceDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void executeUpdate(String sql, Handler<AsyncResult<List<Void>>> resultHandler) {
        executor.execute(dsl -> dsl.query(sql)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Execute query sql: {}, result: {}", sql, ar.result());
                resultHandler.handle(Future.succeededFuture());
            } else {
                log.error("Error while executing the query sql: {}", sql, ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void executeQuery(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
        executor.query(dsl -> dsl.resultQuery(sql))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.debug("Execute query sql: {}, result: {}", sql, ar.result());
                        if (ar.result().unwrap() instanceof ResultSet) {
                            resultHandler.handle(Future.succeededFuture(ar.result().unwrap()));
                        } else {
                            log.error("Cannot get the result of the query sql: {}", sql, ar.cause());
                            resultHandler.handle(Future.failedFuture(
                                    new RuntimeException(String.format("Cannot get the result of the query [%s]", sql))));
                        }
                    } else {
                        log.error("Error while executing the query sql: {}", sql, ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void dropTable(Entity entity, Handler<AsyncResult<Entity>> resultHandler) {
        executor.execute(dsl -> dsl.dropTableIfExists(entity.getName())).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Deleting table [{}] completed successfully", entity.getNameWithSchema());
                resultHandler.handle(Future.succeededFuture(entity));
            } else {
                log.error("Error deleting table [{}]!", entity.getNameWithSchema(), ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
