package ru.ibs.dtm.query.execution.core.dao.ddl.impl;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.calcite.core.extension.ddl.DistributedOperator;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import ru.ibs.dtm.query.execution.core.dao.ddl.DdlServiceDao;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.ArrayList;
import java.util.List;

import static org.jooq.generated.information_schema.Tables.COLUMNS;
import static org.jooq.generated.information_schema.Tables.KEY_COLUMN_USAGE;

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
                log.debug("Исполнен запрос(executeUpdate) sql: {}, результат: {}", sql, ar.result());
                resultHandler.handle(Future.succeededFuture());
            } else {
                log.error("Ошибка при исполнении запроса(executeUpdate) sql: {}", sql, ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void executeQuery(String sql, Handler<AsyncResult<ResultSet>> resultHandler) {
        executor.query(dsl -> dsl.resultQuery(sql))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.debug("Исполнен запрос(executeQuery) sql: {}, результат: {}", sql, ar.result());
                        if (ar.result().unwrap() instanceof ResultSet) {
                            resultHandler.handle(Future.succeededFuture(ar.result().unwrap()));
                        } else {
                            log.error("Невозможно получить результат запроса(executeQuery) sql: {}", sql, ar.cause());
                            resultHandler.handle(Future.failedFuture(String.format("Невозможно получить результат выполнения запроса [%s]", sql)));
                        }
                    } else {
                        log.error("Ошибка при исполнении запроса(executeQuery) sql: {}", sql, ar.cause());
                        resultHandler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    @Override
    public void dropTable(ClassTable classTable, Handler<AsyncResult<Void>> resultHandler) {
        executor.execute(dsl -> dsl.dropTableIfExists(classTable.getName())).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Удаление таблицы [{}] успешно завершено", classTable.getNameWithSchema());
                resultHandler.handle(Future.succeededFuture());
            } else {
                log.error("Ошибка удаления таблицы [{}]", classTable.getNameWithSchema(), ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
