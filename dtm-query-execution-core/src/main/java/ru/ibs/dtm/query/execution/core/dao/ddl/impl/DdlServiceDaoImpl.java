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
    public void getMetadataByTableName(DdlRequestContext context, String tableName, Handler<AsyncResult<List<ClassField>>> resultHandler) {
        //FIXME вынести логику получения объекта <schema> + <таблица> на уровень сервиса
        int indexComma = tableName.indexOf(".");
        String schema = indexComma != -1 ? tableName.substring(0, indexComma) : "test";
        String table = tableName.substring(indexComma + 1);
        executor.query(dsl -> dsl.select(COLUMNS.COLUMN_NAME, COLUMNS.COLUMN_TYPE,
                COLUMNS.IS_NULLABLE, COLUMNS.COLUMN_DEFAULT, KEY_COLUMN_USAGE.ORDINAL_POSITION, KEY_COLUMN_USAGE.CONSTRAINT_NAME)
                .from(COLUMNS)
                .leftJoin(KEY_COLUMN_USAGE).on(COLUMNS.TABLE_SCHEMA.eq(KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA).and(COLUMNS.TABLE_NAME.eq(KEY_COLUMN_USAGE.TABLE_NAME))
                        .and(COLUMNS.COLUMN_NAME.eq(KEY_COLUMN_USAGE.COLUMN_NAME)))
                .where(COLUMNS.TABLE_NAME.eq(table))
                .and(COLUMNS.TABLE_SCHEMA.equalIgnoreCase(schema))
                .orderBy(KEY_COLUMN_USAGE.ORDINAL_POSITION)
        ).setHandler(ar -> {
            if (ar.succeeded()) {
                QueryResult result = ar.result();
                ResultSet resultSet = result.unwrap();
                List<ClassField> classFieldList = new ArrayList<>();
                resultSet.getRows().forEach(row -> {
                    boolean isPrimary = "PRIMARY".equals(row.getString(KEY_COLUMN_USAGE.CONSTRAINT_NAME.getName()));
                    Integer ordinal = row.getInteger(KEY_COLUMN_USAGE.ORDINAL_POSITION.getName());
                    classFieldList.add(
                            new ClassField(row.getString(COLUMNS.COLUMN_NAME.getName()),
                                    row.getString(COLUMNS.COLUMN_TYPE.getName()),
                                    row.getString(COLUMNS.IS_NULLABLE.getName()).contains("YES"),
                                    isPrimary ? ordinal : null,
                                    isInDistributedKey(context, row.getString(COLUMNS.COLUMN_NAME.getName())),
                                    row.getString(COLUMNS.COLUMN_DEFAULT.getName())));
                });
                resultHandler.handle(Future.succeededFuture(classFieldList));
            } else {
                log.error("Невозможно получить метаданные таблицы: {}", ar.cause().getMessage());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Integer isInDistributedKey(final DdlRequestContext context, final String field) {
        if (context.getQuery() instanceof SqlCreateTable) {
            int ind = ((SqlCreateTable) context.getQuery()).getOperandList().stream()
                    .filter(e -> e instanceof DistributedOperator)
                    .map(d -> ((DistributedOperator) d).getDistributedBy())
                    .flatMap(n -> n.getList().stream())
                    .filter(f -> f instanceof SqlIdentifier)
                    .map(i -> ((SqlIdentifier) i).names.indexOf(field))
                    .findFirst()
                    .orElse(-1);
            return (ind == -1) ? null : ind + 1;
        }
        return null;
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
    public void dropTable(ClassTable classTable, Handler<AsyncResult<ClassTable>> resultHandler) {
        executor.execute(dsl -> dsl.dropTableIfExists(classTable.getName())).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Удаление таблицы [{}] успешно завершено", classTable.getNameWithSchema());
                resultHandler.handle(Future.succeededFuture(classTable));
            } else {
                log.error("Ошибка удаления таблицы [{}]", classTable.getNameWithSchema(), ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
