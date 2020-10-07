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
import ru.ibs.dtm.common.converter.SqlTypeConverter;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.query.execution.core.dao.ddl.DdlServiceDao;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
@Slf4j
public class DdlServiceDaoImpl implements DdlServiceDao {

    private final AsyncClassicGenericQueryExecutor executor;
    private final SqlTypeConverter typeConverter;

    @Autowired
    public DdlServiceDaoImpl(@Qualifier("coreQueryExecutor") AsyncClassicGenericQueryExecutor executor,
                             @Qualifier("coreTypeToSqlTypeConverter") SqlTypeConverter typeConverter) {
        this.executor = executor;
        this.typeConverter = typeConverter;
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
    public void executeQuery(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler) {
        executor.query(dsl -> dsl.resultQuery(sql))
                .setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.debug("Execute query sql: {}, result: {}", sql, ar.result());
                        final Map<String, ColumnType> columnTypeMap =
                                metadata.stream().collect(Collectors.toMap(ColumnMetadata::getName, ColumnMetadata::getType));
                        if (ar.result().unwrap() instanceof ResultSet) {
                            try {
                                List<Map<String, Object>> result = createResult(columnTypeMap, ar.result().unwrap());
                                resultHandler.handle(Future.succeededFuture(result));
                            } catch (Exception e) {
                                log.error("Error converting core values to jdbc types!", e);
                                resultHandler.handle(Future.failedFuture(e));
                            }
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

    private List<Map<String, Object>> createResult(Map<String, ColumnType> columnTypeMap, ResultSet rs) {
        List<Map<String, Object>> result = new ArrayList<>();
        rs.getRows().forEach(r -> {
            Map<String, Object> row = new HashMap<>();
            r.stream().forEach(c -> row.put(c.getKey(),
                    typeConverter.convert(columnTypeMap.get(c.getKey()), c.getValue())));
            result.add(row);
        });
        return result;
    }

    @Override
    public void dropTable(ClassTable classTable, Handler<AsyncResult<ClassTable>> resultHandler) {
        executor.execute(dsl -> dsl.dropTableIfExists(classTable.getName())).setHandler(ar -> {
            if (ar.succeeded()) {
                log.debug("Deleting table [{}] completed successfully", classTable.getNameWithSchema());
                resultHandler.handle(Future.succeededFuture(classTable));
            } else {
                log.error("Error deleting table [{}]!", classTable.getNameWithSchema(), ar.cause());
                resultHandler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }
}
