package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class CreateTableDdlExecutor extends QueryResultDdlExecutor {

    private final MetadataCalciteGenerator metadataCalciteGenerator;

    @Autowired
    public CreateTableDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                  MariaProperties mariaProperties,
                                  ServiceDbFacade serviceDbFacade, MetadataCalciteGenerator metadataCalciteGenerator) {
        super(metadataExecutor, mariaProperties, serviceDbFacade);
        this.metadataCalciteGenerator = metadataCalciteGenerator;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            QueryRequest request = context.getRequest().getQueryRequest();
            request.setDatamartMnemonic(schema);
            ClassTable classTable = metadataCalciteGenerator.generateTableMetadata((SqlCreate) context.getQuery());
            getDatamartId(classTable)
                    .compose(datamartId -> isDatamartTableExists(datamartId, classTable.getName()))
                    .onComplete(isExists -> {
                        if (isExists.succeeded()) {
                            if (isExists.result()) {
                                handler.handle(Future.failedFuture(
                                        new RuntimeException(String.format("Table [%s] is already exists in datamart [%s]!",
                                                classTable.getName(), classTable.getSchema()))));
                            } else {
                                createTable(context, classTable, handler);
                            }
                        }
                    })
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error in creating table!", e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void createTable(DdlRequestContext context, ClassTable classTable, Handler<AsyncResult<QueryResult>> handler) {
        context.setClassTable(classTable);
        //создание таблиц в источниках данных через плагины
        metadataExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                //создание метаданных для логической таблицы и ее атрибутов
                createEntity(context, cr -> {
                    if (cr.succeeded()) {
                        handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                    } else {
                        handler.handle(Future.failedFuture(cr.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private Future<Long> getDatamartId(ClassTable classTable) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(classTable.getSchema(), promise));
    }

    private Future<Boolean> isDatamartTableExists(Long datamartId, String tableName) {
        return Future.future((Promise<Boolean> promise) ->
                serviceDbFacade.getServiceDbDao().getEntityDao().existsEntity(datamartId, tableName, promise));
    }

    private void createEntity(DdlRequestContext context, Handler<AsyncResult<Void>> resultHandler) {
        insertEntity(context.getClassTable().getName(), context.getDatamartId())
                .compose(tableId -> Future.future((Promise<Void> promise) -> createAttributes(tableId, context.getClassTable().getFields(), promise)))
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        resultHandler.handle(Future.succeededFuture());
                    } else {
                        resultHandler.handle(Future.failedFuture(ar.cause()));//TODO проверить
                    }
                })
                .onFailure(fail -> resultHandler.handle(Future.failedFuture(fail.getCause())));
    }

    private void createAttributes(Long entityId, List<ClassField> fields, Promise promise) {
        List<Future> futures = fields.stream().map(it -> Future.future(p -> createAttribute(entityId, it, ar -> {
            if (ar.succeeded()) {
                p.complete();
            } else {
                p.fail(ar.cause());
            }
        }))).collect(Collectors.toList());
        CompositeFuture.join(futures).onComplete(ar -> {
            if (ar.succeeded()) {
                promise.complete();
            } else {
                log.error("Error generating table attributes for entityId={}", entityId, ar.cause());
                promise.fail(ar.cause());
            }
        });
    }

    private void createAttribute(Long entityId, ClassField field, Handler<AsyncResult<Void>> handler) {
        serviceDbFacade.getServiceDbDao().getAttributeTypeDao().findTypeIdByDatamartName(field.getType().name(), ar1 -> {
            if (ar1.succeeded()) {
                serviceDbFacade.getServiceDbDao().getAttributeDao().insertAttribute(entityId, field, ar1.result(), ar2 -> {
                    if (ar2.succeeded()) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    private Future<Long> insertEntity(String table, Long datamartId) {
        Promise<Long> promise = Promise.promise();
        serviceDbFacade.getServiceDbDao().getEntityDao().insertEntity(datamartId, table, ar1 -> {
            if (ar1.succeeded()) {
                serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartId, table, ar2 -> {
                    if (ar2.succeeded()) {
                        promise.complete(ar2.result());
                    } else {
                        log.error("", table, ar2.cause());
                        promise.fail(ar2.cause());
                    }
                });
            } else {
                log.error("Error adding table [{}] with datamartId={}", table, ar1.cause());
                promise.fail(ar1.cause());
            }
        });
        return promise.future();
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }
}
