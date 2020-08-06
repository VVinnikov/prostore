package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.jooq.MariaProperties;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;

import java.util.ArrayList;
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
            context.getRequest().getQueryRequest().setDatamartMnemonic(schema);
            context.setDdlType(DdlType.CREATE_TABLE);
            ClassTable classTable = metadataCalciteGenerator.generateTableMetadata((SqlCreate) context.getQuery());
            checkRequiredKeys(classTable.getFields());
            classTable.setNameWithSchema(getTableNameWithSchema(schema, classTable.getName()));
            context.getRequest().setClassTable(classTable);
            context.setDatamartName(schema);
            getDatamartId(classTable)
                    .compose(datamartId -> isDatamartTableExists(datamartId, context))
                    .onSuccess(isExists -> createTableIfNotExists(context, isExists)
                            .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                            .onFailure(fail -> handler.handle(Future.failedFuture(fail))))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error creating table by query request: {}!", context.getRequest().getQueryRequest(), e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private void checkRequiredKeys(List<ClassField> fields) {
        val notExistsKeys = new ArrayList<String>();
        val notExistsPrimaryKeys = fields.stream()
            .noneMatch(f -> f.getPrimaryOrder() != null);
        if (notExistsPrimaryKeys) {
            notExistsKeys.add("primary key(s)");
        }

        val notExistsShardingKey = fields.stream()
            .noneMatch(f -> f.getShardingOrder() != null);
        if (notExistsShardingKey) {
            notExistsKeys.add("sharding key(s)");
        }

        if (!notExistsKeys.isEmpty()) {
            throw new IllegalArgumentException(
                "Primary keys and Sharding keys are required. The following keys do not exist: " + String.join(",", notExistsKeys)
            );
        }
    }

    private Future<Long> getDatamartId(ClassTable classTable) {
        return Future.future((Promise<Long> promise) ->
                serviceDbFacade.getServiceDbDao().getDatamartDao().findDatamart(classTable.getSchema(), ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        log.error("Error finding datamart [{}]", classTable.getSchema(), ar.cause());
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Future<Void> createTableIfNotExists(DdlRequestContext context,
                                                Boolean isTableExists) {
        if (isTableExists) {
            final RuntimeException existsException =
                    new RuntimeException(String.format("Table [%s] is already exists in datamart [%s]!",
                            context.getRequest().getClassTable().getName(),
                            context.getRequest().getClassTable().getSchema()));
            log.error("Error creating table [{}] in datamart [{}]!",
                    context.getRequest().getClassTable().getName(),
                    context.getRequest().getClassTable().getSchema(),
                    existsException);
            return Future.failedFuture(existsException);
        } else {
            return createTable(context);
        }
    }

    private Future<Boolean> isDatamartTableExists(Long datamartId, DdlRequestContext context) {
        return Future.future((Promise<Boolean> promise) -> {
            context.setDatamartId(datamartId);
            serviceDbFacade.getServiceDbDao().getEntityDao().isEntityExists(datamartId,
                    context.getRequest().getClassTable().getName(), ar -> {
                        if (ar.succeeded()) {
                            promise.complete(ar.result());
                        } else {
                            log.error("Error receive status isExists for datamart [{}]",
                                    context.getDatamartName(), ar.cause());
                            promise.fail(ar.cause());
                        }
                    });
        });
    }

    private Future<Void> createTable(DdlRequestContext context) {
        //создание таблиц в источниках данных через плагины
        return Future.future((Promise<Void> promise) -> {
            metadataExecutor.execute(context, ar -> {
                if (ar.succeeded()) {
                    createEntity(context)
                            .onSuccess(ar2 -> {
                                log.debug("Table [{}] in datamart [{}] successfully created",
                                        context.getRequest().getClassTable().getName(),
                                        context.getDatamartName());
                                promise.complete();
                            })
                            .onFailure(fail -> {
                                log.error("Error creating table [{}] in datamart [{}]!",
                                        context.getRequest().getClassTable().getName(),
                                        context.getDatamartName(), fail);
                                promise.fail(fail);
                            });
                } else {
                    log.error("Error creating table [{}], datamart [{}] in datasources!",
                            context.getRequest().getClassTable().getName(),
                            context.getDatamartName(),
                            ar.cause());
                    promise.fail(ar.cause());
                }
            });
        });
    }

    private Future<Void> createEntity(DdlRequestContext context) {
        return insertEntity(context.getRequest().getClassTable().getName(), context.getDatamartId())
                .compose(entityId -> createAttributes(entityId, context.getRequest().getClassTable().getFields()));
    }

    private Future<Long> insertEntity(String table, Long datamartId) {
        return Future.future((Promise<Void> promise) ->
                serviceDbFacade.getServiceDbDao().getEntityDao().insertEntity(datamartId, table, promise))
                .compose(result -> Future.future((Promise<Long> promise) ->
                        serviceDbFacade.getServiceDbDao().getEntityDao().findEntity(datamartId, table, promise)));
    }

    private Future<Void> createAttributes(Long entityId, List<ClassField> fields) {
        return Future.future((Promise<Void> promise) -> {
            List<Future> futures = fields.stream().map(it -> Future.future(p -> createAttribute(entityId, it, ar -> {
                if (ar.succeeded()) {
                    p.complete();
                } else {
                    p.fail(ar.cause());
                }
            }))).collect(Collectors.toList());

            CompositeFuture.join(futures)
                    .onSuccess(ar -> promise.complete())
                    .onFailure(promise::fail);
        });
    }

    private void createAttribute(Long entityId, ClassField field, Handler<AsyncResult<Void>> handler) {
        //TODO this code could be improved
        serviceDbFacade.getServiceDbDao().getAttributeTypeDao().findTypeIdByTypeMnemonic(field.getType().name(), ar -> {
            if (ar.succeeded()) {
                serviceDbFacade.getServiceDbDao().getAttributeDao().insertAttribute(entityId, field, ar.result(), ar2 -> {
                    if (ar2.succeeded()) {
                        handler.handle(Future.succeededFuture());
                    } else {
                        log.error("Error creating table attribute: {} for entityId={}!", field, entityId, ar2.cause());
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }
}
