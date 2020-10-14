package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.exception.datamart.DatamartNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlType;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class CreateTableDdlExecutor extends QueryResultDdlExecutor {

    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;

    @Autowired
    public CreateTableDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                  ServiceDbFacade serviceDbFacade,
                                  MetadataCalciteGenerator metadataCalciteGenerator) {
        super(metadataExecutor, serviceDbFacade);
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            val schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            context.getRequest().getQueryRequest().setDatamartMnemonic(schema);
            context.setDdlType(DdlType.CREATE_TABLE);
            val entity = metadataCalciteGenerator.generateTableMetadata((SqlCreate) context.getQuery());
            entity.setEntityType(EntityType.TABLE);
            checkRequiredKeys(entity.getFields());
            context.getRequest().setEntity(entity);
            context.setDatamartName(schema);
            datamartDao.existsDatamart(schema)
                .compose(isExistsDatamart -> isExistsDatamart ?
                    entityDao.existsEntity(schema, entity.getName()) : getNotExistsDatamartFuture(schema))
                .onSuccess(isExistsEntity -> createTableIfNotExists(context, isExistsEntity)
                    .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                    .onFailure(fail -> handler.handle(Future.failedFuture(fail))))
                .onFailure(fail -> handler.handle(Future.failedFuture(fail)));
        } catch (Exception e) {
            log.error("Error creating table by query request: {}!", context.getRequest().getQueryRequest(), e);
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Boolean> getNotExistsDatamartFuture(String schema) {
        return Future.failedFuture(new DatamartNotExistsException(schema));
    }

    private void checkRequiredKeys(List<EntityField> fields) {
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

    private Future<Void> createTableIfNotExists(DdlRequestContext context,
                                                Boolean isTableExists) {
        if (isTableExists) {
            final RuntimeException existsException =
                new RuntimeException(String.format("Table [%s] is already exists in datamart [%s]!",
                    context.getRequest().getEntity().getName(),
                    context.getRequest().getEntity().getSchema()));
            log.error("Error creating table [{}] in datamart [{}]!",
                context.getRequest().getEntity().getName(),
                context.getRequest().getEntity().getSchema(),
                existsException);
            return Future.failedFuture(existsException);
        } else {
            return createTable(context);
        }
    }

    private Future<Void> createTable(DdlRequestContext context) {
        //creating tables in data sources through plugins
        return Future.future((Promise<Void> promise) -> {
            metadataExecutor.execute(context, ar -> {
                if (ar.succeeded()) {
                    entityDao.createEntity(context.getRequest().getEntity())
                        .onSuccess(ar2 -> {
                            log.debug("Table [{}] in datamart [{}] successfully created",
                                context.getRequest().getEntity().getName(),
                                context.getDatamartName());
                            promise.complete();
                        })
                        .onFailure(fail -> {
                            log.error("Error creating table [{}] in datamart [{}]!",
                                context.getRequest().getEntity().getName(),
                                context.getDatamartName(), fail);
                            promise.fail(fail);
                        });
                } else {
                    log.error("Error creating table [{}], datamart [{}] in datasources!",
                        context.getRequest().getEntity().getName(),
                        context.getDatamartName(),
                        ar.cause());
                    promise.fail(ar.cause());
                }
            });
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_TABLE;
    }
}
