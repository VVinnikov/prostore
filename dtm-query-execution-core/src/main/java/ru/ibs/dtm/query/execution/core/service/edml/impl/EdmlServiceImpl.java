package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.TableInfo;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dto.edml.EdmlAction;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.EdmlService;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Processing EDML-request service
 */
@Slf4j
@Service("coreEdmlService")
public class EdmlServiceImpl implements EdmlService<QueryResult> {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);

    private final EntityDao entityDao;
    private final Map<EdmlAction, EdmlExecutor> executors;

    @Autowired
    public EdmlServiceImpl(ServiceDbFacade serviceDbFacade,
                           List<EdmlExecutor> edmlExecutors) {
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.executors = edmlExecutors.stream().collect(Collectors.toMap(EdmlExecutor::getAction, it -> it));
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        defineExternalTableAndType(context)
                .compose(edmlType -> execute(context, edmlType))
                .setHandler(resultHandler);
    }

    private Future<EdmlAction> defineExternalTableAndType(EdmlRequestContext context) {
        return Future.future(edmlQueryPromise -> {
            initSourceAndTargetTables(context);
            getExternalEntity(Optional.empty(),
                    context.getTargetTable().getSchemaName(),
                    context.getTargetTable().getTableName(),
                    EntityType.DOWNLOAD_EXTERNAL_TABLE)
                    .compose(entity -> getExternalEntity(entity,
                            context.getSourceTable().getSchemaName(),
                            context.getSourceTable().getTableName(),
                            EntityType.UPLOAD_EXTERNAL_TABLE))
                    .onSuccess(entity -> {
                        if (entity.isPresent()) {
                            Entity extEntity = entity.get();
                            if (extEntity.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE) {
                                context.setEntity(extEntity);
                                edmlQueryPromise.complete(EdmlAction.DOWNLOAD);
                            } else if (extEntity.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE) {
                                context.setEntity(extEntity);
                                edmlQueryPromise.complete(EdmlAction.UPLOAD);
                            }
                        } else {
                            edmlQueryPromise.fail(String.format("Can't determine external table in query [%s]",
                                    context.getSqlNode().toSqlString(SQL_DIALECT).toString()));
                        }
                    })
                    .onFailure(edmlQueryPromise::fail);
        });
    }

    private Future<QueryResult> execute(EdmlRequestContext context, EdmlAction edmlAction) {
        return Future.future((Promise<QueryResult> promise) ->
                executors.get(edmlAction).execute(context, ar -> {
                    if (ar.succeeded()) {
                        promise.complete(ar.result());
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    private void initSourceAndTargetTables(EdmlRequestContext context) {
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val defDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val tableInfos = tableAndSnapshots.stream()
                .map(n -> new TableInfo(n.tryGetSchemaName().orElse(defDatamartMnemonic),
                        n.tryGetTableName().orElseThrow(() -> getCantGetTableNameError(context))))
                .collect(Collectors.toList());
        context.setTargetTable(tableInfos.get(0));
        context.setSourceTable(tableInfos.get(1));
    }

    private Future<Optional<Entity>> getExternalEntity(Optional<Entity> entity, String datamartName, String entityName, EntityType type) {
        return Future.future(entityPromise -> {
            if (entity.isPresent()) {
                entityPromise.complete(entity);
            } else {
                entityDao.getEntity(datamartName, entityName)
                        .onSuccess(extEntity -> {
                            if (type == extEntity.getEntityType()) {
                                entityPromise.complete(Optional.of(extEntity));
                            } else {
                                entityPromise.complete(Optional.empty());
                            }
                        })
                        .onFailure(error -> {
                            log.error("Table [{}] in datamart [{}] doesn't exist!",
                                    entityName,
                                    datamartName, error);
                            entityPromise.fail(error);

                        });
            }
        });
    }

    private RuntimeException getCantGetTableNameError(EdmlRequestContext context) {
        val sql = context.getRequest().getQueryRequest().getSql();
        return new RuntimeException("Can't get table name from sql: " + sql);
    }

}
