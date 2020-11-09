package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.EdmlService;
import io.vertx.core.*;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
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
            final List<TableInfo> tableInfoList = getSourceAndTargetTableInfo(context);
            getEntities(tableInfoList)
                    .onSuccess(entities -> {
                        val source = entities.get(0);
                        val destination = entities.get(1);
                        context.setDestinationEntity(destination);
                        context.setSourceEntity(source);
                        if (destination.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE) {
                            edmlQueryPromise.complete(EdmlAction.DOWNLOAD);
                        } else if (source.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE) {
                            edmlQueryPromise.complete(EdmlAction.UPLOAD);
                        }
                    })
                    .onFailure(fail -> edmlQueryPromise.fail(String.format("Can't determine external table in query [%s]",
                            context.getSqlNode().toSqlString(SQL_DIALECT).toString())));
        });
    }

    private Future<List<Entity>> getEntities(List<TableInfo> tableInfoList) {
        final TableInfo destinationTable = tableInfoList.get(0);
        final TableInfo sourceTable = tableInfoList.get(1);
        return Future.future(p -> CompositeFuture.join(
                entityDao.getEntity(destinationTable.getSchemaName(), sourceTable.getTableName()),
                entityDao.getEntity(destinationTable.getSchemaName(), destinationTable.getTableName()))
                .onSuccess(entities -> p.complete(entities.list()))
                .onFailure(p::fail)
        );
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

    private List<TableInfo> getSourceAndTargetTableInfo(EdmlRequestContext context) {
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val defDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return tableAndSnapshots.stream()
                .map(n -> new TableInfo(n.tryGetSchemaName().orElse(defDatamartMnemonic),
                        n.tryGetTableName().orElseThrow(() -> getCantGetTableNameError(context))))
                .collect(Collectors.toList());
    }

    private RuntimeException getCantGetTableNameError(EdmlRequestContext context) {
        val sql = context.getRequest().getQueryRequest().getSql();
        return new RuntimeException("Can't get table name from sql: " + sql);
    }

}
