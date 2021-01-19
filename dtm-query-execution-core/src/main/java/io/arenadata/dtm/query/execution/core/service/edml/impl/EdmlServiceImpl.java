package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlAction;
import io.arenadata.dtm.query.execution.core.exception.table.ExternalTableNotExistsException;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.EdmlService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
    public Future<QueryResult> execute(EdmlRequestContext request) {
        return defineTablesAndType(request)
                .compose(edmlType -> executeInternal(request, edmlType));
    }

    private Future<EdmlAction> defineTablesAndType(EdmlRequestContext context) {
        return Future.future(edmlQueryPromise -> {
            getDestinationAndSourceEntities(context)
                    .onSuccess(entities -> {
                        val destination = entities.get(0);
                        val source = entities.get(1);
                        context.setDestinationEntity(destination);
                        context.setSourceEntity(source);
                        if (destination.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE
                                && checkSourceType(source)) {
                            edmlQueryPromise.complete(EdmlAction.DOWNLOAD);
                        } else if (source.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE
                                && destination.getEntityType() == EntityType.TABLE) {
                            edmlQueryPromise.complete(EdmlAction.UPLOAD);
                        } else {
                            edmlQueryPromise.fail(new ExternalTableNotExistsException(
                                    String.format("Can't determine external table from query [%s]",
                                            context.getSqlNode().toSqlString(SQL_DIALECT).toString())));
                        }
                    })
                    .onFailure(edmlQueryPromise::fail);
        });
    }

    private boolean checkSourceType(Entity source) {
        return source.getEntityType() == EntityType.TABLE || source.getEntityType() == EntityType.VIEW;
    }

    private Future<List<Entity>> getDestinationAndSourceEntities(EdmlRequestContext context) {
        val tableAndSnapshots = new SqlSelectTree(context.getSqlNode()).findAllTableAndSnapshots();
        val defaultDatamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        val tableInfos = tableAndSnapshots.stream()
                .map(n -> new TableInfo(n.tryGetSchemaName().orElse(defaultDatamartMnemonic),
                        n.tryGetTableName().orElseThrow(() -> getCantGetTableNameError(context))))
                .collect(Collectors.toList());
        val destinationTable = tableInfos.get(0);
        val sourceTable = tableInfos.get(1);
        return Future.future(p -> CompositeFuture.join(
                entityDao.getEntity(destinationTable.getSchemaName(),
                        destinationTable.getTableName()),
                entityDao.getEntity(sourceTable.getSchemaName(),
                        sourceTable.getTableName()))
                .onSuccess(entities -> p.complete(entities.list()))
                .onFailure(p::fail)
        );
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context, EdmlAction edmlAction) {
        return Future.future((Promise<QueryResult> promise) ->
                executors.get(edmlAction).execute(context)
                        .onComplete(promise));
    }

    private RuntimeException getCantGetTableNameError(EdmlRequestContext context) {
        val sql = context.getRequest().getQueryRequest().getSql();
        return new DtmException(String.format("Can't determine table from query [%s]", sql));
    }

}
