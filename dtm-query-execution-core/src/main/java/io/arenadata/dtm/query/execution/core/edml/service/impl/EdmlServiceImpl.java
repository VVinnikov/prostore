package io.arenadata.dtm.query.execution.core.edml.service.impl;

import io.arenadata.dtm.common.dto.TableInfo;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlAction;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.service.EdmlExecutor;
import io.arenadata.dtm.query.execution.core.edml.service.EdmlService;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Processing EDML-request service
 */
@Slf4j
@Service("coreEdmlService")
public class EdmlServiceImpl implements EdmlService<QueryResult> {

    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private static final Set<EntityType> DOWNLOAD_SOURCES = EnumSet.of(EntityType.TABLE, EntityType.VIEW, EntityType.MATERIALIZED_VIEW);
    private static final Set<EntityType> UPLOAD_DESTINATIONS = EnumSet.of(EntityType.TABLE);

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
        return defineEdmlAction(request)
                .compose(edmlType -> executeInternal(request, edmlType));
    }

    private Future<EdmlAction> defineEdmlAction(EdmlRequestContext context) {
        if (context.getSqlNode().getKind() == SqlKind.ROLLBACK) {
            return Future.succeededFuture(EdmlAction.ROLLBACK);
        } else {
            return defineTablesAndType(context);
        }
    }

    private Future<EdmlAction> defineTablesAndType(EdmlRequestContext context) {
        return Future.future(edmlQueryPromise -> {
            getDestinationAndSourceEntities(context)
                    .onSuccess(entities -> {
                        val destination = entities.get(0);
                        val source = entities.get(1);
                        context.setDestinationEntity(destination);
                        context.setSourceEntity(source);

                        if (destination.getEntityType() == EntityType.DOWNLOAD_EXTERNAL_TABLE) {
                            if (!DOWNLOAD_SOURCES.contains(source.getEntityType())) {
                                edmlQueryPromise.fail(new DtmException(String.format("DOWNLOAD_EXTERNAL_TABLE source entity type mismatch. %s found, but %s expected.",
                                        source.getEntityType(), DOWNLOAD_SOURCES)));
                                return;
                            }

                            edmlQueryPromise.complete(EdmlAction.DOWNLOAD);
                        } else if (source.getEntityType() == EntityType.UPLOAD_EXTERNAL_TABLE) {
                            if (!UPLOAD_DESTINATIONS.contains(destination.getEntityType())) {
                                edmlQueryPromise.fail(new DtmException(String.format("UPLOAD_EXTERNAL_TABLE destination entity type mismatch. %s found, but %s expected.",
                                        destination.getEntityType(), UPLOAD_DESTINATIONS)));
                                return;
                            }

                            edmlQueryPromise.complete(EdmlAction.UPLOAD);
                        } else {
                            edmlQueryPromise.fail(new DtmException(
                                    String.format("Can't determine external table from query [%s]",
                                            context.getSqlNode().toSqlString(SQL_DIALECT).toString())));
                        }
                    })
                    .onFailure(edmlQueryPromise::fail);
        });
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
        if (!destinationTable.getSchemaName().equals(sourceTable.getSchemaName())) {
            return Future.failedFuture(new DtmException(String.format("Unsupported operation for tables in different datamarts: [%s] and [%s]",
                    destinationTable.getSchemaName(), sourceTable.getSchemaName())));
        }
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
