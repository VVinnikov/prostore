package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.exception.table.TableNotExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlType;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newHashSet;

@Slf4j
@Component
public class DropTableDdlExecutor extends QueryResultDdlExecutor {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityCacheService entityCacheService;
    private final EntityDao entityDao;

    @Autowired
    public DropTableDdlExecutor(@Qualifier("entityCacheService") EntityCacheService entityCacheService,
                                MetadataExecutor<DdlRequestContext> metadataExecutor,
                                ServiceDbFacade serviceDbFacade,
                                DataSourcePluginService dataSourcePluginService) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, AsyncHandler<QueryResult> handler) {
        try {
            String schema = getSchemaName(context.getRequest().getQueryRequest(), sqlNodeName);
            String tableName = getTableName(sqlNodeName);
            entityCacheService.remove(schema, tableName);
            Entity entity = createClassTable(schema, tableName);
            context.getRequest().setEntity(entity);
            context.setDatamartName(schema);
            context.setDdlType(DdlType.DROP_TABLE);
            dropTable(context, containsIfExistsCheck(context.getRequest().getQueryRequest().getSql()))
                    .onSuccess(r -> handler.handleSuccess(QueryResult.emptyResult()))
                    .onFailure(handler::handleError);
        } catch (Exception e) {
            handler.handleError("Error create drop table request", e);
        }
    }

    private Entity createClassTable(String schema, String tableName) {
        return new Entity(getTableNameWithSchema(schema, tableName), null);
    }

    protected Future<Void> dropTable(DdlRequestContext context, boolean ifExists) {
        return getEntity(context, ifExists)
                .compose(entity -> Optional.ofNullable(entity)
                        .map(e -> updateEntity(context, e))
                        .orElse(Future.succeededFuture()));
    }

    private boolean containsIfExistsCheck(String sql) {
        return sql.toLowerCase().contains("if exists");
    }

    private Future<Entity> getEntity(DdlRequestContext context, boolean ifExists) {
        return Future.future(entityPromise -> {
            val datamartName = context.getDatamartName();
            val entityName = context.getRequest().getEntity().getName();
            val tableWithSchema = datamartName + "." + entityName;
            entityDao.getEntity(datamartName, entityName)
                    .onSuccess(entity -> {
                        if (EntityType.TABLE == entity.getEntityType()) {
                            entityPromise.complete(entity);
                        } else {
                            entityPromise.fail(new TableNotExistsException(tableWithSchema));
                        }
                    })
                    .onFailure(error -> {
                        if (error instanceof TableNotExistsException && ifExists) {
                            entityPromise.complete(null);
                        } else {
                            entityPromise.fail(new TableNotExistsException(tableWithSchema));
                        }
                    });
        });
    }

    private Future<Void> updateEntity(DdlRequestContext context, Entity entity) {
        //we have to use source type from queryRequest.sourceType because
        //((SqlDropTable) context.getQuery()).getDestination() is always null,
        // since we cut sourceType from all query in HintExtractor
        Optional<SourceType> requestDestination = Optional.ofNullable(context.getRequest().getQueryRequest().getSourceType());
        if (!requestDestination.isPresent()) {
            context.getRequest().getEntity().setDestination(dataSourcePluginService.getSourceTypes());
            return dropEntityFromEverywhere(context, entity.getName());
        } else {
            final Set<SourceType> reqSourceTypes = newHashSet(requestDestination.get());
            return dropFromDataSource(context, entity, reqSourceTypes);
        }
    }

    private Future<Void> dropFromDataSource(DdlRequestContext context,
                                            Entity entity,
                                            Set<SourceType> requestDestination) {
        final Set<SourceType> notExistsDestination = requestDestination.stream()
                .filter(type -> !entity.getDestination().contains(type))
                .collect(Collectors.toSet());
        if (!notExistsDestination.isEmpty()) {
            return Future.failedFuture(
                    new DtmException(String.format("Table [%s] doesn't exist in [%s]",
                            entity.getName(),
                            notExistsDestination)));
        } else {
            //find corresponding datasources in request and active plugins configuration
            Set<SourceType> resultDropDestination = dataSourcePluginService.getSourceTypes().stream()
                    .filter(requestDestination::contains)
                    .collect(Collectors.toSet());
            if (resultDropDestination.isEmpty()) {
                entity.setDestination(entity.getDestination().stream()
                        .filter(type -> !requestDestination.contains(type))
                        .collect(Collectors.toSet()));
                return entityDao.updateEntity(entity);
            } else {
                entity.setDestination(entity.getDestination().stream()
                        .filter(type -> !resultDropDestination.contains(type))
                        .collect(Collectors.toSet()));
                context.getRequest().getEntity().setDestination(resultDropDestination);
                if (entity.getDestination().isEmpty()) {
                    return dropEntityFromEverywhere(context, entity.getName());
                } else {
                    return dropEntityFromPlugins(context)
                            .compose(v -> entityDao.updateEntity(entity));
                }
            }
        }
    }

    private Future<Void> dropEntityFromEverywhere(DdlRequestContext context, String entityName) {
        return dropEntityFromPlugins(context)
                .compose(v -> {
                    context.getPostActions().add(PostSqlActionType.UPDATE_INFORMATION_SCHEMA);
                    return entityDao.deleteEntity(context.getDatamartName(), entityName);
                });
    }

    private Future<Void> dropEntityFromPlugins(DdlRequestContext context) {
        return Future.future((Promise<Void> metaPromise) -> metadataExecutor.execute(context, ar -> {
            if (ar.succeeded()) {
                metaPromise.complete();
            } else {
                metaPromise.fail(ar.cause());
            }
        }));
    }


    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    public List<PostSqlActionType> getPostActions() {
        return Collections.singletonList(PostSqlActionType.PUBLISH_STATUS);
    }
}
