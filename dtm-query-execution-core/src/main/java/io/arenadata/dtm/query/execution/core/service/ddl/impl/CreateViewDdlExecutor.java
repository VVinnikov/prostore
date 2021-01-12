package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.exception.table.TableAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.exception.view.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.exception.view.ViewDisalowedOrDirectiveException;
import io.arenadata.dtm.query.execution.core.exception.view.ViewNotExistsException;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.utils.SqlPreparer;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Component
public class CreateViewDdlExecutor extends QueryResultDdlExecutor {
    private static final String VIEW_QUERY_PATH = "CREATE_VIEW.SELECT";
    private static final String VIEW_AND_TABLE_PATTERN = "(?i).*(JOIN|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)$";
    protected final SqlDialect sqlDialect;
    protected final EntityDao entityDao;
    protected final CacheService<EntityKey, Entity> entityCacheService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ColumnMetadataService columnMetadataService;

    @Autowired
    public CreateViewDdlExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                 MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 LogicalSchemaProvider logicalSchemaProvider,
                                 ColumnMetadataService columnMetadataService,
                                 ServiceDbFacade serviceDbFacade,
                                 @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.sqlDialect = sqlDialect;
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return checkViewQuery(context)
                .compose(v -> getCreateViewContext(context))
                .compose(this::createOrReplaceEntity);
    }

    protected Future<Void> checkViewQuery(DdlRequestContext context) {
        return checkSnapshotNotExist(context)
                .compose(v -> checkEntitiesType(context));
    }

    protected Future<CreateViewContext> getCreateViewContext(DdlRequestContext context) {
        return Future.future(p -> {
            val tree = new SqlSelectTree(context.getQuery());
            val viewQuery = getViewQuery(tree);
            QueryRequest request = QueryRequest.builder()
                    .datamartMnemonic(context.getDatamartName())
                    .requestId(UUID.randomUUID())
                    .sql(viewQuery)
                    .build();
            getEntityFuture(context, request)
                    .map(entity -> {
                        String sql = context.getRequest().getQueryRequest().getSql();
                        return CreateViewContext.builder()
                                .createOrReplace(SqlPreparer.isCreateOrReplace(sql))
                                .viewEntity(entity)
                                .sql(sql)
                                .build();
                    })
                    .onComplete(p);
        });
    }

    private Future<QueryResult> createOrReplaceEntity(CreateViewContext ctx) {
        return Future.future(promise -> {
            val viewEntity = ctx.getViewEntity();
            entityCacheService.remove(new EntityKey(viewEntity.getSchema(), viewEntity.getName()));
            entityDao.createEntity(viewEntity)
                    .otherwise(error -> checkCreateOrReplace(ctx, error))
                    .compose(r -> entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName()))
                    .map(this::checkEntityType)
                    .compose(r -> entityDao.updateEntity(viewEntity))
                    .onSuccess(success -> {
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });

    }

    private Future<Void> checkSnapshotNotExist(DdlRequestContext context) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(context.getQuery())
                    .findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail(new ViewDisalowedOrDirectiveException(context.getQuery().toSqlString(sqlDialect).getSql()));
            }
        });
    }

    private Future<Void> checkEntitiesType(DdlRequestContext context) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = new SqlSelectTree(context.getQuery())
                    .findNodesByPathRegex(VIEW_AND_TABLE_PATTERN);
            final List<Future> entityFutures = getEntitiesFutures(context, nodes);
            CompositeFuture.join(entityFutures)
                    .onSuccess(result -> {
                        final List<Object> entities = result.list();
                        entities.forEach(e -> {
                            final Entity entity = (Entity) e;
                            if (entity.getEntityType() != EntityType.TABLE) {
                                promise.fail(new ViewDisalowedOrDirectiveException(
                                        context.getQuery().toSqlString(sqlDialect).getSql()));
                            }
                        });
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future> getEntitiesFutures(DdlRequestContext context, List<SqlTreeNode> nodes) {
        final List<Future> entityFutures = new ArrayList<>();
        nodes.forEach(node -> {
            String datamartName = context.getRequest().getQueryRequest().getDatamartMnemonic();
            String tableName;
            final Optional<String> schema = node.tryGetSchemaName();
            final Optional<String> table = node.tryGetTableName();
            if (schema.isPresent()) {
                datamartName = schema.get();
            }
            if (table.isPresent()) {
                tableName = table.get();
            } else {
                throw new DtmException(String.format("Can't extract table name from query %s",
                        context.getQuery().toSqlString(sqlDialect).toString()));
            }
            entityCacheService.remove(new EntityKey(datamartName, tableName));
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }

    private Future<Entity> getEntityFuture(DdlRequestContext ctx, QueryRequest request) {
        return logicalSchemaProvider.getSchemaFromQuery(request)
                .compose(datamarts -> columnMetadataService.getColumnMetadata(new QueryParserRequest(request, datamarts)))
                .map(columnMetadata -> toViewEntity(ctx, columnMetadata));
    }

    private Entity toViewEntity(DdlRequestContext ctx, List<ColumnMetadata> columnMetadata) {
        val tree = new SqlSelectTree(ctx.getQuery());
        val viewNameNode = SqlPreparer.getViewNameNode(tree);
        val schemaName = viewNameNode.tryGetSchemaName()
                .orElseThrow(() -> new DtmException("Unable to get schema of view"));
        val viewName = viewNameNode.tryGetTableName()
                .orElseThrow(() -> new DtmException("Unable to get name of view"));
        val viewQuery = getViewQuery(tree).replace("\n", " ").replace("\r", "");
        ctx.setDatamartName(schemaName);
        return Entity.builder()
                .name(viewName)
                .schema(schemaName)
                .entityType(EntityType.VIEW)
                .viewQuery(viewQuery)
                .fields(getEntityFields(columnMetadata))
                .build();
    }

    private List<EntityField> getEntityFields(List<ColumnMetadata> columnMetadata) {
        return IntStream.range(0, columnMetadata.size())
                .mapToObj(position -> toEntityField(columnMetadata.get(position), position))
                .collect(Collectors.toList());
    }

    private EntityField toEntityField(ColumnMetadata cm, int position) {
        return EntityField.builder()
                .name(cm.getName())
                .nullable(true)
                .type(cm.getType())
                .size(cm.getSize())
                .ordinalPosition(position)
                .build();
    }

    protected String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new DtmException("Unable to get view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    private Void checkCreateOrReplace(CreateViewContext ctx, Throwable error) {
        if (error instanceof TableAlreadyExistsException && ctx.isCreateOrReplace()) {
            // if there is an exception <entity already exists> and <orReplace> is true
            // then continue
            return null;
        } else {
            throw new EntityAlreadyExistsException(ctx.getViewEntity().getNameWithSchema());
        }
    }

    protected Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new ViewNotExistsException(entity.getName()));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_VIEW;
    }

    @Data
    @Builder
    protected final static class CreateViewContext {
        private final boolean createOrReplace;
        private final Entity viewEntity;
        private final String sql;
    }
}
