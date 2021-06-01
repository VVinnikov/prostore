package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateMaterializedView;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.rel2sql.NullNotCastableRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.base.exception.materializedview.MaterializedViewValidationException;
import io.arenadata.dtm.query.execution.core.base.exception.view.ViewDisalowedOrDirectiveException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataCalciteGenerator;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlType;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.checkRequiredKeys;
import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.checkVarcharSize;

@Slf4j
@Component
public class CreateMaterializedViewDdlExecutor extends QueryResultDdlExecutor {
    private static final String VIEW_AND_TABLE_PATTERN = "(?i).*(JOIN|SELECT)\\.(|AS\\.)(SNAPSHOT|IDENTIFIER)$";
    private static final String ALL_COLUMNS = "*";
    private final SqlDialect sqlDialect;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final QueryParserService parserService;
    private final ColumnMetadataService columnMetadataService;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public CreateMaterializedViewDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                             ServiceDbFacade serviceDbFacade,
                                             @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                             @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                             LogicalSchemaProvider logicalSchemaProvider,
                                             ColumnMetadataService columnMetadataService,
                                             @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService,
                                             MetadataCalciteGenerator metadataCalciteGenerator,
                                             DataSourcePluginService dataSourcePluginService) {
        super(metadataExecutor, serviceDbFacade);
        this.sqlDialect = sqlDialect;
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.entityCacheService = entityCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.parserService = parserService;
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return updateContextAndValidate(context)
                .compose(unused -> parseSelect(((SqlCreateMaterializedView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .compose(response -> createMaterializedView(context, response));
    }

    protected Future<Void> updateContextAndValidate(DdlRequestContext context) {
        return Future.future(p -> {
            context.setDdlType(DdlType.CREATE_MATERIALIZED_VIEW);

            val datamartName = context.getDatamartName();
            val sqlNode = (SqlCreateMaterializedView) context.getSqlNode();

            SourceType querySourceType = getQuerySourceType(sqlNode.getQuery());
            if (querySourceType == null) {
                throw MaterializedViewValidationException.queryDataSourceInvalid(sqlNode.getName().toString());
            }

            context.setSourceType(querySourceType);

            Set<SourceType> destination = sqlNode.getDestination();
            if (destination != null && !destination.isEmpty()) {
                Set<SourceType> nonExistDestinations = destination.stream()
                        .filter(sourceType -> !dataSourcePluginService.hasSourceType(sourceType))
                        .collect(Collectors.toSet());
                if (!nonExistDestinations.isEmpty()) {
                    throw MaterializedViewValidationException.viewDataSourceInvalid(sqlNode.getName().toString(), nonExistDestinations);
                }
            }

            checkSnapshotNotExist(sqlNode)
                    .compose(v -> checkEntitiesType(sqlNode, datamartName))
                    .onComplete(p);
        });
    }

    protected Future<QueryParserResponse> parseSelect(SqlNode viewQuery, String datamart) {
        return logicalSchemaProvider.getSchemaFromQuery(viewQuery, datamart)
                .compose(datamarts -> parserService.parse(new QueryParserRequest(viewQuery, datamarts)));
    }

    protected Future<QueryResult> createMaterializedView(DdlRequestContext context, QueryParserResponse parserResponse) {
        return Future.future(p -> {
            val selectSqlNode = getParsedSelect(context.getSqlNode(), parserResponse);
            replaceSqlSelectQuery(context, selectSqlNode);
            prepareEntityFuture(context, selectSqlNode, parserResponse.getSchema())
                    .compose(this::checkEntityNotExists)
                    .compose(e -> metadataExecutor.execute(context))
                    .compose(v -> entityDao.createEntity(context.getEntity()))
                    .onSuccess(v -> p.complete(QueryResult.emptyResult()))
                    .onFailure(p::fail);
        });
    }

    private Future<Entity> checkEntityNotExists(Entity entity) {
        return Future.future(p -> {
            String datamartName = entity.getSchema();
            String entityName = entity.getName();
            datamartDao.existsDatamart(datamartName)
                    .compose(existsDatamart -> existsDatamart ? entityDao.existsEntity(datamartName, entityName) : Future.failedFuture(new DatamartNotExistsException(datamartName)))
                    .onSuccess(existsEntity -> {
                        if (!existsEntity) {
                            p.complete(entity);
                        } else {
                            p.fail(new EntityAlreadyExistsException(entityName));
                        }
                    })
                    .onFailure(p::fail);
        });
    }

    private SourceType getQuerySourceType(SqlNode sqlNode) {
        if (!(sqlNode instanceof SqlDataSourceTypeGetter)) {
            return null;
        }

        SqlCharStringLiteral datasourceType = ((SqlDataSourceTypeGetter) sqlNode).getDatasourceType();
        if (datasourceType == null) {
            return null;
        }

        SourceType sourceType = SourceType.valueOfAvailable(datasourceType.getNlsString().getValue());
        if (!dataSourcePluginService.hasSourceType(sourceType)) {
            return null;
        }

        return sourceType;
    }

    private SqlNode getParsedSelect(SqlNode originalSqlNode, QueryParserResponse response) {
        if (isAllColumnSelect(originalSqlNode)) {
            return response.getSqlNode();
        } else {
            return new NullNotCastableRelToSqlConverter(sqlDialect).visitChild(0, response.getRelNode().project()).asStatement();
        }
    }

    private boolean isAllColumnSelect(SqlNode sqlNode) {
        return sqlNode.toSqlString(sqlDialect).getSql().contains(ALL_COLUMNS);
    }

    @SneakyThrows
    protected void replaceSqlSelectQuery(DdlRequestContext context, SqlNode newSelectNode) {
        val sql = (SqlCreateMaterializedView) context.getSqlNode();
        val newSql = new SqlCreateMaterializedView(sql.getParserPosition(), sql.getName(), sql.getColumnList(), sql.getDistributedBy(), sql.getDestination(), newSelectNode);
        context.setSqlNode(newSql);
    }

    private Future<Void> checkSnapshotNotExist(SqlNode sqlNode) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(sqlNode)
                    .findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail(new ViewDisalowedOrDirectiveException(sqlNode.toSqlString(sqlDialect).getSql()));
            }
        });
    }

    private Future<Void> checkEntitiesType(SqlNode sqlNode, String contextDatamartName) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = new SqlSelectTree(sqlNode)
                    .findNodesByPathRegex(VIEW_AND_TABLE_PATTERN);
            final List<Future> entityFutures = getEntitiesFutures(contextDatamartName, sqlNode, nodes);
            CompositeFuture.join(entityFutures)
                    .onSuccess(result -> {
                        final List<Entity> entities = result.list();
                        if (entities.stream().anyMatch(entity -> entity.getEntityType() != EntityType.TABLE)) {
                            promise.fail(new ViewDisalowedOrDirectiveException(
                                    sqlNode.toSqlString(sqlDialect).getSql()));
                        }
                        promise.complete();
                    })
                    .onFailure(promise::fail);
        });
    }

    @NotNull
    private List<Future> getEntitiesFutures(String contextDatamartName, SqlNode sqlNode, List<SqlTreeNode> nodes) {
        final List<Future> entityFutures = new ArrayList<>();
        nodes.forEach(node -> {
            String datamartName = contextDatamartName;
            String tableName;
            final Optional<String> schema = node.tryGetSchemaName();
            final Optional<String> table = node.tryGetTableName();
            if (schema.isPresent()) {
                datamartName = schema.get();
            }

            tableName = table.orElseThrow(() -> new DtmException(String.format("Can't extract table name from query %s",
                    sqlNode.toSqlString(sqlDialect).toString())));

            entityCacheService.remove(new EntityKey(datamartName, tableName));
            entityFutures.add(entityDao.getEntity(datamartName, tableName));
        });
        return entityFutures;
    }

    private Future<Entity> prepareEntityFuture(DdlRequestContext ctx, SqlNode viewQuery, List<Datamart> datamarts) {
        return columnMetadataService.getColumnMetadata(new QueryParserRequest(viewQuery, datamarts))
                .map(columnMetadata -> toMaterializedViewEntity(ctx, viewQuery, columnMetadata));
    }

    private Entity toMaterializedViewEntity(DdlRequestContext ctx, SqlNode viewQuery, List<ColumnMetadata> columnMetadata) {
        val sqlCreateMaterializedView = (SqlCreateMaterializedView) ctx.getSqlNode();

        val destination = Optional.ofNullable(sqlCreateMaterializedView.getDestination())
                .filter(sourceTypes -> !sourceTypes.isEmpty())
                .orElse(dataSourcePluginService.getSourceTypes());

        val viewQueryString = viewQuery.toSqlString(sqlDialect)
                .getSql()
                .replace("\n", " ").replace("\r", "");

        val entity = metadataCalciteGenerator.generateTableMetadata(sqlCreateMaterializedView);
        entity.setEntityType(EntityType.MATERIALIZED_VIEW);
        entity.setDestination(destination);
        entity.setViewQuery(viewQueryString);
        entity.setMaterializedDeltaNum(null);
        entity.setMaterializedDataSource(ctx.getSourceType());

        validateFields(entity, columnMetadata);
        setNullability(entity);

        ctx.setEntity(entity);
        ctx.setDatamartName(entity.getSchema());
        return entity;
    }

    private void setNullability(Entity entity) {
        for (EntityField field : entity.getFields()) {
            if (field.getPrimaryOrder() != null || field.getShardingOrder() != null) {
                field.setNullable(false);
            }
        }
    }

    private void validateFields(Entity entity, List<ColumnMetadata> columnMetadata) {
        checkRequiredKeys(entity.getFields());
        checkVarcharSize(entity.getFields());
        checkFieldsMatch(entity, columnMetadata);
    }

    private void checkFieldsMatch(Entity entity, List<ColumnMetadata> queryColumns) {
        List<EntityField> viewFields = entity.getFields();
        if (viewFields.size() != queryColumns.size()) {
            throw MaterializedViewValidationException.columnCountConflict(entity.getName(), entity.getFields().size(), queryColumns.size());
        }

        for (int i = 0; i < viewFields.size(); i++) {
            EntityField entityField = viewFields.get(i);
            ColumnMetadata columnMetadata = queryColumns.get(i);

            if (entityField.getType() != columnMetadata.getType()) {
                throw MaterializedViewValidationException.columnTypesConflict(entity.getName(), entityField.getName(), entityField.getType(), columnMetadata.getType());
            }

            switch (entityField.getType()) {
                case TIME:
                case TIMESTAMP:
                    if (isMismatched(entityField.getAccuracy(), columnMetadata)) {
                        throw MaterializedViewValidationException.columnTypeAccuracyConflict(entity.getName(), entityField.getName(), entityField.getSize(), columnMetadata.getSize());
                    }
                    break;
                default:
                    if (isMismatched(entityField.getSize(), columnMetadata)) {
                        throw MaterializedViewValidationException.columnTypeSizeConflict(entity.getName(), entityField.getName(), entityField.getSize(), columnMetadata.getSize());
                    }
                    break;
            }
        }
    }

    private boolean isMismatched(Integer sizeOrAccuracy, ColumnMetadata columnMetadata) {
        return sizeOrAccuracy != null && !sizeOrAccuracy.equals(columnMetadata.getSize()) ||
                sizeOrAccuracy == null && columnMetadata.getSize() != null && !columnMetadata.getSize().equals(-1);
    }

    @Override
    public Set<SqlKind> getSqlKinds() {
        return Collections.singleton(SqlKind.CREATE_MATERIALIZED_VIEW);
    }
}
