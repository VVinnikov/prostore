/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateMaterializedView;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicatePart;
import io.arenadata.dtm.query.calcite.core.node.SqlPredicates;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.rel2sql.DtmRelToSqlConverter;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.visitors.SqlForbiddenNamesFinder;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.dto.cache.MaterializedViewCacheValue;
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
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.core.ddl.utils.ValidationUtils.*;

@Slf4j
@Component
public class CreateMaterializedViewExecutor extends QueryResultDdlExecutor {
    private static final SqlPredicates VIEW_AND_TABLE_PREDICATE = SqlPredicates.builder()
            .anyOf(SqlPredicatePart.eqWithNum(SqlKind.JOIN), SqlPredicatePart.eq(SqlKind.SELECT))
            .maybeOf(SqlPredicatePart.eq(SqlKind.AS))
            .anyOf(SqlPredicatePart.eq(SqlKind.SNAPSHOT), SqlPredicatePart.eq(SqlKind.IDENTIFIER))
            .build();
    private static final String ALL_COLUMNS = "*";
    private static final int UUID_SIZE = 36;
    private final SqlDialect sqlDialect;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final CacheService<EntityKey, Entity> entityCacheService;
    private final CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final QueryParserService parserService;
    private final ColumnMetadataService columnMetadataService;
    private final MetadataCalciteGenerator metadataCalciteGenerator;
    private final DataSourcePluginService dataSourcePluginService;
    private final DtmRelToSqlConverter relToSqlConverter;

    @Autowired
    public CreateMaterializedViewExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                          ServiceDbFacade serviceDbFacade,
                                          @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                          @Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                          @Qualifier("materializedViewCacheService") CacheService<EntityKey, MaterializedViewCacheValue> materializedViewCacheService,
                                          LogicalSchemaProvider logicalSchemaProvider,
                                          ColumnMetadataService columnMetadataService,
                                          @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService,
                                          MetadataCalciteGenerator metadataCalciteGenerator,
                                          DataSourcePluginService dataSourcePluginService,
                                          @Qualifier("coreRelToSqlConverter") DtmRelToSqlConverter relToSqlConverter) {
        super(metadataExecutor, serviceDbFacade);
        this.sqlDialect = sqlDialect;
        this.datamartDao = serviceDbFacade.getServiceDbDao().getDatamartDao();
        this.entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
        this.entityCacheService = entityCacheService;
        this.materializedViewCacheService = materializedViewCacheService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.parserService = parserService;
        this.metadataCalciteGenerator = metadataCalciteGenerator;
        this.dataSourcePluginService = dataSourcePluginService;
        this.relToSqlConverter = relToSqlConverter;
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return updateContextAndValidate(context)
                .compose(unused -> parseSelect(((SqlCreateMaterializedView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .map(response -> {
                    checkTimestampFormat(response.getSqlNode());
                    return response;
                })
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

            checkSystemColumnNames(sqlNode)
                    .compose(v -> checkSnapshotNotExist(sqlNode))
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
                    .compose(e -> validateQueryDatamart(e, parserResponse.getSchema()))
                    .compose(this::checkEntityNotExists)
                    .compose(v -> executeRequest(context))
                    .compose(v -> entityDao.createEntity(context.getEntity()))
                    .onSuccess(v -> {
                        materializedViewCacheService.put(new EntityKey(context.getEntity().getSchema(), context.getEntity().getName()),
                                new MaterializedViewCacheValue(context.getEntity()));
                        p.complete(QueryResult.emptyResult());
                    })
                    .onFailure(p::fail);
        });
    }

    private Future<Entity> validateQueryDatamart(Entity entity, List<Datamart> datamarts) {
        return Future.future(p -> {
            if (datamarts.size() > 1) {
                p.fail(MaterializedViewValidationException.multipleDatamarts(entity.getName(), datamarts, entity.getViewQuery()));
                return;
            }

            if (datamarts.size() == 1) {
                String entityDatamart = entity.getSchema();
                String queryDatamart = datamarts.get(0).getMnemonic();
                if (!Objects.equals(entityDatamart, queryDatamart)) {
                    p.fail(MaterializedViewValidationException.differentDatamarts(entity.getName(), entityDatamart, queryDatamart));
                    return;
                }
            }

            p.complete(entity);
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
            return relToSqlConverter.convert(response.getRelNode().project());
        }
    }

    private boolean isAllColumnSelect(SqlNode sqlNode) {
        return sqlNode.toSqlString(sqlDialect).getSql().contains(ALL_COLUMNS);
    }

    @SneakyThrows
    protected void replaceSqlSelectQuery(DdlRequestContext context, SqlNode newSelectNode) {
        val sql = (SqlCreateMaterializedView) context.getSqlNode();
        val selectList = ((SqlSelect) newSelectNode).getSelectList();

        if (selectList != null) {
            SqlNodeList updatedSelectList = withColumnAliases(selectList, sql.getColumnList(), sql.getName().toString());
            ((SqlSelect) newSelectNode).setSelectList(updatedSelectList);
        }

        val newSql = new SqlCreateMaterializedView(sql.getParserPosition(), sql.getName(), sql.getColumnList(), sql.getDistributedBy(), sql.getDestination(), newSelectNode, sql.isLogicalOnly());
        context.setSqlNode(newSql);
    }

    private SqlNodeList withColumnAliases(SqlNodeList selectList, SqlNodeList matViewColumns, String matView) {
        SqlNodeList updatedSelectList = new SqlNodeList(selectList.getParserPosition());

        val matViewColumnsCount = (int) matViewColumns.getList().stream()
                .filter(sqlNode -> sqlNode instanceof SqlColumnDeclaration)
                .count();
        val queryColumnsCount = selectList.size();
        if (queryColumnsCount != matViewColumnsCount) {
            throw MaterializedViewValidationException.columnCountConflict(matView, matViewColumnsCount, queryColumnsCount);
        }

        for (int i = 0; i < queryColumnsCount; i++) {
            SqlNode current = selectList.get(i);

            SqlBasicCall expression;
            if (current instanceof SqlBasicCall) {
                expression = (SqlBasicCall) current;
                List<SqlNode> operands = expression.getOperandList();

                if (expression.getOperator().getKind() == SqlKind.AS) {
                    SqlIdentifier newAlias = ((SqlIdentifier) operands.get(1)).setName(0, getMatViewColumnAlias(matViewColumns.get(i)));
                    expression.setOperand(1, newAlias);
                } else {
                    expression = buildAlias(expression, matViewColumns.get(i), expression.getParserPosition(), selectList.get(i).getParserPosition());
                }
            } else {
                expression = buildAlias(current, matViewColumns.get(i), current.getParserPosition(), selectList.get(i).getParserPosition());
            }
            updatedSelectList.add(expression);
        }

        return updatedSelectList;
    }

    private SqlBasicCall buildAlias(SqlNode queryColumn, SqlNode matViewColumn, SqlParserPos aliasPos, SqlParserPos pos) {
        return new SqlBasicCall(
                new SqlAsOperator(),
                new SqlNode[]{
                        queryColumn,
                        new SqlIdentifier(
                                getMatViewColumnAlias(matViewColumn),
                                aliasPos
                        )
                },
                pos
        );
    }

    private String getMatViewColumnAlias(SqlNode column) {
        return ((SqlIdentifier) ((SqlColumnDeclaration) column).getOperandList().get(0)).getSimple();
    }

    private Future<Void> checkSystemColumnNames(SqlNode sqlNode) {
        return Future.future(p -> {
            val sqlForbiddenNamesFinder = new SqlForbiddenNamesFinder();
            sqlNode.accept(sqlForbiddenNamesFinder);
            if (!sqlForbiddenNamesFinder.getFoundForbiddenNames().isEmpty()) {
                p.fail(new ViewDisalowedOrDirectiveException(sqlNode.toSqlString(sqlDialect).getSql(),
                        String.format("View query contains forbidden system names: %s", sqlForbiddenNamesFinder.getFoundForbiddenNames())));
                return;
            }

            p.complete();
        });
    }

    private Future<Void> checkSnapshotNotExist(SqlNode sqlNode) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(sqlNode).findSnapshots();
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail(new ViewDisalowedOrDirectiveException(sqlNode.toSqlString(sqlDialect).getSql()));
            }
        });
    }

    private Future<Void> checkEntitiesType(SqlNode sqlNode, String contextDatamartName) {
        return Future.future(promise -> {
            final List<SqlTreeNode> nodes = new SqlSelectTree(sqlNode).findNodes(VIEW_AND_TABLE_PREDICATE, true);
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
        checkFieldsDuplication(entity.getFields());
    }

    private void checkFieldsMatch(Entity entity, List<ColumnMetadata> queryColumns) {
        List<EntityField> viewFields = entity.getFields();
        if (viewFields.size() != queryColumns.size()) {
            throw MaterializedViewValidationException.columnCountConflict(entity.getName(), entity.getFields().size(), queryColumns.size());
        }

        for (int i = 0; i < viewFields.size(); i++) {
            EntityField entityField = viewFields.get(i);
            ColumnMetadata columnMetadata = queryColumns.get(i);

            if (!isCompatibleTypes(entityField, columnMetadata)) {
                throw MaterializedViewValidationException.columnTypesConflict(entity.getName(), entityField.getName(), entityField.getType(), columnMetadata.getType());
            }

            switch (entityField.getType()) {
                case TIME:
                case TIMESTAMP:
                    if (isMismatched(entityField.getAccuracy(), columnMetadata)) {
                        throw MaterializedViewValidationException.columnTypeAccuracyConflict(entity.getName(), entityField.getName(), entityField.getSize(), columnMetadata.getSize());
                    }
                    break;
                case UUID:
                    if (isMismatched(UUID_SIZE, columnMetadata)) {
                        throw MaterializedViewValidationException.columnTypeSizeConflict(entity.getName(), entityField.getName(), UUID_SIZE, columnMetadata.getSize());
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

    private boolean isCompatibleTypes(EntityField entityField, ColumnMetadata columnMetadata) {
        if (entityField.getType() == columnMetadata.getType() ||
                entityField.getType() == ColumnType.ANY ||
                columnMetadata.getType() == ColumnType.ANY) {
            return true;
        }

        if (columnMetadata.getType() == ColumnType.INT || columnMetadata.getType() == ColumnType.BIGINT) {
            return entityField.getType() == ColumnType.INT || entityField.getType() == ColumnType.BIGINT;
        }

        return false;
    }

    private boolean isMismatched(Integer sizeOrAccuracy, ColumnMetadata columnMetadata) {
        return sizeOrAccuracy != null && !sizeOrAccuracy.equals(columnMetadata.getSize()) ||
                sizeOrAccuracy == null && columnMetadata.getSize() != null && !columnMetadata.getSize().equals(-1);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.CREATE_MATERIALIZED_VIEW;
    }
}
