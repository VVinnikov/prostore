package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.calcite.core.node.SqlTreeNode;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.exception.entity.EntityAlreadyExistsException;
import ru.ibs.dtm.query.execution.core.dao.exception.entity.ViewNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.dml.ColumnMetadataService;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Component
public class CreateViewDdlExecutor extends QueryResultDdlExecutor {
    private static final String VIEW_QUERY_PATH = "CREATE_VIEW.SELECT";
    protected final SqlDialect sqlDialect;
    protected final EntityDao entityDao;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final ColumnMetadataService columnMetadataService;

    @Autowired
    public CreateViewDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                                 LogicalSchemaProvider logicalSchemaProvider,
                                 ColumnMetadataService columnMetadataService,
                                 ServiceDbFacade serviceDbFacade,
                                 @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(metadataExecutor, serviceDbFacade);
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.columnMetadataService = columnMetadataService;
        this.sqlDialect = sqlDialect;
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        checkNotExistsSnapshot(context)
            .compose(v -> getCreateViewContext(context))
            .onFailure(error -> handler.handle(Future.failedFuture(error)))
            .onSuccess(ctx -> createOrReplaceEntity(ctx, handler));
    }

    protected Future<Void> checkNotExistsSnapshot(DdlRequestContext context) {
        return Future.future(p -> {
            List<SqlTreeNode> bySnapshot = new SqlSelectTree(context.getQuery())
                .findNodesByPath(SqlSelectTree.SELECT_AS_SNAPSHOT);
            if (bySnapshot.isEmpty()) {
                p.complete();
            } else {
                p.fail("View system_time not allowed");
            }
        });
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

    private Future<Entity> getEntityFuture(DdlRequestContext ctx, QueryRequest request) {
        return getLogicalSchema(request)
            .compose(datamarts -> getColumnMetadata(request, datamarts))
            .map(columnMetadata -> toViewEntity(ctx, columnMetadata));
    }

    private Future<List<Datamart>> getLogicalSchema(QueryRequest request) {
        return Future.future(p -> logicalSchemaProvider.getSchema(request, p));
    }

    private Future<List<ColumnMetadata>> getColumnMetadata(QueryRequest request, List<Datamart> datamarts) {
        return Future.future(p -> columnMetadataService.getColumnMetadata(new QueryParserRequest(request, datamarts), p));
    }

    private Entity toViewEntity(DdlRequestContext ctx, List<ColumnMetadata> columnMetadata) {
        val tree = new SqlSelectTree(ctx.getQuery());
        val viewNameNode = SqlPreparer.getViewNameNode(tree);
        val schemaName = viewNameNode.tryGetSchemaName()
            .orElseThrow(() -> new RuntimeException("Unable to get schema of view"));
        val viewName = viewNameNode.tryGetTableName()
            .orElseThrow(() -> new RuntimeException("Unable to get name of view"));
        val viewQuery = getViewQuery(tree);
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
            .ordinalPosition(position)
            .build();
    }

    protected String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new IllegalArgumentException("Unable to get view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    private void createOrReplaceEntity(CreateViewContext ctx, Handler<AsyncResult<QueryResult>> handler) {
        val viewEntity = ctx.getViewEntity();
        entityDao.createEntity(viewEntity)
            .otherwise(error -> checkCreateOrReplace(ctx, error))
            .compose(r -> entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName()))
            .map(this::checkEntityType)
            .compose(r -> entityDao.updateEntity(viewEntity))
            .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
            .onFailure(error -> handler.handle(Future.failedFuture(error)));
    }

    private Void checkCreateOrReplace(CreateViewContext ctx, Throwable error) {
        if (error instanceof EntityAlreadyExistsException && ctx.isCreateOrReplace()) {
            // if there is an exception <entity already exists> and <orReplace> is true
            // then continue
            return null;
        } else {
            throw new RuntimeException(error);
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
