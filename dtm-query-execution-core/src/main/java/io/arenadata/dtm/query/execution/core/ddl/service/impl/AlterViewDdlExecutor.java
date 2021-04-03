package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlAlterView;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.base.service.metadata.LogicalSchemaProvider;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AlterViewDdlExecutor extends CreateViewDdlExecutor {

    @Autowired
    public AlterViewDdlExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                MetadataExecutor<DdlRequestContext> metadataExecutor,
                                LogicalSchemaProvider logicalSchemaProvider,
                                ColumnMetadataService columnMetadataService,
                                ServiceDbFacade serviceDbFacade,
                                @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                                @Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        super(entityCacheService,
                metadataExecutor,
                logicalSchemaProvider,
                columnMetadataService,
                serviceDbFacade,
                sqlDialect,
                parserService);
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return checkViewQuery(context)
                .compose(v -> parseSelect(((SqlAlterView) context.getSqlNode()).getQuery(), context.getDatamartName()))
                .compose(response -> getCreateViewContext(context, response))
                .compose(viewContext -> updateEntity(viewContext, context));
    }

    private Future<QueryResult> updateEntity(CreateViewContext viewContext, DdlRequestContext context) {
        return Future.future(promise -> {
            val viewEntity = viewContext.getViewEntity();
            context.setDatamartName(viewEntity.getSchema());
            entityDao.getEntity(viewEntity.getSchema(), viewEntity.getName())
                    .map(this::checkEntityType)
                    .compose(r -> entityDao.updateEntity(viewEntity))
                    .onSuccess(success -> {
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    @SneakyThrows
    @Override
    protected void replaceSqlSelectQuery(DdlRequestContext context, boolean replace, SqlNode newSelectNode) {
        val sql = (SqlAlterView) context.getSqlNode();
        val newSql = new SqlAlterView(sql.getParserPosition(), sql.getName(), sql.getColumnList(), newSelectNode);
        context.setSqlNode(newSql);
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.ALTER_VIEW;
    }

}
