package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.exception.table.TableNotExistsException;
import io.arenadata.dtm.query.execution.core.exception.view.ViewNotExistsException;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AlterViewDdlExecutor extends CreateViewDdlExecutor {

    public static final String ALTER_VIEW_QUERY_PATH = "ALTER_VIEW.SELECT";

    @Autowired
    public AlterViewDdlExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                                MetadataExecutor<DdlRequestContext> metadataExecutor,
                                LogicalSchemaProvider logicalSchemaProvider,
                                ColumnMetadataService columnMetadataService,
                                ServiceDbFacade serviceDbFacade,
                                @Qualifier("coreSqlDialect") SqlDialect sqlDialect) {
        super(entityCacheService,
                metadataExecutor,
                logicalSchemaProvider,
                columnMetadataService,
                serviceDbFacade,
                sqlDialect);
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return checkViewQuery(context)
                .compose(v -> getCreateViewContext(context))
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
                    .onFailure(error -> {
                        if (error instanceof TableNotExistsException) {
                            promise.fail(new ViewNotExistsException(viewEntity.getSchema(), viewEntity.getName()));
                        } else {
                            promise.fail(error);
                        }
                    });
        });
    }

    @Override
    protected String getViewQuery(SqlSelectTree tree) {
        val queryByView = tree.findNodesByPath(ALTER_VIEW_QUERY_PATH);
        if (queryByView.isEmpty()) {
            throw new DtmException("Unable to get view query");
        } else {
            return queryByView.get(0).getNode().toSqlString(sqlDialect).toString();
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.ALTER_VIEW;
    }

}
