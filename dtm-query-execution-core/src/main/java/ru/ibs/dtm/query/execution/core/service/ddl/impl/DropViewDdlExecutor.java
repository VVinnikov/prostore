package ru.ibs.dtm.query.execution.core.service.ddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityType;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.node.SqlSelectTree;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.exception.ViewNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataExecutor;
import ru.ibs.dtm.query.execution.core.utils.SqlPreparer;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

@Slf4j
@Component
public class DropViewDdlExecutor extends QueryResultDdlExecutor {
    protected final EntityDao entityDao;

    @Autowired
    public DropViewDdlExecutor(MetadataExecutor<DdlRequestContext> metadataExecutor,
                               ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public void execute(DdlRequestContext context, String sqlNodeName, Handler<AsyncResult<QueryResult>> handler) {
        try {
            val tree = new SqlSelectTree(context.getQuery());
            val viewNameNode = SqlPreparer.getViewNameNode(tree);
            val schemaName = viewNameNode.tryGetSchemaName()
                .orElseThrow(() -> new RuntimeException("Unable to get schema of view"));
            val viewName = viewNameNode.tryGetTableName()
                .orElseThrow(() -> new RuntimeException("Unable to get name of view"));
            entityDao.getEntity(context.getDatamartName(), viewName)
                .compose(this::checkEntityType)
                .compose(v -> entityDao.deleteEntity(schemaName, viewName))
                .onSuccess(success -> handler.handle(Future.succeededFuture(QueryResult.emptyResult())))
                .onFailure(error -> handler.handle(Future.failedFuture(error)));
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
        }
    }

    private Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new ViewNotExistsException(entity.getName()));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_VIEW;
    }
}
