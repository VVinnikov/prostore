package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.service.ddl.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.utils.SqlPreparer;
import io.arenadata.dtm.query.execution.core.dto.ddl.DdlRequestContext;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DropViewDdlExecutor extends QueryResultDdlExecutor {
    private final CacheService<EntityKey, Entity> entityCacheService;
    protected final EntityDao entityDao;

    @Autowired
    public DropViewDdlExecutor(@Qualifier("entityCacheService") CacheService<EntityKey, Entity> entityCacheService,
                               MetadataExecutor<DdlRequestContext> metadataExecutor,
                               ServiceDbFacade serviceDbFacade) {
        super(metadataExecutor, serviceDbFacade);
        this.entityCacheService = entityCacheService;
        entityDao = serviceDbFacade.getServiceDbDao().getEntityDao();
    }

    @Override
    public Future<QueryResult> execute(DdlRequestContext context, String sqlNodeName) {
        return dropView(context);
    }

    private Future<QueryResult> dropView(DdlRequestContext context) {
        return Future.future(promise -> {
            val tree = new SqlSelectTree(context.getSqlNode());
            val viewNameNode = SqlPreparer.getViewNameNode(tree);
            val datamartName = viewNameNode.tryGetSchemaName()
                    .orElseThrow(() -> new DtmException("Unable to get schema of view"));
            val viewName = viewNameNode.tryGetTableName()
                    .orElseThrow(() -> new DtmException("Unable to get name of view"));
            context.setDatamartName(datamartName);
            entityCacheService.remove(new EntityKey(datamartName, viewName));
            entityDao.getEntity(datamartName, viewName)
                    .compose(this::checkEntityType)
                    .compose(v -> entityDao.deleteEntity(datamartName, viewName))
                    .onSuccess(success -> {
                        promise.complete(QueryResult.emptyResult());
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<Void> checkEntityType(Entity entity) {
        if (EntityType.VIEW == entity.getEntityType()) {
            return Future.succeededFuture();
        } else {
            return Future.failedFuture(new EntityNotExistsException(entity.getNameWithSchema()));
        }
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_VIEW;
    }
}
