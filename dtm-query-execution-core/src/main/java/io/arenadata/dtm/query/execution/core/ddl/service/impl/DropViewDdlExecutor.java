package io.arenadata.dtm.query.execution.core.ddl.service.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.execution.core.base.dto.cache.EntityKey;
import io.arenadata.dtm.query.execution.core.base.exception.entity.EntityNotExistsException;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.service.QueryResultDdlExecutor;
import io.arenadata.dtm.query.execution.core.ddl.utils.SqlPreparer;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Set;

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
                    .map(entity -> {
                        context.setEntity(entity);
                        return entity;
                    })
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
    public Set<SqlKind> getSqlKinds() {
        return Collections.singleton(SqlKind.DROP_VIEW);
    }
}
