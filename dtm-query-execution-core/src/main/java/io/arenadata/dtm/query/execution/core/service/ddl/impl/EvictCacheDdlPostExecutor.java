package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EvictCacheDdlPostExecutor implements DdlPostExecutor {
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public EvictCacheDdlPostExecutor(EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        try {
            switch (context.getDdlType()) {
                case DROP_SCHEMA:
                    evictQueryTemplateCacheService.evictByDatamartName(context.getDatamartName());
                    break;
                case DROP_TABLE:
                case DROP_VIEW:
                    Entity entity = context.getRequest().getEntity();
                    evictQueryTemplateCacheService.evictByEntityName(entity.getSchema(), entity.getName(),
                            entity.getEntityType());
                    break;
                default:
            }
            return Future.succeededFuture();
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.EVICT_CACHE;
    }
}
