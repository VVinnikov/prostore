package io.arenadata.dtm.query.execution.core.service.delta.impl;

import io.arenadata.dtm.cache.service.EvictQueryTemplateCacheService;
import io.arenadata.dtm.query.execution.core.service.delta.DeltaPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EvictCacheDeltaPostExecutor implements DeltaPostExecutor {
    private final EvictQueryTemplateCacheService evictQueryTemplateCacheService;

    @Autowired
    public EvictCacheDeltaPostExecutor(EvictQueryTemplateCacheService evictQueryTemplateCacheService) {
        this.evictQueryTemplateCacheService = evictQueryTemplateCacheService;
    }

    @Override
    public Future<Void> execute(DeltaRequestContext context) {
        evictQueryTemplateCacheService.evictByDatamartName(context.getRequest().getQueryRequest().getDatamartMnemonic());
        return Future.succeededFuture();
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.EVICT_CACHE;
    }
}
