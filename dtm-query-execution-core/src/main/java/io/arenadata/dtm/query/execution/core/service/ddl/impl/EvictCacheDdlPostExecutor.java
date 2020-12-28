package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.service.schema.impl.SystemDatamartViewsProviderImpl;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.function.Predicate;

@Service
public class EvictCacheDdlPostExecutor implements DdlPostExecutor {
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService;

    @Autowired
    public EvictCacheDdlPostExecutor(
            @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, SourceQueryTemplateValue>
                    queryCacheService) {
        this.queryCacheService = queryCacheService;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        try {
            getPredicate(context)
                    .ifPresent(predicate -> queryCacheService.removeIf(queryTemplateKey ->
                            queryTemplateKey.getLogicalSchema().stream()
                                    .anyMatch(predicate)));
            return Future.succeededFuture();
        } catch (Exception e) {
            return Future.failedFuture(e);
        }

    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.EVICT_CACHE;
    }

    private Optional<Predicate<Datamart>> getPredicate(DdlRequestContext context) {
        Predicate<Datamart> result;
        switch (context.getDdlType()) {
            case DROP_SCHEMA:
                result = datamart -> datamart.getMnemonic().equals(context.getDatamartName());
                break;
            case DROP_TABLE:
            case DROP_VIEW:
                result = getEntityPredicate(context.getRequest().getEntity());
                break;
            default:
                result = null;
        }
        return Optional.ofNullable(result);
    }

    private Predicate<Datamart> getEntityPredicate(Entity entity) {
        return datamart -> datamart.getMnemonic().equals(entity.getSchema())
                && datamart.getEntities().stream()
                .anyMatch(dmEntity -> dmEntity.getEntityType().equals(entity.getEntityType())
                        && dmEntity.getName().equals(entity.getName()));
    }
}
