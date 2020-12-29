package io.arenadata.dtm.cache.service;

import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Predicate;

@Service("evictQueryTemplateCacheServiceImpl")
public class EvictQueryTemplateCacheServiceImpl implements EvictQueryTemplateCacheService {
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService;
    private final List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList;

    @Autowired
    public EvictQueryTemplateCacheServiceImpl(CacheService<QueryTemplateKey, SourceQueryTemplateValue> cacheService,
                                              List<CacheService<QueryTemplateKey, QueryTemplateValue>> cacheServiceList) {
        this.cacheService = cacheService;
        this.cacheServiceList = cacheServiceList;
    }

    @Override
    public void evictByDatamartName(String datamartName) {
        remove(datamart -> datamart.getMnemonic().equals(datamartName));
    }

    @Override
    public void evictByEntityName(String datamartName, String entityName, EntityType entityType) {
        remove(datamart -> datamart.getMnemonic().equals(datamartName)
                && datamart.getEntities().stream()
                .anyMatch(dmEntity -> dmEntity.getEntityType().equals(entityType)
                        && dmEntity.getName().equals(entityName)));
    }

    private void remove(Predicate<Datamart> predicate) {
        Predicate<QueryTemplateKey> templatePredicate = queryTemplateKey ->
                queryTemplateKey.getLogicalSchema().stream()
                        .anyMatch(predicate);
        cacheService.removeIf(templatePredicate);
        cacheServiceList.forEach(pluginCacheService -> pluginCacheService.removeIf(templatePredicate));
    }
}
