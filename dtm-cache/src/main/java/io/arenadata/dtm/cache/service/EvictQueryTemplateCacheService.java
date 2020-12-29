package io.arenadata.dtm.cache.service;

import io.arenadata.dtm.common.model.ddl.EntityType;

public interface EvictQueryTemplateCacheService {
    void evictByDatamartName(String datamartName);
    void evictByEntityName(String datamartName, String entityName, EntityType entityType);
}
