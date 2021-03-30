package io.arenadata.dtm.cache.service;

public interface EvictQueryTemplateCacheService {
    void evictByDatamartName(String datamartName);
    void evictByEntityName(String datamartName, String entityName);
}
