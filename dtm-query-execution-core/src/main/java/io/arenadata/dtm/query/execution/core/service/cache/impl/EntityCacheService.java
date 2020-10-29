package io.arenadata.dtm.query.execution.core.service.cache.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.configuration.cache.CacheConfiguration;
import io.arenadata.dtm.query.execution.core.service.cache.AbstractCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

@Component("entityCacheService")
public class EntityCacheService extends AbstractCacheService<String, Entity> {

    @Autowired
    public EntityCacheService(@Qualifier("caffeineCacheManager") CacheManager cacheManager) {
        super(CacheConfiguration.ENTITY_CACHE, cacheManager);
    }

}
