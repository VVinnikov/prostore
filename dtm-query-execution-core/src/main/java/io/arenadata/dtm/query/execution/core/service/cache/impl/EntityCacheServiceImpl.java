package io.arenadata.dtm.query.execution.core.service.cache.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.core.configuration.cache.CacheConfiguration;
import io.arenadata.dtm.query.execution.core.service.cache.AbstractCaffeineCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.EntityCacheService;
import io.arenadata.dtm.query.execution.core.service.cache.key.EntityKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

@Component("entityCacheService")
public class EntityCacheServiceImpl extends AbstractCaffeineCacheService<EntityKey, Entity> implements EntityCacheService {

    @Autowired
    public EntityCacheServiceImpl(@Qualifier("caffeineCacheManager") CacheManager cacheManager) {
        super(CacheConfiguration.ENTITY_CACHE, cacheManager);
    }

}
