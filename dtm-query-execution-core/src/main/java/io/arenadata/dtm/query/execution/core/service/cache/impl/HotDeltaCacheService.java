package io.arenadata.dtm.query.execution.core.service.cache.impl;

import io.arenadata.dtm.query.execution.core.configuration.cache.CacheConfiguration;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.service.cache.AbstractCacheService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

@Component("hotDeltaCacheService")
public class HotDeltaCacheService extends AbstractCacheService<String, HotDelta> {

    @Autowired
    public HotDeltaCacheService(@Qualifier("caffeineCacheManager") CacheManager cacheManager) {
        super(CacheConfiguration.HOT_DELTA_CACHE, cacheManager);
    }

}
