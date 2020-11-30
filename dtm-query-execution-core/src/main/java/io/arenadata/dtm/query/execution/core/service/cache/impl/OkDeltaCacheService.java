package io.arenadata.dtm.query.execution.core.service.cache.impl;

import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.arenadata.dtm.query.execution.core.service.cache.AbstractCaffeineCacheService;
import io.arenadata.dtm.query.execution.core.service.impl.DataSourcePluginServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

@Component("okDeltaCacheService")
public class OkDeltaCacheService extends AbstractCaffeineCacheService<String, OkDelta> {

    @Autowired
    public OkDeltaCacheService(@Qualifier("caffeineCacheManager") CacheManager cacheManager) {
        super(DataSourcePluginServiceImpl.OK_DELTA_CACHE, cacheManager);
    }

}
