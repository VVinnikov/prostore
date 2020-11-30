package io.arenadata.dtm.query.execution.core.configuration.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CacheConfiguration {

    @Bean("caffeineCacheManager")
    public CacheManager cacheManager(CacheProperties cacheProperties, DataSourcePluginService dataSourcePluginService) {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager(dataSourcePluginService.getActiveCaches().toArray(new String[0]));
        cacheManager.setCaffeine(caffeineCacheBuilder(cacheProperties));
        return cacheManager;
    }

    private Caffeine<Object, Object> caffeineCacheBuilder(CacheProperties cacheProperties) {
        return Caffeine.newBuilder()
            .initialCapacity(cacheProperties.getInitialCapacity())
            .maximumSize(cacheProperties.getMaximumSize())
            .expireAfterAccess(cacheProperties.getExpireAfterAccessMinutes(), TimeUnit.MINUTES)
            .recordStats();
    }

}
