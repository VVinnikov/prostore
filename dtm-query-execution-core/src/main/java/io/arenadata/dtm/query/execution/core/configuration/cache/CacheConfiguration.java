package io.arenadata.dtm.query.execution.core.configuration.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
@EnableCaching
public class CacheConfiguration {

    public static final String ENTITY_CACHE = "entity";
    public static final String HOT_DELTA_CACHE = "hotDelta";
    public static final String OK_DELTA_CACHE = "okDelta";

    @Bean("caffeineCacheManager")
    public CacheManager cacheManager(CacheProperties cacheProperties) {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager(ENTITY_CACHE, HOT_DELTA_CACHE, OK_DELTA_CACHE);
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
