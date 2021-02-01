package io.arenadata.dtm.cache.factory;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.arenadata.dtm.cache.configuration.CacheProperties;
import org.springframework.cache.caffeine.CaffeineCacheManager;

import java.util.concurrent.TimeUnit;

public class CaffeineCacheManagerFactory implements CacheManagerFactory {

    @Override
    public CaffeineCacheManager create(CacheProperties cacheProperties) {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
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
