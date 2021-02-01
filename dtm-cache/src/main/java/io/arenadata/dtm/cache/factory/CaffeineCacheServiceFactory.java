package io.arenadata.dtm.cache.factory;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.cache.service.CaffeineCacheService;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCacheManager;

import java.util.Collections;

public class CaffeineCacheServiceFactory<K, V> implements CacheServiceFactory<K, V> {

    private final CacheManager cacheManager;

    public CaffeineCacheServiceFactory(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    public CacheService<K, V> create(String cacheConfiguration) {
        CaffeineCacheManager caffeineCacheManager = (CaffeineCacheManager) this.cacheManager;
        caffeineCacheManager.setCacheNames(Collections.singleton(cacheConfiguration));
        return new CaffeineCacheService<>(cacheConfiguration, this.cacheManager);
    }
}
