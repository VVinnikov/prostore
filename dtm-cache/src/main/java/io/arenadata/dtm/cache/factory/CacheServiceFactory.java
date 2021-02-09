package io.arenadata.dtm.cache.factory;

import io.arenadata.dtm.cache.service.CacheService;

public interface CacheServiceFactory<K, V> {

    CacheService<K, V> create(String cacheConfiguration);
}
