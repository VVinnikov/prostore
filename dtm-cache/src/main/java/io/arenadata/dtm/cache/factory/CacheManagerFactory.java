package io.arenadata.dtm.cache.factory;

import io.arenadata.dtm.cache.configuration.CacheProperties;
import org.springframework.cache.CacheManager;

public interface CacheManagerFactory {

    CacheManager create(CacheProperties cacheProperties);
}
