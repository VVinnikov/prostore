package io.arenadata.dtm.query.execution.core.service.cache;

import com.github.benmanes.caffeine.cache.Cache;
import lombok.val;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;

import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractCaffeineCacheService<K, V> extends AbstractCacheService<K, V> {
    public AbstractCaffeineCacheService(String cacheConfiguration, CacheManager cacheManager) {
        super(cacheConfiguration, cacheManager);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeIf(Predicate<K> removeCondition) {
        CaffeineCache caffeineCache = (CaffeineCache) cacheManager.getCache(cacheConfiguration);
        Cache<Object, Object> nativeCache = Objects.requireNonNull(caffeineCache).getNativeCache();
        val byRemove = nativeCache.asMap().keySet().stream()
            .map(key -> (K) key)
            .filter(removeCondition)
            .collect(Collectors.toSet());
        byRemove.forEach(this::remove);
    }
}
