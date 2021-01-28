package io.arenadata.dtm.query.execution.core.service.cache;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Objects;

public abstract class AbstractCacheService<K, V> implements CacheService<K, V> {
    protected final String cacheConfiguration;
    protected final CacheManager cacheManager;

    public AbstractCacheService(String cacheConfiguration, CacheManager cacheManager) {
        this.cacheConfiguration = cacheConfiguration;
        this.cacheManager = cacheManager;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        val valueWrapper = Objects.requireNonNull(cacheManager.getCache(cacheConfiguration)).get(key);
        if (valueWrapper == null) {
            return null;
        } else {
            return ((Future<V>) valueWrapper.get()).result();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<V> getFuture(K key) {
        val valueWrapper = Objects.requireNonNull(cacheManager.getCache(cacheConfiguration)).get(key);
        if (valueWrapper == null) {
            return null;
        } else {
            return (Future<V>) valueWrapper.get();
        }
    }

    @Override
    public Future<V> put(K key, V value) {
        return Future.succeededFuture(value);
    }

    @Override
    public void remove(K key) {
        Cache cache = cacheManager.getCache(cacheConfiguration);
        Objects.requireNonNull(cache).evict(key);
    }

    @Override
    public void clear() {
        Objects.requireNonNull(cacheManager.getCache(cacheConfiguration)).clear();
    }
}
