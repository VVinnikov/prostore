package io.arenadata.dtm.cache.service;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CaffeineCacheService<K, V> implements CacheService<K, V> {
    protected final String cacheConfiguration;
    protected final CacheManager cacheManager;
    protected final Cache cache;

    public CaffeineCacheService(String cacheConfiguration, CacheManager cacheManager) {
        this.cacheConfiguration = cacheConfiguration;
        this.cacheManager = cacheManager;
        this.cache = Objects.requireNonNull(cacheManager.getCache(cacheConfiguration));
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        val valueWrapper = cache.get(key);
        if (valueWrapper == null) {
            return null;
        } else {
            return ((Future<V>) valueWrapper.get()).result();
        }
    }

    @Override
    public Map<K, V> asMap() {
        final com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache = ((CaffeineCache) cache).getNativeCache();
        return nativeCache.asMap().entrySet().stream()
                .collect(Collectors.toMap(e -> (K) e.getKey(), e -> ((Future<V>) e.getValue()).result()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public Future<V> getFuture(K key) {
        val valueWrapper = cache.get(key);
        if (valueWrapper == null) {
            return null;
        } else {
            return (Future<V>) valueWrapper.get();
        }
    }

    @Override
    public Future<V> put(K key, V value) {
        return Future.future(promise -> {
            cache.put(key, Future.succeededFuture(value));
          promise.complete(value);
        });
    }

    @Override
    public void remove(K key) {
        cache.evict(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void removeIf(Predicate<K> removeCondition) {
        final com.github.benmanes.caffeine.cache.Cache<Object, Object> nativeCache =
                ((CaffeineCache) cache).getNativeCache();
        val byRemove = nativeCache.asMap().keySet().stream()
                .map(key -> (K) key)
                .filter(removeCondition)
                .collect(Collectors.toSet());
        byRemove.forEach(this::remove);
    }

    @Override
    public void clear() {
        cache.clear();
    }
}
