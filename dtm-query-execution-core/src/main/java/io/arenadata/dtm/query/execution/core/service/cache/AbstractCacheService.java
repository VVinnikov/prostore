package io.arenadata.dtm.query.execution.core.service.cache;

import io.vertx.core.Future;
import lombok.val;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class AbstractCacheService<K, V> implements CacheService<K, V> {
    protected final String cacheConfiguration;
    protected final CacheManager cacheManager;
    private final Set<K> keys;

    public AbstractCacheService(String cacheConfiguration, CacheManager cacheManager) {
        this.cacheConfiguration = cacheConfiguration;
        this.cacheManager = cacheManager;
        this.keys = new HashSet<>();
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
        final Future<V> deltaFuture = Future.succeededFuture(value);
        Objects.requireNonNull(cacheManager.getCache(cacheConfiguration)).put(key, deltaFuture);
        keys.add(key);
        return deltaFuture;
    }

    @Override
    public void remove(K key) {
        Cache cache = cacheManager.getCache(cacheConfiguration);
        Objects.requireNonNull(cache).evict(key);
        keys.remove(key);
    }

    @Override
    public void clear() {
        Objects.requireNonNull(cacheManager.getCache(cacheConfiguration)).clear();
        keys.clear();
    }

    @Override
    public void removeIf(Predicate<K> removeCondition) {
        val byRemove = keys.stream()
                .filter(removeCondition)
                .collect(Collectors.toSet());
        byRemove.forEach(this::remove);
    }
}
