package io.arenadata.dtm.cache.service;

import io.vertx.core.Future;

import java.util.function.Predicate;

public interface CacheService<K, V> {

    V get(K key);

    Future<V> getFuture(K key);

    Future<V> put(K key, V value);

    void remove(K key);

    void removeIf(Predicate<K> removeCondition);

    void clear();
}
