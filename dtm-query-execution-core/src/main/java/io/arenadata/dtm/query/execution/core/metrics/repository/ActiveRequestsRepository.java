package io.arenadata.dtm.query.execution.core.metrics.repository;

import io.arenadata.dtm.common.metrics.RequestMetrics;

import java.util.List;
import java.util.UUID;

public interface ActiveRequestsRepository<T extends RequestMetrics> {

    void add(T request);

    void remove(T request);

    T get(UUID requestId);

    List<T> getList();

    void deleteAll();
}
