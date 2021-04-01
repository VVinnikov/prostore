package io.arenadata.dtm.query.execution.core.metrics.factory;

public interface MetricsEventFactory<T> {

    T create(String eventData);
}
