package io.arenadata.dtm.query.execution.core.factory;

public interface MetricsEventFactory<T> {

    T create(String eventData);
}
