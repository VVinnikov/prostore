package io.arenadata.dtm.query.execution.core.metrics.service;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public interface MetricsService<T extends RequestMetrics> {

    <R> Handler<AsyncResult<R>> sendMetrics(SourceType type,
                                            SqlProcessingType actionType,
                                            T metrics,
                                            Handler<AsyncResult<R>> asyncResultHandler);

    Future<Void> sendMetrics(SourceType type,
                             SqlProcessingType actionType,
                             T requestMetrics);
}
