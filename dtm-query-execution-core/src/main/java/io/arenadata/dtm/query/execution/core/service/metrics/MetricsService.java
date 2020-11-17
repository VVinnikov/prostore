package io.arenadata.dtm.query.execution.core.service.metrics;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public interface MetricsService<T extends RequestMetrics> {

    <R> Handler<AsyncResult<R>> updateMetrics(SourceType type, SqlProcessingType actionType,
                                              T metrics, Handler<AsyncResult<R>> asyncResultHandler);
}
