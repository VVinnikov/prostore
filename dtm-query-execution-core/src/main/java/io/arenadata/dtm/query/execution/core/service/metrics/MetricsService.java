package io.arenadata.dtm.query.execution.core.service.metrics;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.Future;

public interface MetricsService<T extends RequestMetrics> {

    <R> AsyncHandler<R> sendMetrics(SourceType type,
                                    SqlProcessingType actionType,
                                    T metrics,
                                    AsyncHandler<R> asyncResultHandler);

    Future<Void> sendMetrics(SourceType type,
                             SqlProcessingType actionType,
                             T requestMetrics);
}
