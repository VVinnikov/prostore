package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.MetricsTopic;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.core.configuration.metrics.MetricsSettings;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProducer;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.time.LocalDateTime;

public abstract class AbstractMetricsService<T extends RequestMetrics> implements MetricsService<T> {

    private final MetricsProducer metricsProducer;
    private final DtmConfig dtmSettings;
    private final MetricsSettings metricsSettings;

    public AbstractMetricsService(MetricsProducer metricsProducer,
                                  DtmConfig dtmSettings,
                                  MetricsSettings metricsSettings) {
        this.metricsProducer = metricsProducer;
        this.dtmSettings = dtmSettings;
        this.metricsSettings = metricsSettings;
    }

    @Override
    public <R> AsyncHandler<R> sendMetrics(SourceType type,
                                                   SqlProcessingType actionType,
                                                   T requestMetrics,
                                                   AsyncHandler<R> handler) {
        if (!metricsSettings.isEnabled()) {
            return ar -> {
                if (ar.succeeded()) {
                    handler.handleSuccess(ar.result());
                } else {
                    handler.handleError(ar.cause());
                }
            };
        } else {
            return ar -> {
                updateMetrics(type, actionType, requestMetrics);
                if (ar.succeeded()) {
                    requestMetrics.setStatus(RequestStatus.SUCCESS);
                    metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                    handler.handleSuccess(ar.result());
                } else {
                    requestMetrics.setStatus(RequestStatus.ERROR);
                    metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                    handler.handleError(ar.cause());
                }
            };
        }

    }

    @Override
    public Future<Void> sendMetrics(SourceType type,
                                    SqlProcessingType actionType,
                                    T requestMetrics) {
        if (!metricsSettings.isEnabled()) {
            return Future.succeededFuture();
        } else {
            return Future.future(promise -> {
                requestMetrics.setSourceType(type);
                requestMetrics.setActionType(actionType);
                metricsProducer.publish(MetricsTopic.ALL_EVENTS, requestMetrics);
                promise.complete();
            });
        }
    }

    private void updateMetrics(SourceType type, SqlProcessingType actionType, RequestMetrics metrics) {
        metrics.setActive(false);
        metrics.setEndTime(LocalDateTime.now(dtmSettings.getTimeZone()));
        metrics.setSourceType(type);
        metrics.setActionType(actionType);
    }
}
