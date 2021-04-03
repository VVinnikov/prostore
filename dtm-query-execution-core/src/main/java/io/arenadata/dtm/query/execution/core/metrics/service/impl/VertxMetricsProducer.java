package io.arenadata.dtm.query.execution.core.metrics.service.impl;

import io.arenadata.dtm.common.metrics.MetricsEventCode;
import io.arenadata.dtm.common.metrics.MetricsHeader;
import io.arenadata.dtm.common.metrics.MetricsTopic;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsProducer;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.SneakyThrows;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class VertxMetricsProducer implements MetricsProducer {

    private final Vertx vertx;

    @Autowired
    public VertxMetricsProducer(Vertx vertx) {
        this.vertx = vertx;
    }

    @SneakyThrows
    @Override
    public void publish(MetricsTopic metricsTopic, Object value) {
        val message = DatabindCodec.mapper().writeValueAsString(value);
        val options = new DeliveryOptions();
        options.addHeader(MetricsHeader.METRICS_EVENT_CODE.getValue(), MetricsEventCode.ALL.getValue());
        vertx.eventBus().send(metricsTopic.getValue(), message, options);
    }
}
