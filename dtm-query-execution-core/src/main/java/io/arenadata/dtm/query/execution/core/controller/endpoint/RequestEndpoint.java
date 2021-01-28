package io.arenadata.dtm.query.execution.core.controller.endpoint;

import io.arenadata.dtm.query.execution.core.dto.metrics.ResultMetrics;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;
import org.springframework.stereotype.Component;

@WebEndpoint(id = "requests")
@Component
public class RequestEndpoint {

    private final MetricsProvider metricsProvider;

    @Autowired
    public RequestEndpoint(MetricsProvider metricsProvider) {
        this.metricsProvider = metricsProvider;
    }

    @ReadOperation
    public ResultMetrics metrics() {
        return metricsProvider.get();
    }
}