package io.arenadata.dtm.query.execution.core.factory.impl.metrics;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.core.factory.AbstractMetricsEventFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OperationMetricsEventFactory extends AbstractMetricsEventFactory<RequestMetrics> {

    @Autowired
    public OperationMetricsEventFactory(DtmConfig dtmSettings) {
        super(RequestMetrics.class, dtmSettings);
    }
}
