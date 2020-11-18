package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.core.configuration.metrics.MetricsSettings;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("coreMetricsService")
public class MetricsServiceImpl extends AbstractMetricsService<RequestMetrics> {

    @Autowired
    public MetricsServiceImpl(MetricsProducer metricsProducer, DtmConfig dtmSettings, MetricsSettings metricsSettings) {
        super(metricsProducer, dtmSettings, metricsSettings);
    }
}
