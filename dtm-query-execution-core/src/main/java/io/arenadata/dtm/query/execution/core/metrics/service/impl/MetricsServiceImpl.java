package io.arenadata.dtm.query.execution.core.metrics.service.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.core.metrics.dto.MetricsSettings;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("coreMetricsService")
public class MetricsServiceImpl extends AbstractMetricsService<RequestMetrics> {

    @Autowired
    public MetricsServiceImpl(MetricsProducer metricsProducer, DtmConfig dtmSettings, MetricsSettings metricsSettings) {
        super(metricsProducer, dtmSettings, metricsSettings);
    }
}
