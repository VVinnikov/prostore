package io.arenadata.dtm.query.execution.core.metrics.service.impl;

import io.arenadata.dtm.query.execution.core.metrics.dto.MetricsSettings;
import io.arenadata.dtm.query.execution.core.metrics.dto.MetricsSettingsUpdateResult;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsManagementService;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsProvider;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MetricsManagementServiceImpl implements MetricsManagementService {

    private final MetricsProvider metricsProvider;
    private final MetricsSettings metricsSettings;

    @Autowired
    public MetricsManagementServiceImpl(MetricsProvider metricsProvider,
                                        MetricsSettings metricsSettings) {
        this.metricsProvider = metricsProvider;
        this.metricsSettings = metricsSettings;
    }

    @Override
    public MetricsSettingsUpdateResult turnOnMetrics() {
        try {
            if (metricsSettings.isEnabled()) {
                return new MetricsSettingsUpdateResult(true, "Metrics is already turned on");
            } else {
                metricsProvider.clear();
                metricsSettings.setEnabled(true);
                final String turnedOnMsg = "Metrics have been turned on";
                log.info(turnedOnMsg);
                return new MetricsSettingsUpdateResult(true, turnedOnMsg);
            }
        } catch (Exception e) {
            throw new DtmException("Error in turning on metrics", e);
        }
    }

    @Override
    public MetricsSettingsUpdateResult turnOffMetrics() {
        try {
            if (!metricsSettings.isEnabled()) {
                return new MetricsSettingsUpdateResult(false, "Metrics is already turned off");
            } else {
                metricsSettings.setEnabled(false);
                final String turnedOffMsg = "Metrics have been turned off";
                log.info(turnedOffMsg);
                return new MetricsSettingsUpdateResult(false, turnedOffMsg);
            }
        } catch (Exception e) {
            throw new DtmException("Error in turning off metrics", e);
        }
    }
}