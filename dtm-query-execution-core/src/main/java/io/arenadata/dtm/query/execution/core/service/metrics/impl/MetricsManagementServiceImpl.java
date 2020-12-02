package io.arenadata.dtm.query.execution.core.service.metrics.impl;

import io.arenadata.dtm.query.execution.core.configuration.metrics.MetricsSettings;
import io.arenadata.dtm.query.execution.core.dto.metrics.MetricsSettingsUpdateResult;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsManagementService;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsProvider;
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
            final String error = "Error in turning on metrics";
            log.error(error, e);
            throw new RuntimeException(error, e);
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
            final String error = "Error in turning off metrics";
            log.error(error, e);
            throw new RuntimeException(error, e);
        }
    }
}