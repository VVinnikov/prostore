package io.arenadata.dtm.common.metrics;

public enum MetricsHeader {

    METRICS_EVENT_CODE("metricsEventCode");

    private final String value;

    MetricsHeader(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
