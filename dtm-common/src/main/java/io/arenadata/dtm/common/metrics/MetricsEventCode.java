package io.arenadata.dtm.common.metrics;

public enum MetricsEventCode {
    ALL("all_metrics");

    private final String value;

    MetricsEventCode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
