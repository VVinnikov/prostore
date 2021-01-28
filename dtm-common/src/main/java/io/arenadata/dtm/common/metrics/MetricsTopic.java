package io.arenadata.dtm.common.metrics;

public enum MetricsTopic {

    ALL_EVENTS("metrics.all.events");

    private final String value;

    MetricsTopic(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
