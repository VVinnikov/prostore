package io.arenadata.dtm.query.execution.plugin.adqm.status.dto;

import lombok.Data;

@Data
public final class StatusReportDto {
    private final String topic;
    private final String consumerGroup;

    public StatusReportDto(String topic, String consumerGroup) {
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    public StatusReportDto(String topic) {
        this(topic, "");
    }
}