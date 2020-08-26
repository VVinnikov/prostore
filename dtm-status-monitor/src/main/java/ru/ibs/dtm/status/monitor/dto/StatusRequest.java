package ru.ibs.dtm.status.monitor.dto;

import lombok.Data;

@Data
public class StatusRequest {
    private String topic;
    private String consumerGroup;
}
