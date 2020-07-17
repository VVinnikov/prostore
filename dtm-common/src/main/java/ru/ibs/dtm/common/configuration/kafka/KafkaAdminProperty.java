package ru.ibs.dtm.common.configuration.kafka;

import lombok.Data;

@Data
public class KafkaAdminProperty {
    private Integer monitorPoolingTimeoutMs;
    private Integer inputStreamTimeoutMs;
    private KafkaUploadProperty upload = new KafkaUploadProperty();
}
