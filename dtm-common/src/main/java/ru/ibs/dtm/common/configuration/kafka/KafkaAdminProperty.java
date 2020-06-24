package ru.ibs.dtm.common.configuration.kafka;

import lombok.Data;

@Data
public class KafkaAdminProperty {
    private KafkaUploadProperty upload = new KafkaUploadProperty();
}
