package ru.ibs.dtm.common.configuration.kafka;

public interface KafkaConfig {

    KafkaAdminProperty getKafkaAdminProperty();

    KafkaClusterProperty getKafkaClusterProperty();
}
