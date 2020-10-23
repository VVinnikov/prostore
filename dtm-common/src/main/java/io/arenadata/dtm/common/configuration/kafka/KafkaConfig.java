package io.arenadata.dtm.common.configuration.kafka;

public interface KafkaConfig {

    KafkaAdminProperty getKafkaAdminProperty();

    KafkaClusterProperty getKafkaClusterProperty();
}
