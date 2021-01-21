package io.arenadata.dtm.query.execution.plugin.adg.service;

import io.arenadata.dtm.common.configuration.kafka.*;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component("coreKafkaProperties")
@Data
public class KafkaProperties implements KafkaConfig {

    KafkaConsumerProperty consumer = new KafkaConsumerProperty();
    KafkaProducerProperty producer = new KafkaProducerProperty();
    KafkaClusterProperty cluster = new KafkaClusterProperty();
    KafkaAdminProperty admin = new KafkaAdminProperty();

    @Override
    public KafkaAdminProperty getKafkaAdminProperty() {
        return admin;
    }

    @Override
    public KafkaClusterProperty getKafkaClusterProperty() {
        return cluster;
    }

}
