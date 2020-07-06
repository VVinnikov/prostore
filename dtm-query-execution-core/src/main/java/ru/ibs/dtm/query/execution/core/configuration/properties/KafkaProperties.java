package ru.ibs.dtm.query.execution.core.configuration.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.configuration.kafka.*;

@ConfigurationProperties(prefix = "kafka")
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
