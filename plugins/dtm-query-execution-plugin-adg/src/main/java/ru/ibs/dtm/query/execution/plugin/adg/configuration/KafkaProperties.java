package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaClusterProperty;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaConsumerProperty;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.kafka.KafkaProducerProperty;

@Data
@Component("adgKafkaProperties")
@ConfigurationProperties(prefix = "kafka.adg")
public class KafkaProperties {
  KafkaConsumerProperty consumer = new KafkaConsumerProperty();
  KafkaProducerProperty producer = new KafkaProducerProperty();
  KafkaClusterProperty cluster = new KafkaClusterProperty();
  KafkaAdminProperty admin = new KafkaAdminProperty();
}
