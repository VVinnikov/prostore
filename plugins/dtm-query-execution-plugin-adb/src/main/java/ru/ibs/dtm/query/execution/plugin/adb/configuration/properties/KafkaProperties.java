package ru.ibs.dtm.query.execution.plugin.adb.configuration.properties;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.kafka.KafkaClusterProperty;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.kafka.KafkaConsumerProperty;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.kafka.KafkaProducerProperty;

@ConfigurationProperties(prefix = "kafka.adb")
@Qualifier("adbKafkaProperties")
@Component
public class KafkaProperties {

  KafkaConsumerProperty consumer = new KafkaConsumerProperty();
  KafkaProducerProperty producer = new KafkaProducerProperty();
  KafkaClusterProperty cluster = new KafkaClusterProperty();
  KafkaAdminProperty admin = new KafkaAdminProperty();

  public KafkaConsumerProperty getConsumer() {
    return consumer;
  }

  public void setConsumer(KafkaConsumerProperty consumer) {
    this.consumer = consumer;
  }

  public KafkaProducerProperty getProducer() {
    return producer;
  }

  public void setProducer(KafkaProducerProperty producer) {
    this.producer = producer;
  }

  public KafkaClusterProperty getCluster() {
    return cluster;
  }

  public void setCluster(KafkaClusterProperty cluster) {
    this.cluster = cluster;
  }

  public KafkaAdminProperty getAdmin() {
    return admin;
  }

  public void setAdmin(KafkaAdminProperty admin) {
    this.admin = admin;
  }
}
