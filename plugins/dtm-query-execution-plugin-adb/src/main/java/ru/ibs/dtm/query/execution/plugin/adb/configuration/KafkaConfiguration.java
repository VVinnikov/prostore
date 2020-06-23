package ru.ibs.dtm.query.execution.plugin.adb.configuration;

import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adb.factory.KafkaConsumerFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.KafkaProducerFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.VertxKafkaConsumerFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.VertxKafkaProducerFactory;

@Configuration
public class KafkaConfiguration {

  @Bean("adbKafkaProducerFactory")
  public KafkaProducerFactory<String, String> kafkaProviderFactory(@Qualifier("adbKafkaProperties")KafkaProperties kafkaProperties,
                                                                   @Qualifier("adbVertx") Vertx vertx) {
    return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
  }

  @Bean("adbKafkaConsumerFactory")
  public KafkaConsumerFactory<String, String> kafkaConsumerFactory(@Qualifier("adbKafkaProperties")KafkaProperties kafkaProperties,
                                                                   @Qualifier("adbVertx") Vertx vertx) {
    return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getProperty());
  }

  @Bean("adbByteArrayKafkaProviderFactory")
  public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(@Qualifier("adbKafkaProperties")KafkaProperties kafkaProperties,
                                                                            @Qualifier("adbVertx") Vertx vertx) {
    return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
  }

  @Bean("adbByteArrayKafkaConsumerFactory")
  public KafkaConsumerFactory<String, Byte[]> byteArrayKafkaConsumerFactory(@Qualifier("adbKafkaProperties")KafkaProperties kafkaProperties,
                                                                            @Qualifier("adbVertx") Vertx vertx) {
    return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getProperty());
  }

  @Bean("adbKafkaAdminClient")
  public KafkaAdminClient kafkaAdminClient(@Qualifier("adbKafkaProperties")KafkaProperties kafkaProperties,
                                           @Qualifier("adbVertx") Vertx vertx) {
    return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getProperty());
  }
}
