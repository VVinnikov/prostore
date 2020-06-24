package ru.ibs.dtm.query.execution.core.configuration.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.core.factory.KafkaConsumerFactory;
import ru.ibs.dtm.query.execution.core.factory.KafkaProducerFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.VertxKafkaConsumerFactory;
import ru.ibs.dtm.query.execution.core.factory.impl.VertxKafkaProducerFactory;

@Configuration
public class KafkaConfiguration {

  @Bean("coreKafkaProducerFactory")
  public KafkaProducerFactory<String, String> kafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                   @Qualifier("coreVertx") Vertx vertx) {
    return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
  }

  @Bean("coreKafkaConsumerFactory")
  public KafkaConsumerFactory<String, String> kafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                   @Qualifier("coreVertx") Vertx vertx) {
    return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getProperty());
  }

  @Bean("coreByteArrayKafkaProviderFactory")
  public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                            @Qualifier("coreVertx") Vertx vertx) {
    return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
  }

  @Bean("coreByteArrayKafkaConsumerFactory")
  public KafkaConsumerFactory<String, Byte[]> byteArrayKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                            @Qualifier("coreVertx") Vertx vertx) {
    return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getProperty());
  }

  @Bean("coreKafkaAdminClient")
  public KafkaAdminClient kafkaAdminClient(KafkaProperties kafkaProperties,
                                           @Qualifier("coreVertx") Vertx vertx) {
    return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getProperty());
  }
}
