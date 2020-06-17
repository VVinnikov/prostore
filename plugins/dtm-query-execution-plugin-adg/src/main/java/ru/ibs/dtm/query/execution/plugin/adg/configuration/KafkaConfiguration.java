package ru.ibs.dtm.query.execution.plugin.adg.configuration;

import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adg.factory.KafkaConsumerFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.KafkaProducerFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.VertxKafkaConsumerFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.VertxKafkaProducerFactory;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConfiguration {

  @Bean("adgKafkaProviderFactory")
  public KafkaProducerFactory<String, String> kafkaProviderFactory(@Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, @Qualifier("adgVertx") Vertx vertx) {
    return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.producer.getProperty());
  }

  @Bean("adgKafkaConsumerFactory")
  public KafkaConsumerFactory<String, String> kafkaConsumerFactory(@Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, @Qualifier("adgVertx") Vertx vertx) {
    return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.consumer.getProperty());
  }

  @Bean("adgByteArrayKafkaProviderFactory")
  public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(@Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, @Qualifier("adgVertx") Vertx vertx) {
    return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.producer.getProperty());
  }

  @Bean("adgByteArrayKafkaConsumerFactory")
  public KafkaConsumerFactory<String, Byte[]> byteArrayKafkaConsumerFactory(@Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, @Qualifier("adgVertx") Vertx vertx) {
    return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.consumer.getProperty());
  }

  @Bean("adgKafkaAdminClient")
  public KafkaAdminClient kafkaAdminClient(@Qualifier("adgKafkaProperties") KafkaProperties kafkaProperties, @Qualifier("adgVertx") Vertx vertx) {
    return KafkaAdminClient.create(vertx, kafkaProperties.consumer.getProperty());
  }
}
