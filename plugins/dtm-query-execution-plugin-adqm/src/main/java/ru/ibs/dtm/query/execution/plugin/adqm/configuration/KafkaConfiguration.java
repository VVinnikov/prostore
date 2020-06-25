package ru.ibs.dtm.query.execution.plugin.adqm.configuration;

import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.KafkaConsumerFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.KafkaProducerFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.VertxKafkaConsumerFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.VertxKafkaProducerFactory;

@Configuration
public class KafkaConfiguration {

    @Bean("adqmKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                     @Qualifier("adqmVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("adqmKafkaConsumerFactory")
    public KafkaConsumerFactory<String, String> kafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                     @Qualifier("adqmVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getProperty());
    }

    @Bean("adqmByteArrayKafkaProviderFactory")
    public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                              @Qualifier("adqmVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("adqmByteArrayKafkaConsumerFactory")
    public KafkaConsumerFactory<String, Byte[]> byteArrayKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                              @Qualifier("adqmVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getProperty());
    }

    @Bean("adqmKafkaAdminClient")
    public KafkaAdminClient kafkaAdminClient(KafkaProperties kafkaProperties,
                                             @Qualifier("adqmVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getProperty());
    }
}
