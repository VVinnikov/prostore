package io.arenadata.dtm.kafka.core.configuration.kafka;


import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.kafka.core.factory.KafkaConsumerFactory;
import io.arenadata.dtm.kafka.core.factory.KafkaProducerFactory;
import io.arenadata.dtm.kafka.core.factory.impl.VertxKafkaConsumerFactory;
import io.arenadata.dtm.kafka.core.factory.impl.VertxKafkaProducerFactory;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.kafka.core.service.kafka.RestConsumerMonitorImpl;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
@DependsOn("coreKafkaProperties")
public class KafkaConfiguration {


    @Bean("coreKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                     @Qualifier("coreVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("coreKafkaConsumerFactory")
    public KafkaConsumerFactory<byte[], byte[]> coreKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                         @Qualifier("coreVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getCore());
    }

    @Bean("coreByteArrayKafkaProviderFactory")
    public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                              @Qualifier("coreVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("coreKafkaAdminClient")
    public KafkaAdminClient coreKafkaAdminClient(KafkaProperties kafkaProperties,
                                                 @Qualifier("coreVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getCore());
    }

    @Bean("coreKafkaConsumerMonitor")
    public KafkaConsumerMonitor kafkaConsumerMonitor(@Qualifier("coreVertx") Vertx vertx,
                                                     KafkaProperties kafkaProperties) {
        return new RestConsumerMonitorImpl(vertx, kafkaProperties);
    }

    @Bean("jsonCoreKafkaProducer")
    public KafkaProducer<String, String> jsonCoreKafkaProducer(@Qualifier("coreKafkaProducerFactory") KafkaProducerFactory<String, String> producerFactory,
                                                               @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties) {
        return producerFactory.create(kafkaProperties.getProducer().getProperty());
    }
}
