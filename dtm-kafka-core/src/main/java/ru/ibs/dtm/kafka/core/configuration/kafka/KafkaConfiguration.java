package ru.ibs.dtm.kafka.core.configuration.kafka;


import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;
import ru.ibs.dtm.kafka.core.factory.KafkaConsumerFactory;
import ru.ibs.dtm.kafka.core.factory.KafkaProducerFactory;
import ru.ibs.dtm.kafka.core.factory.impl.VertxKafkaConsumerFactory;
import ru.ibs.dtm.kafka.core.factory.impl.VertxKafkaProducerFactory;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaConsumerMonitorImpl;


@Configuration
@DependsOn("coreKafkaProperties")
public class KafkaConfiguration {


    @Bean("coreKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                     @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("coreKafkaConsumerFactory")
    public KafkaConsumerFactory<byte[], byte[]> coreKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                         @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getCore());
    }

    @Bean("coreByteArrayKafkaProviderFactory")
    public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                              @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("coreKafkaAdminClient")
    public KafkaAdminClient coreKafkaAdminClient(KafkaProperties kafkaProperties,
                                                 @Qualifier("kafkaVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getCore());
    }

    @Bean("coreKafkaConsumerMonitor")
    public KafkaConsumerMonitor kafkaConsumerMonitor(@Qualifier("coreKafkaAdminClient") KafkaAdminClient adminClient,
                                                     @Qualifier("coreKafkaConsumerFactory") KafkaConsumerFactory<byte[], byte[]> consumerFactory,
                                                     @Qualifier("kafkaVertx") Vertx vertx,
                                                     KafkaProperties kafkaProperties) {
        return new KafkaConsumerMonitorImpl(adminClient, consumerFactory, vertx, kafkaProperties);
    }
}
