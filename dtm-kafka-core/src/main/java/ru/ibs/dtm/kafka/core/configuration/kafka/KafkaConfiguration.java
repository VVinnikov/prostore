package ru.ibs.dtm.kafka.core.configuration.kafka;


import io.vertx.core.Vertx;
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


@Configuration
@DependsOn("coreKafkaProperties")
public class KafkaConfiguration {


    @Bean("coreKafkaProducerFactory")
    public KafkaProducerFactory<String, String> kafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                     @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("coreKafkaConsumerFactory")
    public KafkaConsumerFactory<byte[],byte[]> coreKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                        @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getCore());
    }

    @Bean("adbKafkaConsumerFactory")
    public KafkaConsumerFactory<String, String> adbKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                        @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getAdb());
    }

    @Bean("adgKafkaConsumerFactory")
    public KafkaConsumerFactory<String, String> adgKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                        @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getAdg());
    }

    @Bean("adqmKafkaConsumerFactory")
    public KafkaConsumerFactory<String, String> adqmKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                         @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getAdqm());
    }

    @Bean("coreByteArrayKafkaProviderFactory")
    public KafkaProducerFactory<String, Byte[]> byteArrayKafkaProviderFactory(KafkaProperties kafkaProperties,
                                                                              @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaProducerFactory<>(vertx, kafkaProperties.getProducer().getProperty());
    }

    @Bean("adbByteArrayKafkaConsumerFactory")
    public KafkaConsumerFactory<String, Byte[]> adbByteArrayKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                                 @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getAdb());
    }

    @Bean("adgByteArrayKafkaConsumerFactory")
    public KafkaConsumerFactory<String, Byte[]> adgByteArrayKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                                 @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getAdg());
    }

    @Bean("adqmByteArrayKafkaConsumerFactory")
    public KafkaConsumerFactory<String, Byte[]> adqmByteArrayKafkaConsumerFactory(KafkaProperties kafkaProperties,
                                                                                  @Qualifier("kafkaVertx") Vertx vertx) {
        return new VertxKafkaConsumerFactory<>(vertx, kafkaProperties.getConsumer().getAdqm());
    }

    @Bean("coreKafkaAdminClient")
    public KafkaAdminClient coreKafkaAdminClient(KafkaProperties kafkaProperties,
                                                 @Qualifier("kafkaVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getCore());
    }

    @Bean("adbKafkaAdminClient")
    public KafkaAdminClient adbKafkaAdminClient(KafkaProperties kafkaProperties,
                                                @Qualifier("kafkaVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getAdb());
    }

    @Bean("adgKafkaAdminClient")
    public KafkaAdminClient adgKafkaAdminClient(KafkaProperties kafkaProperties,
                                                @Qualifier("kafkaVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getAdg());
    }

    @Bean("adqmKafkaAdminClient")
    public KafkaAdminClient adqmKafkaAdminClient(KafkaProperties kafkaProperties,
                                                 @Qualifier("kafkaVertx") Vertx vertx) {
        return KafkaAdminClient.create(vertx, kafkaProperties.getConsumer().getAdqm());
    }

}
