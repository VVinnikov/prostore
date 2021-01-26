package io.arenadata.dtm.kafka.core.configuration.kafka;


import io.arenadata.dtm.kafka.core.configuration.properties.KafkaProperties;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.kafka.core.service.kafka.RestConsumerMonitorImpl;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
@DependsOn({"coreKafkaProperties", "mapZkKafkaProviderRepository"})
public class KafkaConfiguration {


    @Bean("coreKafkaConsumerMonitor")
    public KafkaConsumerMonitor kafkaConsumerMonitor(@Qualifier("coreVertx") Vertx vertx,
                                                     KafkaProperties kafkaProperties) {
        return new RestConsumerMonitorImpl(vertx, kafkaProperties);
    }


}
